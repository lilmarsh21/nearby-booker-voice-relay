import http from 'node:http';
import { URL } from 'node:url';
import { createHash, createHmac, randomBytes } from 'node:crypto';
import WebSocket, { WebSocketServer } from 'ws';

const PORT = Number.parseInt(process.env.PORT || '8080', 10);
const PROXY_BASE = (process.env.GEOVEE_PROXY_VOICE_BASE_URL || 'https://sub.geovee.io/wp-json/geovee/v1/voice').replace(/\/$/, '');
const RELAY_SECRET = String(process.env.GEOVEE_VOICE_RELAY_SECRET || '').trim();
const OPENAI_WS_BASE = String(process.env.OPENAI_REALTIME_WS_BASE || 'wss://api.openai.com/v1/realtime').trim();
const LOG_LEVEL = String(process.env.LOG_LEVEL || 'info').toLowerCase();

function log(level, message, data = {}) {
  const ranks = { debug: 10, info: 20, warn: 30, error: 40 };
  if ((ranks[level] || 20) < (ranks[LOG_LEVEL] || 20)) return;
  // Never log audio payloads, ephemeral OpenAI secrets, relay secret, transcripts, or phone numbers.
  const safe = { ...data };
  for (const key of ['audio', 'delta', 'payload', 'openai_client_secret', 'session_token', 'transcript', 'from', 'to']) delete safe[key];
  console.log(JSON.stringify({ ts: new Date().toISOString(), level, message, ...safe }));
}

async function proxyPost(path, body) {
  if (!RELAY_SECRET) throw new Error('GEOVEE_VOICE_RELAY_SECRET is not configured');
  const raw = JSON.stringify(body);
  const timestamp = String(Math.floor(Date.now() / 1000));
  const nonce = randomBytes(16).toString('hex');
  const route = `/geovee/v1/voice${path}`;
  const bodyHash = createHash('sha256').update(raw).digest('hex');
  const canonical = ['POST', route, timestamp, nonce, bodyHash].join('\n');
  const signature = createHmac('sha256', RELAY_SECRET).update(canonical).digest('hex');

  const response = await fetch(`${PROXY_BASE}${path}`, {
    method: 'POST',
    headers: {
      'content-type': 'application/json',
      'accept': 'application/json',
      'x-geovee-voice-timestamp': timestamp,
      'x-geovee-voice-nonce': nonce,
      'x-geovee-voice-signature': signature,
    },
    body: raw,
    signal: AbortSignal.timeout(15000),
  });
  const text = await response.text();
  let json = {};
  try { json = text ? JSON.parse(text) : {}; } catch { json = {}; }
  if (!response.ok || !json || json.success === false) {
    const error = new Error(json?.message || `GeoVee Proxy returned HTTP ${response.status}`);
    error.status = response.status;
    error.code = json?.code || 'proxy_error';
    throw error;
  }
  return json;
}

function safeSend(ws, object) {
  if (ws && ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(object));
}

function normalizedText(value) {
  return String(value || '').toLowerCase().replace(/[^a-z0-9' ]+/g, ' ').replace(/\s+/g, ' ').trim();
}

function asksForHuman(text) {
  const t = normalizedText(text);
  if (!t) return false;
  const phrases = [
    'talk to someone', 'speak to someone', 'talk to somebody', 'speak to somebody',
    'talk to a person', 'speak to a person', 'real person', 'live person',
    'talk to a human', 'speak to a human', 'human please', 'representative',
    'customer service', 'talk to the owner', 'speak to the owner', 'talk to the office',
    'speak to the office', 'talk to staff', 'speak to staff'
  ];
  return phrases.some((phrase) => t.includes(phrase));
}

function matchesTenantPriority(text, keywords) {
  const t = normalizedText(text);
  if (!t || !Array.isArray(keywords)) return false;
  return keywords.some((keyword) => {
    const k = normalizedText(keyword);
    return k.length >= 3 && t.includes(k);
  });
}

function createConnectionState(twilioWs) {
  return {
    twilioWs,
    openaiWs: null,
    streamSid: '',
    callSid: '',
    sessionId: '',
    model: '',
    rules: {},
    startedAt: Date.now(),
    bootstrapped: false,
    transferInProgress: false,
    ended: false,
    maxTimer: null,
    pendingAudio: [],
    pendingFunctionArgs: new Map(),
    handledFunctionCalls: new Set(),
  };
}

async function beginOpenAI(state, sessionToken) {
  const boot = await proxyPost('/session/bootstrap', { session_token: sessionToken });
  if (!boot?.openai_client_secret || !boot?.session_id || !boot?.model) throw new Error('Voice bootstrap response is incomplete');
  state.sessionId = String(boot.session_id);
  state.callSid = String(boot.call_sid || state.callSid || '');
  state.model = String(boot.model);
  state.rules = boot.rules || {};
  state.bootstrapped = true;

  const url = new URL(OPENAI_WS_BASE);
  url.searchParams.set('model', state.model);
  const openaiWs = new WebSocket(url.toString(), {
    headers: {
      Authorization: `Bearer ${boot.openai_client_secret}`,
      'OpenAI-Safety-Identifier': `nbb-voice-${state.sessionId.slice(0, 24)}`,
    },
  });
  state.openaiWs = openaiWs;

  openaiWs.on('open', () => {
    log('info', 'OpenAI Realtime connected', { callSid: state.callSid, model: state.model });
    for (const audio of state.pendingAudio.splice(0)) {
      safeSend(openaiWs, { type: 'input_audio_buffer.append', audio });
    }
    // Client secret already carries the authoritative session config. This merely starts the greeting.
    safeSend(openaiWs, {
      type: 'response.create',
      response: { instructions: 'Greet the caller now using the tenant preferred greeting and continue as the configured phone assistant.' },
    });
  });

  openaiWs.on('message', (data) => handleOpenAIEvent(state, data));
  openaiWs.on('error', (error) => {
    log('error', 'OpenAI Realtime socket error', { callSid: state.callSid, error: error?.message || 'socket_error' });
  });
  openaiWs.on('close', (code) => {
    log('info', 'OpenAI Realtime disconnected', { callSid: state.callSid, code });
  });

  const maxMinutes = Math.max(1, Math.min(180, Number(boot.max_call_minutes || 20)));
  state.maxTimer = setTimeout(() => {
    log('info', 'Maximum AI call duration reached', { callSid: state.callSid, maxMinutes });
    safeSend(state.twilioWs, { event: 'clear', streamSid: state.streamSid });
    try { state.twilioWs.close(1000, 'AI call limit reached'); } catch {}
  }, maxMinutes * 60 * 1000);
}

async function executeTransfer(state, reason, source = 'rule') {
  if (state.transferInProgress || state.ended || !state.sessionId) return;
  state.transferInProgress = true;
  try {
    safeSend(state.twilioWs, { event: 'clear', streamSid: state.streamSid });
    safeSend(state.openaiWs, { type: 'response.cancel' });
    await proxyPost('/call/transfer', { session_id: state.sessionId, reason });
    log('info', 'Human transfer initiated', { callSid: state.callSid, reason, source });
    if (state.openaiWs?.readyState === WebSocket.OPEN) state.openaiWs.close(1000, 'Transferred');
  } catch (error) {
    state.transferInProgress = false;
    log('warn', 'Human transfer failed', { callSid: state.callSid, reason, source, error: error?.message || 'transfer_failed' });
    // Tell the model the tool failed so it can transparently continue instead of pretending transfer succeeded.
    safeSend(state.openaiWs, {
      type: 'conversation.item.create',
      item: { type: 'message', role: 'system', content: [{ type: 'input_text', text: 'The attempted human transfer failed. Tell the caller briefly that the transfer could not be completed and offer to continue helping. Do not claim the transfer succeeded.' }] },
    });
    safeSend(state.openaiWs, { type: 'response.create' });
  }
}

function acknowledgeFunction(state, callId, output) {
  if (!callId) return;
  safeSend(state.openaiWs, {
    type: 'conversation.item.create',
    item: { type: 'function_call_output', call_id: callId, output: JSON.stringify(output) },
  });
}

async function handleFunctionCall(state, name, callId, args = {}) {
  if (callId && state.handledFunctionCalls.has(callId)) return;
  if (callId) state.handledFunctionCalls.add(callId);
  if (name === 'request_human_transfer') {
    if (!state.rules?.human_transfer_enabled) {
      acknowledgeFunction(state, callId, { success: false, reason: 'transfer_disabled' });
      safeSend(state.openaiWs, { type: 'response.create' });
      return;
    }
    acknowledgeFunction(state, callId, { success: true, action: 'transferring' });
    await executeTransfer(state, 'human_request', 'model_tool');
    return;
  }
  if (name === 'report_priority_issue') {
    if (state.rules?.emergency_enabled && state.rules?.emergency_action === 'transfer') {
      acknowledgeFunction(state, callId, { success: true, action: 'transferring' });
      await executeTransfer(state, 'emergency', 'model_tool');
    } else {
      acknowledgeFunction(state, callId, { success: true, action: 'continue_and_collect_callback' });
      safeSend(state.openaiWs, { type: 'response.create', response: { instructions: 'This tenant does not transfer this priority rule. Continue the call and collect callback details conversationally. Do not claim they were stored externally.' } });
    }
  }
}

function handleOpenAIEvent(state, raw) {
  let event;
  try { event = JSON.parse(raw.toString()); } catch { return; }
  const type = String(event?.type || '');

  // GA event is response.output_audio.delta. Accept the older alias defensively during API transitions.
  if ((type === 'response.output_audio.delta' || type === 'response.audio.delta') && event.delta) {
    safeSend(state.twilioWs, { event: 'media', streamSid: state.streamSid, media: { payload: event.delta } });
    return;
  }

  if (type === 'input_audio_buffer.speech_started') {
    // Twilio buffers outbound media. Clear it immediately so barge-in feels real.
    if (state.rules?.barge_in_enabled) safeSend(state.twilioWs, { event: 'clear', streamSid: state.streamSid });
    return;
  }

  if (type === 'conversation.item.input_audio_transcription.completed') {
    const transcript = String(event.transcript || '');
    if (!state.transferInProgress && state.rules?.human_transfer_enabled && asksForHuman(transcript)) {
      executeTransfer(state, 'human_request', 'deterministic_transcript_rule');
      return;
    }
    if (!state.transferInProgress && state.rules?.emergency_enabled && matchesTenantPriority(transcript, state.rules?.emergency_keywords || [])) {
      if (state.rules?.emergency_action === 'transfer') executeTransfer(state, 'emergency', 'deterministic_transcript_rule');
    }
    return;
  }

  if (type === 'response.function_call_arguments.delta' && event.call_id) {
    const prior = state.pendingFunctionArgs.get(event.call_id) || { name: event.name || '', args: '' };
    prior.name = prior.name || event.name || '';
    prior.args += String(event.delta || '');
    state.pendingFunctionArgs.set(event.call_id, prior);
    return;
  }

  if (type === 'response.function_call_arguments.done') {
    const prior = state.pendingFunctionArgs.get(event.call_id) || {};
    const name = String(event.name || prior.name || '');
    const argText = String(event.arguments || prior.args || '{}');
    // GA function-argument events may omit the function name. Keep the final
    // arguments until response.output_item.done supplies the authoritative name.
    state.pendingFunctionArgs.set(event.call_id, { name, args: argText });
    if (name) {
      let args = {};
      try { args = JSON.parse(argText || '{}'); } catch {}
      state.pendingFunctionArgs.delete(event.call_id);
      handleFunctionCall(state, name, event.call_id, args);
    }
    return;
  }

  // Completed output item is the authoritative source for function name/call id.
  if (type === 'response.output_item.done' && event.item?.type === 'function_call') {
    const callId = String(event.item.call_id || '');
    const prior = state.pendingFunctionArgs.get(callId) || {};
    const argText = String(event.item.arguments || prior.args || '{}');
    let args = {};
    try { args = JSON.parse(argText || '{}'); } catch {}
    state.pendingFunctionArgs.delete(callId);
    handleFunctionCall(state, String(event.item.name || prior.name || ''), callId, args);
    return;
  }

  if (type === 'error') {
    log('warn', 'OpenAI Realtime returned an error event', { callSid: state.callSid, code: event?.error?.code || 'realtime_error', type: event?.error?.type || '' });
  }
}

async function finishSession(state, disposition = 'ended') {
  if (state.ended) return;
  state.ended = true;
  if (state.maxTimer) clearTimeout(state.maxTimer);
  const durationSeconds = Math.max(0, Math.round((Date.now() - state.startedAt) / 1000));
  if (state.openaiWs && (state.openaiWs.readyState === WebSocket.OPEN || state.openaiWs.readyState === WebSocket.CONNECTING)) {
    try { state.openaiWs.close(1000, 'Twilio stream ended'); } catch {}
  }
  if (state.sessionId) {
    try { await proxyPost('/session/end', { session_id: state.sessionId, duration_seconds: durationSeconds, disposition }); }
    catch (error) { log('warn', 'Could not report Voice session end', { callSid: state.callSid, error: error?.message || 'end_report_failed' }); }
  }
}

const server = http.createServer((req, res) => {
  if (req.url === '/health' || req.url === '/') {
    res.writeHead(200, { 'content-type': 'application/json' });
    res.end(JSON.stringify({ ok: true, service: 'nearby-booker-voice-relay', version: '0.1.2' }));
    return;
  }
  res.writeHead(404, { 'content-type': 'application/json' });
  res.end(JSON.stringify({ ok: false, error: 'not_found' }));
});

const wss = new WebSocketServer({ noServer: true });
server.on('upgrade', (request, socket, head) => {
  const path = new URL(request.url || '/', 'http://localhost').pathname;
  if (path !== '/twilio/media') {
    socket.write('HTTP/1.1 404 Not Found\r\n\r\n');
    socket.destroy();
    return;
  }
  wss.handleUpgrade(request, socket, head, (ws) => wss.emit('connection', ws, request));
});

wss.on('connection', (twilioWs) => {
  const state = createConnectionState(twilioWs);
  log('info', 'Twilio Media Stream connected');

  twilioWs.on('message', async (raw) => {
    let msg;
    try { msg = JSON.parse(raw.toString()); } catch { return; }
    if (msg.event === 'start') {
      state.streamSid = String(msg.start?.streamSid || msg.streamSid || '');
      state.callSid = String(msg.start?.callSid || '');
      const sessionToken = String(msg.start?.customParameters?.session_token || '');
      if (!state.streamSid || !sessionToken) {
        log('error', 'Twilio start event missing stream/session token', { callSid: state.callSid });
        twilioWs.close(1008, 'Missing session token');
        return;
      }
      try { await beginOpenAI(state, sessionToken); }
      catch (error) {
        log('error', 'Voice bootstrap failed', { callSid: state.callSid, error: error?.message || 'bootstrap_failed', code: error?.code || '' });
        twilioWs.close(1011, 'Voice bootstrap failed');
      }
      return;
    }
    if (msg.event === 'media' && msg.media?.payload) {
      if (state.openaiWs?.readyState === WebSocket.OPEN) safeSend(state.openaiWs, { type: 'input_audio_buffer.append', audio: msg.media.payload });
      else if (state.pendingAudio.length < 500) state.pendingAudio.push(msg.media.payload);
      return;
    }
    if (msg.event === 'stop') {
      await finishSession(state, state.transferInProgress ? 'transferred' : 'twilio_stop');
    }
  });

  twilioWs.on('close', () => finishSession(state, state.transferInProgress ? 'transferred' : 'socket_closed'));
  twilioWs.on('error', (error) => {
    log('warn', 'Twilio Media Stream socket error', { callSid: state.callSid, error: error?.message || 'socket_error' });
  });
});

server.listen(PORT, '0.0.0.0', () => {
  log('info', 'Nearby Booker Voice Relay started', { port: PORT, websocketPath: '/twilio/media' });
});
