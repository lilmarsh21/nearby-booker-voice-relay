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
    // Availability/live-calendar checks can legitimately require more than the
    // short control-plane timeout. Keep bootstrap/transfer fast while allowing
    // NBB tool calls to finish instead of aborting before the tenant's 25s cap.
    signal: AbortSignal.timeout(path === '/tool' ? 40000 : 15000),
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

function isAffirmativeBookingConfirmation(text) {
  const t = normalizedText(text);
  if (!t) return false;
  const exact = new Set(['yes', 'yeah', 'yep', 'correct', 'confirmed', 'confirm', 'sure', 'absolutely']);
  if (exact.has(t)) return true;
  const phrases = [
    'yes please', 'that is correct', "that's correct", 'sounds good', 'that works',
    'go ahead', 'book it', 'schedule it', 'please book it', 'please schedule it',
    'confirm it', 'do it', 'yes book it', 'yes schedule it'
  ];
  return phrases.some((phrase) => t.includes(phrase));
}

function isNegativeBookingConfirmation(text) {
  const t = normalizedText(text);
  if (!t) return false;
  const phrases = ['no', 'nope', 'not yet', 'wait', 'hold on', 'change it', 'different time', 'cancel that', 'do not book', "don't book"];
  return phrases.some((phrase) => t === phrase || t.includes(phrase));
}

function asksForTextMessage(text) {
  const t = normalizedText(text);
  if (!t) return false;

  // Never turn an explicit refusal into consent just because the word "text"
  // appears in the sentence.
  const negative = [
    "don't text", 'do not text', 'dont text', 'no text', 'not by text',
    'not text', "don't sms", 'do not sms', 'dont sms', 'no sms', 'not by sms'
  ];
  if (negative.some((phrase) => t.includes(phrase))) return false;

  if (['text', 'sms', 'text message', 'by text', 'by sms', 'via text', 'via sms'].includes(t)) return true;
  const phrases = [
    'text me', 'send me a text', 'send that by text', 'send it by text',
    'send by text', 'send by sms', 'send me that in a text', 'sms me',
    'send me an sms', 'text that to me', 'can you text', 'could you text',
    'by text', 'via text', 'by sms', 'via sms', 'text please', 'sms please'
  ];
  if (phrases.some((phrase) => t.includes(phrase))) return true;

  // Realtime transcription often turns short delivery answers into variants like
  // "and by text" / "and I text". For a short affirmative answer, a standalone
  // channel word is enough as long as no negative intent was detected above.
  const words = t.split(' ').filter(Boolean);
  return words.length <= 8 && (words.includes('text') || words.includes('sms'));
}

function asksForEmailMessage(text) {
  const t = normalizedText(text);
  if (!t) return false;

  const negative = [
    "don't email", 'do not email', 'dont email', 'no email', 'not by email',
    'not email', "don't e mail", 'do not e mail', 'not by e mail'
  ];
  if (negative.some((phrase) => t.includes(phrase))) return false;

  if (['email', 'e mail', 'by email', 'by e mail', 'via email', 'via e mail'].includes(t)) return true;
  const phrases = [
    'email me', 'send me an email', 'send that by email', 'send it by email',
    'send by email', 'send that to my email', 'email that to me', 'can you email',
    'could you email', 'by email', 'via email', 'email please'
  ];
  if (phrases.some((phrase) => t.includes(phrase))) return true;

  const words = t.split(' ').filter(Boolean);
  return words.length <= 8 && (words.includes('email') || (words.includes('e') && words.includes('mail')));
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
    startupAudioGate: true,
    greetingAudioStarted: false,
    openingGreetingRequested: false,
    droppedStartupAudioFrames: 0,
    pendingFunctionArgs: new Map(),
    handledFunctionCalls: new Set(),
    lastReviewToken: '',
    preparedAt: 0,
    bookingConfirmationAt: 0,
    awaitingBookingConfirmation: false,
    smsRequestAt: 0,
    emailRequestAt: 0,
    responseActive: false,
    responseRequestPending: false,
    queuedResponseInstructions: '',
    queuedResponseReason: '',
    continuationTimer: null,
    continuationStartedAt: 0,
    continuationReason: '',
    continuationRetryCount: 0,
    toolProgressTimers: new Map(),
    transcriptChain: Promise.resolve(),
  };
}

function voiceConversationId(state) {
  const safe = String(state.sessionId || '').replace(/[^A-Za-z0-9_-]/g, '').slice(0, 56);
  return safe ? `voice_${safe}` : '';
}

function reportVoiceEvent(state, payload = {}) {
  if (!state.rules?.transcripts_enabled || !state.sessionId || state.ended) return Promise.resolve();
  const conversationId = voiceConversationId(state);
  if (!conversationId) return Promise.resolve();
  const args = { conversation_id: conversationId, ...payload };

  // Serialize tenant transcript writes per call. The NBB transcript store uses
  // read/merge/upsert semantics, so ordered writes prevent a fast diagnostic
  // event from overwriting a just-arrived caller/assistant message.
  state.transcriptChain = (state.transcriptChain || Promise.resolve())
    .catch(() => {})
    .then(() => proxyPost('/tool', {
      session_id: state.sessionId,
      tool_name: 'nbb_log_voice_event',
      arguments: args,
    }))
    .catch((error) => {
      log('warn', 'Voice transcript event could not be stored', {
        callSid: state.callSid,
        code: error?.code || 'voice_transcript_store_failed',
      });
    });
  return state.transcriptChain;
}

function clearContinuationWatchdog(state) {
  if (state.continuationTimer) clearTimeout(state.continuationTimer);
  state.continuationTimer = null;
  state.continuationStartedAt = 0;
  state.continuationReason = '';
}

function scheduleContinuationWatchdog(state, instructions, reason) {
  clearContinuationWatchdog(state);
  const seconds = Math.max(4, Math.min(20, Number(state.rules?.dead_air_seconds || 7)));
  state.continuationStartedAt = Date.now();
  state.continuationReason = reason;
  state.continuationTimer = setTimeout(() => {
    if (state.ended || !state.openaiWs || state.openaiWs.readyState !== WebSocket.OPEN) return;
    state.continuationRetryCount += 1;
    log('warn', 'Voice continuation produced no audio; retrying once', {
      callSid: state.callSid,
      reason,
      retry: state.continuationRetryCount,
    });
    reportVoiceEvent(state, {
      kind: 'diagnostic',
      event: 'voice_continuation_retry',
      realtime_event: 'dead_air_watchdog',
      continuation_retry: state.continuationRetryCount,
      duration_ms: Date.now() - state.continuationStartedAt,
      diagnostic_message: `No AI audio began after ${seconds} seconds; continuation retried.`,
    });
    clearContinuationWatchdog(state);
    if (state.continuationRetryCount > 1) return;
    safeSend(state.openaiWs, { type: 'response.cancel' });
    state.responseActive = false;
    state.responseRequestPending = false;
    setTimeout(() => {
      requestModelResponse(
        state,
        instructions || 'Continue the phone conversation now using the latest tool result. Do not wait for the caller to prompt you.',
        `${reason}_retry`,
        false
      );
    }, 250);
  }, seconds * 1000);
}

function dynamicVoiceResponseRules(state) {
  const rules = [];
  if (state.rules?.show_arrival_time) {
    rules.push("SHOW ARRIVAL TIME IS ON: every appointment time you speak must use the exact words 'arrival time between' followed by both the start and end times. Never speak only the start time.");
  }
  if (state.rules?.quotes_enabled === false) {
    rules.push('PHONE SPOKEN PRICING IS OFF: do not state or infer service prices, line-item costs, quote totals, appointment totals, minimum charges, or grand totals. This does NOT block creating a real estimate: if the caller wants a quote sent, collect the required customer/service information and use nbb_send_estimate. Never tell the caller that pricing visibility is off or disabled. A savings_amount/spoken_savings explicitly returned for a savings date is allowed and should be stated because it reveals only the amount saved.');
  }
  return rules.join(' ');
}

function applyVoicePresentationFromResult(state, result) {
  const presentation = result && typeof result === 'object' && result.presentation && typeof result.presentation === 'object'
    ? result.presentation
    : null;
  if (!presentation) return;
  if (Object.prototype.hasOwnProperty.call(presentation, 'show_arrival_time')) {
    state.rules.show_arrival_time = Boolean(Number(presentation.show_arrival_time));
  }
  if (Object.prototype.hasOwnProperty.call(presentation, 'pricing_spoken_allowed')) {
    state.rules.quotes_enabled = Boolean(Number(presentation.pricing_spoken_allowed));
  }
}

function afterToolResponseInstructions(state, name, result) {
  const parts = [
    'Continue naturally now using only the exact Nearby Booker tool result. Do not wait for the caller to prompt you. If the tool validated, sent, or booked something, state only what the result confirms.'
  ];

  if (name === 'nbb_get_business_context') {
    parts.push('Consume this business context SILENTLY. Never read or paraphrase capability state, toggles, configuration, pricing visibility, booking enablement, service metadata, or internal status to the caller. Use it only to ask the next natural customer-facing question.');
  }

  if (name === 'nbb_send_estimate' && result && typeof result === 'object') {
    parts.push('This is a real Nearby Booker estimate-delivery result. If sent is true, tell the caller briefly that the estimate was sent using the returned delivery_label/spoken_confirmation. Do NOT speak the estimate total when spoken pricing is off. Do not mention tools, settings, review tokens, internal workflow, or pricing visibility. Do not ask the caller to reconfirm the caller phone number.');
  }

  if (name === 'nbb_validate_address' && result && typeof result === 'object') {
    parts.push('The address check just completed. If Nearby Booker accepted/canonicalized the address, do NOT say technical phrases such as address validation succeeded, eligible to continue, validation result, or confirmation policy. Acknowledge naturally in a few words and CONTINUE THE SCHEDULING FLOW IN THIS SAME TURN. If the caller has not yet supplied the configured service selections/quantities needed for availability, ask exactly one concise service question next. If the required services/quantities are already known from the conversation, call nbb_get_availability immediately using the validated address. Never end this turn with only a status statement after a successful address check. If the address was rejected or materially incomplete, ask only for the missing/corrected address information.');
  }

  if (state.rules?.show_arrival_time && ['nbb_get_availability', 'nbb_prepare_booking', 'nbb_commit_booking'].includes(name)) {
    parts.push("For every appointment window in this response, say 'arrival time between' and both endpoints. If spoken_window is present, use that wording. Never shorten a window to only its start time.");
  }

  if (name === 'nbb_get_availability' && result && typeof result === 'object') {
    const savings = Array.isArray(result.savings_slots) ? result.savings_slots : [];
    if (savings.length) {
      parts.push("Present all savings dates before standard availability. The first time you explain them, say they are dates when the business is already scheduled to be in the caller's area, which allows a better rate. For EACH savings date with a positive savings_amount or non-empty spoken_savings, state the exact amount saved when presenting that slot. Prefer the exact spoken_savings text when supplied. Do not replace the dollar amount saved with only a percent-off label.");
    }
  }

  if (state.rules?.quotes_enabled === false) {
    parts.push('Do not reveal any full service price or estimate total from this result or from memory. Savings amounts explicitly returned for savings dates are the only pricing-like amounts you may say.');
  }
  return parts.join(' ');
}

function requestModelResponse(state, instructions = '', reason = 'normal', watchForAudio = false) {
  if (!state.openaiWs || state.openaiWs.readyState !== WebSocket.OPEN || state.ended) return false;
  if (state.responseActive || state.responseRequestPending) {
    state.queuedResponseInstructions = instructions;
    state.queuedResponseReason = reason;
    return false;
  }
  const dynamicRules = dynamicVoiceResponseRules(state);
  const effectiveInstructions = dynamicRules
    ? (instructions ? `${instructions} ${dynamicRules}` : dynamicRules)
    : instructions;
  const response = {};
  if (effectiveInstructions) response.instructions = effectiveInstructions;
  safeSend(state.openaiWs, { type: 'response.create', response });
  state.responseRequestPending = true;
  if (watchForAudio) scheduleContinuationWatchdog(state, instructions, reason);
  return true;
}

function flushQueuedResponse(state) {
  if (!state.queuedResponseInstructions && !state.queuedResponseReason) return;
  const instructions = state.queuedResponseInstructions;
  const reason = state.queuedResponseReason || 'queued';
  state.queuedResponseInstructions = '';
  state.queuedResponseReason = '';
  setTimeout(() => requestModelResponse(state, instructions, reason, reason === 'after_tool'), 50);
}

async function beginOpenAI(state, sessionToken) {
  const boot = await proxyPost('/session/bootstrap', { session_token: sessionToken });
  if (!boot?.openai_client_secret || !boot?.session_id || !boot?.model) throw new Error('Voice bootstrap response is incomplete');
  state.sessionId = String(boot.session_id);
  state.callSid = String(boot.call_sid || state.callSid || '');
  state.model = String(boot.model);
  state.rules = boot.rules || {};
  state.openingGreeting = String(boot.opening_greeting || 'Thanks for calling. How can I help you today?');
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

    // Reliability rule: never feed startup caller audio into Realtime before the
    // configured greeting has actually begun. Otherwise early speech/background
    // noise can trigger VAD/barge-in and clear the greeting before the caller hears it.
    const buffered = state.pendingAudio.splice(0).length;
    if (buffered > 0) state.droppedStartupAudioFrames += buffered;
    state.openingGreetingRequested = true;
    requestModelResponse(
      state,
      `Speak exactly this opening greeting and nothing else before waiting for the caller: ${JSON.stringify(state.openingGreeting)}. Do not mention settings, enabled features, tools, integrations, or configuration.`,
      'opening_greeting',
      true
    );
  });

  openaiWs.on('message', (data) => handleOpenAIEvent(state, data));
  openaiWs.on('error', (error) => {
    log('error', 'OpenAI Realtime socket error', { callSid: state.callSid, error: error?.message || 'socket_error' });
  });
  openaiWs.on('close', (code) => {
    log('info', 'OpenAI Realtime disconnected', { callSid: state.callSid, code });
    // If Realtime dies unexpectedly, end the Media Stream so Twilio can continue
    // to the Voice-only fallback TwiML supplied by the Proxy instead of leaving
    // the caller in dead air. Intentional transfers/session shutdowns are excluded.
    if (!state.ended && !state.transferInProgress && state.twilioWs?.readyState === WebSocket.OPEN) {
      log('warn', 'Realtime ended unexpectedly; releasing Twilio stream to fallback', { callSid: state.callSid, code });
      try { state.twilioWs.close(1011, 'Realtime unavailable'); } catch {}
    }
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
    requestModelResponse(state, 'Tell the caller briefly that the transfer could not be completed and offer to continue helping.', 'transfer_failed', true);
  }
}

function acknowledgeFunction(state, callId, output) {
  if (!callId) return;
  safeSend(state.openaiWs, {
    type: 'conversation.item.create',
    item: { type: 'function_call_output', call_id: callId, output: JSON.stringify(output) },
  });
}

async function executeNbbTool(state, name, callId, args = {}) {
  if (!state.sessionId) {
    acknowledgeFunction(state, callId, { success: false, code: 'voice_session_missing', message: 'Nearby Booker session is not ready yet.' });
    requestModelResponse(state, 'Briefly explain that the phone assistant is still initializing and ask the caller to try that request again.', 'tool_not_ready', true);
    return;
  }

  let toolArgs = args && typeof args === 'object' ? { ...args } : {};

  if (name === 'nbb_get_quote' && state.rules?.quotes_enabled === false) {
    acknowledgeFunction(state, callId, {
      success: false,
      code: 'voice_spoken_pricing_disabled',
      message: 'Spoken pricing is unavailable; use the estimate-delivery workflow when the caller wants a quote sent.',
    });
    requestModelResponse(
      state,
      'Do not mention pricing settings or say pricing is disabled. If the caller wants a quote, continue gathering the configured services and quantities and ask whether they want the real estimate sent by text or email, unless they already chose a delivery method. Use nbb_send_estimate once the required information is collected.',
      'pricing_delivery_redirect',
      true
    );
    return;
  }

  if (name === 'nbb_send_estimate') {
    const delivery = normalizedText(toolArgs?.delivery || '');
    const wantsSms = delivery === 'sms' || delivery === 'text' || delivery === 'both';
    const wantsEmail = delivery === 'email' || delivery === 'e mail' || delivery === 'both';
    const smsFresh = !wantsSms || (state.rules?.sms_enabled && state.smsRequestAt > 0 && (Date.now() - state.smsRequestAt) <= 120000);
    const emailFresh = !wantsEmail || (state.rules?.email_enabled && state.emailRequestAt > 0 && (Date.now() - state.emailRequestAt) <= 120000);
    if (!smsFresh || !emailFresh) {
      acknowledgeFunction(state, callId, {
        success: false,
        code: 'explicit_estimate_delivery_request_required',
        message: 'The caller has not recently and explicitly selected the requested estimate delivery channel.',
      });
      requestModelResponse(
        state,
        'Do not send the estimate yet. Ask one short customer-facing question: should I send the estimate by text or email? If the caller already clearly chose one in their latest request, use that choice instead of asking again.',
        'estimate_delivery_confirmation_required',
        true
      );
      return;
    }
  }

  if (name === 'nbb_send_sms') {
    const fresh = state.rules?.sms_enabled && state.smsRequestAt > 0 && (Date.now() - state.smsRequestAt) <= 120000;
    if (!fresh) {
      acknowledgeFunction(state, callId, {
        success: false,
        code: 'explicit_sms_request_required',
        message: 'The caller has not recently and explicitly asked to receive a text message.',
      });
      requestModelResponse(state, 'Do not send a text yet. Ask whether the caller wants the requested information sent by text message.', 'sms_confirmation_required', true);
      return;
    }
  }

  if (name === 'nbb_send_email') {
    const fresh = state.rules?.email_enabled && state.emailRequestAt > 0 && (Date.now() - state.emailRequestAt) <= 120000;
    if (!fresh) {
      acknowledgeFunction(state, callId, {
        success: false,
        code: 'explicit_email_request_required',
        message: 'The caller has not recently and explicitly asked to receive an email.',
      });
      requestModelResponse(state, 'Do not send an email yet. Ask whether the caller wants the requested information sent by email.', 'email_confirmation_required', true);
      return;
    }
  }

  if (name === 'nbb_commit_booking') {
    if (!state.lastReviewToken) {
      acknowledgeFunction(state, callId, { success: false, code: 'booking_not_prepared', message: 'No validated Nearby Booker booking is ready to submit.' });
      requestModelResponse(state, 'Explain briefly that the appointment must be prepared and reviewed before it can be submitted.', 'booking_not_prepared', true);
      return;
    }
    const confirmationFresh = state.bookingConfirmationAt > 0
      && state.bookingConfirmationAt >= state.preparedAt
      && (Date.now() - state.bookingConfirmationAt) <= 120000;
    if (!confirmationFresh) {
      acknowledgeFunction(state, callId, { success: false, code: 'explicit_confirmation_required', message: 'The caller has not explicitly confirmed the prepared booking yet.' });
      requestModelResponse(state, 'Read back the validated booking review and ask the caller for a clear yes before trying to submit it.', 'booking_confirmation_required', true);
      return;
    }
    // The model never supplies or chooses the review token. The Relay commits
    // only the most recent review returned by the tenant's real NBB engine.
    toolArgs = { review_token: state.lastReviewToken };
  }

  if (['nbb_prepare_booking', 'nbb_send_estimate'].includes(name) && toolArgs?.customer && typeof toolArgs.customer === 'object') {
    state.customerName = String(toolArgs.customer.name || state.customerName || '');
    state.customerPhone = String(toolArgs.customer.phone || state.customerPhone || '');
    state.customerEmail = String(toolArgs.customer.email || state.customerEmail || '');
  }

  const startedAt = Date.now();
  reportVoiceEvent(state, {
    kind: 'diagnostic',
    event: 'voice_tool_start',
    tool_name: name,
    tool_phase: 'start',
  });

  const progressDelay = Math.max(4, Math.min(20, Number(state.rules?.dead_air_seconds || 7))) * 1000;
  const progressTimer = setTimeout(() => {
    if (state.ended) return;
    requestModelResponse(
      state,
      'Say one short sentence that you are still checking that information. Do not ask a question, do not claim a result, and do not mention technical details.',
      'tool_progress',
      false
    );
  }, progressDelay);
  if (callId) state.toolProgressTimers.set(callId, progressTimer);

  try {
    const response = await proxyPost('/tool', {
      session_id: state.sessionId,
      tool_name: name,
      arguments: toolArgs,
    });
    clearTimeout(progressTimer);
    if (callId) state.toolProgressTimers.delete(callId);

    const result = response?.result ?? {};
    const durationMs = Date.now() - startedAt;
    applyVoicePresentationFromResult(state, result);

    if (name === 'nbb_prepare_booking' && result?.review_token) {
      state.lastReviewToken = String(result.review_token);
      state.preparedAt = Date.now();
      state.bookingConfirmationAt = 0;
      state.awaitingBookingConfirmation = true;
    }
    if (name === 'nbb_commit_booking' && result?.booking_id) {
      state.lastReviewToken = '';
      state.preparedAt = 0;
      state.bookingConfirmationAt = 0;
      state.awaitingBookingConfirmation = false;
    }
    if (name === 'nbb_send_sms') state.smsRequestAt = 0;
    if (name === 'nbb_send_email') state.emailRequestAt = 0;
    if (name === 'nbb_send_estimate') {
      const delivery = normalizedText(result?.delivery || toolArgs?.delivery || '');
      if (delivery === 'sms' || delivery === 'text' || delivery === 'both') state.smsRequestAt = 0;
      if (delivery === 'email' || delivery === 'e mail' || delivery === 'both') state.emailRequestAt = 0;
    }

    reportVoiceEvent(state, {
      kind: 'diagnostic',
      event: 'voice_tool_complete',
      tool_name: name,
      tool_phase: 'complete',
      duration_ms: durationMs,
      booking_id: name === 'nbb_commit_booking' ? String(result?.booking_id || '') : '',
      estimate_id: name === 'nbb_send_estimate' ? String(result?.estimate_id || '') : '',
      customer_name: String(state.customerName || ''),
      customer_phone: String(state.customerPhone || ''),
      customer_email: String(state.customerEmail || ''),
    });

    acknowledgeFunction(state, callId, { success: true, data: result });
    requestModelResponse(
      state,
      afterToolResponseInstructions(state, name, result),
      'after_tool',
      true
    );
  } catch (error) {
    clearTimeout(progressTimer);
    if (callId) state.toolProgressTimers.delete(callId);
    const durationMs = Date.now() - startedAt;

    if (name === 'nbb_get_quote' && ['nbb_voice_quotes_disabled', 'geovee_voice_capability_disabled'].includes(String(error?.code || ''))) {
      state.rules.quotes_enabled = false;
    }

    reportVoiceEvent(state, {
      kind: 'diagnostic',
      event: 'voice_tool_failed',
      tool_name: name,
      tool_phase: 'failed',
      duration_ms: durationMs,
      diagnostic_message: String(error?.code || error?.message || 'nbb_tool_failed'),
    });

    acknowledgeFunction(state, callId, {
      success: false,
      code: error?.code || 'nbb_tool_failed',
      message: error?.message || 'Nearby Booker tool failed',
    });
    const failureInstructions = name === 'nbb_get_quote' && state.rules?.quotes_enabled === false
      ? 'Do not mention pricing settings or say pricing is disabled. Continue gathering the quote details and offer to send the real estimate by text or email using nbb_send_estimate.'
      : (name === 'nbb_send_estimate'
        ? 'The real Nearby Booker estimate was NOT confirmed as sent. Do not claim it was sent. Briefly explain the specific customer-facing problem from the tool result, then ask for the one missing or corrected item needed to create and send the estimate. Never mention internal tools, review tokens, configuration, or pricing visibility.'
        : 'The Nearby Booker tool failed. Briefly explain that the requested check or action could not be completed, do not invent a result, and continue by asking for the one missing or corrected piece of information needed next.');
    requestModelResponse(
      state,
      failureInstructions,
      'after_tool',
      true
    );
  }
}

async function handleFunctionCall(state, name, callId, args = {}) {
  // A function call is itself valid model continuation. Stop any no-audio
  // watchdog from the prior response; this tool's progress timer now owns
  // caller-facing dead-air handling until the function result returns.
  clearContinuationWatchdog(state);
  state.continuationRetryCount = 0;
  if (callId && state.handledFunctionCalls.has(callId)) return;
  if (callId) state.handledFunctionCalls.add(callId);
  if (name === 'request_human_transfer') {
    if (!state.rules?.human_transfer_enabled) {
      acknowledgeFunction(state, callId, { success: false, reason: 'transfer_disabled' });
      requestModelResponse(state, 'Briefly explain that human transfer is unavailable and offer to continue helping.', 'transfer_disabled', true);
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
      requestModelResponse(state, 'This tenant does not transfer this priority rule. Continue the call and collect callback details conversationally. Do not claim they were stored externally.', 'priority_continue', true);
    }
    return;
  }
  if (String(name || '').startsWith('nbb_')) {
    await executeNbbTool(state, name, callId, args);
    return;
  }
  acknowledgeFunction(state, callId, { success: false, code: 'unknown_tool', message: 'This Voice tool is not supported by the Relay.' });
  requestModelResponse(state, 'Briefly explain that the requested phone action is unavailable and continue helping.', 'unknown_tool', true);
}

function handleOpenAIEvent(state, raw) {
  let event;
  try { event = JSON.parse(raw.toString()); } catch { return; }
  const type = String(event?.type || '');

  if (type === 'response.created') {
    state.responseActive = true;
    state.responseRequestPending = false;
    return;
  }

  // GA event is response.output_audio.delta. Accept the older alias defensively during API transitions.
  if ((type === 'response.output_audio.delta' || type === 'response.audio.delta') && event.delta) {
    clearContinuationWatchdog(state);
    state.continuationRetryCount = 0;
    if (state.startupAudioGate) {
      state.startupAudioGate = false;
      state.greetingAudioStarted = true;
      log('info', 'Opening greeting audio started; caller audio gate opened', {
        callSid: state.callSid,
        droppedStartupAudioFrames: state.droppedStartupAudioFrames,
      });
      reportVoiceEvent(state, {
        kind: 'diagnostic',
        event: 'voice_opening_greeting_started',
        realtime_event: type,
        diagnostic_message: `Opening greeting audio began; ${state.droppedStartupAudioFrames} startup caller-audio frames were suppressed.`,
      });
    }
    safeSend(state.twilioWs, { event: 'media', streamSid: state.streamSid, media: { payload: event.delta } });
    return;
  }

  // Store customer text only in the tenant's NBB transcript store when that tenant opted in.
  if (type === 'conversation.item.input_audio_transcription.completed') {
    const transcript = String(event.transcript || '').trim();
    if (transcript) {
      const normalized = normalizedText(transcript);
      if (asksForTextMessage(transcript) || normalized === 'both') state.smsRequestAt = Date.now();
      if (asksForEmailMessage(transcript) || normalized === 'both') state.emailRequestAt = Date.now();
      reportVoiceEvent(state, {
        kind: 'message',
        role: 'user',
        content: transcript,
        event: 'voice_customer_message',
      });
    }

    if (state.awaitingBookingConfirmation) {
      if (isAffirmativeBookingConfirmation(transcript)) {
        state.bookingConfirmationAt = Date.now();
      } else if (isNegativeBookingConfirmation(transcript)) {
        state.bookingConfirmationAt = 0;
      }
    }
    if (!state.transferInProgress && state.rules?.human_transfer_enabled && asksForHuman(transcript)) {
      executeTransfer(state, 'human_request', 'deterministic_transcript_rule');
      return;
    }
    if (!state.transferInProgress && state.rules?.emergency_enabled && matchesTenantPriority(transcript, state.rules?.emergency_keywords || [])) {
      if (state.rules?.emergency_action === 'transfer') executeTransfer(state, 'emergency', 'deterministic_transcript_rule');
    }
    return;
  }

  // Store the spoken AI text, never the audio, and never write the transcript to Render logs.
  if (type === 'response.output_audio_transcript.done' || type === 'response.audio_transcript.done') {
    const transcript = String(event.transcript || '').trim();
    if (transcript) {
      reportVoiceEvent(state, {
        kind: 'message',
        role: 'assistant',
        content: transcript,
        event: 'voice_agent_message',
      });
    }
    return;
  }

  if (type === 'input_audio_buffer.speech_started') {
    // Startup caller audio is deliberately suppressed until greeting audio starts,
    // so a stale/early VAD event must never clear the deterministic greeting.
    if (state.startupAudioGate) return;
    // A real caller turn takes precedence over a dead-air retry.
    clearContinuationWatchdog(state);
    state.continuationRetryCount = 0;
    // Twilio buffers outbound media. Clear it immediately so barge-in feels real.
    if (state.rules?.barge_in_enabled) safeSend(state.twilioWs, { event: 'clear', streamSid: state.streamSid });
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

  if (type === 'response.done' || type === 'response.cancelled' || type === 'response.failed') {
    state.responseActive = false;
    state.responseRequestPending = false;
    if (type === 'response.failed') {
      reportVoiceEvent(state, {
        kind: 'diagnostic',
        event: 'voice_realtime_response_failed',
        realtime_event: type,
        diagnostic_message: String(event?.response?.status_details?.error?.code || event?.response?.status_details?.error?.message || 'Realtime response failed.'),
      });
    }
    flushQueuedResponse(state);
    return;
  }

  if (type === 'error') {
    const code = String(event?.error?.code || 'realtime_error');
    const errorType = String(event?.error?.type || '');
    log('warn', 'OpenAI Realtime returned an error event', { callSid: state.callSid, code, type: errorType });
    reportVoiceEvent(state, {
      kind: 'diagnostic',
      event: 'voice_realtime_error',
      realtime_event: type,
      diagnostic_message: `${code}${errorType ? ` (${errorType})` : ''}`,
    });
  }
}
async function finishSession(state, disposition = 'ended') {
  if (state.ended) return;
  clearContinuationWatchdog(state);
  for (const timer of state.toolProgressTimers.values()) clearTimeout(timer);
  state.toolProgressTimers.clear();
  state.ended = true;
  if (state.maxTimer) clearTimeout(state.maxTimer);
  const durationSeconds = Math.max(0, Math.round((Date.now() - state.startedAt) / 1000));
  if (state.openaiWs && (state.openaiWs.readyState === WebSocket.OPEN || state.openaiWs.readyState === WebSocket.CONNECTING)) {
    try { state.openaiWs.close(1000, 'Twilio stream ended'); } catch {}
  }
  if (state.sessionId) {
    // Give already-queued transcript writes a brief chance to land before the
    // Proxy destroys the live Voice session used to authenticate them.
    try {
      await Promise.race([
        state.transcriptChain || Promise.resolve(),
        new Promise((resolve) => setTimeout(resolve, 3000)),
      ]);
    } catch {}
    try { await proxyPost('/session/end', { session_id: state.sessionId, duration_seconds: durationSeconds, disposition }); }
    catch (error) { log('warn', 'Could not report Voice session end', { callSid: state.callSid, error: error?.message || 'end_report_failed' }); }
  }
}

const server = http.createServer((req, res) => {
  if (req.url === '/health' || req.url === '/') {
    res.writeHead(200, { 'content-type': 'application/json' });
    res.end(JSON.stringify({ ok: true, service: 'nearby-booker-voice-relay', version: '0.2.6' }));
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
      // Do not let pre-greeting speech/background noise race the deterministic
      // opening. Once the first greeting audio frame is emitted, normal caller
      // audio and barge-in behavior resume immediately.
      if (state.startupAudioGate) {
        state.droppedStartupAudioFrames += 1;
        return;
      }
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
