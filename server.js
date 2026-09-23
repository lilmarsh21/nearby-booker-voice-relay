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

function proxyNetworkErrorDetails(error) {
  const cause = error?.cause || null;
  return {
    errorName: String(error?.name || ''),
    causeName: String(cause?.name || ''),
    causeCode: String(cause?.code || error?.code || ''),
    causeErrno: typeof cause?.errno === 'number' || typeof cause?.errno === 'string' ? String(cause.errno) : '',
    causeSyscall: String(cause?.syscall || ''),
  };
}

function bootstrapRetrySafe(error) {
  // Bootstrap tokens are one-time at the Proxy. Retry only failures that indicate
  // the HTTP request never reached WordPress. Ambiguous post-connect failures
  // (for example ECONNRESET) are logged but intentionally not replayed.
  const code = String(error?.cause?.code || '').toUpperCase();
  return new Set([
    'ENOTFOUND',
    'EAI_AGAIN',
    'ECONNREFUSED',
    'UND_ERR_CONNECT_TIMEOUT',
  ]).has(code);
}

function wait(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
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
      ...(path === '/session/bootstrap' ? { 'connection': 'close' } : {}),
    },
    body: raw,
    // Availability/live-calendar checks can legitimately require more than the
    // short control-plane timeout. Keep bootstrap/transfer fast while allowing
    // NBB tool calls to finish instead of aborting before the tenant's 25s cap.
    signal: AbortSignal.timeout(
      path === '/tool'
        ? (body?.tool_name === 'nbb_get_availability' ? 65000 : 40000)
        : 15000
    ),
  });
  const text = await response.text();
  let json = {};
  try { json = text ? JSON.parse(text) : {}; } catch { json = {}; }
  if (!response.ok || !json || json.success === false) {
    const error = new Error(json?.error?.message || json?.message || `GeoVee Proxy returned HTTP ${response.status}`);
    error.status = response.status;
    error.code = json?.error?.code || json?.code || 'proxy_error';
    throw error;
  }
  return json;
}

async function bootstrapProxySession(state, sessionToken) {
  const delays = [0, 350, 900];
  let lastError = null;

  for (let attempt = 0; attempt < delays.length; attempt += 1) {
    if (delays[attempt] > 0) await wait(delays[attempt]);
    try {
      return await proxyPost('/session/bootstrap', { session_token: sessionToken });
    } catch (error) {
      lastError = error;
      const details = proxyNetworkErrorDetails(error);
      const canRetry = bootstrapRetrySafe(error) && attempt < delays.length - 1;
      log(canRetry ? 'warn' : 'error', canRetry ? 'Voice bootstrap transport failed; retrying safely' : 'Voice bootstrap transport failed', {
        callSid: state.callSid,
        attempt: attempt + 1,
        maxAttempts: delays.length,
        retrySafe: bootstrapRetrySafe(error),
        ...details,
      });
      if (!canRetry) break;
    }
  }

  throw lastError || new Error('Voice bootstrap failed');
}

function safeSend(ws, object) {
  if (ws && ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(object));
}

function automaticInterruptionEnabled(state) {
  return state.rules?.barge_in_enabled && String(state.rules?.interruption_mode || '') === 'auto';
}

// Twilio Media Streams use G.711 mu-law (PCMU). Decode only enough to derive
// a small per-frame RMS envelope; raw audio is never stored or logged.
function muLawByteToPcm(value) {
  let u = (~value) & 0xff;
  const sign = u & 0x80;
  const exponent = (u >> 4) & 0x07;
  const mantissa = u & 0x0f;
  let sample = ((mantissa << 3) + 0x84) << exponent;
  sample -= 0x84;
  return sign ? -sample : sample;
}

function pcMuRms(payload) {
  try {
    const bytes = Buffer.from(String(payload || ''), 'base64');
    if (!bytes.length) return 0;
    let sumSquares = 0;
    for (const byte of bytes) {
      const sample = muLawByteToPcm(byte);
      sumSquares += sample * sample;
    }
    return Math.sqrt(sumSquares / bytes.length);
  } catch {
    return 0;
  }
}

function observeInboundAudio(state, payload) {
  if (String(state.rules?.interruption_mode || '') !== 'auto') return;
  const rms = pcMuRms(payload);
  if (!Number.isFinite(rms)) return;
  const now = Date.now();
  state.autoRecentRms.push({ at: now, rms });
  while (state.autoRecentRms.length && now - state.autoRecentRms[0].at > 900) state.autoRecentRms.shift();

  // While the AI is talking, inbound audio is normally the best sample of the
  // caller's ambient environment. Learn that floor conservatively and do not
  // chase a sudden spike that may actually be the caller beginning to speak.
  const aiAudible = state.responseActive || (now - state.lastAiAudioAt) < 1800;
  if (!aiAudible || state.autoSpeechStartedAt) return;
  if (state.autoNoiseFloorRms <= 0) {
    state.autoNoiseFloorRms = rms;
    return;
  }
  const cap = Math.max(state.autoNoiseFloorRms * 1.6, state.autoNoiseFloorRms + 200);
  const sample = Math.min(rms, cap);
  state.autoNoiseFloorRms = (state.autoNoiseFloorRms * 0.96) + (sample * 0.04);
}

function clearAutomaticBargeTimer(state) {
  if (state.autoBargeTimer) clearTimeout(state.autoBargeTimer);
  state.autoBargeTimer = null;
}

function adaptiveBargeGuardMs(state) {
  // Keep normal interruptions responsive, but give the adaptive meter enough
  // time to distinguish sustained caller speech from a short car/HVAC/clatter
  // spike. Repeated false starts make only this call progressively stricter.
  return Math.max(220, Math.min(560, 230 + (state.autoFalseStarts * 70)));
}

function automaticSpeechEnergyLooksReal(state) {
  const now = Date.now();
  const startedAt = state.autoSpeechStartedAt || (now - 250);
  const frames = state.autoRecentRms.filter((x) => x.at >= startedAt - 80 && x.at <= now);
  if (!frames.length) return true; // Never block a caller merely because local metering failed.

  if (!state.autoNoiseFloorRms) return true;
  const floor = Math.max(80, Number(state.autoNoiseFloorRms || 0));
  const ratio = Math.min(2.15, 1.48 + (state.autoFalseStarts * 0.12));
  const required = Math.max(300, floor * ratio);
  const aboveFrames = frames.filter((x) => x.rms >= required);
  const peak = Math.max(...frames.map((x) => x.rms));

  // A single bump or brief radio/road-noise burst can create several loud 20 ms
  // frames. Require the elevated energy to occupy a meaningful portion of the
  // candidate window instead of accepting only three loud frames.
  const requiredAboveCount = Math.max(4, Math.ceil(frames.length * 0.50));
  if (aboveFrames.length < requiredAboveCount || peak < required) return false;

  // Also require the above-floor activity to span real time. This preserves
  // natural caller interruption while rejecting a compact transient cluster.
  if (aboveFrames.length >= 2) {
    const aboveSpanMs = aboveFrames[aboveFrames.length - 1].at - aboveFrames[0].at;
    if (aboveSpanMs < 80) return false;
  }

  return true;
}

function confirmAutomaticBargeIn(state) {
  if (state.ended || state.startupAudioGate || !automaticInterruptionEnabled(state)) return;
  const now = Date.now();
  if (!automaticSpeechEnergyLooksReal(state)) {
    state.autoFalseStarts = Math.min(5, state.autoFalseStarts + 1);
    reportVoiceEvent(state, {
      kind: 'diagnostic',
      event: 'voice_auto_barge_noise_rejected',
      realtime_event: 'input_audio_buffer.speech_started',
      diagnostic_message: `Automatic interruption protection rejected a likely background-noise trigger; adaptive guard level ${state.autoFalseStarts}.`,
    });
    state.autoSpeechStartedAt = 0;
    armSilenceHangupFromLastActivity(state);
    return;
  }

  state.callerSpeechActive = true;
  markSpokenActivity(state, true);
  state.autoFalseStarts = Math.max(0, state.autoFalseStarts - 1);
  clearContinuationWatchdog(state);
  state.continuationRetryCount = 0;

  const aiAudible = state.responseActive || (now - state.lastAiAudioAt) < 1800;
  if (aiAudible) {
    state.autoBargeTriggered = true;
    safeSend(state.twilioWs, { event: 'clear', streamSid: state.streamSid });
    if (state.responseActive) safeSend(state.openaiWs, { type: 'response.cancel' });
    reportVoiceEvent(state, {
      kind: 'diagnostic',
      event: 'voice_auto_barge_confirmed',
      realtime_event: 'input_audio_buffer.speech_started',
      diagnostic_message: `Automatic interruption protection confirmed caller speech after ${Date.now() - state.autoSpeechStartedAt} ms.`,
    });
  }
}

function startAutomaticBargeCandidate(state) {
  clearAutomaticBargeTimer(state);
  state.autoSpeechStartedAt = Date.now();
  state.autoBargeTriggered = false;
  const guardMs = adaptiveBargeGuardMs(state);
  state.autoBargeTimer = setTimeout(() => {
    state.autoBargeTimer = null;
    confirmAutomaticBargeIn(state);
  }, guardMs);
}

function stopAutomaticBargeCandidate(state) {
  const hadPendingTimer = Boolean(state.autoBargeTimer);
  const hadConfirmedSpeech = Boolean(state.callerSpeechActive);
  clearAutomaticBargeTimer(state);
  if (hadPendingTimer && !hadConfirmedSpeech) {
    state.autoFalseStarts = Math.min(5, state.autoFalseStarts + 1);
    reportVoiceEvent(state, {
      kind: 'diagnostic',
      event: 'voice_auto_barge_short_noise_rejected',
      realtime_event: 'input_audio_buffer.speech_stopped',
      diagnostic_message: `Automatic interruption protection ignored a short VAD trigger; adaptive guard level ${state.autoFalseStarts}.`,
    });
  }
  state.callerSpeechActive = false;
  if (hadConfirmedSpeech) markSpokenActivity(state);
  else armSilenceHangupFromLastActivity(state);
  state.autoSpeechStartedAt = 0;
  state.autoBargeTriggered = false;
}

function normalizedText(value) {
  return String(value || '').toLowerCase().replace(/[^a-z0-9' ]+/g, ' ').replace(/\s+/g, ' ').trim();
}

function normalizedPhone(value) {
  const digits = String(value || '').replace(/\D+/g, '');
  if (digits.length === 10) return `+1${digits}`;
  if (digits.length === 11 && digits.startsWith('1')) return `+${digits}`;
  return digits ? `+${digits}` : '';
}

function normalizedEmail(value) {
  return String(value || '').trim().toLowerCase();
}

function normalizeEstimateDelivery(value) {
  const t = normalizedText(value);
  if (['sms', 'text', 'text message'].includes(t)) return 'sms';
  if (['email', 'e mail'].includes(t)) return 'email';
  if (['both', 'sms and email', 'email and sms', 'text and email', 'email and text'].includes(t)) return 'both';
  return '';
}

function spokenEmail(value) {
  const email = normalizedEmail(value);
  if (!email) return '';
  return email
    .replace(/@/g, ' at ')
    .replace(/\./g, ' dot ')
    .replace(/_/g, ' underscore ')
    .replace(/-/g, ' dash ')
    .replace(/\+/g, ' plus ')
    .replace(/\s+/g, ' ')
    .trim();
}

function estimateDeliveryLabel(value) {
  const delivery = normalizeEstimateDelivery(value);
  if (delivery === 'sms') return 'text message';
  if (delivery === 'email') return 'email';
  if (delivery === 'both') return 'both text message and email';
  return '';
}

function estimatePayloadSignature(toolArgs = {}) {
  const customer = toolArgs && typeof toolArgs.customer === 'object' ? toolArgs.customer : {};
  const services = Array.isArray(toolArgs?.services) ? toolArgs.services : [];
  const normalizedServices = services
    .map((row) => ({
      service_uuid: String(row?.service_uuid || '').trim(),
      quantity: Number(row?.quantity || 0),
    }))
    .filter((row) => row.service_uuid && Number.isFinite(row.quantity) && row.quantity > 0)
    .sort((a, b) => a.service_uuid.localeCompare(b.service_uuid) || a.quantity - b.quantity);
  return JSON.stringify({
    name: normalizedText(customer?.name || ''),
    services: normalizedServices,
  });
}

function explicitEstimateDeliveryChoice(text) {
  const t = normalizedText(text);
  if (!t) return '';
  const wantsSms = asksForTextMessage(text);
  const wantsEmail = asksForEmailMessage(text);
  if (wantsSms && wantsEmail) return 'both';
  if (wantsSms) return 'sms';
  if (wantsEmail) return 'email';
  return '';
}

function pushInternalCallContext(state, text) {
  if (!text || !state.openaiWs || state.openaiWs.readyState !== WebSocket.OPEN) return;
  safeSend(state.openaiWs, {
    type: 'conversation.item.create',
    item: {
      type: 'message',
      role: 'system',
      content: [{ type: 'input_text', text: `INTERNAL CALL STATE — never read this aloud: ${text}` }],
    },
  });
}

function customerContinuityInstruction(state) {
  const known = [];
  if (state.customerName) known.push(`name ${JSON.stringify(state.customerName)}`);
  if (state.customerPhone) known.push(`phone ${JSON.stringify(state.customerPhone)}`);
  if (state.customerEmail) known.push(`email ${JSON.stringify(state.customerEmail)}`);
  if (state.customerAddress) known.push(`service address ${JSON.stringify(state.customerAddress)}`);
  if (!known.length) return '';
  return `Customer details already captured earlier in this call: ${known.join(', ')}. Reuse these exact values for later estimate/booking steps and do not ask for them again unless a required field is missing or the caller explicitly changes it. Never read this internal state list aloud.`;
}

function slotSortKey(slot) {
  if (!slot || typeof slot !== 'object') return '9999-99-99T99:99';
  const date = String(slot.date || slot.service_date || '9999-99-99');
  const window = slot.window && typeof slot.window === 'object' ? slot.window : {};
  const start = String(window.start || slot.start || slot.start_time || '99:99');
  return `${date}T${start}`;
}

function sortVoiceAvailabilityResult(result) {
  if (!result || typeof result !== 'object') return result;
  const sortSlots = (rows) => Array.isArray(rows)
    ? [...rows].sort((a, b) => slotSortKey(a).localeCompare(slotSortKey(b)))
    : [];
  const savings = sortSlots(result.savings_slots);
  const nearby = sortSlots(result.nearby_slots);
  const standard = sortSlots(result.standard_slots);
  const offers = Array.isArray(result.offer_slots)
    ? sortSlots(result.offer_slots)
    : sortSlots([...savings, ...nearby]);
  if (Array.isArray(result.offer_slots)) result.offer_slots = offers;
  if (Array.isArray(result.savings_slots)) result.savings_slots = savings;
  if (Array.isArray(result.nearby_slots)) result.nearby_slots = nearby;
  if (Array.isArray(result.standard_slots)) result.standard_slots = standard;
  if (offers.length || standard.length) result.slots = [...offers, ...standard];
  return result;
}

function mergeKnownCustomer(state, toolArgs) {
  const current = toolArgs?.customer && typeof toolArgs.customer === 'object' ? { ...toolArgs.customer } : {};
  if (!current.name && state.customerName) current.name = state.customerName;
  if (!current.phone && state.customerPhone) current.phone = state.customerPhone;
  if (!current.email && state.customerEmail) current.email = state.customerEmail;
  if (!current.address && state.customerAddress) current.address = state.customerAddress;
  return { ...toolArgs, customer: current };
}

function bookingWrapupSatisfied(text) {
  const t = normalizedText(text);
  if (!t) return false;
  const allSet = t.includes('all set') || t.includes("you're set") || t.includes('you are set');
  const goodbye = t.includes('thanks for calling') || t.includes('thank you for calling') || t.includes('have a great') || t.includes('goodbye') || t.includes('good bye');
  return allSet && goodbye;
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

function isSpokenPricingRequest(text) {
  const t = normalizedText(text);
  if (!t) return false;

  // Savings-only questions remain allowed when NBB returned an explicit
  // savings_amount/spoken_savings. A caller asking for the actual service
  // price/total is redirected to estimate delivery when spoken pricing is off.
  const asksSavingsOnly = /\b(?:save|savings|discount|discounted)\b/.test(t)
    && !/\b(?:cost|price|priced|total|amount|charge|charges|quote total|estimate total)\b/.test(t);
  if (asksSavingsOnly) return false;

  return [
    /\bhow much\b.*\b(?:cost|costs|costing|price|priced|total|charge|charges|owe|pay)\b/,
    /\bwhat(?: is|'s) (?:the )?(?:cost|price|total|amount|charge|quote total|estimate total)\b/,
    /\bwhat does .*\bcost\b/,
    /\bwhat will .*\bcost\b/,
    /\bwhat would .*\bcost\b/,
    /\b(?:tell|give) me (?:the )?(?:cost|price|total|amount|quote total|estimate total)\b/,
    /\b(?:cost|price|total) (?:for|of) (?:this|that|everything|all of it|the job|the service|the services)\b/,
  ].some((pattern) => pattern.test(t));
}

function explicitWorkflowIntent(text) {
  const t = normalizedText(text);
  if (!t) return '';

  // Existing-appointment actions outrank generic booking words because phrases
  // such as "reschedule my appointment" also contain appointment language.
  if (/\b(?:reschedule|re-schedule)\b/.test(t)
      || /\b(?:move|change)\b.*\b(?:appointment|booking)\b/.test(t)
      || /\b(?:appointment|booking)\b.*\b(?:move|change)\b/.test(t)) return 'reschedule';
  if (/\b(?:cancel|cancellation)\b.*\b(?:appointment|booking)\b/.test(t)
      || /\b(?:appointment|booking)\b.*\b(?:cancel|cancellation)\b/.test(t)) return 'cancel';

  const booking = [
    /\b(?:schedule|book)\b.*\b(?:cleaning|service|appointment|job|visit|carpet|upholstery|vent|couch|sofa|sectional|stair|tile|rug|detail|detailing|pressure washing)\b/,
    /\b(?:make|set up|setup|arrange)\b.*\b(?:an? )?appointment\b/,
    /\b(?:i|we) (?:want|need|would like|would love)\b.*\b(?:schedule|book|appointment)\b/,
    /\b(?:can|could|would) (?:i|we)\b.*\b(?:schedule|book)\b/,
    /\b(?:ready|want|need) to (?:schedule|book)\b/,
    /\b(?:schedule|book)\s+(?:it|this|that|the estimate|the quote)\b/,
  ].some((pattern) => pattern.test(t));
  if (booking) return 'booking';

  const estimate = [
    /\b(?:quote|estimate)\b/,
    /\bhow much\b/,
    /\b(?:what|how much).*(?:cost|price|charge)\b/,
    /\b(?:cost|price)\b.*\b(?:service|cleaning|job|it|that|this)\b/,
  ].some((pattern) => pattern.test(t));
  if (estimate) return 'estimate';

  return '';
}

function explicitlySwitchesToEstimateOnly(text) {
  const t = normalizedText(text);
  if (!t) return false;
  const hasEstimate = /\b(?:quote|estimate|price|cost)\b/.test(t);
  if (!hasEstimate) return false;
  return /\b(?:instead|only|just)\b/.test(t)
    || /\b(?:do not|don't|dont|not)\b.*\b(?:schedule|book|appointment)\b/.test(t)
    || /\b(?:forget|stop)\b.*\b(?:schedule|booking|appointment)\b/.test(t);
}

function workflowIntentInstruction(state) {
  const goal = String(state.workflowIntent || '');
  if (goal === 'booking') {
    return 'CALL GOAL IS LOCKED TO BOOKING/SCHEDULING because the caller explicitly asked to make an appointment. Keep that goal across service-detail questions and tool results. A price/quote question may be answered as a side request, but it does NOT replace the booking goal unless the caller explicitly says they only want an estimate instead. After service collection is complete, continue the booking path: service address, live availability, slot selection, required contact details, optional configured notes checkpoint, booking review, explicit confirmation, then submission. Do not proactively switch into estimate-delivery mode.';
  }
  if (goal === 'estimate') {
    return 'CALL GOAL IS LOCKED TO QUOTE/ESTIMATE. Complete the estimate workflow and do not start availability or booking unless the caller explicitly asks to schedule/book.';
  }
  if (goal === 'reschedule') {
    return 'CALL GOAL IS LOCKED TO RESCHEDULING AN EXISTING APPOINTMENT. Use the existing-booking verification/reschedule path; do not reconstruct the order as a new quote or booking.';
  }
  if (goal === 'cancel') {
    return 'CALL GOAL IS LOCKED TO CANCELLING AN EXISTING APPOINTMENT. Use the existing-booking verification/cancellation path; do not reconstruct the order as a new quote or booking.';
  }
  return '';
}

function updateWorkflowIntentFromTranscript(state, text) {
  const detected = explicitWorkflowIntent(text);
  if (!detected) return;

  const current = String(state.workflowIntent || '');
  const now = Date.now();

  // A quote/price question during an active booking is a side request by default,
  // not a silent conversion of the whole call into estimate-delivery mode.
  if (current === 'booking' && detected === 'estimate' && !explicitlySwitchesToEstimateOnly(text)) {
    state.quoteSideRequestAt = now;
    pushInternalCallContext(state, 'The caller asked a pricing/estimate side question, but their active call goal remains BOOKING. Handle the requested pricing/estimate information if allowed, then resume the booking flow without asking them to choose a goal again.');
    return;
  }

  // Moving from estimate -> booking is a common, explicit progression and should
  // take effect immediately. Existing-appointment actions also always take
  // precedence when the caller clearly requests them.
  if (!current || detected !== current) {
    state.workflowIntent = detected;
    state.workflowIntentSetAt = now;
    if (detected !== 'booking') state.quoteSideRequestAt = 0;
    pushInternalCallContext(state, workflowIntentInstruction(state));
  }
}

function recentQuoteSideRequest(state) {
  const at = Number(state.quoteSideRequestAt || 0);
  return at > 0 && (Date.now() - at) <= 120000;
}

function postServiceContinuationInstruction(state) {
  const goal = String(state.workflowIntent || '');
  if (goal === 'booking') {
    if (recentQuoteSideRequest(state)) {
      return 'The caller said they do not want any additional services and also asked a pricing/estimate side question. Their active goal remains BOOKING. Do not ask about additional services again. Handle only the requested pricing/estimate side question using the allowed Nearby Booker workflow, then resume booking with the next missing item; normally the service address if it is not already known.';
    }
    return 'The caller said they do not want any additional services. Their active goal is BOOKING. Do not ask about additional services again and do not switch to quote/estimate delivery. Continue with the next missing booking item; normally ask for the service address if it is not already known, then validate it and continue to live availability.';
  }
  if (goal === 'estimate') {
    return 'The caller said they do not want any additional services. Their active goal is a QUOTE/ESTIMATE. Do not ask about additional services again. Continue the estimate workflow using the already selected services and quantities.';
  }
  return 'The caller said they do not want any additional services. Do not ask that question again. Continue with the next single missing item in the caller\'s active request.';
}

function pricingRedirectInstruction(state) {
  const sms = Boolean(state.rules?.sms_enabled);
  const email = Boolean(state.rules?.email_enabled);
  if (sms && email) {
    return 'Speak exactly this sentence and nothing else: "I can send you the full estimate by text, email, or both. Which would you prefer?"';
  }
  if (sms) {
    return 'Speak exactly this sentence and nothing else: "I can send you the full estimate by text. Would you like me to do that?"';
  }
  if (email) {
    return 'Speak exactly this sentence and nothing else: "I can send you the full estimate by email. Would you like me to do that?"';
  }
  return 'Speak exactly this sentence and nothing else: "I can keep helping with the service details and scheduling, but I cannot provide a price over the phone."';
}

function isNoMoreServicesReply(text) {
  const t = normalizedText(text);
  if (!t) return false;

  // This is an intent classifier for the answer to the deterministic
  // "any other services?" checkpoint. Addition/change intent always wins so
  // a phrase such as "no thanks, but add upholstery" can never accidentally
  // close service collection.
  const additionIntent = [
    /\b(?:but|however|except|besides)\b.*\b(?:add|also|need|want|include|clean|cleaning|service|treatment)\b/,
    /\b(?:actually|also|and)\b.*\b(?:add|need|want|include|clean|cleaning)\b/,
    /\b(?:add|include)\b.*\b(?:another|one more|service|cleaning|cleaned|treatment|vent|carpet|upholstery|couch|sofa|sectional|stair|tile|rug|detail)/,
    /\b(?:also need|also want|need another|want another|another service|one more thing|one more service)\b/,
    /\b(?:i|we) (?:still )?(?:need|want|would like)\b.*\b(?:another|more|service|cleaning|treatment|carpet|upholstery|vent|couch|sofa|sectional|stair|tile|rug|detail|detailing|pressure washing)\b/,
    /\bno (?:not|instead of) (?:that|this)\b/,
    /\b(?:change|switch|replace)\b.*\b(?:service|cleaning|treatment|to|with)\b/,
    /\b(?:what about|how about|do you do|can you add)\b.*\b(?:service|cleaning|treatment|carpet|upholstery|vent|couch|sofa|sectional|stair|tile|rug|detail|detailing|pressure washing)\b/,
    /\b(?:schedule|book)\b.*\b(?:service|cleaning|treatment|carpet|upholstery|vent|couch|sofa|sectional|stair|tile|rug|detail|detailing|pressure washing)\b.*\b(?:too|also|as well)\b/,
  ];
  if (additionIntent.some((pattern) => pattern.test(t))) return false;

  // Explicit statements that service collection is NOT finished must also
  // remain open even though they contain completion words such as "all".
  if (/\b(?:don't|dont|do not) think (?:that(?:'| )?s|that is) (?:all|it|everything)\b/.test(t)) return false;
  if (/\b(?:not done|not finished|not all set)\b/.test(t)) return false;

  // Strong negative answer. Because addition/change intent was checked first,
  // natural variations such as "for the fourth time, no" safely close without
  // needing an ordinal-by-ordinal phrase whitelist.
  if (/\b(?:no|nope|nah)\b/.test(t)) return true;

  // General completion language. These patterns describe intent rather than
  // complete sentences, so harmless wording around them does not matter.
  const completionIntent = [
    /\b(?:i am|i(?:'| )?m|we are|we(?:'| )?re) (?:all )?(?:set|done|good|finished)\b/,
    /\b(?:all set|all done|we(?:'| )?re good|i(?:'| )?m good)\b/,
    /\b(?:that|this) (?:will|would|should) do(?: it)?\b/,
    /\bthat(?:'| )?ll do(?: it)?\b/,
    /\b(?:that(?:'| )?s|that is) (?:all|it|everything|enough)\b/,
    /\b(?:nothing|none) else\b/,
    /\bno more (?:service|services|cleaning|cleanings|things|anything)\b/,
    /\b(?:don't|dont|do not) (?:need|want) (?:anything|anything else|anything more|more|another service|any other services?)\b/,
    /\b(?:just|please|go ahead and|let's|lets) (?:move on|continue|proceed)\b/,
    /\b(?:let's|lets|go ahead and|just|please) (?:schedule|book)(?: it| this| the appointment| the job| what we have| what we've got| what weve got)?\b/,
    /\b(?:send|text|email) (?:me )?(?:the |my )?(?:estimate|quote)\b/,
    /\bready to (?:schedule|book|continue|proceed|move on)\b/,
    /\b(?:that|this) does it\b/,
    /\b(?:only|just) (?:that|those|these)\b/,
  ];
  return completionIntent.some((pattern) => pattern.test(t));
}

function isAdditionalServiceQuestion(text) {
  const t = normalizedText(text);
  if (!t) return false;
  const asks = [
    'add any other service', 'add any other services', 'add another service',
    'any other service', 'any other services', 'anything else you would like cleaned',
    'anything else youd like cleaned', 'anything else you want cleaned',
    'would you like anything else', 'do you need anything else cleaned'
  ];
  return asks.some((phrase) => t.includes(phrase));
}

function serviceRowsSignature(args = {}) {
  if (!Array.isArray(args?.services)) return '';
  const rows = args.services
    .map((row) => ({
      service_uuid: String(row?.service_uuid || '').trim(),
      quantity: Number(row?.quantity || 0),
    }))
    .filter((row) => row.service_uuid && Number.isFinite(row.quantity) && row.quantity > 0)
    .sort((a, b) => a.service_uuid.localeCompare(b.service_uuid) || a.quantity - b.quantity);
  return rows.length ? JSON.stringify(rows) : '';
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
    businessName: '',
    callerPhone: '',
    customerName: '',
    customerPhone: '',
    customerEmail: '',
    customerAddress: '',
    activeEstimateId: '',
    activeEstimateServiceSignature: '',
    activeEstimateServices: [],
    confirmedPhone: '',
    confirmedEmail: '',
    bookingPhoneChoiceResolved: false,
    bookingPhoneSource: '',
    awaitingBookingPhoneChoice: false,
    bookingNotesChoiceResolved: false,
    bookingNotes: '',
    awaitingBookingNotesChoice: false,
    estimateDeliveryPreference: '',
    estimateDeliverySelectedAt: 0,
    estimateSentAt: 0,
    estimateSentDelivery: '',
    lastEstimateId: '',
    lastEstimateSignature: '',
    conversationMessages: [],
    sessionPurpose: 'normal',
    pendingContactConfirmation: null,
    serviceCollectionClosed: false,
    awaitingAdditionalServiceDecision: false,
    serviceCollectionSignature: '',
    workflowIntent: '',
    workflowIntentSetAt: 0,
    quoteSideRequestAt: 0,
    callerAppointmentHint: null,
    callerAppointmentHintPromise: null,
    callerAppointmentHintContextSent: false,
    transcriptPermissionVerified: false,
    bookingWrapupRequired: false,
    bookingWrapupRetryCount: 0,
    callbackWrapupRequired: false,
    callbackWrapupRetryCount: 0,
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
    lastAiAudioAt: 0,
    aiAudioActive: false,
    aiPlaybackPending: false,
    responseHadAudio: false,
    playbackMarkSeq: 0,
    pendingPlaybackMarks: new Set(),
    callerSpeechActive: false,
    lastSpokenActivityAt: 0,
    silenceTimer: null,
    silenceHangupInProgress: false,
    autoNoiseFloorRms: 0,
    autoRecentRms: [],
    autoSpeechStartedAt: 0,
    autoBargeTimer: null,
    autoBargeTriggered: false,
    autoFalseStarts: 0,
    // Deterministic DTMF message mode. This is isolated from normal booking/tool
    // state and is inactive unless the tenant explicitly enables it.
    messageMode: false,
    messageParts: [],
    messageFinalizing: false,
    messageFinalized: false,
    messageFinalizePromise: null,
    messagePromptPending: false,
    messageResponsePhase: '',
    messageTranscriptPending: false,
    messageSilenceTimer: null,
    messageHangupAfterPlayback: false,
  };
}

function voiceConversationId(state) {
  const safe = String(state.sessionId || '').replace(/[^A-Za-z0-9_-]/g, '').slice(0, 56);
  return safe ? `voice_${safe}` : '';
}

async function refreshTranscriptPermission(state) {
  state.transcriptPermissionVerified = false;

  // Preferred path: ask the tenant NBB itself for the current Phone transcript
  // decision. This is content-free and avoids trusting a stale Proxy snapshot.
  try {
    const response = await proxyPost('/tool', {
      session_id: state.sessionId,
      tool_name: 'nbb_get_transcript_permission',
      arguments: {},
    });
    const enabled = Boolean(response?.result?.enabled);
    state.rules.transcripts_enabled = enabled;
    state.transcriptPermissionVerified = true;
    log('info', 'Voice transcript permission verified by tenant', {
      callSid: state.callSid,
      enabled,
      phoneEnabled: Boolean(response?.result?.phone_transcripts_enabled),
      conversationEnabled: Boolean(response?.result?.conversation_transcripts_enabled),
    });
    return enabled;
  } catch (error) {
    const code = String(error?.code || '');
    if (code !== 'nbb_voice_tool_unknown') {
      log('warn', 'Voice transcript permission refresh unavailable; transcript transport remains unverified', {
        callSid: state.callSid,
        code: code || 'transcript_permission_refresh_failed',
      });
      return false;
    }
  }

  // Backward compatibility: tenants from before nbb_get_transcript_permission
  // still have the established nbb_log_voice_event endpoint. Probe THAT endpoint
  // with a diagnostic event containing no caller/assistant text. The tenant then
  // proves whether transcript storage is currently allowed. Do not trust the
  // cached/synced Proxy flag here; it was the source of silent false negatives.
  const conversationId = voiceConversationId(state);
  if (!conversationId) return false;
  try {
    const probe = await proxyPost('/tool', {
      session_id: state.sessionId,
      tool_name: 'nbb_log_voice_event',
      arguments: {
        conversation_id: conversationId,
        kind: 'diagnostic',
        event: 'voice_transcript_permission_probe',
        status: 'active',
        diagnostic_message: 'Phone transcript transport verified.',
      },
    });
    if (probe?.result?.stored === true) {
      state.rules.transcripts_enabled = true;
      state.transcriptPermissionVerified = true;
      log('info', 'Voice transcript permission verified by legacy tenant write probe', {
        callSid: state.callSid,
        enabled: true,
      });
      return true;
    }

    const reason = String(probe?.result?.reason || 'voice_transcript_probe_not_stored');
    if (reason === 'disabled' || reason === 'phone_transcripts_disabled') {
      state.rules.transcripts_enabled = false;
      state.transcriptPermissionVerified = true;
      log('info', 'Voice transcript storage is disabled by tenant', {
        callSid: state.callSid,
        enabled: false,
        code: reason,
      });
      return false;
    }

    const probeError = new Error('Tenant did not confirm transcript permission probe');
    probeError.code = reason;
    throw probeError;
  } catch (error) {
    state.rules.transcripts_enabled = false;
    log('warn', 'Legacy tenant transcript permission probe failed; transcript transport remains unverified', {
      callSid: state.callSid,
      code: error?.code || 'legacy_transcript_permission_probe_failed',
    });
    return false;
  }
}

function reportVoiceEvent(state, payload = {}) {
  if (!state.sessionId || state.ended) return Promise.resolve();
  const conversationId = voiceConversationId(state);
  if (!conversationId) return Promise.resolve();
  const args = { conversation_id: conversationId, ...payload };

  // Central GeoVee support transcripts and tenant-local NBB transcripts are
  // separate sinks. Central storage is attempted first for every Voice event,
  // regardless of the tenant's local transcript setting. If the new central
  // action is unavailable or fails, tenant-local logging still proceeds and the
  // Proxy's legacy post-tenant central-copy path remains available as fallback.
  // Ordered writes preserve event sequence and avoid duplicate central copies.
  state.transcriptChain = (state.transcriptChain || Promise.resolve())
    .catch(() => {})
    .then(async () => {
      let centralStored = false;
      try {
        const central = await proxyPost('/tool', {
          session_id: state.sessionId,
          tool_name: 'geovee_log_central_voice_event',
          arguments: args,
        });
        if (central?.result?.stored === true) {
          centralStored = true;
        } else {
          const error = new Error('GeoVee Proxy did not confirm the central Voice transcript write');
          error.code = central?.result?.reason || 'central_voice_transcript_store_unconfirmed';
          throw error;
        }
      } catch (error) {
        log('warn', 'Central Voice transcript event could not be stored; tenant path remains available', {
          callSid: state.callSid,
          code: error?.code || 'central_voice_transcript_store_failed',
        });
      }

      if (!state.transcriptPermissionVerified || !state.rules?.transcripts_enabled) return;

      try {
        const tenantArgs = centralStored
          ? { ...args, proxy_central_copy_handled: true }
          : args;
        const response = await proxyPost('/tool', {
          session_id: state.sessionId,
          tool_name: 'nbb_log_voice_event',
          arguments: tenantArgs,
        });
        if (response?.result?.stored !== true) {
          const error = new Error('Nearby Booker did not confirm the Voice transcript write');
          error.code = response?.result?.reason || 'voice_transcript_store_unconfirmed';
          throw error;
        }
      } catch (error) {
        log('warn', 'Tenant Voice transcript event could not be stored', {
          callSid: state.callSid,
          code: error?.code || 'voice_transcript_store_failed',
        });
      }
    })
    .catch((error) => {
      log('warn', 'Unexpected Voice transcript pipeline failure', {
        callSid: state.callSid,
        code: error?.code || 'voice_transcript_pipeline_failed',
      });
    });
  return state.transcriptChain;
}

function clearVoiceMessageSilenceTimer(state) {
  if (state.messageSilenceTimer) clearTimeout(state.messageSilenceTimer);
  state.messageSilenceTimer = null;
}

function voiceMessageText(state) {
  return (Array.isArray(state.messageParts) ? state.messageParts : [])
    .map((part) => String(part || '').trim())
    .filter(Boolean)
    .join(' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function sendVoiceMessageResponse(state, text, phase) {
  if (!state.openaiWs || state.openaiWs.readyState !== WebSocket.OPEN || state.ended) return false;
  state.messageResponsePhase = String(phase || '');
  state.responseRequestPending = true;
  safeSend(state.openaiWs, {
    type: 'response.create',
    response: { instructions: `Speak exactly this sentence and nothing else: ${JSON.stringify(String(text || ''))}` },
  });
  return true;
}

function tryStartVoiceMessagePrompt(state) {
  if (!state.messageMode || !state.messagePromptPending || state.ended) return false;
  if (state.responseActive || state.responseRequestPending) return false;
  state.messagePromptPending = false;
  return sendVoiceMessageResponse(
    state,
    'Sure. Please leave your message now. Press pound when you are finished, or just hang up.',
    'message_entry_prompt'
  );
}

function armVoiceMessageSilenceTimer(state) {
  clearVoiceMessageSilenceTimer(state);
  if (!state.messageMode || state.messageFinalizing || state.messageFinalized || !voiceMessageText(state)) return;
  state.messageSilenceTimer = setTimeout(() => {
    finalizeVoiceMessage(state, 'silence', true).catch((error) => {
      log('warn', 'Voice message silence finalization failed safely', { callSid: state.callSid, code: error?.code || 'message_finalize_failed' });
    });
  }, 8000);
}

async function enterVoiceMessageMode(state) {
  if (state.ended || !state.rules?.leave_message_enabled) return false;
  if (state.messageMode) return true;

  state.messageMode = true;
  // Message mode replaces the deterministic startup/greeting path. Accept caller
  // audio immediately even when 1 is pressed before the first greeting frame.
  state.startupAudioGate = false;
  state.messageParts = [];
  state.messageFinalizing = false;
  state.messageFinalized = false;
  state.messageFinalizePromise = null;
  state.messagePromptPending = true;
  state.messageResponsePhase = '';
  state.messageTranscriptPending = false;
  state.messageHangupAfterPlayback = false;
  // Anything queued before DTMF 1 belongs to the abandoned AI conversation.
  // Twilio is cleared below, so discard its local playback bookkeeping too;
  // late marks from that cleared audio are intentionally ignored.
  state.pendingPlaybackMarks.clear();
  state.aiPlaybackPending = false;
  state.aiAudioActive = false;
  state.responseHadAudio = false;
  state.queuedResponseInstructions = '';
  state.queuedResponseReason = '';
  clearContinuationWatchdog(state);
  clearAutomaticBargeTimer(state);
  clearSilenceHangupTimer(state);
  clearVoiceMessageSilenceTimer(state);
  for (const timer of state.toolProgressTimers.values()) clearTimeout(timer);
  state.toolProgressTimers.clear();

  // Pressing 1 must take priority over whatever the AI was saying or doing.
  safeSend(state.twilioWs, { event: 'clear', streamSid: state.streamSid });
  if (state.openaiWs?.readyState === WebSocket.OPEN && (state.responseActive || state.responseRequestPending)) {
    safeSend(state.openaiWs, { type: 'response.cancel' });
  }

  reportVoiceEvent(state, {
    kind: 'diagnostic',
    event: 'voice_message_mode_started',
    realtime_event: 'twilio.dtmf',
    diagnostic_message: 'Caller pressed 1 and entered Phone AI message mode.',
  });

  // If a response is being cancelled, response.done/cancelled will start the
  // prompt. If no response is active, start it immediately.
  if (!tryStartVoiceMessagePrompt(state)) {
    setTimeout(() => tryStartVoiceMessagePrompt(state), 250);
  }
  return true;
}

async function flushVoiceMessageTranscription(state, timeoutMs = 1600) {
  if (!state.messageMode || state.ended) return;
  if (!state.openaiWs || state.openaiWs.readyState !== WebSocket.OPEN) return;

  // A caller commonly presses # or hangs up immediately after the last word.
  // Give Realtime a bounded chance to finish that final transcription before
  // composing the admin SMS. This wait happens only while leaving a message.
  if (state.callerSpeechActive) {
    safeSend(state.openaiWs, { type: 'input_audio_buffer.commit' });
  }
  if (!state.messageTranscriptPending) return;

  const deadline = Date.now() + Math.max(200, Math.min(2500, Number(timeoutMs || 1600)));
  while (state.messageTranscriptPending && !state.ended && Date.now() < deadline) {
    await wait(80);
  }
}

async function finalizeVoiceMessage(state, endedBy = 'pound', speak = true) {
  if (!state.messageMode || state.messageFinalized) return true;
  if (state.messageFinalizePromise) return state.messageFinalizePromise;

  state.messageFinalizePromise = (async () => {
    state.messageFinalizing = true;
    clearVoiceMessageSilenceTimer(state);
    const message = voiceMessageText(state);

    if (!message) {
      state.messageFinalizing = false;
      state.messageFinalizePromise = null;
      if (speak && !state.ended) {
        sendVoiceMessageResponse(
          state,
          'I did not catch a message. Please leave your message now, then press pound when you are finished.',
          'message_entry_prompt'
        );
      }
      return false;
    }

    let sent = false;
    try {
      const response = await proxyPost('/tool', {
        session_id: state.sessionId,
        tool_name: 'geovee_send_voice_message_notification',
        arguments: { message, ended_by: String(endedBy || 'unknown') },
      });
      sent = response?.result?.sent === true;
      if (!sent) {
        const error = new Error('GeoVee Proxy did not confirm the caller message notification');
        error.code = response?.result?.reason || 'voice_message_notification_unconfirmed';
        throw error;
      }
    } catch (error) {
      log('warn', 'Voice caller message notification failed safely', {
        callSid: state.callSid,
        code: error?.code || 'voice_message_notification_failed',
      });
    }

    state.messageFinalized = true;
    state.messageFinalizing = false;
    reportVoiceEvent(state, {
      kind: 'diagnostic',
      event: sent ? 'voice_message_notification_sent' : 'voice_message_notification_failed',
      realtime_event: `message_finalize_${String(endedBy || 'unknown')}`,
      diagnostic_message: sent ? 'Caller message notification sent to tenant admin.' : 'Caller message was transcribed but the admin SMS notification was not confirmed.',
    });

    if (speak && !state.ended) {
      state.messageHangupAfterPlayback = true;
      const queued = sendVoiceMessageResponse(
        state,
        sent
          ? `Thanks. Your message has been sent to ${state.businessName || 'the business'}. Goodbye.`
          : 'Thanks. I captured your message, but I could not send the text notification right now. Goodbye.',
        'message_final_ack'
      );
      // If Realtime is unavailable there will be no response.done or Twilio mark.
      // End cleanly instead of leaving the caller stranded in message mode.
      if (!queued) {
        state.messageHangupAfterPlayback = false;
        setTimeout(() => endVoiceMessageCall(state).catch(() => {}), 0);
      }
    }
    return sent;
  })();

  try { return await state.messageFinalizePromise; }
  finally { if (state.messageFinalized) state.messageFinalizePromise = null; }
}

async function endVoiceMessageCall(state) {
  if (state.ended) return;
  try {
    if (state.sessionId) await proxyPost('/call/hangup', { session_id: state.sessionId, reason: 'voice_message_complete' });
  } catch (error) {
    log('warn', 'Voice message call hangup request failed safely', { callSid: state.callSid, code: error?.code || 'message_hangup_failed' });
  }
  await finishSession(state, 'voice_message_complete');
}

function clearSilenceHangupTimer(state) {
  if (state.silenceTimer) clearTimeout(state.silenceTimer);
  state.silenceTimer = null;
}

function silenceHangupSeconds(state) {
  return Math.max(5, Math.min(60, Number(state.rules?.silence_hangup_seconds || 10)));
}

function armSilenceHangupFromLastActivity(state) {
  clearSilenceHangupTimer(state);
  if (state.messageMode) return;
  if (state.ended || state.silenceHangupInProgress || state.startupAudioGate || !state.lastSpokenActivityAt) return;
  if (state.callerSpeechActive || state.aiAudioActive || state.aiPlaybackPending) return;
  const seconds = silenceHangupSeconds(state);
  const elapsed = Date.now() - state.lastSpokenActivityAt;
  const remaining = Math.max(50, (seconds * 1000) - elapsed);
  state.silenceTimer = setTimeout(() => executeSilenceHangup(state), remaining);
}

function markSpokenActivity(state, keepPaused = false) {
  state.lastSpokenActivityAt = Date.now();
  clearSilenceHangupTimer(state);
  if (!keepPaused) armSilenceHangupFromLastActivity(state);
}

async function executeSilenceHangup(state) {
  if (state.ended || state.silenceHangupInProgress || state.callerSpeechActive || state.aiAudioActive || state.aiPlaybackPending) {
    armSilenceHangupFromLastActivity(state);
    return;
  }
  const seconds = silenceHangupSeconds(state);
  if (!state.lastSpokenActivityAt || (Date.now() - state.lastSpokenActivityAt) < (seconds * 1000) - 50) {
    armSilenceHangupFromLastActivity(state);
    return;
  }
  state.silenceHangupInProgress = true;
  clearContinuationWatchdog(state);
  clearAutomaticBargeTimer(state);
  clearSilenceHangupTimer(state);
  for (const timer of state.toolProgressTimers.values()) clearTimeout(timer);
  state.toolProgressTimers.clear();
  reportVoiceEvent(state, {
    kind: 'diagnostic',
    event: 'voice_silence_timeout',
    realtime_event: 'silence_watchdog',
    duration_ms: Date.now() - state.lastSpokenActivityAt,
    diagnostic_message: `No caller or Phone Agent speech was detected for ${seconds} seconds; ending the call.`,
  });
  try {
    if (state.sessionId) await proxyPost('/call/hangup', { session_id: state.sessionId, reason: 'silence_timeout' });
  } catch (error) {
    log('warn', 'Silent-call hangup request failed', { callSid: state.callSid, code: error?.code || 'silence_hangup_failed' });
  }
  await finishSession(state, 'silence_timeout');
  try { if (state.twilioWs?.readyState === WebSocket.OPEN) state.twilioWs.close(1000, 'Silence timeout'); } catch {}
}

function sendTwilioPlaybackMark(state) {
  if (!state.twilioWs || state.twilioWs.readyState !== WebSocket.OPEN || !state.streamSid) return false;
  state.playbackMarkSeq += 1;
  const name = `nbb_ai_playback_${state.playbackMarkSeq}`;
  state.pendingPlaybackMarks.add(name);
  state.aiPlaybackPending = true;
  safeSend(state.twilioWs, {
    event: 'mark',
    streamSid: state.streamSid,
    mark: { name },
  });
  return true;
}

function handleTwilioPlaybackMark(state, name) {
  const markName = String(name || '');
  if (!markName || !state.pendingPlaybackMarks.has(markName)) return false;
  state.pendingPlaybackMarks.delete(markName);
  if (state.pendingPlaybackMarks.size > 0) return true;

  // Twilio returns this mark only after all outbound media queued before it has
  // either finished playing or has been explicitly cleared. Start the silence
  // clock from what the caller actually heard, not from when OpenAI finished
  // generating audio upstream.
  state.aiPlaybackPending = false;
  state.lastSpokenActivityAt = Date.now();
  if (state.messageHangupAfterPlayback) {
    state.messageHangupAfterPlayback = false;
    setTimeout(() => endVoiceMessageCall(state).catch(() => {}), 10);
    return true;
  }
  armSilenceHangupFromLastActivity(state);
  return true;
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
  const fire = () => {
    if (state.ended || !state.openaiWs || state.openaiWs.readyState !== WebSocket.OPEN) return;
    // Raw VAD/noise must never permanently disable dead-air recovery. If the
    // caller is actually speaking, wait for that turn to finish and then re-check.
    if (state.callerSpeechActive) {
      state.continuationTimer = setTimeout(fire, 400);
      return;
    }
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
        true
      );
    }, 250);
  };
  state.continuationTimer = setTimeout(fire, seconds * 1000);
}

function callerAppointmentHintInstruction(state) {
  const hint = state.callerAppointmentHint;
  const count = Number(hint?.matchCount || 0);
  if (count <= 0) return '';
  return count > 1
    ? 'CALLER APPOINTMENT HINT: the authenticated caller number is associated with more than one active/future appointment. This is NOT verification. Do not reveal candidate dates, times, addresses, customer names, or booking IDs from caller ID alone. If the caller wants to change an appointment, ask for the service address and use nbb_lookup_existing_booking. If more than one appointment remains after the address, ask the caller for the CURRENT appointment date, and if still ambiguous ask for the current start time. Never guess which appointment they mean.'
    : 'CALLER APPOINTMENT HINT: the authenticated caller number is associated with an active/future appointment. This is NOT verification. Do not reveal its date, time, address, customer name, or booking ID from caller ID alone. If the caller wants to change it, ask for the service address and use nbb_lookup_existing_booking.';
}

function pushCallerAppointmentHintContext(state) {
  if (state.callerAppointmentHintContextSent) return;
  const instruction = callerAppointmentHintInstruction(state);
  if (!instruction) return;
  pushInternalCallContext(state, instruction);
  state.callerAppointmentHintContextSent = true;
}

async function lookupCallerAppointmentHint(state) {
  if (!state.sessionId || !state.callerPhone || state.rules?.callback_only) return null;
  if (!state.rules?.reschedule_enabled && !state.rules?.cancel_enabled) return null;
  try {
    const response = await proxyPost('/tool', {
      session_id: state.sessionId,
      tool_name: 'nbb_lookup_caller_appointments',
      arguments: {},
    });
    const result = response?.result && typeof response.result === 'object' ? response.result : {};
    const matchCount = Math.max(0, Math.min(100, Number(result.match_count || 0)));
    const hint = {
      matchCount,
      multiple: matchCount > 1 || result.multiple === true,
    };
    state.callerAppointmentHint = hint;
    if (matchCount > 0 && state.openaiWs?.readyState === WebSocket.OPEN) {
      pushCallerAppointmentHintContext(state);
    }
    return hint;
  } catch (error) {
    // Backward compatibility: an older tenant NBB may not yet expose this
    // privacy-safe preflight action. Never delay or fail the call for the hint.
    log('info', 'Existing-appointment caller hint unavailable; continuing normal greeting', {
      callSid: state.callSid,
      code: error?.code || 'caller_appointment_hint_unavailable',
    });
    return null;
  }
}

function dynamicVoiceResponseRules(state) {
  const rules = [];
  const workflowRule = workflowIntentInstruction(state);
  if (workflowRule) rules.push(workflowRule);
  const appointmentHintRule = callerAppointmentHintInstruction(state);
  if (appointmentHintRule) rules.push(appointmentHintRule);
  if (state.rules?.ask_one_question) {
    rules.push('ONE-QUESTION RULE IS ON: never ask more than one customer-facing question in this response. When gathering information, ask for exactly one missing item, then STOP and wait for the caller before asking for the next item. Do not bundle service, quantity, address, name, email, delivery preference, scheduling preference, or other missing fields into one turn. This applies after tool results and continuation responses too.');
  }
  if (state.rules?.show_arrival_time) {
    rules.push("SHOW ARRIVAL TIME IS ON: every appointment time you speak must use the exact words 'arrival time between' followed by both the start and end times. Never speak only the start time.");
  }
  if (state.rules?.quotes_enabled === false) {
    rules.push('PHONE SPOKEN PRICING IS OFF: do not state or infer service prices, line-item costs, quote totals, appointment totals, minimum charges, or grand totals. This does NOT block creating a real estimate: if the caller wants a quote sent, collect the required customer/service information and use nbb_send_estimate. Never tell the caller that pricing visibility is off or disabled. A savings_amount/spoken_savings explicitly returned for a savings date is allowed and should be stated because it reveals only the amount saved.');
  }
  if (state.rules?.callback_only) {
    rules.push('CALLBACK-COLLECTION MODE: collect a brief callback reason and customer name when practical, reuse the verified caller phone unless they explicitly provide a different confirmed number, submit nbb_request_callback, then confirm success and close with thanks/goodbye. Do not start quote, availability, estimate, or booking flows.');
  }
  if (state.estimateDeliveryPreference) {
    rules.push(`ESTIMATE DELIVERY IS LOCKED FOR THIS CALL: the caller selected ${estimateDeliveryLabel(state.estimateDeliveryPreference)}. Do not ask text/email/both again unless the caller explicitly changes that preference.`);
  }
  if (state.confirmedEmail) {
    rules.push(`EMAIL IS CONFIRMED AND LOCKED FOR THIS CALL: use the exact canonical address ${JSON.stringify(state.confirmedEmail)}. Do not ask the caller to confirm it again and do not substitute a different address unless the caller explicitly changes it.`);
  } else {
    rules.push('EMAIL CONFIRMATION IS RELAY-OWNED: when the caller dictates an email, capture it but do NOT read it back or ask whether it is correct on your own. Put it into the next relevant estimate/booking tool call; the Voice Relay will perform the one exact confirmation read-back.');
  }
  if (!state.serviceCollectionClosed) {
    rules.push('MULTI-SERVICE GATE IS OPEN: a service is NOT complete until every configured required quantity, variant, option, and required service question for that selected service has been answered by the caller. Do not say a service is noted, finished, all set, or move to the final additional-services checkpoint while any configured detail is unresolved. Only after the currently selected service is fully resolved may you ask exactly once whether the caller wants to add any other services BEFORE moving to service address, quote delivery, availability, or booking. Do not assume one service means the order is complete.');
  } else {
    rules.push('SERVICE COLLECTION IS CLOSED FOR NOW: the caller already said they do not want additional services. Do not ask about adding services again unless they later add or change a service.');
  }
  if (state.bookingPhoneChoiceResolved && state.confirmedPhone) {
    rules.push(`BOOKING PHONE IS CONFIRMED AND LOCKED: use ${JSON.stringify(state.confirmedPhone)} as the booking phone. Source is ${state.bookingPhoneSource || 'confirmed'}. Do not ask which phone to use again and do not reconfirm this same number unless the caller explicitly changes it.`);
  } else if (state.awaitingBookingPhoneChoice) {
    rules.push('BOOKING PHONE SOURCE IS WAITING FOR THE CALLER: ask only whether they want to use the number they are calling from or a different number. Do not assume the caller number. Do not prepare the booking until this choice is resolved.');
  }
  if (state.rules?.ask_booking_notes) {
    if (state.bookingNotesChoiceResolved) {
      rules.push(state.bookingNotes
        ? 'APPOINTMENT NOTES CHECKPOINT IS RESOLVED: the caller supplied an appointment note and Relay will attach it to the booking. Do not ask for notes again unless the caller explicitly changes them.'
        : 'APPOINTMENT NOTES CHECKPOINT IS RESOLVED: the caller declined to add appointment notes. Do not ask again unless the caller explicitly changes their decision.');
    } else {
      rules.push('APPOINTMENT NOTES CHECKPOINT IS REQUIRED FOR NEW BOOKINGS: near the end, before nbb_prepare_booking, ask exactly one question: "Would you like to add any notes for the appointment?" If no, call voice_set_booking_notes with mode none. If yes but they have not supplied the note yet, ask exactly one follow-up question for the note, then call voice_set_booking_notes with mode set.');
    }
  } else {
    rules.push('APPOINTMENT NOTES CHECKPOINT IS OFF: do not ask for appointment notes and do not invent booking_notes.');
  }
  const continuity = customerContinuityInstruction(state);
  if (continuity) rules.push(continuity);
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
    parts.push('Consume this business context SILENTLY. Never read or paraphrase capability state, toggles, configuration, pricing visibility, booking enablement, service metadata, or internal status to the caller. Use it only to ask the next natural customer-facing question. SERVICE SELECTION SAFETY: compare every plausible configured service using its category, service label, aliases, private AI description, and private selection rules. If the caller wording could match more than one service and the distinction changes fulfillment or workflow, such as in-home versus pickup/drop-off, do not guess. Ask exactly one concise clarifying question and wait before quote, availability, or booking.');
  }

  if (name === 'nbb_lookup_estimate' && result && typeof result === 'object' && result.found) {
    parts.push('EXISTING ESTIMATE LOADED. Treat the returned estimate_id, services, quantities, saved address, and customer prefill as authoritative NBB data for this call. If the caller asked what is included, summarize the returned service labels and quantities now; do not say you cannot see the estimate. Do not volunteer saved phone/email/address details unless they are needed for the caller request. If the caller asks to schedule the unchanged estimate, DO NOT ask what service they want and DO NOT make them reconstruct the estimate. Reuse these exact services and saved address, continue to live availability, and pass the same estimate_id to nbb_prepare_booking. If the caller changes any service or quantity, the original saved estimate remains unchanged; use the modified service set and do not pass the original estimate_id into booking unless a revised estimate was actually created first.');
  }

  if (name === 'nbb_send_estimate' && result && typeof result === 'object') {
    parts.push('This is a real Nearby Booker estimate-delivery result. If sent is true, use the returned spoken_confirmation as the customer-facing status. Say it was submitted for delivery; do not promise that an email has already reached the inbox or that a deferred text has already arrived. The delivery preference and any confirmed email/phone are already locked in Relay state: do NOT ask text/email/both again and do NOT reconfirm an already-confirmed email or phone. Do NOT speak the estimate total when spoken pricing is off. Do not mention tools, settings, review tokens, internal workflow, or pricing visibility. The customer identity captured for this estimate remains valid for the rest of this call: if they next ask to schedule, reuse their known name/phone/email and do not collect those fields again unless one is actually missing or they explicitly change it. Do not ask the caller to reconfirm the Twilio caller phone number.');
  }

  if (name === 'nbb_lookup_existing_booking' && result && typeof result === 'object') {
    if (result.verified === true && result.verification_token) {
      parts.push('EXISTING APPOINTMENT VERIFIED. You may now use only the matched appointment details returned by Nearby Booker. Confirm the matched current appointment date/time naturally before continuing the reschedule/cancel workflow. Do not ask the caller to reconstruct the existing services.');
    } else if (result.needs === 'address_retry') {
      parts.push('DO NOT SAY THERE IS NO APPOINTMENT. Nearby Booker found one or more active/future appointments under the authenticated caller number, but the spoken service address did not verify. Ask exactly one concise question for the street number and street name again, including apartment/unit when applicable.');
    } else if (result.needs === 'appointment_date' || result.needs === 'appointment_date_retry') {
      parts.push('DO NOT SAY THERE IS NO APPOINTMENT. The caller number and service address match one or more active/future appointments, but a current appointment date is needed to select the correct one. Ask the caller for the CURRENT appointment date. Do not reveal candidate dates or guess.');
    } else if (result.needs === 'appointment_time' || result.needs === 'appointment_time_retry') {
      parts.push('DO NOT SAY THERE IS NO APPOINTMENT. More than one appointment still matches after phone/address/date checks, or the stated start time did not verify. Ask for the CURRENT appointment start time. Do not reveal candidate times or guess.');
    } else if (result.needs === 'phone_match') {
      parts.push('Nearby Booker did not find an active/future appointment under the authenticated live caller number. State only that limitation; do not claim the address itself was wrong and do not invent an appointment.');
    }
  }

  if (name === 'nbb_validate_address' && result && typeof result === 'object') {
    parts.push('The address check just completed. If Nearby Booker accepted/canonicalized the address, do NOT say technical phrases such as address validation succeeded, eligible to continue, validation result, or confirmation policy. Acknowledge naturally in a few words and CONTINUE THE SCHEDULING FLOW IN THIS SAME TURN. If the caller has not yet supplied the configured service selections/quantities needed for availability, ask exactly one concise service question next. If the required services/quantities are already known from the conversation, call nbb_get_availability immediately using the validated address. Never end this turn with only a status statement after a successful address check. If the address was rejected or materially incomplete, ask only for the missing/corrected address information.');
  }

  if (state.rules?.show_arrival_time && ['nbb_get_availability', 'nbb_prepare_booking', 'nbb_commit_booking'].includes(name)) {
    parts.push("For every appointment window in this response, say 'arrival time between' and both endpoints. If spoken_window is present, use that wording. Never shorten a window to only its start time.");
  }

  if (name === 'nbb_get_availability' && result && typeof result === 'object') {
    const savings = Array.isArray(result.savings_slots) ? result.savings_slots : [];
    const nearby = Array.isArray(result.nearby_slots) ? result.nearby_slots : [];
    const offers = Array.isArray(result.offer_slots) ? result.offer_slots : [...savings, ...nearby];
    if (offers.length) {
      const allMonetary = offers.every((slot) => Boolean(slot?.has_monetary_savings) && Number(slot?.savings_amount || 0) > 0);
      if (allMonetary) {
        parts.push("Keep the first phone presentation short. The offer slots are already sorted chronologically. Present ONLY the first three offer slots initially, then stop and ask which one they prefer or whether they want more options. Do NOT begin reading standard availability in the same response while offer dates exist unless the caller specifically asked for standard/other dates. The first time you explain these dates, say they are dates when the business is already scheduled to be in the caller's area, which allows a better rate. For each slot you actually present, state the exact positive savings_amount/spoken_savings.");
      } else {
        parts.push("Keep the first phone presentation short. The Radius/Offer slots are already sorted chronologically. Present ONLY the first three offer slots initially, then stop and ask which one they prefer or whether they want more options. Do NOT begin reading standard availability in the same response while offer dates exist unless the caller specifically asked for standard/other dates. IMPORTANT: classify each slot individually. If has_monetary_savings is true and savings_amount is positive, you may call that specific slot a savings date and state spoken_savings. If has_monetary_savings is false or savings_amount is zero, NEVER call that slot a savings date, discount, special savings, or better rate. For a zero-savings offer slot, say the exact configured offer_text/spoken_offer naturally, then give its date and appointment window. Do not invent a monetary benefit. If all presented offer slots have zero savings, describe them only as dates when the business is already scheduled nearby; do not use the word savings.");
      }
    } else {
      parts.push('The standard slots are sorted chronologically. Present at most the first three openings initially, then ask which one they prefer or whether they want more options. Do not dump a long list of appointment times over the phone.');
    }
  }

  if (name === 'nbb_commit_booking' && result && typeof result === 'object' && result.booking_id) {
    parts.push(`BOOKING SUBMISSION SUCCEEDED. This response MUST close the transaction naturally. State only the status Nearby Booker returned, repeat the appointment window using the required arrival wording, explain the approval/confirmation next step if applicable, then explicitly say "You're all set", thank the caller for calling ${state.businessName || 'the business'}, and give a brief goodbye. Do not leave silence after the confirmation and do not end with "anything else?" or another open question.`);
  }

  if (name === 'nbb_request_callback' && result && typeof result === 'object' && result.sent) {
    parts.push(`CALLBACK REQUEST SUCCEEDED. Tell the caller the callback request was sent to ${state.businessName || 'the business'}, confirm the callback number only if useful, thank them for calling, and give a brief goodbye. Do not ask another question and do not claim a specific callback time unless the tool result explicitly provided one.`);
  }

  if (state.rules?.quotes_enabled === false) {
    parts.push('Do not reveal any full service price or estimate total from this result or from memory. Savings amounts explicitly returned for savings dates are the only pricing-like amounts you may say.');
  }
  return parts.join(' ');
}

function requestModelResponse(state, instructions = '', reason = 'normal', watchForAudio = false) {
  if (state.messageMode) return false;
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
  const boot = await bootstrapProxySession(state, sessionToken);
  if (!boot?.openai_client_secret || !boot?.session_id || !boot?.model) throw new Error('Voice bootstrap response is incomplete');
  state.sessionId = String(boot.session_id);
  state.callSid = String(boot.call_sid || state.callSid || '');
  if (String(boot.model) !== 'gpt-realtime-2.1') throw new Error('GeoVee Phone AI model policy mismatch');
  state.model = 'gpt-realtime-2.1';
  state.rules = boot.rules || {};
  state.sessionPurpose = String(boot.session_purpose || (state.rules?.callback_only ? 'callback_only' : 'normal'));
  state.businessName = String(boot.business_name || '');
  state.callerPhone = normalizedPhone(boot.caller_phone || '');
  state.openingGreeting = String(boot.opening_greeting || 'Thanks for calling. How can I help you today?');
  state.bootstrapped = true;

  // Start the privacy-safe existing-appointment preflight immediately, in
  // parallel with transcript setup/OpenAI connection. It is bound by Proxy to
  // the live caller ID and returns only a count hint, never appointment details.
  state.callerAppointmentHintPromise = lookupCallerAppointmentHint(state);

  // Confirm transcript storage against the tenant's live NBB setting before any
  // customer/assistant text can be written. This prevents a stale central sync
  // flag from silently suppressing enabled Phone Agent transcripts.
  await refreshTranscriptPermission(state);
  // Create/verify the central support transcript row for every Phone AI call.
  // reportVoiceEvent independently applies the tenant-local permission gate, so
  // an NBB copy is written only when the tenant has enabled Phone transcripts.
  await reportVoiceEvent(state, {
    kind: 'diagnostic',
    event: 'voice_transcript_session_started',
    status: 'active',
    diagnostic_message: 'Phone transcript session initialized.',
  });

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
    const startGreeting = async () => {
      log('info', 'OpenAI Realtime connected', { callSid: state.callSid, model: state.model });

      // Reliability rule: never feed startup caller audio into Realtime before the
      // configured greeting has actually begun. Otherwise early speech/background
      // noise can trigger VAD/barge-in and clear the greeting before the caller hears it.
      const buffered = state.pendingAudio.splice(0).length;
      if (buffered > 0) state.droppedStartupAudioFrames += buffered;
      if (state.callerPhone) {
        pushInternalCallContext(state, `The verified Twilio caller number is ${state.callerPhone}. For BOOKING, do not assume this is the appointment contact number. Before preparing a booking, explicitly ask whether the caller wants to use the number they are calling from or a different number. For estimate SMS/callback behavior, continue following the existing channel-specific rules.`);
      }

      // Wait only briefly for the preflight. A slow/older tenant must never hold
      // up the opening greeting. If the result arrives later it still becomes
      // internal context for any reschedule/cancel request during the call.
      let hint = state.callerAppointmentHint;
      if (!hint && state.callerAppointmentHintPromise) {
        hint = await Promise.race([
          state.callerAppointmentHintPromise,
          wait(900).then(() => null),
        ]).catch(() => null);
      }
      if (hint?.matchCount > 0) pushCallerAppointmentHintContext(state);

      let greeting = state.openingGreeting;
      if (hint?.matchCount > 0) {
        greeting = `${greeting} I recognize this number may be associated with an upcoming appointment or appointment request. Are you calling about an existing appointment, or can I help you with something else?`;
      }
      state.openingGreetingRequested = true;
      requestModelResponse(
        state,
        `Speak exactly this opening greeting and nothing else before waiting for the caller: ${JSON.stringify(greeting)}. Do not mention settings, enabled features, tools, integrations, or configuration.`,
        'opening_greeting',
        true
      );
    };
    startGreeting().catch((error) => {
      log('warn', 'Opening greeting preflight failed safely; using configured greeting', { callSid: state.callSid, code: error?.code || 'opening_greeting_preflight_failed' });
      if (!state.openingGreetingRequested) {
        state.openingGreetingRequested = true;
        requestModelResponse(
          state,
          `Speak exactly this opening greeting and nothing else before waiting for the caller: ${JSON.stringify(state.openingGreeting)}. Do not mention settings, enabled features, tools, integrations, or configuration.`,
          'opening_greeting',
          true
        );
      }
    });
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

function invalidatePreparedBookingForContactChange(state) {
  // A prepared review token represents the exact customer details at that moment.
  // If phone/email changes, discard it so the next review is rebuilt with the
  // newly confirmed contact data. This is Voice-only state; NBB core is untouched.
  state.lastReviewToken = '';
  state.preparedAt = 0;
  state.bookingConfirmationAt = 0;
  state.awaitingBookingConfirmation = false;
}

function queueDeterministicContinuation(state, instructions, reason) {
  // Realtime VAD may already have started an automatic response to a simple
  // "yes". Cancel that response so a contact confirmation cannot produce a
  // duplicate confirmation question, then continue with our authoritative state.
  if (state.responseActive || state.responseRequestPending) {
    safeSend(state.openaiWs, { type: 'response.cancel' });
  }
  requestModelResponse(state, instructions, reason, true);
}

function handleBookingPhonePreferenceTool(state, callId, args = {}) {
  const mode = normalizedText(args?.mode || '');
  const phone = normalizedPhone(args?.phone || '');

  if (mode === 'caller') {
    if (!state.callerPhone) {
      acknowledgeFunction(state, callId, { success: false, code: 'caller_phone_unavailable', message: 'The live caller number is unavailable. Ask for the booking phone number.' });
      state.awaitingBookingPhoneChoice = false;
      requestModelResponse(state, 'Ask for the phone number they want to use for the appointment. Ask only that one question.', 'booking_phone_missing', true);
      return;
    }
    state.customerPhone = state.callerPhone;
    state.confirmedPhone = state.callerPhone;
    state.bookingPhoneChoiceResolved = true;
    state.bookingPhoneSource = 'caller';
    state.awaitingBookingPhoneChoice = false;
    state.pendingContactConfirmation = null;
    invalidatePreparedBookingForContactChange(state);
    acknowledgeFunction(state, callId, { success: true, booking_phone: state.confirmedPhone, source: 'caller', confirmed: true });
    pushInternalCallContext(state, `Booking phone source resolved: use the verified caller number ${state.confirmedPhone}. It is already confirmed. Do not ask which number to use again and do not read it back unless the caller asks.`);
    requestModelResponse(state, 'The caller chose the number they are calling from. Continue the booking flow with the next single missing item. Do not reconfirm the phone number.', 'booking_phone_caller_selected', true);
    return;
  }

  if (mode === 'alternate' || mode === 'different') {
    state.awaitingBookingPhoneChoice = false;
    if (!phone) {
      acknowledgeFunction(state, callId, { success: false, code: 'alternate_phone_missing', message: 'Ask for the different booking phone number.' });
      requestModelResponse(state, 'Ask exactly one question: "What phone number would you like me to use for the appointment?" Then stop and wait.', 'alternate_booking_phone_needed', true);
      return;
    }
    if (phone === state.callerPhone) {
      // If the dictated number normalizes to the live caller number, treat it as
      // the caller-number choice and avoid an unnecessary confirmation loop.
      state.customerPhone = state.callerPhone;
      state.confirmedPhone = state.callerPhone;
      state.bookingPhoneChoiceResolved = true;
      state.bookingPhoneSource = 'caller';
      invalidatePreparedBookingForContactChange(state);
      acknowledgeFunction(state, callId, { success: true, booking_phone: state.confirmedPhone, source: 'caller', confirmed: true });
      requestModelResponse(state, 'That number matches the number they are calling from, so it is already confirmed. Continue with the next single missing booking item and do not reconfirm the phone.', 'booking_phone_matches_caller', true);
      return;
    }
    state.pendingContactConfirmation = { type: 'phone', value: phone, context: 'booking_phone' };
    state.customerPhone = phone;
    state.confirmedPhone = '';
    state.bookingPhoneChoiceResolved = false;
    state.bookingPhoneSource = 'alternate';
    invalidatePreparedBookingForContactChange(state);
    acknowledgeFunction(state, callId, { success: false, code: 'voice_phone_confirmation_required', message: 'Confirm the different booking phone number once.' });
    requestModelResponse(state, `Read back this booking phone number exactly once: ${JSON.stringify(phone)}. Ask if it is correct, then stop and wait. Do not ask any other question.`, 'booking_phone_confirmation_required', true);
    return;
  }

  acknowledgeFunction(state, callId, { success: false, code: 'booking_phone_mode_invalid', message: 'Choose caller or alternate.' });
  state.awaitingBookingPhoneChoice = true;
  requestModelResponse(state, `Ask exactly one question: "Would you like me to use the number you're calling from for this appointment, or use a different number?" Then stop and wait.`, 'booking_phone_choice_required', true);
}

function handleBookingNotesTool(state, callId, args = {}) {
  if (!state.rules?.ask_booking_notes) {
    acknowledgeFunction(state, callId, { success: false, code: 'booking_notes_disabled', message: 'Appointment-note collection is not enabled for this Voice session.' });
    return;
  }

  const mode = normalizedText(args?.mode || '');
  const notes = String(args?.notes || '').trim().slice(0, 4000);

  if (mode === 'none' || mode === 'no') {
    state.bookingNotesChoiceResolved = true;
    state.bookingNotes = '';
    state.awaitingBookingNotesChoice = false;
    invalidatePreparedBookingForContactChange(state);
    acknowledgeFunction(state, callId, { success: true, notes_added: false, resolved: true });
    pushInternalCallContext(state, 'The caller explicitly declined appointment notes. The notes checkpoint is complete. Do not ask again unless the caller changes their mind.');
    requestModelResponse(state, 'The caller declined appointment notes. Continue with the next single missing booking step. Do not ask about notes again.', 'booking_notes_none', true);
    return;
  }

  if (mode === 'set' || mode === 'add') {
    if (!notes) {
      state.awaitingBookingNotesChoice = true;
      acknowledgeFunction(state, callId, { success: false, code: 'booking_notes_text_required', message: 'The caller chose to add a note, but the note text is missing.' });
      requestModelResponse(state, 'Ask exactly one question: "What notes would you like me to add to the appointment?" Then stop and wait.', 'booking_notes_text_required', true);
      return;
    }
    state.bookingNotesChoiceResolved = true;
    state.bookingNotes = notes;
    state.awaitingBookingNotesChoice = false;
    invalidatePreparedBookingForContactChange(state);
    acknowledgeFunction(state, callId, { success: true, notes_added: true, resolved: true });
    pushInternalCallContext(state, 'The caller supplied an appointment note. Relay has stored it for the booking. Do not ask for notes again unless the caller explicitly changes them.');
    requestModelResponse(state, 'The appointment note is recorded for this booking. Continue with the next single missing booking step and do not ask for notes again.', 'booking_notes_set', true);
    return;
  }

  acknowledgeFunction(state, callId, { success: false, code: 'booking_notes_mode_invalid', message: 'Use mode none or set.' });
  state.awaitingBookingNotesChoice = true;
  requestModelResponse(state, 'Ask exactly one question: "Would you like to add any notes for the appointment?" Then stop and wait.', 'booking_notes_choice_required', true);
}

async function executeNbbTool(state, name, callId, args = {}) {
  if (!state.sessionId) {
    acknowledgeFunction(state, callId, { success: false, code: 'voice_session_missing', message: 'Nearby Booker session is not ready yet.' });
    requestModelResponse(state, 'Briefly explain that the phone assistant is still initializing and ask the caller to try that request again.', 'tool_not_ready', true);
    return;
  }

  let toolArgs = args && typeof args === 'object' ? { ...args } : {};

  // If the assistant already asked the deterministic "any other services?"
  // question, progression tools must wait for an actual caller answer. Returning
  // a function result without requesting another model response prevents a
  // tool/retry loop from asking the same question twice during caller silence.
  if (state.awaitingAdditionalServiceDecision
      && ['nbb_get_quote', 'nbb_get_availability', 'nbb_prepare_booking', 'nbb_send_estimate'].includes(name)) {
    acknowledgeFunction(state, callId, {
      success: false,
      code: 'voice_waiting_for_additional_service_decision',
      message: 'Waiting for the caller to answer whether they want any additional services.',
    });
    return;
  }

  // Deterministic multi-service checkpoint. Prompt instructions handle the normal
  // conversational order, but this Relay guard prevents the model from silently
  // treating the first service as the whole order. It only engages once a
  // progression tool actually contains one or more concrete service selections.
  if (['nbb_get_quote', 'nbb_get_availability', 'nbb_prepare_booking', 'nbb_send_estimate'].includes(name)) {
    const serviceSignature = serviceRowsSignature(toolArgs);
    if (serviceSignature) {
      // If the caller had closed service collection and the service set later
      // changes, reopen the checkpoint so a newly added/changed service does not
      // silently become the last service either. A first post-confirmation tool
      // call may establish the baseline signature without re-asking.
      if (state.serviceCollectionClosed && !state.serviceCollectionSignature) {
        state.serviceCollectionSignature = serviceSignature;
      } else if (state.serviceCollectionClosed && state.serviceCollectionSignature !== serviceSignature) {
        state.serviceCollectionClosed = false;
      }

      if (!state.serviceCollectionClosed) {
        state.serviceCollectionSignature = serviceSignature;
        state.awaitingAdditionalServiceDecision = true;
        acknowledgeFunction(state, callId, {
          success: false,
          code: 'voice_additional_service_check_required',
          message: 'Ask whether the caller wants to add any other services before continuing.',
        });
        requestModelResponse(
          state,
          'Before moving on, ask exactly one question: "Would you like to add any other services?" Then stop and wait. Do not ask for the service address yet.',
          'additional_service_check_required',
          true
        );
        return;
      }
    }
  }

  // Preserve a caller's explicit booking goal across service-detail collection.
  // Quote tools are still available when the caller actually asks a pricing/
  // estimate side question, but the model may not proactively replace booking
  // with a quote workflow simply because service collection just completed.
  if (state.workflowIntent === 'booking'
      && ['nbb_get_quote', 'nbb_send_estimate'].includes(name)
      && !recentQuoteSideRequest(state)) {
    acknowledgeFunction(state, callId, {
      success: false,
      code: 'voice_booking_goal_locked',
      message: 'The caller asked to schedule/book. Continue the booking flow; do not proactively switch to a quote/estimate workflow.',
    });
    requestModelResponse(
      state,
      'The caller\'s active goal is BOOKING. Do not offer quote delivery here. Continue with the next missing booking item; if service collection is already complete, ask for the service address if it is not known, then continue to validation and live availability.',
      'booking_goal_locked',
      true
    );
    return;
  }

  if (['nbb_prepare_booking', 'nbb_send_estimate', 'nbb_request_callback'].includes(name)) {
    toolArgs = mergeKnownCustomer(state, toolArgs);
  }

  if (['nbb_get_availability', 'nbb_prepare_booking'].includes(name) && state.activeEstimateId) {
    const suppliedServices = Array.isArray(toolArgs?.services) ? toolArgs.services : [];
    const suppliedSignature = serviceRowsSignature({ services: suppliedServices });
    const estimateStillUnchanged = Boolean(
      state.activeEstimateServiceSignature
      && state.serviceCollectionClosed
      && state.serviceCollectionSignature === state.activeEstimateServiceSignature
      && (!suppliedSignature || suppliedSignature === state.activeEstimateServiceSignature)
    );

    // Scheduling an unchanged loaded estimate must not depend on the model
    // reconstructing saved services/address from prose. Hydrate only missing
    // values from per-call state and bind the saved estimate id. If the caller
    // changed the service set, never force the original estimate back in.
    if (estimateStillUnchanged) {
      if (!suppliedSignature && Array.isArray(state.activeEstimateServices) && state.activeEstimateServices.length) {
        toolArgs.services = state.activeEstimateServices.map((row) => ({
          service_uuid: String(row?.service_uuid || '').trim(),
          quantity: Number(row?.quantity || 0),
        })).filter((row) => row.service_uuid && Number.isFinite(row.quantity) && row.quantity > 0);
      }
      if (!toolArgs.estimate_id) toolArgs.estimate_id = state.activeEstimateId;
    }
    if (name === 'nbb_get_availability' && !toolArgs.address && state.customerAddress) toolArgs.address = state.customerAddress;
  }

  if (name === 'nbb_prepare_booking') {
    if (!toolArgs.customer || typeof toolArgs.customer !== 'object') toolArgs.customer = {};
    if (!state.bookingPhoneChoiceResolved || !state.confirmedPhone) {
      const candidatePhone = normalizedPhone(toolArgs.customer.phone || '');
      // A genuinely different number volunteered before the explicit choice can
      // be accepted as the alternate candidate, but it still gets one read-back.
      if (candidatePhone && candidatePhone !== state.callerPhone) {
        state.awaitingBookingPhoneChoice = false;
        state.customerPhone = candidatePhone;
        state.bookingPhoneSource = 'alternate';
        state.pendingContactConfirmation = { type: 'phone', value: candidatePhone, context: 'booking_phone' };
        invalidatePreparedBookingForContactChange(state);
        acknowledgeFunction(state, callId, { success: false, code: 'voice_phone_confirmation_required', message: 'Confirm the alternate booking phone number once.' });
        requestModelResponse(state, `Read back this booking phone number exactly once: ${JSON.stringify(candidatePhone)}. Ask if it is correct, then stop and wait.`, 'booking_phone_confirmation_required', true);
        return;
      }
      state.awaitingBookingPhoneChoice = true;
      acknowledgeFunction(state, callId, { success: false, code: 'booking_phone_choice_required', message: 'The caller must choose whether to use the live caller number or a different booking number.' });
      requestModelResponse(state, `Ask exactly one question: "Would you like me to use the number you're calling from for this appointment, or use a different number?" Then stop and wait. Do not assume the caller number.`, 'booking_phone_choice_required', true);
      return;
    }
    toolArgs.customer.phone = state.confirmedPhone;
  }

  // Precision contact fields: server-owned state is authoritative. Once a
  // contact value has been explicitly confirmed by the caller, later model tool
  // arguments are NEVER allowed to mutate it. This prevents a model retry or
  // transcription drift from turning one confirmed digit/character into a new
  // contact candidate and trapping the caller in a confirmation loop.
  if (['nbb_prepare_booking', 'nbb_send_estimate', 'nbb_request_callback'].includes(name) && toolArgs?.customer && typeof toolArgs.customer === 'object') {
    let candidatePhone = normalizedPhone(toolArgs.customer.phone || '');
    let candidateEmail = normalizedEmail(toolArgs.customer.email || '');
    const requestedEstimateDelivery = normalizeEstimateDelivery(toolArgs.delivery || '');
    const effectiveEstimateDelivery = requestedEstimateDelivery || state.estimateDeliveryPreference;
    const usesEmail = name === 'nbb_prepare_booking' || (name === 'nbb_send_estimate' && ['email', 'both'].includes(effectiveEstimateDelivery));

    if (state.confirmedPhone) {
      // Confirmed phone is immutable for this call. Ignore any later differing
      // model-proposed value; only the caller's original confirmation owns state.
      candidatePhone = state.confirmedPhone;
      toolArgs.customer.phone = state.confirmedPhone;
    } else if (candidatePhone && candidatePhone !== state.callerPhone) {
      state.pendingContactConfirmation = { type: 'phone', value: candidatePhone };
      acknowledgeFunction(state, callId, { success: false, code: 'voice_phone_confirmation_required', message: 'The alternate booking phone number must be confirmed by the caller.' });
      requestModelResponse(state, `Before continuing, read back this alternate phone number exactly once: ${JSON.stringify(candidatePhone)}. Ask the caller if that is correct. Do not call the booking/estimate tool again until they answer.`, 'phone_confirmation_required', true);
      return;
    }

    if (usesEmail && state.confirmedEmail) {
      // Confirmed email is immutable for this call. A later tool retry must use
      // the exact caller-confirmed value even if the model proposes a different
      // spelling, digit, punctuation mark, or casing.
      candidateEmail = state.confirmedEmail;
      toolArgs.customer.email = state.confirmedEmail;
    } else if (usesEmail && candidateEmail) {
      state.pendingContactConfirmation = { type: 'email', value: candidateEmail };
      const speech = spokenEmail(candidateEmail);
      acknowledgeFunction(state, callId, { success: false, code: 'voice_email_confirmation_required', message: 'The email address must be confirmed by the caller.' });
      requestModelResponse(
        state,
        `Confirm the email exactly once. Speak the punctuation as words so the caller can hear every separator. Say exactly: "I have ${speech}. Is that correct?" Do NOT speak or display the @ symbol in this confirmation; say the word "at". Do not silently correct, autocomplete, or guess any part of the address. Do not call the booking/estimate tool again until the caller answers.`,
        'email_confirmation_required',
        true
      );
      return;
    }
  }

  if (name === 'nbb_prepare_booking') {
    if (!toolArgs.customer || typeof toolArgs.customer !== 'object') toolArgs.customer = {};
    if (state.rules?.ask_booking_notes) {
      if (!state.bookingNotesChoiceResolved) {
        state.awaitingBookingNotesChoice = true;
        acknowledgeFunction(state, callId, { success: false, code: 'booking_notes_choice_required', message: 'Ask whether the caller wants to add appointment notes before preparing the booking.' });
        requestModelResponse(state, 'Ask exactly one question: "Would you like to add any notes for the appointment?" Then stop and wait. If they decline, use voice_set_booking_notes with mode none. If they want notes but have not supplied them yet, ask for the note as the next single question.', 'booking_notes_choice_required', true);
        return;
      }
      if (state.bookingNotes) toolArgs.customer.booking_notes = state.bookingNotes;
      else delete toolArgs.customer.booking_notes;
    } else {
      // OFF must preserve the historical Voice behavior even if the model tries
      // to populate this already-existing NBB transport field.
      delete toolArgs.customer.booking_notes;
    }
  }

  if (name === 'nbb_get_quote' && state.rules?.quotes_enabled === false) {
    acknowledgeFunction(state, callId, {
      success: false,
      code: 'voice_spoken_pricing_disabled',
      message: 'Spoken pricing is unavailable; use the estimate-delivery workflow when the caller wants a quote sent.',
    });
    requestModelResponse(
      state,
      'Do not mention pricing settings or say pricing is disabled. If the caller wants a quote, continue gathering the configured services and quantities and ask whether they want the real estimate sent by text, email, or both, unless they already chose a delivery method. Use nbb_send_estimate once the required information is collected.',
      'pricing_delivery_redirect',
      true
    );
    return;
  }

  if (name === 'nbb_send_estimate') {
    const requestedDelivery = normalizeEstimateDelivery(toolArgs?.delivery || '');

    // The normalized estimate tool argument represents the caller's CURRENT
    // delivery choice. Never let an older Relay value override a newer explicit
    // choice. This removes the old phrase-dependent "switch/instead/only" gate.
    if (requestedDelivery && requestedDelivery !== state.estimateDeliveryPreference) {
      const previous = state.estimateDeliveryPreference;
      state.estimateDeliveryPreference = requestedDelivery;
      state.estimateDeliverySelectedAt = Date.now();
      pushInternalCallContext(
        state,
        previous
          ? `The caller's current estimate delivery choice is now ${estimateDeliveryLabel(requestedDelivery)}. Replace the previous ${estimateDeliveryLabel(previous)} choice. Do not ask again.`
          : `Estimate delivery preference is now ${estimateDeliveryLabel(requestedDelivery)}. Do not ask for the delivery method again unless the caller changes it.`
      );
    }

    const delivery = requestedDelivery || state.estimateDeliveryPreference;
    if (!delivery) {
      acknowledgeFunction(state, callId, {
        success: false,
        code: 'estimate_delivery_missing',
        message: 'No estimate delivery channel has been selected.',
      });
      requestModelResponse(
        state,
        'Ask one concise question for the estimate delivery preference: text, email, or both. After the caller answers, use that current choice.',
        'estimate_delivery_required',
        true
      );
      return;
    }

    const wantsSms = delivery === 'sms' || delivery === 'both';
    const wantsEmail = delivery === 'email' || delivery === 'both';
    if ((wantsSms && state.rules?.sms_enabled === false) || (wantsEmail && state.rules?.email_enabled === false)) {
      acknowledgeFunction(state, callId, {
        success: false,
        code: 'estimate_delivery_capability_unavailable',
        message: 'The selected estimate delivery channel is not available for this tenant.',
      });
      requestModelResponse(state, 'Briefly explain that the selected delivery method is unavailable and offer only the enabled delivery methods.', 'estimate_delivery_unavailable', true);
      return;
    }

    toolArgs.delivery = delivery;

    // Repeated delivery of unchanged quote details is a RESEND, not a new quote.
    // Relay supplies the authoritative estimate id to the signed NBB Voice adapter;
    // NBB then reuses its existing estimate-delivery system without creating a
    // second estimate row. If the customer name or quoted service/quantity details changed, the signature
    // changes and NBB creates a fresh estimate instead.
    const signature = estimatePayloadSignature(toolArgs);
    if (state.lastEstimateId && state.lastEstimateSignature && signature === state.lastEstimateSignature) {
      toolArgs.existing_estimate_id = state.lastEstimateId;
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

  if (name === 'nbb_request_callback') {
    if (!state.rules?.callback_enabled) {
      acknowledgeFunction(state, callId, { success: false, code: 'callback_disabled', message: 'Callback collection is not enabled for this tenant.' });
      requestModelResponse(state, 'Briefly explain that a callback request cannot be submitted right now. Do not claim it was sent.', 'callback_disabled', true);
      return;
    }
    toolArgs = mergeKnownCustomer(state, toolArgs);
    if (!toolArgs.customer || typeof toolArgs.customer !== 'object') toolArgs.customer = {};
    if (!toolArgs.customer.phone && state.callerPhone) toolArgs.customer.phone = state.callerPhone;
    toolArgs.conversation_id = voiceConversationId(state);
    toolArgs.conversation = state.conversationMessages.slice(-24);
    if (!toolArgs.request_text) toolArgs.request_text = 'Caller requested a callback from the business.';
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

  // Voice-only integrity metadata. This is attached by Relay, not supplied by
  // the model, and lets tenant NBB verify that a selected configured variant was
  // actually grounded in the caller-visible conversation before progression.
  if (['nbb_get_quote', 'nbb_get_availability', 'nbb_prepare_booking', 'nbb_send_estimate'].includes(name)) {
    toolArgs.voice_conversation = state.conversationMessages.slice(-40).map((row) => ({
      role: String(row?.role || ''),
      content: String(row?.content || '').slice(0, 2000),
    }));
  }

  const startedAt = Date.now();
  reportVoiceEvent(state, {
    kind: 'diagnostic',
    event: 'voice_tool_start',
    tool_name: name,
    tool_phase: 'start',
  });

  const progressDelay = Math.max(4, Math.min(20, Number(state.rules?.dead_air_seconds || 7))) * 1000;
  const speakToolProgress = () => {
    if (state.ended) return;
    if (state.callerSpeechActive) {
      const deferred = setTimeout(speakToolProgress, 400);
      if (callId) state.toolProgressTimers.set(callId, deferred);
      return;
    }
    requestModelResponse(
      state,
      'Say one short sentence that you are still checking that information. Do not ask a question, do not claim a result, and do not mention technical details.',
      'tool_progress',
      false
    );
  };
  const progressTimer = setTimeout(speakToolProgress, progressDelay);
  if (callId) state.toolProgressTimers.set(callId, progressTimer);

  try {
    const response = await proxyPost('/tool', {
      session_id: state.sessionId,
      tool_name: name,
      arguments: toolArgs,
    });
    clearTimeout(progressTimer);
    if (callId) state.toolProgressTimers.delete(callId);

    let result = response?.result ?? {};
    if (name === 'nbb_get_availability') result = sortVoiceAvailabilityResult(result);
    const durationMs = Date.now() - startedAt;
    applyVoicePresentationFromResult(state, result);
    if (state.workflowIntent === 'booking' && ['nbb_get_quote', 'nbb_send_estimate'].includes(name)) {
      state.quoteSideRequestAt = 0;
      pushInternalCallContext(state, "The caller's pricing/estimate side request was handled. Their active goal remains BOOKING; resume the booking flow unless they explicitly change goals.");
    }

    if (name === 'nbb_lookup_estimate' && result?.found && result?.estimate_id) {
      const loadedCustomer = result?.customer && typeof result.customer === 'object' ? result.customer : {};
      const loadedServices = Array.isArray(result?.services) ? result.services : [];
      state.activeEstimateId = String(result.estimate_id || '').trim();
      state.activeEstimateServiceSignature = serviceRowsSignature({ services: loadedServices });
      state.activeEstimateServices = loadedServices
        .map((row) => ({
          service_uuid: String(row?.service_uuid || '').trim(),
          quantity: Number(row?.quantity || 0),
        }))
        .filter((row) => row.service_uuid && Number.isFinite(row.quantity) && row.quantity > 0);
      state.serviceCollectionSignature = state.activeEstimateServiceSignature;
      state.serviceCollectionClosed = Boolean(state.activeEstimateServiceSignature);
      state.awaitingAdditionalServiceDecision = false;
      if (loadedCustomer.name) state.customerName = String(loadedCustomer.name);
      if (loadedCustomer.email) state.customerEmail = normalizedEmail(loadedCustomer.email);
      if (loadedCustomer.address) state.customerAddress = String(loadedCustomer.address);
      // Do not make the saved estimate phone the booking phone automatically.
      // Existing booking-phone choice/confirmation rules remain authoritative.
      const loadedSignature = estimatePayloadSignature({ customer: loadedCustomer, services: loadedServices });
      if (state.activeEstimateId && loadedSignature) {
        state.lastEstimateId = state.activeEstimateId;
        state.lastEstimateSignature = loadedSignature;
      }
      pushInternalCallContext(
        state,
        `Existing estimate #${state.activeEstimateId} is loaded read-only. Its saved service set is complete for scheduling unless the caller explicitly changes it. Reuse the saved service address and services; do not ask the caller to identify those services again. The saved phone is NOT automatically the booking phone.`
      );
      pushInternalCallContext(state, customerContinuityInstruction(state));
    }

    if (['nbb_prepare_booking', 'nbb_send_estimate', 'nbb_request_callback'].includes(name) && toolArgs?.customer && typeof toolArgs.customer === 'object') {
      state.customerName = String(toolArgs.customer.name || state.customerName || '');
      state.customerPhone = normalizedPhone(toolArgs.customer.phone || state.customerPhone || '');
      state.customerEmail = normalizedEmail(toolArgs.customer.email || state.customerEmail || '');
      pushInternalCallContext(state, customerContinuityInstruction(state));
    }

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
      state.bookingWrapupRequired = true;
      state.bookingWrapupRetryCount = 0;
    }
    if (name === 'nbb_request_callback' && result?.sent) {
      state.callbackWrapupRequired = true;
      state.callbackWrapupRetryCount = 0;
    }
    if (name === 'nbb_send_sms') state.smsRequestAt = 0;
    if (name === 'nbb_send_email') state.emailRequestAt = 0;
    if (name === 'nbb_send_estimate') {
      const delivery = normalizeEstimateDelivery(result?.delivery || toolArgs?.delivery || state.estimateDeliveryPreference || '');
      const estimateId = String(result?.estimate_id || toolArgs?.existing_estimate_id || '').trim();
      if (delivery) {
        state.estimateDeliveryPreference = delivery;
        state.estimateDeliverySelectedAt = Date.now();
        state.estimateSentAt = Date.now();
        state.estimateSentDelivery = delivery;
      }
      if (estimateId) {
        state.lastEstimateId = estimateId;
        state.lastEstimateSignature = estimatePayloadSignature(toolArgs);
      }
      if (delivery === 'sms' || delivery === 'both') {
        state.smsRequestAt = 0;
        if (state.customerPhone) state.confirmedPhone = state.customerPhone;
      }
      if (delivery === 'email' || delivery === 'both') {
        state.emailRequestAt = 0;
        if (state.customerEmail) state.confirmedEmail = state.customerEmail;
      }
      pushInternalCallContext(
        state,
        `Estimate ${estimateId ? '#' + estimateId + ' ' : ''}delivery completed using ${estimateDeliveryLabel(delivery)}. If the caller asks to resend or changes only the delivery channel, call nbb_send_estimate again with the SAME quote details and the caller's CURRENT channel; Relay will resend this same estimate instead of creating a duplicate.`
      );
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
      ? 'Do not mention pricing settings or say pricing is disabled. Continue gathering the quote details and offer to send the real estimate by text, email, or both using nbb_send_estimate.'
      : (name === 'nbb_send_estimate'
        ? 'The real Nearby Booker estimate was NOT confirmed as sent. Do not claim it was sent. Briefly explain the specific customer-facing problem from the tool result, then ask for the one missing or corrected item needed to create and send the estimate. Never mention internal tools, review tokens, configuration, or pricing visibility.'
        : (name === 'nbb_request_callback'
          ? 'The callback request was NOT confirmed as sent. Do not claim someone will call back. Briefly explain that the request could not be submitted and ask only for any corrected callback detail the tool says is needed.'
          : 'The Nearby Booker tool failed. Briefly explain that the requested check or action could not be completed, do not invent a result, and continue by asking for the one missing or corrected piece of information needed next.'));
    requestModelResponse(
      state,
      failureInstructions,
      'after_tool',
      true
    );
  }
}

async function handleFunctionCall(state, name, callId, args = {}) {
  if (state.messageMode) {
    acknowledgeFunction(state, callId, { success: false, code: 'voice_message_mode_active', message: 'Caller entered message mode.' });
    return;
  }
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
  if (name === 'voice_set_booking_phone') {
    handleBookingPhonePreferenceTool(state, callId, args);
    return;
  }
  if (name === 'voice_set_booking_notes') {
    handleBookingNotesTool(state, callId, args);
    return;
  }
  if (name === 'report_priority_issue') {
    if (state.rules?.emergency_enabled && state.rules?.emergency_action === 'transfer') {
      acknowledgeFunction(state, callId, { success: true, action: 'transferring' });
      await executeTransfer(state, 'emergency', 'model_tool');
    } else {
      acknowledgeFunction(state, callId, { success: true, action: 'continue_and_collect_callback' });
      if (state.rules?.callback_enabled) {
        requestModelResponse(state, 'Collect a brief callback reason and customer name if needed, reuse the verified caller number unless they explicitly provide a different confirmed number, then call nbb_request_callback. Do not claim the callback was submitted until that tool returns success.', 'priority_continue', true);
      } else {
        requestModelResponse(state, 'Briefly explain that a callback request cannot be submitted right now and continue helping with any safe question you can answer.', 'priority_continue', true);
      }
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
    state.responseHadAudio = false;
    if (state.messageMode && !state.messageResponsePhase) {
      safeSend(state.openaiWs, { type: 'response.cancel' });
    }
    return;
  }

  // GA event is response.output_audio.delta. Accept the older alias defensively during API transitions.
  if ((type === 'response.output_audio.delta' || type === 'response.audio.delta') && event.delta) {
    if (state.messageMode && !state.messageResponsePhase) {
      safeSend(state.openaiWs, { type: 'response.cancel' });
      return;
    }
    state.lastAiAudioAt = Date.now();
    state.aiAudioActive = true;
    state.aiPlaybackPending = true;
    state.responseHadAudio = true;
    markSpokenActivity(state, true);
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
    if (state.messageMode) {
      state.callerSpeechActive = false;
      state.messageTranscriptPending = false;
      markSpokenActivity(state);
      if (transcript) {
        state.messageParts.push(transcript);
        if (state.messageParts.length > 30) state.messageParts = state.messageParts.slice(-30);
        reportVoiceEvent(state, {
          kind: 'message',
          role: 'user',
          content: transcript,
          event: 'voice_message_caller_text',
        });
        armVoiceMessageSilenceTimer(state);
      }
      if (state.responseActive && !state.messageResponsePhase) {
        safeSend(state.openaiWs, { type: 'response.cancel' });
        safeSend(state.twilioWs, { event: 'clear', streamSid: state.streamSid });
      }
      return;
    }
    if (transcript) {
      state.callerSpeechActive = false;
      markSpokenActivity(state);
      clearContinuationWatchdog(state);
      state.continuationRetryCount = 0;
      if (String(state.rules?.interruption_mode || '') === 'auto') state.autoFalseStarts = Math.max(0, state.autoFalseStarts - 1);
      const normalized = normalizedText(transcript);
      const asksSms = asksForTextMessage(transcript);
      const asksEmail = asksForEmailMessage(transcript);
      if (asksSms || normalized === 'both') state.smsRequestAt = Date.now();
      if (asksEmail || normalized === 'both') state.emailRequestAt = Date.now();

      // Transcript parsing may establish the FIRST clear delivery choice, but it
      // never overrides an existing choice. Later changes are committed from the
      // normalized nbb_send_estimate tool argument, which is estimate-specific and
      // avoids brittle phrase rules such as requiring "switch" or "instead".
      const explicitDelivery = normalized === 'both' ? 'both' : explicitEstimateDeliveryChoice(transcript);
      if (!state.estimateDeliveryPreference && explicitDelivery) {
        state.estimateDeliveryPreference = explicitDelivery;
        state.estimateDeliverySelectedAt = Date.now();
      }

      state.conversationMessages.push({ role: 'user', content: transcript });
      if (state.conversationMessages.length > 40) state.conversationMessages = state.conversationMessages.slice(-40);
      reportVoiceEvent(state, {
        kind: 'message',
        role: 'user',
        content: transcript,
        event: 'voice_customer_message',
      });

      updateWorkflowIntentFromTranscript(state, transcript);

      if (state.rules?.quotes_enabled === false && isSpokenPricingRequest(transcript)) {
        pushInternalCallContext(state, 'The caller asked to hear the actual price/total while Phone spoken pricing is off. Do not calculate, infer, reconstruct, or speak any service price or total. Estimate delivery remains allowed.');
        queueDeterministicContinuation(
          state,
          pricingRedirectInstruction(state),
          'spoken_pricing_blocked'
        );
        return;
      }
    }

    if (state.awaitingAdditionalServiceDecision) {
      state.awaitingAdditionalServiceDecision = false;
      if (isNoMoreServicesReply(transcript)) {
        state.serviceCollectionClosed = true;
        const nextInstruction = postServiceContinuationInstruction(state);
        pushInternalCallContext(state, nextInstruction);
        queueDeterministicContinuation(
          state,
          nextInstruction,
          'service_collection_closed_continue'
        );
        return;
      }
      // Any other answer means service collection remains open. The model can
      // interpret a direct service name/quantity from this same caller turn, or
      // ask which additional service they want when the answer was only "yes".
      state.serviceCollectionClosed = false;
      pushInternalCallContext(state, 'The caller did not close service collection and may be adding another service. Handle this caller turn as additional-service intent. Do not move to the service address until the added service is resolved, then ask whether they want anything else.');
    }

    let consumedAsContactConfirmation = false;
    if (state.pendingContactConfirmation) {
      const pending = state.pendingContactConfirmation;
      if (isAffirmativeBookingConfirmation(transcript)) {
        consumedAsContactConfirmation = true;
        if (pending.type === 'email') {
          state.customerEmail = normalizedEmail(pending.value);
          state.confirmedEmail = state.customerEmail;
          invalidatePreparedBookingForContactChange(state);
        } else if (pending.type === 'phone') {
          state.customerPhone = normalizedPhone(pending.value);
          state.confirmedPhone = state.customerPhone;
          if (pending.context === 'booking_phone') {
            state.bookingPhoneChoiceResolved = true;
            state.bookingPhoneSource = state.customerPhone === state.callerPhone ? 'caller' : 'alternate';
            state.awaitingBookingPhoneChoice = false;
          }
          invalidatePreparedBookingForContactChange(state);
        }
        state.pendingContactConfirmation = null;
        pushInternalCallContext(state, `The caller explicitly confirmed the ${pending.type} ${pending.value}. This value is now locked for the call. Continue the pending estimate/booking flow using that exact value without asking again, even if a later model retry proposes a different spelling or formatting.`);
        queueDeterministicContinuation(
          state,
          `The caller just confirmed the ${pending.type}. It is locked. Do NOT ask them to confirm it again. Continue with the next single missing item in the current flow; if all required details are present, proceed to the appropriate Nearby Booker tool.`,
          `${pending.type}_confirmed_continue`
        );
      } else if (isNegativeBookingConfirmation(transcript)) {
        consumedAsContactConfirmation = true;
        state.pendingContactConfirmation = null;
        if (pending.type === 'email') { state.customerEmail = ''; state.confirmedEmail = ''; }
        if (pending.type === 'phone') {
          state.customerPhone = '';
          state.confirmedPhone = '';
          if (pending.context === 'booking_phone') {
            state.bookingPhoneChoiceResolved = false;
            state.bookingPhoneSource = 'alternate';
            state.awaitingBookingPhoneChoice = false;
          }
        }
        invalidatePreparedBookingForContactChange(state);
        pushInternalCallContext(state, `The caller rejected the previously captured ${pending.type}. Ask only for the corrected ${pending.type} next; do not reuse the rejected value.`);
        queueDeterministicContinuation(
          state,
          pending.type === 'phone'
            ? 'Ask exactly one question for the corrected booking phone number, then stop and wait.'
            : 'Ask exactly one question for the corrected email address, then stop and wait.',
          `${pending.type}_rejected_correct`
        );
      }
    }

    // A yes/no used to confirm contact data must NEVER also count as the final
    // booking confirmation. Those are separate user decisions.
    if (!consumedAsContactConfirmation && state.awaitingBookingConfirmation) {
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
    if (state.messageMode) {
      if (transcript && state.messageResponsePhase) {
        reportVoiceEvent(state, {
          kind: 'message',
          role: 'assistant',
          content: transcript,
          event: 'voice_agent_message',
        });
      }
      return;
    }
    if (transcript) {
      state.conversationMessages.push({ role: 'assistant', content: transcript });
      if (state.conversationMessages.length > 40) state.conversationMessages = state.conversationMessages.slice(-40);
      reportVoiceEvent(state, {
        kind: 'message',
        role: 'assistant',
        content: transcript,
        event: 'voice_agent_message',
      });
      if (!state.serviceCollectionClosed && isAdditionalServiceQuestion(transcript)) {
        state.awaitingAdditionalServiceDecision = true;
      }
      if (state.bookingWrapupRequired && bookingWrapupSatisfied(transcript)) {
        state.bookingWrapupRequired = false;
      }
      if (state.callbackWrapupRequired) {
        const t = normalizedText(transcript);
        const closed = (t.includes('callback') || t.includes('call you back') || t.includes('request')) &&
          (t.includes('thank') || t.includes('goodbye') || t.includes('good bye') || t.includes('have a great'));
        if (closed) state.callbackWrapupRequired = false;
      }
    }
    return;
  }

  if (type === 'input_audio_buffer.speech_started') {
    if (state.messageMode) {
      state.callerSpeechActive = true;
      state.messageTranscriptPending = true;
      clearVoiceMessageSilenceTimer(state);
      return;
    }
    // Startup caller audio is deliberately suppressed until greeting audio starts,
    // so a stale/early VAD event must never clear the deterministic greeting.
    if (state.startupAudioGate) return;

    // Pause the silence cutoff while a possible caller turn is being evaluated.
    // A false/noisy VAD start does NOT permanently cancel dead-air recovery.
    clearSilenceHangupTimer(state);

    if (automaticInterruptionEnabled(state)) {
      // Auto mode does NOT immediately clear/cancel on the raw VAD event. It
      // waits a short adaptive interval and compares the caller signal with the
      // per-call ambient floor. This is the key protection for cars/outdoors.
      startAutomaticBargeCandidate(state);
      return;
    }

    // Manual modes assume the VAD start is caller speech, but continuation
    // recovery stays armed until actual transcription completes.
    state.callerSpeechActive = true;
    markSpokenActivity(state, true);
    if (state.rules?.barge_in_enabled) safeSend(state.twilioWs, { event: 'clear', streamSid: state.streamSid });
    return;
  }

  if (type === 'input_audio_buffer.speech_stopped') {
    if (state.messageMode) {
      state.callerSpeechActive = false;
      return;
    }
    if (automaticInterruptionEnabled(state)) {
      stopAutomaticBargeCandidate(state);
    } else {
      state.callerSpeechActive = false;
      markSpokenActivity(state);
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

  if (type === 'response.done' || type === 'response.cancelled' || type === 'response.failed') {
    state.responseActive = false;
    state.responseRequestPending = false;
    state.aiAudioActive = false;
    if (state.responseHadAudio) {
      // OpenAI can finish generating substantially before Twilio finishes playing
      // buffered outbound audio. A Twilio mark tells us when the caller has actually
      // heard everything queued before this point. Until that mark returns, silence
      // timeout MUST remain paused.
      if (!sendTwilioPlaybackMark(state)) {
        state.aiPlaybackPending = false;
        markSpokenActivity(state);
      }
    } else {
      state.aiPlaybackPending = false;
      armSilenceHangupFromLastActivity(state);
    }
    state.responseHadAudio = false;
    if (state.messageMode) {
      const phase = state.messageResponsePhase;
      state.messageResponsePhase = '';
      if (state.messagePromptPending) tryStartVoiceMessagePrompt(state);
      if (phase === 'message_entry_prompt') {
        armVoiceMessageSilenceTimer(state);
      }
      // Normal final acknowledgements hang up after Twilio confirms playback. If
      // Realtime produced no playable audio (or the playback mark could not be
      // queued), there is nothing to wait for, so end the message call cleanly.
      if (phase === 'message_final_ack' && !state.aiPlaybackPending) {
        state.messageHangupAfterPlayback = false;
        setTimeout(() => endVoiceMessageCall(state).catch(() => {}), 0);
      }
      // No normal booking/tool continuation is allowed in message mode.
      return;
    }
    if (type === 'response.failed') {
      reportVoiceEvent(state, {
        kind: 'diagnostic',
        event: 'voice_realtime_response_failed',
        realtime_event: type,
        diagnostic_message: String(event?.response?.status_details?.error?.code || event?.response?.status_details?.error?.message || 'Realtime response failed.'),
      });
    }
    if (state.queuedResponseInstructions || state.queuedResponseReason) {
      flushQueuedResponse(state);
      return;
    }
    if (type === 'response.done' && state.bookingWrapupRequired && state.bookingWrapupRetryCount < 1) {
      state.bookingWrapupRetryCount += 1;
      setTimeout(() => requestModelResponse(
        state,
        `The booking already succeeded. The previous response did not include a complete closing. Say one brief final wrap-up now: repeat the appointment status/window if useful, explicitly say "You're all set", explain that approval/confirmation will follow if applicable, thank the caller for calling ${state.businessName || 'the business'}, and say goodbye. Do not ask another question.`,
        'booking_wrapup_retry',
        true
      ), 75);
      return;
    }
    if (type === 'response.done' && state.callbackWrapupRequired && state.callbackWrapupRetryCount < 1) {
      state.callbackWrapupRetryCount += 1;
      setTimeout(() => requestModelResponse(
        state,
        `The callback request already succeeded. Say one brief final closing now: tell the caller the callback request was sent to ${state.businessName || 'the business'}, thank them for calling, and say goodbye. Do not ask another question and do not promise a callback time.`,
        'callback_wrapup_retry',
        true
      ), 75);
      return;
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
  clearAutomaticBargeTimer(state);
  clearSilenceHangupTimer(state);
  clearVoiceMessageSilenceTimer(state);
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
    res.end(JSON.stringify({ ok: true, service: 'nearby-booker-voice-relay', version: '0.2.30' }));
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
    if (msg.event === 'mark') {
      handleTwilioPlaybackMark(state, msg.mark?.name);
      return;
    }
    if (msg.event === 'dtmf') {
      const digit = String(msg.dtmf?.digit || '');
      if (digit === '1' && state.rules?.leave_message_enabled) {
        await enterVoiceMessageMode(state);
      } else if (digit === '#' && state.messageMode) {
        await flushVoiceMessageTranscription(state);
        await finalizeVoiceMessage(state, 'pound', true);
      }
      return;
    }
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
      observeInboundAudio(state, msg.media.payload);
      if (state.openaiWs?.readyState === WebSocket.OPEN) safeSend(state.openaiWs, { type: 'input_audio_buffer.append', audio: msg.media.payload });
      else if (state.pendingAudio.length < 500) state.pendingAudio.push(msg.media.payload);
      return;
    }
    if (msg.event === 'stop') {
      if (state.messageMode && !state.messageFinalized) {
        await flushVoiceMessageTranscription(state);
        await finalizeVoiceMessage(state, 'hangup', false);
      }
      await finishSession(state, state.transferInProgress ? 'transferred' : (state.messageMode ? 'voice_message_hangup' : 'twilio_stop'));
    }
  });

  twilioWs.on('close', async () => {
    if (state.messageMode && !state.messageFinalized) {
      try {
        await flushVoiceMessageTranscription(state);
        await finalizeVoiceMessage(state, 'hangup', false);
      } catch {}
    }
    await finishSession(state, state.transferInProgress ? 'transferred' : (state.messageMode ? 'voice_message_hangup' : 'socket_closed'));
  });
  twilioWs.on('error', (error) => {
    log('warn', 'Twilio Media Stream socket error', { callSid: state.callSid, error: error?.message || 'socket_error' });
  });
});

server.listen(PORT, '0.0.0.0', () => {
  log('info', 'Nearby Booker Voice Relay started', { port: PORT, websocketPath: '/twilio/media' });
});
