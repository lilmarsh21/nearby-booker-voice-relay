# Nearby Booker Voice Relay v0.2.42

This is the **standalone, isolated realtime media service** for Nearby Booker Phone AI.

It does not contain Nearby Booker scheduling logic and it does not store Twilio or OpenAI permanent credentials. Twilio calls enter through the GeoVee Proxy Voice endpoint first; the Proxy validates Twilio, identifies the tenant, and issues a one-time session token. The Relay then requests a short-lived OpenAI Realtime client secret from the Proxy.

## Historical notes and scope

- Real inbound phone call via Twilio bidirectional Media Streams.
- Direct PCMU / G.711 μ-law audio bridge between Twilio and OpenAI Realtime; no transcoding.
- Natural speech-to-speech conversation.
- Caller interruption / barge-in, including Twilio playback buffer clear.
- Input transcription for rule detection.
- Deterministic "talk to a person" rule.
- Tenant-defined priority/emergency phrase rule.
- Controlled human transfer through the Proxy/Twilio Call API.
- Call duration/status metadata reporting.
- Booking, rescheduling, availability and other tenant tools are mediated by the authenticated Proxy/NBB integrations; the relay does not contain scheduling or invoice storage.

## Requirements

- Node.js 18+
- Public HTTPS/WSS hosting that allows WebSocket upgrades on port 443
- `npm install`
- Environment variables from `.env.example`

## Environment

```bash
PORT=8080
GEOVEE_PROXY_VOICE_BASE_URL=https://sub.geovee.io/wp-json/geovee/v1/voice
GEOVEE_VOICE_RELAY_SECRET=<exact secret from GeoVee Proxy > Voice AI>
OPENAI_REALTIME_WS_BASE=wss://api.openai.com/v1/realtime
LOG_LEVEL=info
```

The permanent OpenAI API key stays in the GeoVee Proxy AI settings. The permanent Twilio Account SID/Auth Token stay in the GeoVee Proxy SMS/Twilio settings.

## Deployment order

1. Deploy this Node service and confirm `https://YOUR-RELAY/health` returns `ok: true`.
2. The Twilio WebSocket endpoint is `wss://YOUR-RELAY/twilio/media`.
3. In **GeoVee Proxy > Voice AI**, paste that WSS URL, copy the relay shared secret into this service's environment, and enable the Voice pilot.
4. In the tenant NBB site, open **AI Settings > Voice**, configure the tenant, and save. That syncs settings to the Proxy and should show the assigned Twilio number.
5. In Twilio, configure that assigned number's incoming **Voice webhook** to the URL shown in **GeoVee Proxy > Voice AI** (normally `https://sub.geovee.io/wp-json/geovee/v1/voice/twilio/incoming`) using HTTP POST.
6. Call the Twilio number. The AI should answer audibly.

For existing businesses, their public business number can forward voice calls to the assigned Twilio number. The human-transfer destination must be a different number to prevent a forwarding loop.

## Security boundary

- Twilio webhook signature is verified by the Proxy.
- Relay-to-Proxy calls are HMAC-SHA256 signed with the platform relay secret, a timestamp, and a one-time nonce. Replayed signed requests are rejected by the Proxy.
- Twilio/OpenAI long-lived credentials never enter tenant WordPress sites or this Relay package.
- Bootstrap tokens are one-time and short-lived.
- The Relay intentionally does not log audio, transcripts, phone numbers, or secrets.

## v0.1.1 security change

Historical note: v0.1.1 was originally paired with GeoVee Proxy v1.3.51. It replaced the rejected v1.3.50 static relay-secret request header with per-request HMAC signatures and nonces. v0.1.2 preserves that audited signed-request contract and is paired with the current-base Proxy Voice port v1.3.49.2.3.


## v0.1.2 current-base port

This package preserves the audited v0.1.1 relay behavior and HMAC request contract while pairing it with the current production-line Voice port: NBB v1.38.299.3.7 and GeoVee Proxy v1.3.49.2.3. No scheduling, booking, pricing, availability, or tenant business logic is added to the Relay.

## v0.2.40 targeted change (2026-10-17)

- Recognize an explicit request to **check appointment availability** as a booking goal even without the word "book" or "schedule".
- Preserve that goal and do not ask "estimate or availability" again after the customer chose availability.
- Do not add any booking tools or historical-invoice lookup paths in this release. Repeat-booking history still needs tenant-side and authenticated Proxy tool integration and customer verification.
- This protects Voice AI conversation intent **only**. The website chatbot uses separate NBB conversational logic and must be repaired/tested independently.
- No changes to Twilio signaling, transfer, approval, reminder, winback, calendar, existing-tool routing, or pricing policies.

## v0.2.41 pending-tool silence guard (2026-10-18)
The idle-call watchdog cannot terminate an active Nearby Booker tool request. It restarts the normal silence window only after the tool finishes. The original idle-call setting and all Twilio call routing remain unchanged.

## v0.2.42 targeted voice-wait and service-state corrections (2026-10-25)

- During long NBB tools, repeat short, truthful progress messages at a 9–12 second interval after the configured initial delay; avoid overlapping caller speech or active AI responses. Clear *all* tool progress timers on either success or failure, including speech-deferred timers.
- Close the additional-services checkpoint for an explicit caller completion response to a recently spoken additional-services question, even when that question was generated without the deterministic tool gate.
- After successfully validating an address, if the caller already closed service collection, tell the Realtime agent to proceed directly to availability instead of reopening the optional-service question.
- Does not alter Proxy/NBB live-calendar searches, slot eligibility, pricing, booking writes, or Twilio routing. A 48-second availability lookup still requires NBB-side performance profiling.
