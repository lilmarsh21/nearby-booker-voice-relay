# Nearby Booker Voice Relay v0.1.2 — Phase 1 Current-Base Port

This is the **standalone, isolated realtime media service** for Nearby Booker Phone AI.

It does not contain Nearby Booker scheduling logic and it does not store Twilio or OpenAI permanent credentials. Twilio calls enter through the GeoVee Proxy Voice endpoint first; the Proxy validates Twilio, identifies the tenant, and issues a one-time session token. The Relay then requests a short-lived OpenAI Realtime client secret from the Proxy.

## Phase 1 scope

- Real inbound phone call via Twilio bidirectional Media Streams.
- Direct PCMU / G.711 μ-law audio bridge between Twilio and OpenAI Realtime; no transcoding.
- Natural speech-to-speech conversation.
- Caller interruption / barge-in, including Twilio playback buffer clear.
- Input transcription for rule detection.
- Deterministic "talk to a person" rule.
- Tenant-defined priority/emergency phrase rule.
- Controlled human transfer through the Proxy/Twilio Call API.
- Call duration/status metadata reporting.
- **No booking, reschedule, cancel, pricing, availability, Google Calendar, Radius/Offers, or Team Calendar write tools.**

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
