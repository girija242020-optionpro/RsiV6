# Dhan TOTP Realtime Backend v6.1 — Feed Fix

This build fixes the live-feed subscription issue in v6.

## Important fix

The previous build used `RequestCode=8` for the SUBSCRIBE request.
Dhan v2 defines `8` as a **response code**, not a subscription request code.

This build uses:

- `FEED_REQUEST_CODE=21`
- Dhan v2 **Subscribe - Full Packet**
- LTP + Volume + OI + 5-level depth
- Automatic TOTP access-token generation
- Automatic token refresh before expiry
- WebSocket reconnect
- Subscription retry if no packet arrives
- Correct v2 binary packet offsets
- Dhan disconnect-code reporting

Dhan's official v2 documentation specifies JSON subscription requests and request code 21 for Full packets. See:
https://dhanhq.co/docs/v2/live-market-feed/
https://dhanhq.co/docs/v2/annexure/

## Render

Root Directory:
(blank)

Build Command:
npm install

Start Command:
npm start

## Required Render Environment Variables

DHAN_CLIENT_ID=YOUR_DHAN_CLIENT_ID
DHAN_PIN=YOUR_6_DIGIT_DHAN_PIN
DHAN_TOTP_SECRET=YOUR_DHAN_TOTP_SECRET

VAPID_PUBLIC_KEY=BIvFATwgw88bdaLNN2NAfU2oivxUnajCkwp3YNMfPtq9g0ikpJiVc4yTFSiDnudMqjdWLqt-lelZtF0dtfsjqGc
VAPID_PRIVATE_KEY=wmaISQOQg3wOHOA3m8a_yybms0LzGupqQMz56kMScoE
VAPID_SUBJECT=mailto:YOUR_EMAIL@gmail.com

## Recommended live-feed variables

PORT=10000
CORS_ORIGIN=*
FEED_REQUEST_CODE=21
RECONNECT_MS=3000
FEED_STALE_MS=15000
SUBSCRIBE_RETRY_MS=5000
TOKEN_REFRESH_BUFFER_MS=300000

Do NOT add DHAN_ACCESS_TOKEN. The backend generates it using TOTP.

## Test

Open:

/api/status

Expected during market/live feed:

feedState: LIVE
feedStale: false
feedLastMessageAt: recent timestamp
ticks: > 0
packets: > 0
subscribedInstruments: 5
tokenPresent: true
lastTokenError: ""
lastFeedError: ""

## PWA WebSocket

wss://YOUR-RENDER-DOMAIN.onrender.com/ws
