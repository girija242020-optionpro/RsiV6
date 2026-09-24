# Dhan TOTP Realtime Backend v6

This backend automatically generates the Dhan access token from `DHAN_CLIENT_ID + DHAN_PIN + DHAN_TOTP_SECRET`. No daily manual `DHAN_ACCESS_TOKEN` is required.

## Render
Root Directory: **blank** (repo root)
Build Command: `npm install`
Start Command: `npm start`

## Environment Variables
Copy `.env.example` into Render Environment Variables. Do NOT commit `.env`.

Required:
- `DHAN_CLIENT_ID`
- `DHAN_PIN`
- `DHAN_TOTP_SECRET`
- `VAPID_PUBLIC_KEY`
- `VAPID_PRIVATE_KEY`
- `VAPID_SUBJECT`

Optional:
- `PORT=10000`
- `CORS_ORIGIN=*`
- `FEED_REQUEST_CODE=8`
- `RECONNECT_MS=3000`
- `TOKEN_REFRESH_BUFFER_MS=300000`

## Health
- `/api/health`
- `/api/status`
- `/api/state`
- `/api/ticks`
- `/api/history`
- `/api/instruments`
- `/api/config`
- `/ws`

The backend is a data provider. Trading/entry logic should remain in the PWA.
