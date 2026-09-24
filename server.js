const express = require('express');
const cors = require('cors');
const axios = require('axios');
const WebSocket = require('ws');
const { authenticator } = require('otplib');

const app = express();
app.use(cors({ origin: process.env.CORS_ORIGIN || '*' }));
app.use(express.json({ limit: '1mb' }));

const PORT = Number(process.env.PORT || 10000);
const CLIENT_ID = String(process.env.DHAN_CLIENT_ID || '').trim();
const PIN = String(process.env.DHAN_PIN || '').trim();
const TOTP_SECRET = String(process.env.DHAN_TOTP_SECRET || '').replace(/\s+/g, '').trim();

const FEED_REQUEST_CODE = Number(process.env.FEED_REQUEST_CODE || 21); // 21 = FULL packet
const RECONNECT_MS = Number(process.env.RECONNECT_MS || 3000);
const TOKEN_REFRESH_BUFFER_MS = Number(process.env.TOKEN_REFRESH_BUFFER_MS || 300000);
const FEED_STALE_MS = Number(process.env.FEED_STALE_MS || 15000);
const SUBSCRIBE_RETRY_MS = Number(process.env.SUBSCRIBE_RETRY_MS || 5000);
const INSTRUMENT_MASTER_URL = process.env.INSTRUMENT_MASTER_URL ||
  'https://images.dhan.co/api-data/api-scrip-master.csv';

let accessToken = '';
let tokenExpiryMs = 0;
let tokenTimer = null;
let feedWs = null;
let reconnectTimer = null;
let subscribeRetryTimer = null;

let feedState = 'NO_TOKEN';
let feedLastMessageAt = null;
let packetCount = 0;
let tickCount = 0;
let subscribedInstruments = 0;
let instruments = 0;
let lastTokenError = '';
let lastFeedError = '';
let lastTokenAt = null;
let lastSubscribeAt = null;
let lastDisconnectCode = null;

let instrumentMaster = [];
const ticks = new Map();
const clients = new Set();
const history = [];
const MAX_HISTORY = 5000;

const DEFAULT_INSTRUMENTS = [
  { ExchangeSegment: 'IDX_I', SecurityId: '13', name: 'NIFTY' },
  { ExchangeSegment: 'IDX_I', SecurityId: '25', name: 'BANKNIFTY' },
  { ExchangeSegment: 'IDX_I', SecurityId: '27', name: 'FINNIFTY' },
  { ExchangeSegment: 'IDX_I', SecurityId: '442', name: 'MIDCPNIFTY' },
  { ExchangeSegment: 'IDX_I', SecurityId: '51', name: 'SENSEX' }
];

function nowIso() {
  return new Date().toISOString();
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function generateAccessToken() {
  if (!CLIENT_ID || !PIN || !TOTP_SECRET) {
    throw new Error('Missing DHAN_CLIENT_ID, DHAN_PIN or DHAN_TOTP_SECRET');
  }

  authenticator.options = { window: 1 };
  const totp = authenticator.generate(TOTP_SECRET);

  const response = await axios.post(
    'https://auth.dhan.co/app/generateAccessToken',
    null,
    {
      params: { dhanClientId: CLIENT_ID, pin: PIN, totp },
      timeout: 15000,
      headers: { Accept: 'application/json' }
    }
  );

  const data = response.data || {};

  if (!data.accessToken) {
    throw new Error(
      'Dhan token response did not contain accessToken: ' + JSON.stringify(data)
    );
  }

  accessToken = String(data.accessToken);
  tokenExpiryMs = data.expiryTime
    ? Date.parse(data.expiryTime)
    : Date.now() + 23 * 60 * 60 * 1000;

  lastTokenAt = nowIso();
  lastTokenError = '';
  scheduleTokenRefresh();

  return data;
}

function scheduleTokenRefresh() {
  if (tokenTimer) clearTimeout(tokenTimer);

  const delay = Math.max(
    60000,
    tokenExpiryMs - Date.now() - TOKEN_REFRESH_BUFFER_MS
  );

  tokenTimer = setTimeout(async () => {
    try {
      await refreshTokenAndFeed();
    } catch (e) {
      lastTokenError = e.message;
      feedState = 'TOKEN_ERROR';

      setTimeout(() => {
        refreshTokenAndFeed().catch(err => {
          lastTokenError = err.message;
        });
      }, 60000);
    }
  }, delay);
}

async function refreshTokenAndFeed() {
  const oldToken = accessToken;
  const data = await generateAccessToken();

  if (oldToken !== accessToken) {
    closeFeed();
    await sleep(500);
    connectFeed();
  }

  return data;
}

function closeFeed() {
  clearTimeout(reconnectTimer);
  clearTimeout(subscribeRetryTimer);

  const ws = feedWs;
  feedWs = null;

  if (ws) {
    try {
      ws.removeAllListeners();
      ws.close();
    } catch (_) {}
  }

  subscribedInstruments = 0;
}

function parseCsvLine(line) {
  const out = [];
  let cur = '';
  let quoted = false;

  for (let i = 0; i < line.length; i++) {
    const c = line[i];

    if (c === '"' && line[i + 1] === '"') {
      cur += '"';
      i++;
      continue;
    }

    if (c === '"') {
      quoted = !quoted;
      continue;
    }

    if (c === ',' && !quoted) {
      out.push(cur);
      cur = '';
    } else {
      cur += c;
    }
  }

  out.push(cur);
  return out;
}

async function loadInstrumentMaster() {
  try {
    const r = await axios.get(INSTRUMENT_MASTER_URL, {
      timeout: 30000,
      responseType: 'text'
    });

    const lines = r.data.split(/\r?\n/).filter(Boolean);
    if (!lines.length) throw new Error('Instrument master is empty');

    const headers = parseCsvLine(lines[0]).map(x => x.trim());
    const idx = Object.fromEntries(headers.map((h, i) => [h, i]));

    const get = (row, names) => {
      for (const name of names) {
        if (idx[name] !== undefined) return row[idx[name]];
      }
      return '';
    };

    const rows = [];

    for (let i = 1; i < lines.length; i++) {
      const row = parseCsvLine(lines[i]);
      const exchangeSegment = get(row, [
        'SEM_EXM_EXCH_ID',
        'exchange_segment',
        'EXCH_ID'
      ]);
      const securityId = get(row, [
        'SEM_SMST_SECURITY_ID',
        'security_id',
        'SECURITY_ID'
      ]);
      const symbol = get(row, [
        'SEM_CUSTOM_SYMBOL',
        'SEM_TRADING_SYMBOL',
        'trading_symbol',
        'SYMBOL'
      ]);

      if (securityId) {
        rows.push({
          exchangeSegment,
          securityId,
          symbol,
          instrumentType: get(row, [
            'SEM_INSTRUMENT_NAME',
            'instrument_type'
          ]),
          expiry: get(row, ['SEM_EXPIRY_DATE', 'expiry']),
          strike: get(row, ['SEM_STRIKE_PRICE', 'strike']),
          optionType: get(row, ['SEM_OPTION_TYPE', 'option_type'])
        });
      }
    }

    instrumentMaster = rows;
    instruments = rows.length;
    console.log(`Instrument master loaded: ${instruments}`);
  } catch (e) {
    instrumentMaster = [];
    instruments = 0;
    lastFeedError = 'Instrument master: ' + e.message;
    console.error(lastFeedError);
  }
}

function subscribeDefault() {
  if (!feedWs || feedWs.readyState !== WebSocket.OPEN) return false;

  try {
    for (let i = 0; i < DEFAULT_INSTRUMENTS.length; i += 100) {
      const part = DEFAULT_INSTRUMENTS.slice(i, i + 100).map(x => ({
        ExchangeSegment: x.ExchangeSegment,
        SecurityId: x.SecurityId
      }));

      // Dhan v2: 21 = Subscribe - Full Packet.
      // This provides LTP + volume + OI + 5-level depth.
      const request = {
        RequestCode: FEED_REQUEST_CODE,
        InstrumentCount: part.length,
        InstrumentList: part
      };

      feedWs.send(JSON.stringify(request));
    }

    subscribedInstruments = DEFAULT_INSTRUMENTS.length;
    lastSubscribeAt = nowIso();

    console.log(
      `Dhan subscription sent: code=${FEED_REQUEST_CODE}, instruments=${subscribedInstruments}`
    );

    // Dhan sends a previous-close packet after subscription.
    // If absolutely nothing arrives, resend once after a short delay.
    clearTimeout(subscribeRetryTimer);
    subscribeRetryTimer = setTimeout(() => {
      if (!feedLastMessageAt && feedWs && feedWs.readyState === WebSocket.OPEN) {
        console.log('No feed packet received yet; resending subscription.');
        subscribeDefault();
      }
    }, SUBSCRIBE_RETRY_MS);

    return true;
  } catch (e) {
    lastFeedError = 'Subscribe: ' + e.message;
    console.error(lastFeedError);
    return false;
  }
}

function connectFeed() {
  if (!accessToken || !CLIENT_ID) {
    feedState = 'NO_TOKEN';
    return;
  }

  if (
    feedWs &&
    (feedWs.readyState === WebSocket.OPEN ||
      feedWs.readyState === WebSocket.CONNECTING)
  ) {
    return;
  }

  feedState = 'CONNECTING';

  const url =
    `wss://api-feed.dhan.co?version=2&token=${encodeURIComponent(accessToken)}` +
    `&clientId=${encodeURIComponent(CLIENT_ID)}&authType=2`;

  const ws = new WebSocket(url);
  ws.binaryType = 'arraybuffer';
  feedWs = ws;

  ws.on('open', () => {
    feedState = 'CONNECTED';
    lastFeedError = '';
    lastDisconnectCode = null;
    console.log('Dhan WebSocket OPEN');
    subscribeDefault();
  });

  ws.on('message', data => {
    feedLastMessageAt = nowIso();
    packetCount++;

    const buf = Buffer.isBuffer(data) ? data : Buffer.from(data);
    decodePacket(buf);

    if (feedState === 'CONNECTED' || feedState === 'STALE') {
      feedState = 'LIVE';
    }
  });

  ws.on('error', e => {
    lastFeedError = e.message || String(e);
    feedState = 'ERROR';
    console.error('Dhan WebSocket error:', lastFeedError);
  });

  ws.on('close', () => {
    if (feedWs === ws) feedWs = null;

    if (feedState !== 'TOKEN_ERROR') {
      feedState = 'DISCONNECTED';
    }

    clearTimeout(reconnectTimer);
    reconnectTimer = setTimeout(() => {
      if (accessToken) connectFeed();
    }, RECONNECT_MS);
  });
}

function decodePacket(buf) {
  if (!buf || buf.length < 8) return;

  // Dhan v2 response header:
  // byte 0 = response code
  // bytes 1-2 = message length (int16 LE)
  // byte 3 = exchange segment
  // bytes 4-7 = security ID (int32 LE)
  const code = buf.readUInt8(0);
  const segment = buf.readUInt8(3);
  const securityId = buf.readInt32LE(4);

  if (code === 50) {
    const reason = buf.length >= 10 ? buf.readInt16LE(8) : null;
    lastDisconnectCode = reason;
    lastFeedError = `Dhan feed disconnect code: ${reason}`;
    console.error(lastFeedError);
    return;
  }

  let ltp = null;
  let ltt = null;
  let oi = null;
  let volume = null;
  let dayOpen = null;
  let dayHigh = null;
  let dayLow = null;

  try {
    // Ticker packet: code 2
    if (code === 2 && buf.length >= 16) {
      ltp = buf.readFloatLE(8);
      ltt = buf.readInt32LE(12);
    }

    // Quote packet: code 4
    else if (code === 4 && buf.length >= 51) {
      ltp = buf.readFloatLE(8);
      ltt = buf.readInt32LE(14);
      volume = buf.readInt32LE(22);
      dayOpen = buf.readFloatLE(34);
      dayHigh = buf.readFloatLE(42);
      dayLow = buf.readFloatLE(46);
    }

    // OI packet: code 5
    else if (code === 5 && buf.length >= 12) {
      oi = buf.readInt32LE(8);
    }

    // Previous close packet: code 6
    else if (code === 6 && buf.length >= 16) {
      ltp = buf.readFloatLE(8);
      oi = buf.readInt32LE(12);
    }

    // Full packet: code 8
    else if (code === 8 && buf.length >= 63) {
      ltp = buf.readFloatLE(8);
      ltt = buf.readInt32LE(14);
      volume = buf.readInt32LE(22);
      oi = buf.readInt32LE(34);
      dayOpen = buf.readFloatLE(46);
      dayHigh = buf.readFloatLE(54);
      dayLow = buf.readFloatLE(58);
    }
  } catch (e) {
    lastFeedError = `Packet decode: ${e.message}`;
    return;
  }

  const item = {
    securityId: String(securityId),
    segment,
    code,
    ltp,
    ltt,
    oi,
    volume,
    dayOpen,
    dayHigh,
    dayLow,
    ts: Date.now()
  };

  ticks.set(item.securityId, item);
  tickCount++;

  history.push(item);
  if (history.length > MAX_HISTORY) history.shift();

  const msg = JSON.stringify({ type: 'tick', data: item });

  for (const c of clients) {
    try {
      if (c.readyState === WebSocket.OPEN) c.send(msg);
    } catch (_) {}
  }
}

function status() {
  const lastMs = feedLastMessageAt
    ? Date.parse(feedLastMessageAt)
    : 0;

  const stale =
    !lastMs || Date.now() - lastMs > FEED_STALE_MS;

  return {
    success: true,
    version: '6.1-TOTP-REALTIME-FEEDFIX',
    feedState: stale && feedState === 'LIVE' ? 'STALE' : feedState,
    feedLastMessageAt,
    feedAgeMs: lastMs ? Date.now() - lastMs : null,
    feedStale: stale,
    ticks: tickCount,
    packets: packetCount,
    subscribedInstruments,
    instruments,
    connectedClients: clients.size,
    pushSubscribers: 0,
    vapidReady: Boolean(
      process.env.VAPID_PUBLIC_KEY && process.env.VAPID_PRIVATE_KEY
    ),
    tokenPresent: Boolean(accessToken),
    tokenExpiry: tokenExpiryMs
      ? new Date(tokenExpiryMs).toISOString()
      : null,
    lastTokenAt,
    lastSubscribeAt,
    lastDisconnectCode,
    lastTokenError,
    lastFeedError,
    feedRequestCode: FEED_REQUEST_CODE
  };
}

app.get('/', (req, res) =>
  res.type('html').send(
    `<h2>Dhan TOTP Realtime Backend v6.1</h2><pre>${JSON.stringify(
      status(),
      null,
      2
    )}</pre>`
  )
);

app.get('/api/health', (req, res) =>
  res.json({ ok: true, time: nowIso() })
);

app.get('/api/status', (req, res) => res.json(status()));

app.get('/api/state', (req, res) =>
  res.json({
    success: true,
    state: status(),
    ticks: [...ticks.values()]
  })
);

app.get('/api/ticks', (req, res) =>
  res.json({ success: true, data: [...ticks.values()] })
);

app.get('/api/history', (req, res) => {
  const limit = Math.min(
    Number(req.query.limit || 500),
    MAX_HISTORY
  );
  res.json({
    success: true,
    data: history.slice(-limit)
  });
});

app.get('/api/instruments', (req, res) => {
  const limit = Math.min(
    Number(req.query.limit || 100),
    instrumentMaster.length
  );
  res.json({
    success: true,
    count: instrumentMaster.length,
    data: instrumentMaster.slice(0, limit)
  });
});

app.get('/api/config', (req, res) =>
  res.json({
    success: true,
    vapidPublicKey: process.env.VAPID_PUBLIC_KEY || '',
    version: '6.1-TOTP-REALTIME-FEEDFIX'
  })
);

app.post('/api/subscribe', (req, res) => {
  const list = Array.isArray(req.body?.instruments)
    ? req.body.instruments
    : [];

  if (!list.length) {
    return res.status(400).json({
      success: false,
      error: 'instruments[] required'
    });
  }

  if (!feedWs || feedWs.readyState !== WebSocket.OPEN) {
    return res.status(503).json({
      success: false,
      error: 'feed not connected'
    });
  }

  try {
    for (let i = 0; i < list.length; i += 100) {
      const part = list.slice(i, i + 100).map(x => ({
        ExchangeSegment: String(
          x.ExchangeSegment || x.exchangeSegment
        ),
        SecurityId: String(x.SecurityId || x.securityId)
      }));

      feedWs.send(
        JSON.stringify({
          RequestCode: FEED_REQUEST_CODE,
          InstrumentCount: part.length,
          InstrumentList: part
        })
      );
    }

    subscribedInstruments += list.length;

    res.json({
      success: true,
      requestCode: FEED_REQUEST_CODE,
      subscribed: list.length
    });
  } catch (e) {
    res.status(500).json({
      success: false,
      error: e.message
    });
  }
});

const server = app.listen(PORT, async () => {
  console.log(
    `Dhan TOTP Realtime Backend v6.1 listening on :${PORT}`
  );

  await loadInstrumentMaster();

  try {
    await generateAccessToken();
    console.log(
      'Dhan TOTP access token generated automatically.'
    );
    connectFeed();
  } catch (e) {
    lastTokenError = e.message;
    feedState = 'TOKEN_ERROR';
    console.error(
      'Dhan TOTP token generation failed:',
      e.message
    );
  }
});

// PWA/browser WebSocket endpoint.
const wss = new WebSocket.Server({ noServer: true });

server.on('upgrade', (req, socket, head) => {
  if (req.url !== '/ws') {
    socket.destroy();
    return;
  }

  wss.handleUpgrade(req, socket, head, ws => {
    clients.add(ws);
    ws.send(
      JSON.stringify({
        type: 'status',
        data: status()
      })
    );

    ws.on('close', () => clients.delete(ws));
  });
});

process.on('SIGTERM', () => {
  clearTimeout(tokenTimer);
  clearTimeout(reconnectTimer);
  clearTimeout(subscribeRetryTimer);

  try {
    feedWs?.close();
  } catch (_) {}

  server.close(() => process.exit(0));
});
