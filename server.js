const express = require('express');
const cors = require('cors');
const axios = require('axios');
const WebSocket = require('ws');
const crypto = require('crypto');
const { authenticator } = require('otplib');

const app = express();
app.use(cors({ origin: process.env.CORS_ORIGIN || '*' }));
app.use(express.json({ limit: '1mb' }));

const PORT = Number(process.env.PORT || 10000);
const CLIENT_ID = String(process.env.DHAN_CLIENT_ID || '').trim();
const PIN = String(process.env.DHAN_PIN || '').trim();
const TOTP_SECRET = String(process.env.DHAN_TOTP_SECRET || '').replace(/\s+/g, '').trim();
const FEED_REQUEST_CODE = Number(process.env.FEED_REQUEST_CODE || 8);
const RECONNECT_MS = Number(process.env.RECONNECT_MS || 3000);
const TOKEN_REFRESH_BUFFER_MS = Number(process.env.TOKEN_REFRESH_BUFFER_MS || 300000);
const INSTRUMENT_MASTER_URL = process.env.INSTRUMENT_MASTER_URL || 'https://images.dhan.co/api-data/api-scrip-master.csv';

let accessToken = '';
let tokenExpiryMs = 0;
let tokenTimer = null;
let feedWs = null;
let reconnectTimer = null;
let feedState = 'NO_TOKEN';
let feedLastMessageAt = null;
let packetCount = 0;
let tickCount = 0;
let subscribedInstruments = 0;
let instruments = 0;
let lastTokenError = '';
let lastFeedError = '';
let lastTokenAt = null;
let instrumentMaster = [];
const ticks = new Map();
const clients = new Set();
const history = [];
const MAX_HISTORY = 5000;

function nowIso() { return new Date().toISOString(); }
function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }
function b64url(buf) { return Buffer.from(buf).toString('base64').replace(/\+/g,'-').replace(/\//g,'_').replace(/=+$/,''); }

async function generateAccessToken() {
  if (!CLIENT_ID || !PIN || !TOTP_SECRET) {
    throw new Error('Missing DHAN_CLIENT_ID, DHAN_PIN or DHAN_TOTP_SECRET');
  }
  authenticator.options = { window: 1 };
  const totp = authenticator.generate(TOTP_SECRET);
  const url = 'https://auth.dhan.co/app/generateAccessToken';
  const response = await axios.post(url, null, {
    params: { dhanClientId: CLIENT_ID, pin: PIN, totp },
    timeout: 15000,
    headers: { Accept: 'application/json' }
  });
  const data = response.data || {};
  if (!data.accessToken) {
    throw new Error('Dhan token response did not contain accessToken: ' + JSON.stringify(data));
  }
  accessToken = String(data.accessToken);
  tokenExpiryMs = data.expiryTime ? Date.parse(data.expiryTime) : (Date.now() + 23 * 60 * 60 * 1000);
  lastTokenAt = nowIso();
  lastTokenError = '';
  scheduleTokenRefresh();
  return data;
}

function scheduleTokenRefresh() {
  if (tokenTimer) clearTimeout(tokenTimer);
  const delay = Math.max(60000, tokenExpiryMs - Date.now() - TOKEN_REFRESH_BUFFER_MS);
  tokenTimer = setTimeout(async () => {
    try {
      await refreshTokenAndFeed();
    } catch (e) {
      lastTokenError = e.message;
      feedState = 'TOKEN_ERROR';
      setTimeout(() => refreshTokenAndFeed().catch(err => { lastTokenError = err.message; }), 60000);
    }
  }, delay);
}

async function refreshTokenAndFeed() {
  const old = accessToken;
  const data = await generateAccessToken();
  if (old !== accessToken) {
    closeFeed();
    await sleep(200);
    connectFeed().catch(e => { lastFeedError = e.message; });
  }
  return data;
}

function parseCsvLine(line) {
  const out=[]; let cur='', q=false;
  for (let i=0;i<line.length;i++) {
    const c=line[i];
    if(c==='"' && line[i+1]==='"'){cur+='"';i++;continue;}
    if(c==='"'){q=!q;continue;}
    if(c===',' && !q){out.push(cur);cur='';} else cur+=c;
  }
  out.push(cur); return out;
}

async function loadInstrumentMaster() {
  try {
    const r = await axios.get(INSTRUMENT_MASTER_URL, { timeout: 30000, responseType: 'text' });
    const lines = r.data.split(/\r?\n/).filter(Boolean);
    if (!lines.length) return;
    const headers = parseCsvLine(lines[0]).map(x=>x.trim());
    const idx = Object.fromEntries(headers.map((h,i)=>[h,i]));
    const get = (row, names) => { for (const n of names) if (idx[n] !== undefined) return row[idx[n]]; return ''; };
    const rows=[];
    for(let i=1;i<lines.length;i++){
      const row=parseCsvLine(lines[i]);
      const exchangeSegment=get(row,['SEM_EXM_EXCH_ID','exchange_segment','EXCH_ID']);
      const securityId=get(row,['SEM_SMST_SECURITY_ID','security_id','SECURITY_ID']);
      const symbol=get(row,['SEM_CUSTOM_SYMBOL','SEM_TRADING_SYMBOL','trading_symbol','SYMBOL']);
      if(securityId) rows.push({ exchangeSegment, securityId, symbol,
        instrumentType:get(row,['SEM_INSTRUMENT_NAME','instrument_type']),
        expiry:get(row,['SEM_EXPIRY_DATE','expiry']), strike:get(row,['SEM_STRIKE_PRICE','strike']), optionType:get(row,['SEM_OPTION_TYPE','option_type']) });
    }
    instrumentMaster=rows; instruments=rows.length;
  } catch(e) {
    instrumentMaster=[]; instruments=0;
    lastFeedError='Instrument master: '+e.message;
  }
}

function connectFeed() {
  if (!accessToken || !CLIENT_ID) { feedState='NO_TOKEN'; return; }
  if (feedWs && (feedWs.readyState===WebSocket.OPEN || feedWs.readyState===WebSocket.CONNECTING)) return;
  feedState='CONNECTING';
  const url=`wss://api-feed.dhan.co?version=2&token=${encodeURIComponent(accessToken)}&clientId=${encodeURIComponent(CLIENT_ID)}&authType=2`;
  const ws=new WebSocket(url);
  feedWs=ws;
  ws.binaryType='arraybuffer';
  ws.on('open',()=>{
    feedState='CONNECTED';
    lastFeedError='';
    subscribeDefault();
  });
  ws.on('message',(data)=>{
    feedLastMessageAt=nowIso(); packetCount++;
    const buf=Buffer.from(data);
    decodePacket(buf);
  });
  ws.on('error',(e)=>{ lastFeedError=e.message; feedState='ERROR'; });
  ws.on('close',()=>{
    if(feedWs===ws) feedWs=null;
    if(feedState!=='TOKEN_ERROR') feedState='DISCONNECTED';
    clearTimeout(reconnectTimer);
    reconnectTimer=setTimeout(()=>connectFeed().catch(e=>{lastFeedError=e.message;}),RECONNECT_MS);
  });
}

function subscribeDefault() {
  // NIFTY 50, BANKNIFTY, FINNIFTY, MIDCPNIFTY and SENSEX index IDs commonly used by Dhan.
  // The frontend can override subscriptions through /api/subscribe.
  const list=[
    {ExchangeSegment:'IDX_I',SecurityId:'13'},
    {ExchangeSegment:'IDX_I',SecurityId:'25'},
    {ExchangeSegment:'IDX_I',SecurityId:'27'},
    {ExchangeSegment:'IDX_I',SecurityId:'442'},
    {ExchangeSegment:'IDX_I',SecurityId:'51'}
  ];
  try {
    if(feedWs && feedWs.readyState===WebSocket.OPEN){
      for(let i=0;i<list.length;i+=100){
        const part=list.slice(i,i+100);
        feedWs.send(JSON.stringify({RequestCode:FEED_REQUEST_CODE,InstrumentCount:part.length,InstrumentList:part}));
      }
      subscribedInstruments=list.length;
    }
  } catch(e){lastFeedError=e.message;}
}

function decodePacket(buf) {
  if(buf.length < 12) return;
  const code=buf.readUInt8(0);
  const segment=buf.readUInt8(3);
  const securityId=buf.readInt32LE(4);
  // Dhan packet layouts vary by response code. Decode the common ticker/full LTP safely.
  let ltp=null, ltt=null, oi=null, volume=null;
  try {
    if(code===2 && buf.length>=16){ ltp=buf.readFloatLE(8); ltt=buf.readInt32LE(12); }
    else if(code===4 && buf.length>=51){ ltp=buf.readFloatLE(8); ltt=buf.readInt32LE(14); volume=buf.readInt32LE(22); }
    else if(code===5 && buf.length>=12){ oi=buf.readInt32LE(8); }
    else if(code===8 && buf.length>=63){ ltp=buf.readFloatLE(8); ltt=buf.readInt32LE(14); volume=buf.readInt32LE(22); oi=buf.readInt32LE(34); }
  } catch(_){}
  const item={securityId:String(securityId),segment,code,ltp,ltt,oi,volume,ts:Date.now()};
  ticks.set(item.securityId,item); tickCount++;
  history.push(item); if(history.length>MAX_HISTORY) history.shift();
  const msg=JSON.stringify({type:'tick',data:item});
  for(const c of clients){try{if(c.readyState===WebSocket.OPEN)c.send(msg);}catch(_){}
  }
}

app.get('/',(req,res)=>res.type('html').send(`<h2>Dhan TOTP Realtime Backend</h2><pre>${JSON.stringify(status(),null,2)}</pre>`));
function status(){
  return {success:true,version:'6.0-TOTP-REALTIME',feedState,feedLastMessageAt,feedStale:!feedLastMessageAt || (Date.now()-Date.parse(feedLastMessageAt)>10000),ticks:tickCount,packets:packetCount,subscribedInstruments,instruments,connectedClients:clients.size,pushSubscribers:0,vapidReady:Boolean(process.env.VAPID_PUBLIC_KEY&&process.env.VAPID_PRIVATE_KEY),tokenPresent:Boolean(accessToken),tokenExpiry:tokenExpiryMs?new Date(tokenExpiryMs).toISOString():null,lastTokenAt,lastTokenError,lastFeedError};
}
app.get('/api/health',(req,res)=>res.json({ok:true,time:nowIso()}));
app.get('/api/status',(req,res)=>res.json(status()));
app.get('/api/state',(req,res)=>res.json({success:true,state:status(),ticks:[...ticks.values()]}));
app.get('/api/ticks',(req,res)=>res.json({success:true,data:[...ticks.values()]}));
app.get('/api/history',(req,res)=>res.json({success:true,data:history.slice(-Math.min(Number(req.query.limit||500),MAX_HISTORY))}));
app.get('/api/instruments',(req,res)=>res.json({success:true,count:instrumentMaster.length,data:instrumentMaster.slice(0,Number(req.query.limit||100))}));
app.get('/api/config',(req,res)=>res.json({success:true,vapidPublicKey:process.env.VAPID_PUBLIC_KEY||'',version:'6.0-TOTP-REALTIME'}));
app.post('/api/subscribe',(req,res)=>{
  const list=Array.isArray(req.body?.instruments)?req.body.instruments:[];
  if(!list.length)return res.status(400).json({success:false,error:'instruments[] required'});
  if(!feedWs || feedWs.readyState!==WebSocket.OPEN)return res.status(503).json({success:false,error:'feed not connected'});
  try{
    for(let i=0;i<list.length;i+=100){
      const part=list.slice(i,i+100).map(x=>({ExchangeSegment:String(x.ExchangeSegment||x.exchangeSegment),SecurityId:String(x.SecurityId||x.securityId)}));
      feedWs.send(JSON.stringify({RequestCode:FEED_REQUEST_CODE,InstrumentCount:part.length,InstrumentList:part}));
    }
    subscribedInstruments+=list.length;
    res.json({success:true,subscribed:list.length});
  }catch(e){res.status(500).json({success:false,error:e.message});}
});

const server=app.listen(PORT,async()=>{
  console.log(`Dhan TOTP Realtime Backend v6 listening on :${PORT}`);
  await loadInstrumentMaster();
  try { await generateAccessToken(); console.log('Dhan TOTP access token generated automatically.'); connectFeed(); }
  catch(e){ lastTokenError=e.message; feedState='TOKEN_ERROR'; console.error('Dhan TOTP token generation failed:',e.message); }
});

server.on('upgrade',(req,socket,head)=>{
  if(req.url!=='/ws'){socket.destroy();return;}
  const wss=new WebSocket.Server({noServer:true});
  wss.handleUpgrade(req,socket,head,ws=>{
    clients.add(ws);
    ws.send(JSON.stringify({type:'status',data:status()}));
    ws.on('close',()=>clients.delete(ws));
  });
});

process.on('SIGTERM',()=>{try{feedWs?.close();}catch(_){};server.close(()=>process.exit(0));});
