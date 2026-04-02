// bot.js – UT Bot Trading System (Node.js + Upstash Redis REST)
// ✅ RAM Cache: trades, riskConfig, tradingState, usdtInr, riskState all in memory
// ✅ Redis reads only on boot + writes only on actual state changes
// ✅ WebSocket syncs RAM from Redis every 30s (OHLC candle is primary SL/TP)
// ✅ /signal route: browser computes klines+signal, POSTs to /compute-signal
// ✅ Browser caches history locally — no Redis read per dashboard poll
// ✅ Target: ~3,000 Redis ops/day (was 820,000+)
// ✅ All prior features preserved: asymmetric exit, cooldown, trailing stop,
//    partial TP1, breakeven, daily limits, force-close, IST timestamps

require('dotenv').config();
const express   = require('express');
const { Redis } = require('@upstash/redis');
const fetch     = require('node-fetch');
const cron      = require('node-cron');
const WebSocket = require('ws');

const app  = express();
const port = process.env.PORT || 5000;
app.use(express.json());
app.use(express.static(__dirname));

// ── Upstash Redis ─────────────────────────────────────────────
const HARDCODED_URL   = 'https://robust-kitten-78595.upstash.io';
const HARDCODED_TOKEN = 'gQAAAAAAATMDAAIncDEyZjJkNzQyMDQyN2Q0ODEwOTI1ZGY4MTczMWM4MGQzYnAxNzg1OTU';

const redis = new Redis({
  url:   process.env.UPSTASH_REDIS_REST_URL   || HARDCODED_URL,
  token: process.env.UPSTASH_REDIS_REST_TOKEN || HARDCODED_TOKEN,
});
redis.ping().then(() => console.log('✅ Redis connected')).catch(e => console.error('Redis error:', e));

// ── Redis Keys ────────────────────────────────────────────────
const TRADES_KEY        = 'demo_trades';
const RISK_STATE_KEY    = 'risk_state';
const RISK_CONFIG_KEY   = 'risk_config';
const TRADING_STATE_KEY = 'trading_state';
const USDT_INR_KEY      = 'usdt_inr_rate';

const START_BALANCE = 10000;

// ════════════════════════════════════════════════════════════════
// IN-MEMORY CACHE
// All state lives here in RAM. Redis is only written when state
// actually changes. WebSocket reads from RAM — zero Redis on ticks.
// ════════════════════════════════════════════════════════════════
let mem = {
  trades:       null,   // { balance, open_trade, history, order_log, ... }
  riskConfig:   null,
  riskState:    null,
  tradingState: null,
  usdtInr:      85,
};

// Load everything from Redis on boot
async function loadAllFromRedis() {
  console.log('[CACHE] Loading all state from Redis...');
  try {
    const [tradesRaw, cfgRaw, stateRaw, tradingRaw, rateRaw] = await Promise.all([
      redis.get(TRADES_KEY),
      redis.get(RISK_CONFIG_KEY),
      redis.get(RISK_STATE_KEY),
      redis.get(TRADING_STATE_KEY),
      redis.get(USDT_INR_KEY),
    ]);
    mem.trades       = tradesRaw   ? (typeof tradesRaw   === 'string' ? JSON.parse(tradesRaw)   : tradesRaw)   : null;
    mem.riskConfig   = cfgRaw      ? (typeof cfgRaw      === 'string' ? JSON.parse(cfgRaw)      : cfgRaw)      : null;
    mem.riskState    = stateRaw    ? (typeof stateRaw    === 'string' ? JSON.parse(stateRaw)    : stateRaw)    : null;
    mem.tradingState = tradingRaw  ? (typeof tradingRaw  === 'string' ? JSON.parse(tradingRaw)  : tradingRaw)  : null;
    mem.usdtInr      = rateRaw     ? parseFloat(rateRaw) : 85;

    if (!mem.trades)       mem.trades       = makeDefaultTrades();
    if (!mem.riskConfig)   mem.riskConfig   = makeDefaultRiskConfig();
    if (!mem.riskState)    mem.riskState    = makeDefaultRiskState();
    if (!mem.tradingState) mem.tradingState = makeDefaultTradingState();

    console.log(`[CACHE] Loaded — trade:${mem.trades.open_trade?.type || 'none'} bal:₹${mem.trades.balance?.toFixed(0)}`);
  } catch (e) {
    console.error('[CACHE] Load error:', e.message);
    mem.trades       = makeDefaultTrades();
    mem.riskConfig   = makeDefaultRiskConfig();
    mem.riskState    = makeDefaultRiskState();
    mem.tradingState = makeDefaultTradingState();
  }
}

// Periodic RAM→Redis sync every 30s (safety net in case of crash)
// This is the ONLY periodic Redis write — not per-tick
setInterval(async () => {
  try {
    await Promise.all([
      redis.set(TRADES_KEY,        JSON.stringify(mem.trades)),
      redis.set(RISK_STATE_KEY,    JSON.stringify(mem.riskState)),
    ]);
  } catch (e) { console.error('[SYNC] Periodic save error:', e.message); }
}, 30 * 1000);

// ── Default state factories ───────────────────────────────────
function makeDefaultTrades() {
  return {
    balance: START_BALANCE, open_trade: null, history: [], order_log: [],
    last_signal: null, last_closed_time: null, last_closed_side: null,
    last_closed_sl_price: null, last_close_reason: null,
    last_entry_price: null, last_entry_signal: null,
    pending_opposite_side: null, pending_opposite_time: null,
  };
}

function makeDefaultRiskState() {
  const nowIST   = new Date().toLocaleString('en-US', { timeZone: 'Asia/Kolkata' });
  const todayStr = new Date(nowIST).toISOString().split('T')[0];
  return { daily_loss: 0, daily_profit: 0, daily_trades: 0, last_reset: todayStr, peak_balance: START_BALANCE };
}

function makeDefaultTradingState() {
  return { enabled: true, start_hour: 18, end_hour: 23, manual_pause: false, force_start: false };
}

function makeDefaultRiskConfig() {
  return {
    stop_loss: {
      enabled: true, type: 'hybrid', atr_multiplier: 2.5,
      max_loss_percentage: 3.0, trailing_enabled: true, trailing_atr_multiplier: 1.5,
    },
    take_profit: {
      enabled: true, type: 'asymmetric',
      levels: [
        { percentage: 60, atr_multiplier: 1.5, name: 'TP1' },
        { percentage: 40, atr_multiplier: 3.0, name: 'TP2' },
      ],
    },
    position_sizing: {
      method: 'risk_fixed', value: 5.0,
      min_position_size: 0.0001, max_position_size: 0.01, risk_amount: 200,
    },
    daily_limits: {
      enabled: true, max_daily_loss: 400.0, max_daily_trades: 20, reset_hour: 0,
    },
    account_protection: {
      max_drawdown_percentage: 15.0, min_balance: 5000.0, emergency_stop: false,
    },
    different_rules_for_position_type: {
      enabled: true,
      long:  { tp_atr_multipliers: [1.5, 3.0] },
      short: { tp_atr_multipliers: [1.5] },
    },
    cooldown: {
      sl_condition_based: true, sl_cooldown_ms: 600000, sl_fallback_ms: 600000,
      tp_cooldown_ms: 0, distance_filter_enabled: true, distance_filter_pct: 0.4,
      opposite_confirmation_ms: 30000,
    },
    rr_mode: 'none',
  };
}

// ── RAM helpers (no Redis) ────────────────────────────────────
function getTradesFromMem()       { return mem.trades; }
function getRiskConfigFromMem()   { return mem.riskConfig; }
function getRiskStateFromMem()    { return mem.riskState; }
function getTradingStateFromMem() { return mem.tradingState; }
function getUSDTINRFromMem()      { return mem.usdtInr; }

// Write-through: update RAM immediately, persist to Redis async
function saveTradesAsync()       { redis.set(TRADES_KEY,        JSON.stringify(mem.trades)).catch(e => console.error('[REDIS] trades save:', e.message)); }
function saveRiskStateAsync()    { redis.set(RISK_STATE_KEY,    JSON.stringify(mem.riskState)).catch(e => console.error('[REDIS] riskState save:', e.message)); }
function saveRiskConfigAsync()   { redis.set(RISK_CONFIG_KEY,   JSON.stringify(mem.riskConfig)).catch(e => console.error('[REDIS] riskConfig save:', e.message)); }
function saveTradingStateAsync() { redis.set(TRADING_STATE_KEY, JSON.stringify(mem.tradingState)).catch(e => console.error('[REDIS] tradingState save:', e.message)); }
function saveUSDTINRAsync()      { redis.set(USDT_INR_KEY,      String(mem.usdtInr)).catch(e => console.error('[REDIS] usdtInr save:', e.message)); }

// ════════════════════════════════════════════════════════════════
// WEBSOCKET — PRICE FEED + 30s RAM SAFETY SYNC
// Price ticks update livePrice in RAM only — zero Redis.
// SL/TP checked against mem.trades.open_trade — zero Redis.
// Every 30s: re-read open_trade from Redis as safety net.
// ════════════════════════════════════════════════════════════════
let livePrice        = null;
let wsConnected      = false;
let wsSource         = null;
let wsInstance       = null;
let wsFailCount      = 0;
let wsReconnectTimer = null;
let lastWsSync       = 0;     // timestamp of last RAM←Redis sync
const WS_SYNC_MS     = 30000; // re-sync every 30s

let wsCheckInProgress = false;

function onPriceTick(price) {
  livePrice = price;
  if (wsCheckInProgress) return;
  wsCheckInProgress = true;
  checkSlTpInMem(price).catch(e => console.error('WS SL/TP error:', e.message))
    .finally(() => { wsCheckInProgress = false; });
}

async function checkSlTpInMem(price) {
  // Every 30s: re-sync open_trade from Redis (safety net only — 1 GET per 30s)
  const now = Date.now();
  if (now - lastWsSync >= WS_SYNC_MS) {
    lastWsSync = now;
    try {
      const raw = await redis.get(TRADES_KEY);
      if (raw) {
        const fresh = typeof raw === 'string' ? JSON.parse(raw) : raw;
        // Only sync open_trade — don't overwrite history we may have written in RAM
        mem.trades.open_trade = fresh.open_trade;
      }
    } catch (e) { console.error('[WS SYNC]', e.message); }
  }

  const openTrade = mem.trades.open_trade;
  if (!openTrade) return;

  const sl          = openTrade.stop_loss;
  const nextTP      = openTrade.tp_levels?.find(t => !t.hit) || null;
  const nextTPPrice = nextTP?.price || null;
  const nextTPName  = nextTP?.name  || 'TP';
  const posType     = openTrade.type;

  const slHit = sl          && ((posType === 'LONG' && price <= sl)          || (posType === 'SHORT' && price >= sl));
  const tpHit = nextTPPrice && ((posType === 'LONG' && price >= nextTPPrice) || (posType === 'SHORT' && price <= nextTPPrice));

  if (!slHit && !tpHit) return;

  const usdtInr = getUSDTINRFromMem();

  if (slHit) {
    console.log(`🛑 [WS] SL @ $${price.toFixed(2)} | SL=$${sl.toFixed(2)}`);
    const { tradeRecord, side } = await closeFullPositionInMem(openTrade, price, 'Stop-Loss Hit (WS)', usdtInr);
    addOrderLog('STOP_LOSS_WS', posType === 'LONG' ? 'Sell' : 'Buy', price, openTrade.amount, tradeRecord.profit_inr);
    mem.trades.last_closed_time     = Date.now();
    mem.trades.last_closed_side     = side;
    mem.trades.last_closed_sl_price = sl;
    mem.trades.last_close_reason    = 'sl';
    mem.trades.pending_opposite_side  = null;
    mem.trades.pending_opposite_time  = null;
    saveTradesAsync();
    saveRiskStateAsync();
    console.log(`✅ [WS] SL closed. P/L:₹${tradeRecord.profit_inr.toFixed(2)} Bal:₹${mem.trades.balance.toFixed(2)}`);

  } else if (tpHit) {
    const isTP1 = nextTPName === 'TP1';
    const isTP2 = nextTPName === 'TP2';

    if (posType === 'LONG' && isTP1) {
      console.log(`✅ [WS] TP1 LONG @ $${price.toFixed(2)} — closing 60%`);
      const { tradeRecord } = await closePartialPositionInMem(openTrade, price, 0.6, 'TP1 Hit', usdtInr);
      addOrderLog('TP1_PARTIAL_60_WS', 'Sell', price, parseFloat((openTrade.amount * 0.6).toFixed(6)), tradeRecord.profit_inr);
      mem.trades.last_close_reason = 'tp';
      saveTradesAsync();
      saveRiskStateAsync();
      console.log(`✅ [WS] TP1 60% done. SL→BE $${openTrade.entry_price.toFixed(2)} | P/L:₹${tradeRecord.profit_inr.toFixed(2)}`);

    } else if (posType === 'LONG' && isTP2) {
      console.log(`✅ [WS] TP2 LONG @ $${price.toFixed(2)} — closing 40%`);
      const { tradeRecord, side } = await closeFullPositionInMem(openTrade, price, 'TP2 Hit - Full Exit', usdtInr);
      addOrderLog('TP2_FULL_WS', 'Sell', price, openTrade.amount, tradeRecord.profit_inr);
      mem.trades.last_closed_time     = Date.now();
      mem.trades.last_closed_side     = side;
      mem.trades.last_closed_sl_price = null;
      mem.trades.last_close_reason    = 'tp';
      mem.trades.pending_opposite_side  = null;
      mem.trades.pending_opposite_time  = null;
      saveTradesAsync();
      saveRiskStateAsync();
      console.log(`✅ [WS] TP2 done. P/L:₹${tradeRecord.profit_inr.toFixed(2)} Bal:₹${mem.trades.balance.toFixed(2)}`);

    } else {
      console.log(`✅ [WS] ${nextTPName} SHORT @ $${price.toFixed(2)} — full exit`);
      const { tradeRecord, side } = await closeFullPositionInMem(openTrade, price, `${nextTPName} Hit - Full Exit (Short)`, usdtInr);
      addOrderLog(`${nextTPName}_FULL_SHORT_WS`, 'Buy', price, openTrade.amount, tradeRecord.profit_inr);
      mem.trades.last_closed_time     = Date.now();
      mem.trades.last_closed_side     = side;
      mem.trades.last_closed_sl_price = null;
      mem.trades.last_close_reason    = 'tp';
      mem.trades.pending_opposite_side  = null;
      mem.trades.pending_opposite_time  = null;
      saveTradesAsync();
      saveRiskStateAsync();
      console.log(`✅ [WS] SHORT ${nextTPName} done. P/L:₹${tradeRecord.profit_inr.toFixed(2)}`);
    }
  }
}

function addOrderLog(action, side, price, quantity, plInr) {
  if (!mem.trades.order_log) mem.trades.order_log = [];
  mem.trades.order_log.push({ time: new Date().toISOString(), side, action, price, quantity, pl_inr: plInr });
  if (mem.trades.order_log.length > 100) mem.trades.order_log.shift();
}

// ── Bybit WebSocket ───────────────────────────────────────────
function connectBybitWS() {
  console.log('🔌 Connecting to Bybit WebSocket...');
  const ws = new WebSocket('wss://stream.bybit.com/v5/public/spot');

  ws.on('open', () => {
    console.log('✅ Bybit WS connected');
    wsConnected = true; wsSource = 'bybit'; wsFailCount = 0; wsInstance = ws;
    ws.send(JSON.stringify({ op: 'subscribe', args: ['publicTrade.BTCUSDT'] }));
  });

  ws.on('message', (raw) => {
    try {
      const msg = JSON.parse(raw);
      if (msg.topic === 'publicTrade.BTCUSDT' && msg.data?.length > 0) {
        const price = parseFloat(msg.data[0].p);
        if (!isNaN(price)) onPriceTick(price);
      }
    } catch (_) {}
  });

  ws.on('error', (err) => { console.warn('⚠️ Bybit WS error:', err.message); wsConnected = false; });

  ws.on('close', () => {
    wsConnected = false; wsInstance = null; wsFailCount++;
    const delay = Math.min(5000 * wsFailCount, 30000);
    console.warn(`⚠️ Bybit WS closed (fail#${wsFailCount}). Retry in ${delay/1000}s...`);
    wsReconnectTimer = setTimeout(connectBybitWS, delay);
  });

  const pingInterval = setInterval(() => {
    if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify({ op: 'ping' }));
    else clearInterval(pingInterval);
  }, 20000);
}

// Polling fallback when WS is down
let pollingFallbackTimer = null;
function startPollingFallback() {
  if (pollingFallbackTimer) return;
  console.log('⚠️ WS down — REST polling fallback every 10s');
  pollingFallbackTimer = setInterval(async () => {
    if (wsConnected) {
      clearInterval(pollingFallbackTimer);
      pollingFallbackTimer = null;
      console.log('✅ WS back — stopping fallback');
      return;
    }
    const price = await getCurrentPrice();
    if (price) onPriceTick(price);
  }, 10000);
}
setInterval(() => { if (!wsConnected) startPollingFallback(); }, 30000);

// ── Self-pinger ───────────────────────────────────────────────
const SELF_URL = process.env.RENDER_EXTERNAL_URL || `http://localhost:${port}`;

async function selfPing() {
  try {
    const res = await fetchWithTimeout(`${SELF_URL}/ping`, {}, 10000);
    const text = await res.text();
    console.log(`🏓 Self-ping OK | WS:${wsConnected ? wsSource : 'down'} | $${livePrice?.toFixed(2) || 'N/A'}`);
  } catch (e) { console.warn('⚠️ Self-ping failed:', e.message); }
}
setInterval(selfPing, 4 * 60 * 1000);
setTimeout(selfPing, 30000);

// ════════════════════════════════════════════════════════════════
// PRICE + KLINES (REST — browser fetches klines, server uses
// WS price for live display + REST as fallback)
// ════════════════════════════════════════════════════════════════
const BINANCE_ENDPOINTS = [
  'https://data-api.binance.vision', 'https://api.binance.us',
  'https://api1.binance.com', 'https://api2.binance.com',
  'https://api3.binance.com', 'https://api4.binance.com',
  'https://api.binance.com',  'https://api-gcp.binance.com',
];

async function fetchWithTimeout(url, options = {}, timeoutMs = 7000) {
  const ctrl  = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), timeoutMs);
  try {
    const res = await fetch(url, { ...options, signal: ctrl.signal });
    clearTimeout(timer); return res;
  } catch (err) { clearTimeout(timer); throw err; }
}

async function binanceRequest(path, params = {}) {
  for (const ep of BINANCE_ENDPOINTS) {
    const ctrl    = new AbortController();
    const timeout = setTimeout(() => ctrl.abort(), 6000);
    try {
      const url = new URL(path, ep);
      Object.entries(params).forEach(([k, v]) => url.searchParams.append(k, v));
      const res = await fetch(url.toString(), {
        headers: { 'User-Agent': 'Mozilla/5.0', 'Accept': 'application/json', 'Cache-Control': 'no-cache' },
        signal: ctrl.signal,
      });
      clearTimeout(timeout);
      if (res.status === 200) { console.log(`✅ Binance from: ${ep}`); return await res.json(); }
    } catch (err) { clearTimeout(timeout); }
  }
  return null;
}

async function getCurrentPrice() {
  if (wsConnected && livePrice) return livePrice;
  const bp = await binanceRequest('/api/v3/ticker/price', { symbol: 'BTCUSDT' });
  if (bp?.price) return parseFloat(bp.price);
  try {
    const res  = await fetchWithTimeout('https://api.bybit.com/v5/market/tickers?category=spot&symbol=BTCUSDT', {}, 6000);
    const data = await res.json();
    if (data?.result?.list?.[0]?.lastPrice) return parseFloat(data.result.list[0].lastPrice);
  } catch (_) {}
  try {
    const res  = await fetchWithTimeout('https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd', {}, 6000);
    const data = await res.json();
    if (data?.bitcoin?.usd) return data.bitcoin.usd;
  } catch (_) {}
  return null;
}

// ════════════════════════════════════════════════════════════════
// UT BOT SIGNAL LOGIC (server-side, used by /compute-signal)
// Browser sends pre-computed klines signal to avoid geo-block.
// Server still runs its own signal computation as backup.
// ════════════════════════════════════════════════════════════════
function calcUtbot(klines, keyvalue, atrPeriod) {
  const close = klines.map(k => parseFloat(k[4]));
  const high  = klines.map(k => parseFloat(k[2]));
  const low   = klines.map(k => parseFloat(k[3]));
  const tr = high.map((h, i) => i === 0 ? h - low[i] : Math.max(h - low[i], Math.abs(h - close[i-1]), Math.abs(low[i] - close[i-1])));
  const atr = tr.map((_, i) => {
    if (i < atrPeriod - 1) return null;
    return tr.slice(i - atrPeriod + 1, i + 1).reduce((a, b) => a + b, 0) / atrPeriod;
  });
  const nLoss = atr.map(a => a === null ? null : keyvalue * a);
  const xATRTrailingStop = [close[0]];
  const pos = [0];
  for (let i = 1; i < close.length; i++) {
    const src = close[i], src1 = close[i-1], prev = xATRTrailingStop[i-1], nl = nLoss[i];
    let newStop;
    if (src > prev && src1 > prev)      newStop = Math.max(prev, src - nl);
    else if (src < prev && src1 < prev) newStop = Math.min(prev, src + nl);
    else                                 newStop = src > prev ? src - nl : src + nl;
    xATRTrailingStop.push(newStop);
    let newPos;
    if (src1 < prev && src > prev)      newPos = 1;
    else if (src1 > prev && src < prev) newPos = -1;
    else                                 newPos = pos[i-1];
    pos.push(newPos);
  }
  return { stop: xATRTrailingStop, pos, atr };
}

// ════════════════════════════════════════════════════════════════
// RISK MANAGEMENT (all reads from RAM — zero Redis)
// ════════════════════════════════════════════════════════════════
async function calculatePositionSize(balance, entryPrice, type, stopLoss, atr, config) {
  const sizing  = config.position_sizing;
  const usdtInr = getUSDTINRFromMem();
  let size;
  switch (sizing.method) {
    case 'percentage': size = (balance * (sizing.value / 100)) / (entryPrice * usdtInr); break;
    case 'fixed':      size = sizing.value; break;
    case 'risk_fixed':
      size = !stopLoss
        ? (balance * 0.05) / (entryPrice * usdtInr)
        : sizing.risk_amount / (Math.abs(entryPrice - stopLoss) * usdtInr);
      break;
    default: size = 0.001;
  }
  return parseFloat(Math.min(sizing.max_position_size, Math.max(sizing.min_position_size, size)).toFixed(6));
}

function calculateStopLoss(entry, type, atr, utbotStop, config) {
  const sl = config.stop_loss;
  if (!sl.enabled) return null;
  const atrStop   = type === 'LONG' ? entry - atr * sl.atr_multiplier   : entry + atr * sl.atr_multiplier;
  const fixedStop = type === 'LONG' ? entry * (1 - sl.max_loss_percentage / 100) : entry * (1 + sl.max_loss_percentage / 100);
  let stop;
  switch (sl.type) {
    case 'atr':        stop = atrStop; break;
    case 'percentage': stop = fixedStop; break;
    case 'utbot':      stop = utbotStop || fixedStop; break;
    default:
      stop = type === 'LONG' ? Math.max(atrStop, fixedStop) : Math.min(atrStop, fixedStop);
      if (utbotStop) stop = type === 'LONG' ? Math.max(stop, utbotStop) : Math.min(stop, utbotStop);
  }
  return parseFloat(stop.toFixed(2));
}

function calculateTakeProfitLevels(entry, type, atr, config) {
  const tp = config.take_profit;
  if (!tp.enabled) return [];
  const multipliers = config.different_rules_for_position_type?.enabled
    ? (type === 'LONG' ? config.different_rules_for_position_type.long.tp_atr_multipliers : config.different_rules_for_position_type.short.tp_atr_multipliers)
    : tp.levels.map(l => l.atr_multiplier);
  return multipliers.map((mult, i) => {
    const price = type === 'LONG' ? entry + atr * mult : entry - atr * mult;
    const info  = tp.levels[i] || { percentage: Math.floor(100 / multipliers.length), name: `TP${i+1}` };
    return { price: parseFloat(price.toFixed(2)), percentage: info.percentage, name: info.name, hit: false };
  });
}

function updateTrailingStop(currentPrice, type, stopLoss, atr, config) {
  if (!config.stop_loss.trailing_enabled) return null;
  const trail   = atr * config.stop_loss.trailing_atr_multiplier;
  const newStop = type === 'LONG' ? currentPrice - trail : currentPrice + trail;
  if (type === 'LONG'  && newStop > stopLoss) return parseFloat(newStop.toFixed(2));
  if (type === 'SHORT' && newStop < stopLoss) return parseFloat(newStop.toFixed(2));
  return null;
}

function checkDailyLimits() {
  const config = getRiskConfigFromMem();
  const state  = getRiskStateFromMem();
  const limits = config.daily_limits;
  if (!limits.enabled) return { allowed: true, reason: null };
  if (state.daily_loss   >= limits.max_daily_loss)   return { allowed: false, reason: `Daily loss limit ₹${state.daily_loss.toFixed(2)}/₹${limits.max_daily_loss}` };
  if (state.daily_trades >= limits.max_daily_trades) return { allowed: false, reason: `Daily trades limit ${state.daily_trades}/${limits.max_daily_trades}` };
  return { allowed: true, reason: null };
}

function checkAccountProtection(balance) {
  const config = getRiskConfigFromMem();
  const state  = getRiskStateFromMem();
  const prot   = config.account_protection;
  if (prot.emergency_stop)       return { allowed: false, reason: 'Emergency stop activated' };
  if (balance < prot.min_balance) return { allowed: false, reason: `Balance below minimum ₹${balance.toFixed(2)}` };
  if (state.peak_balance > 0) {
    const dd = ((state.peak_balance - balance) / state.peak_balance) * 100;
    if (dd >= prot.max_drawdown_percentage) return { allowed: false, reason: `Max drawdown ${dd.toFixed(2)}%` };
  }
  if (balance > state.peak_balance) {
    state.peak_balance = balance;
    saveRiskStateAsync();
  }
  return { allowed: true, reason: null };
}

function canOpenTrade(balance) {
  const daily   = checkDailyLimits();    if (!daily.allowed)   return daily;
  const account = checkAccountProtection(balance); if (!account.allowed) return account;
  return { allowed: true, reason: null };
}

function recordTradeResult(profitLoss) {
  const state = getRiskStateFromMem();
  state.daily_trades += 1;
  if (profitLoss < 0) state.daily_loss   += Math.abs(profitLoss);
  else                state.daily_profit += profitLoss;
  // No async needed — already in mem, saveRiskStateAsync called by caller
}

// ════════════════════════════════════════════════════════════════
// POSITION CLOSE HELPERS (all operate on mem.trades in RAM)
// ════════════════════════════════════════════════════════════════
function calculateLivePL(openTrade, currentPrice) {
  if (!openTrade) return null;
  return openTrade.type === 'LONG'
    ? (currentPrice - openTrade.entry_price) * openTrade.amount
    : (openTrade.entry_price - currentPrice) * openTrade.amount;
}

async function closeFullPositionInMem(openTrade, currentPrice, reason, usdtInr) {
  usdtInr = usdtInr || getUSDTINRFromMem();
  const profitUsdt = openTrade.type === 'LONG'
    ? (currentPrice - openTrade.entry_price) * openTrade.amount
    : (openTrade.entry_price - currentPrice) * openTrade.amount;
  const profitInr  = profitUsdt * usdtInr;
  const balBefore  = mem.trades.balance;
  mem.trades.balance += profitInr;

  const closedAt = new Date();
  const tradeRecord = {
    type: openTrade.type, entry_price: openTrade.entry_price,
    exit_price: currentPrice, amount: openTrade.amount,
    profit_usdt: parseFloat(profitUsdt.toFixed(2)),
    profit_inr:  parseFloat(profitInr.toFixed(2)),
    balance_before: parseFloat(balBefore.toFixed(2)),
    balance_after:  parseFloat(mem.trades.balance.toFixed(2)),
    opened_at: openTrade.opened_at, closed_at: closedAt.toISOString(),
    opened_at_ist: new Date(openTrade.opened_at).toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' }),
    closed_at_ist: closedAt.toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' }),
    duration_ms: closedAt - new Date(openTrade.opened_at),
    exit_reason: reason, partial: false,
    stop_loss: openTrade.stop_loss, tp1_price: openTrade.tp1_price,
    tp2_price: openTrade.tp_levels?.[1]?.price || null,
  };
  mem.trades.history.push(tradeRecord);
  recordTradeResult(profitInr);
  mem.trades.open_trade = null;
  return { tradeRecord, side: openTrade.type === 'LONG' ? 'Buy' : 'Sell' };
}

async function closePartialPositionInMem(openTrade, currentPrice, partialPct, reason, usdtInr) {
  usdtInr = usdtInr || getUSDTINRFromMem();
  const closeAmount = parseFloat((openTrade.amount * partialPct).toFixed(6));
  const profitUsdt  = openTrade.type === 'LONG'
    ? (currentPrice - openTrade.entry_price) * closeAmount
    : (openTrade.entry_price - currentPrice) * closeAmount;
  const profitInr   = profitUsdt * usdtInr;
  const balBefore   = mem.trades.balance;
  mem.trades.balance += profitInr;

  const now = new Date();
  const tradeRecord = {
    type: openTrade.type, entry_price: openTrade.entry_price,
    exit_price: currentPrice, amount: closeAmount,
    profit_usdt: parseFloat(profitUsdt.toFixed(2)),
    profit_inr:  parseFloat(profitInr.toFixed(2)),
    balance_before: parseFloat(balBefore.toFixed(2)),
    balance_after:  parseFloat(mem.trades.balance.toFixed(2)),
    opened_at: openTrade.opened_at, closed_at: now.toISOString(),
    opened_at_ist: new Date(openTrade.opened_at).toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' }),
    closed_at_ist: now.toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' }),
    duration_ms: now - new Date(openTrade.opened_at),
    exit_reason: reason + ` (partial ${Math.round(partialPct * 100)}%)`,
    partial: true, stop_loss: openTrade.stop_loss,
    tp1_price: openTrade.tp1_price,
    tp2_price: openTrade.tp_levels?.[1]?.price || null,
  };
  mem.trades.history.push(tradeRecord);
  recordTradeResult(profitInr);
  // Update open trade — mark TP1, move SL to breakeven, reduce amount
  openTrade.amount          = parseFloat((openTrade.amount - closeAmount).toFixed(6));
  openTrade.stop_loss       = parseFloat(openTrade.entry_price.toFixed(2));
  openTrade.breakeven_moved = true;
  const firstUnhit = openTrade.tp_levels?.findIndex(t => !t.hit);
  if (firstUnhit !== undefined && firstUnhit !== -1) openTrade.tp_levels[firstUnhit].hit = true;
  mem.trades.open_trade = openTrade;
  return { tradeRecord };
}

// ════════════════════════════════════════════════════════════════
// SMART COOLDOWN (reads from mem only — zero Redis)
// ════════════════════════════════════════════════════════════════
function checkCooldown(signal, currentPrice, cooldownConfig) {
  const data = mem.trades;
  const cfg  = cooldownConfig;
  const now  = Date.now();
  if (!data.last_closed_side) return { canOpen: true, reason: null };

  const sameSide = (data.last_closed_side === signal);

  if (sameSide) {
    const closeReason    = data.last_close_reason;
    const lastEntryPrice = data.last_entry_price;
    const elapsed        = now - data.last_closed_time;

    if (closeReason === 'sl') {
      if (elapsed < cfg.sl_cooldown_ms) {
        const remaining = Math.ceil((cfg.sl_cooldown_ms - elapsed) / 1000);
        const m = Math.floor(remaining / 60), s = remaining % 60;
        return { canOpen: false, reason: `⏳ SL cooldown: ${m > 0 ? m + 'm ' : ''}${s}s remaining` };
      }
      if (cfg.distance_filter_enabled && lastEntryPrice) {
        const distPct = Math.abs(currentPrice - lastEntryPrice) / lastEntryPrice * 100;
        if (distPct > cfg.distance_filter_pct)
          return { canOpen: false, reason: `⛔ Distance filter: moved ${distPct.toFixed(2)}% from last entry $${lastEntryPrice.toFixed(2)} (max ${cfg.distance_filter_pct}%)` };
      }
      return { canOpen: true, reason: null };
    }

    if (cfg.distance_filter_enabled && lastEntryPrice) {
      const distPct = Math.abs(currentPrice - lastEntryPrice) / lastEntryPrice * 100;
      if (distPct > cfg.distance_filter_pct)
        return { canOpen: false, reason: `⛔ Distance filter: ${distPct.toFixed(2)}% from last entry` };
    }
    return { canOpen: true, reason: null };

  } else {
    if (data.pending_opposite_side === signal) {
      const elapsed = now - data.pending_opposite_time;
      if (elapsed >= cfg.opposite_confirmation_ms) return { canOpen: true, reason: null };
      const remaining = Math.ceil((cfg.opposite_confirmation_ms - elapsed) / 1000);
      return { canOpen: false, reason: `⏳ Opposite confirmation: ${remaining}s for ${signal}` };
    } else {
      data.pending_opposite_side = signal;
      data.pending_opposite_time = now;
      return { canOpen: false, reason: `⏳ Opposite signal ${signal} — confirming for ${Math.ceil(cfg.opposite_confirmation_ms/1000)}s` };
    }
  }
}

// ════════════════════════════════════════════════════════════════
// CORE TRADE LOGIC — all from RAM, no Redis reads
// ════════════════════════════════════════════════════════════════
async function updateDemoTrade(signal, price, atrValue, utbotStop) {
  signal = signal.charAt(0).toUpperCase() + signal.slice(1).toLowerCase();
  const data      = mem.trades;
  const config    = getRiskConfigFromMem();
  let openTrade   = data.open_trade;
  let actionMessage = '';

  const logEntry = { time: new Date().toISOString(), side: signal, price, quantity: 0 };

  // Trailing stop update
  if (openTrade && openTrade.breakeven_moved && config.stop_loss.trailing_enabled) {
    const newStop = updateTrailingStop(price, openTrade.type, openTrade.stop_loss, atrValue, config);
    if (newStop) {
      openTrade.stop_loss = newStop;
      actionMessage = `📈 Trailing stop → $${newStop.toFixed(2)}`;
      logEntry.action = 'TRAILING_STOP_UPDATE';
    }
  }

  if (signal === 'Hold') {
    if (!actionMessage) { actionMessage = 'Holding — waiting for signal.'; logEntry.action = 'HOLD'; }
    data.pending_opposite_side = null; data.pending_opposite_time = null;

  } else {
    const canTrade = canOpenTrade(data.balance);
    if (!canTrade.allowed) {
      actionMessage = `⚠️ Cannot open ${signal}: ${canTrade.reason}`;
      logEntry.action = 'BLOCKED';

    } else if (openTrade && openTrade.type === (signal === 'Buy' ? 'LONG' : 'SHORT')) {
      actionMessage = `Ignoring repeated "${signal}" — already in ${signal === 'Buy' ? 'LONG' : 'SHORT'}.`;
      logEntry.action = 'IGNORED';

    } else {
      // Close opposite first
      if (openTrade) {
        const oppSide = openTrade.type === 'LONG' ? 'Sell' : 'Buy';
        if (oppSide === signal) {
          const { tradeRecord, side } = await closeFullPositionInMem(openTrade, price, 'Opposite Signal');
          actionMessage = `CLOSED ${openTrade.type} @ $${price.toFixed(2)}, P/L:₹${tradeRecord.profit_inr.toFixed(2)}. | `;
          logEntry.action = `CLOSE_${openTrade.type}`;
          openTrade = null;
          data.last_closed_time     = Date.now(); data.last_closed_side = side;
          data.last_closed_sl_price = null;       data.last_close_reason = 'signal';
          data.pending_opposite_side = null;      data.pending_opposite_time = null;
        }
      }

      const cooldownCfg = config.cooldown || {
        sl_condition_based: true, sl_cooldown_ms: 600000,
        tp_cooldown_ms: 0, distance_filter_enabled: true,
        distance_filter_pct: 0.4, opposite_confirmation_ms: 30000,
      };
      const { canOpen, reason: cooldownReason } = checkCooldown(signal, price, cooldownCfg);

      if (canOpen) {
        data.last_closed_side = null; data.last_closed_time = null;
        data.last_closed_sl_price = null; data.last_close_reason = null;
        data.pending_opposite_side = null; data.pending_opposite_time = null;

        const posType      = signal === 'Buy' ? 'LONG' : 'SHORT';
        const stopLoss     = calculateStopLoss(price, posType, atrValue, utbotStop, config);
        const positionSize = await calculatePositionSize(data.balance, price, posType, stopLoss, atrValue, config);
        const tpLevels     = calculateTakeProfitLevels(price, posType, atrValue, config);
        const tp1Price     = tpLevels[0]?.price || null;

        const openedAt = new Date();
        openTrade = {
          type: posType, entry_price: price, amount: positionSize, original_amount: positionSize,
          stop_loss: stopLoss, tp1_price: tp1Price,
          tp2_price: tpLevels[1]?.price || null, tp_levels: tpLevels,
          opened_at: openedAt.toISOString(),
          opened_at_ist: openedAt.toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' }),
          strategy: signal === 'Buy' ? 'UT Bot #2 (KV=2, ATR=300)' : 'UT Bot #1 (KV=2, ATR=1)',
          atr_at_entry: atrValue, breakeven_moved: false,
        };
        logEntry.quantity = positionSize; logEntry.stop_loss = stopLoss; logEntry.tp1 = tp1Price;
        actionMessage += (signal === 'Buy' ? '🟢 OPENED LONG' : '🔴 OPENED SHORT') +
          ` @ $${price.toFixed(2)} | ${positionSize} BTC | SL:$${stopLoss?.toFixed(2) || 'N/A'} | TP1:$${tp1Price?.toFixed(2) || 'N/A'}`;
        logEntry.action = signal === 'Buy' ? 'OPEN_LONG' : 'OPEN_SHORT';
        data.last_signal      = signal;
        data.last_entry_price  = price;
        data.last_entry_signal = signal;

      } else {
        actionMessage = cooldownReason || 'Cooldown active';
        logEntry.action = 'COOLDOWN';
      }
    }
  }

  addOrderLog(logEntry.action, signal, price, logEntry.quantity || 0, null);
  data.open_trade = openTrade;
  // Write-through to Redis
  saveTradesAsync();
  saveRiskStateAsync();

  const cooldownInfo = logEntry.action === 'COOLDOWN'
    ? { active: true, message: actionMessage }
    : { active: false, message: null };

  return {
    balance: data.balance, holding: !!openTrade,
    position_type: openTrade?.type || null, action: actionMessage,
    stop_loss: openTrade?.stop_loss || null, tp_levels: openTrade?.tp_levels || [],
    position_size: openTrade?.amount || 0, cooldown: cooldownInfo,
  };
}

async function forceClosePosition(currentPrice, reason) {
  const openTrade = mem.trades.open_trade;
  if (!openTrade) return null;
  const { tradeRecord, side } = await closeFullPositionInMem(openTrade, currentPrice, reason);
  addOrderLog('FORCE_CLOSE', 'CLOSE', currentPrice, openTrade.amount, tradeRecord.profit_inr);
  mem.trades.last_closed_time     = Date.now(); mem.trades.last_closed_side = side;
  mem.trades.last_closed_sl_price = null;       mem.trades.last_close_reason = 'force';
  mem.trades.pending_opposite_side = null;      mem.trades.pending_opposite_time = null;
  saveTradesAsync();
  saveRiskStateAsync();
  return tradeRecord;
}

// ════════════════════════════════════════════════════════════════
// TRADING HOURS (from RAM)
// ════════════════════════════════════════════════════════════════
function getCurrentHourIST() {
  return parseInt(new Date().toLocaleString('en-US', { timeZone: 'Asia/Kolkata', hour: 'numeric', hour12: false }));
}

function getCurrentMinuteIST() {
  return parseInt(new Date().toLocaleString('en-US', { timeZone: 'Asia/Kolkata', minute: 'numeric', hour12: false }));
}

function isTradingAllowed() {
  const state = getTradingStateFromMem();
  if (state.force_start)  return { allowed: true,  reason: null };
  if (state.manual_pause) return { allowed: false, reason: 'Trading manually paused' };
  const hour = getCurrentHourIST();
  if (state.enabled && (hour < state.start_hour || hour >= state.end_hour))
    return { allowed: false, reason: `Outside hours (${state.start_hour}:00–${state.end_hour}:00 IST)` };
  return { allowed: true, reason: null };
}

// Daily reset cron (IST midnight)
async function resetDailyIfNeeded() {
  const state    = getRiskStateFromMem();
  const nowIST   = new Date().toLocaleString('en-US', { timeZone: 'Asia/Kolkata' });
  const todayStr = new Date(nowIST).toISOString().split('T')[0];
  if (state.last_reset !== todayStr) {
    mem.riskState = makeDefaultRiskState();
    mem.riskState.last_reset = todayStr;
    saveRiskStateAsync();
    console.log('🔄 Daily risk state reset (IST midnight)');
  }
}
cron.schedule('0 * * * *', resetDailyIfNeeded);

// ════════════════════════════════════════════════════════════════
// SERVER-SIDE SIGNAL ENGINE
// Primary: browser POSTs signal via /compute-signal when open.
// Fallback: server fetches klines itself when browser is absent
//           (lastBrowserSignal not received for > 10 min).
// Kline source chain: Bybit → CryptoCompare → CoinGecko OHLC
// Runs every 5 min, only during trading hours (18:00–23:00 IST).
// ════════════════════════════════════════════════════════════════
let lastBrowserSignalAt = 0;           // timestamp of last /compute-signal POST from browser
const BROWSER_TIMEOUT_MS = 10 * 60 * 1000; // 10 min — if exceeded, server takes over
let serverEngineRunning  = false;       // lock to prevent overlapping runs

// ── Multi-source kline fetcher ────────────────────────────────
async function fetchKlinesServer(limit = 350) {
  // Source 1: Bybit REST klines (not geo-blocked on Render for klines)
  try {
    const res  = await fetchWithTimeout(
      `https://api.bybit.com/v5/market/kline?category=spot&symbol=BTCUSDT&interval=5&limit=${limit}`,
      {}, 8000
    );
    const data = await res.json();
    if (data?.result?.list?.length > 0) {
      console.log('[ENGINE] Klines from Bybit');
      // Bybit returns newest-first → reverse to oldest-first
      return data.result.list.reverse().map(c => [
        parseInt(c[0]), c[1], c[2], c[3], c[4], c[5],
        parseInt(c[0]) + 299999, c[6], 0, '0', '0', '0',
      ]);
    }
  } catch (e) { console.warn('[ENGINE] Bybit klines failed:', e.message); }

  // Source 2: CryptoCompare (5-min aggregate)
  try {
    const res  = await fetchWithTimeout(
      `https://min-api.cryptocompare.com/data/v2/histominute?fsym=BTC&tsym=USD&limit=${limit}&aggregate=5`,
      {}, 8000
    );
    const data = await res.json();
    if (data?.Data?.Data?.length > 0) {
      console.log('[ENGINE] Klines from CryptoCompare');
      return data.Data.Data.map(c => [
        c.time * 1000, String(c.open), String(c.high), String(c.low), String(c.close),
        String(c.volumefrom), c.time * 1000 + 299999, String(c.volumeto), 0, '0', '0', '0',
      ]);
    }
  } catch (e) { console.warn('[ENGINE] CryptoCompare klines failed:', e.message); }

  // Source 3: CoinGecko OHLC (hourly only — last resort, less accurate for 5m)
  try {
    const res  = await fetchWithTimeout(
      'https://api.coingecko.com/api/v3/coins/bitcoin/ohlc?vs_currency=usd&days=1',
      {}, 8000
    );
    const data = await res.json();
    if (data?.length > 0) {
      console.log('[ENGINE] Klines from CoinGecko OHLC (hourly — reduced accuracy)');
      return data.map(c => [
        c[0], String(c[1]), String(c[2]), String(c[3]), String(c[4]),
        '0', c[0] + 3599999, '0', 0, '0', '0', '0',
      ]);
    }
  } catch (e) { console.warn('[ENGINE] CoinGecko OHLC failed:', e.message); }

  console.error('[ENGINE] All kline sources failed');
  return null;
}

// ── Core server signal tick ───────────────────────────────────
async function serverSignalTick() {
  if (serverEngineRunning) return;
  serverEngineRunning = true;
  try {
    const { allowed } = isTradingAllowed();
    if (!allowed) { console.log('[ENGINE] Outside trading hours — skipping'); return; }

    // If browser recently posted a signal, don't duplicate
    const browserActive = (Date.now() - lastBrowserSignalAt) < BROWSER_TIMEOUT_MS;
    if (browserActive) {
      console.log('[ENGINE] Browser active — server fallback not needed');
      return;
    }

    console.log('[ENGINE] Browser absent — running server-side signal computation');
    const klines = await fetchKlinesServer(350);
    if (!klines || klines.length < 50) {
      console.error('[ENGINE] Not enough klines — aborting tick');
      return;
    }

    // Exclude last forming candle
    const confirmed = klines.slice(0, -1);
    const last      = confirmed[confirmed.length - 1];

    const df1 = calcUtbot(confirmed, 2, 1);
    const df2 = calcUtbot(confirmed, 2, 300);

    const sig1  = df1.pos[df1.pos.length - 1];
    const sig2  = df2.pos[df2.pos.length - 1];
    const stop1 = df1.stop[df1.stop.length - 1];
    const stop2 = df2.stop[df2.stop.length - 1];
    const atr   = df1.atr[df1.atr.length - 1] || 0;
    const price = parseFloat(last[4]);

    let signal = 'Hold', utbotStop = null;
    if (sig2 === 1)  { signal = 'Buy';  utbotStop = stop2; }
    if (sig1 === -1) { signal = 'Sell'; utbotStop = stop1; }

    const candle = {
      ts:    parseInt(last[0]),
      open:  parseFloat(last[1]),
      high:  parseFloat(last[2]),
      low:   parseFloat(last[3]),
      close: price,
    };

    console.log(`[ENGINE] Server signal: ${signal} @ $${price.toFixed(2)} ATR:${atr.toFixed(2)}`);

    // Run through the same trade logic as /compute-signal
    await processSignal(signal, price, atr, utbotStop || price, candle);

  } catch (e) {
    console.error('[ENGINE] Server tick error:', e.message);
  } finally {
    serverEngineRunning = false;
  }
}

// Shared signal processing — used by both /compute-signal and serverSignalTick
async function processSignal(signal, price, atr, utbotStop, candle) {
  const { allowed, reason } = isTradingAllowed();
  if (!allowed) return { ok: true, action: 'PAUSED', reason };

  const openTrade = mem.trades.open_trade;

  // OHLC SL/TP check (primary)
  if (openTrade && candle) {
    if (openTrade.type === 'LONG') {
      if (candle.low <= openTrade.stop_loss) {
        const { tradeRecord } = await closeFullPositionInMem(openTrade, openTrade.stop_loss, 'SL Hit (OHLC)');
        addOrderLog('SL_HIT_OHLC', 'Sell', openTrade.stop_loss, openTrade.amount, tradeRecord.profit_inr);
        mem.trades.last_closed_time = Date.now(); mem.trades.last_close_reason = 'sl';
        saveTradesAsync(); saveRiskStateAsync();
        return { ok: true, action: 'SL_HIT', trade: tradeRecord };
      }
      const nextTP = openTrade.tp_levels?.find(t => !t.hit);
      if (nextTP && candle.high >= nextTP.price) {
        if (nextTP.name === 'TP1') {
          const { tradeRecord } = await closePartialPositionInMem(openTrade, nextTP.price, 0.6, 'TP1 Hit (OHLC)');
          addOrderLog('TP1_OHLC', 'Sell', nextTP.price, 0, tradeRecord.profit_inr);
          mem.trades.last_close_reason = 'tp';
          saveTradesAsync(); saveRiskStateAsync();
          return { ok: true, action: 'TP1_HIT', trade: tradeRecord };
        } else {
          const { tradeRecord } = await closeFullPositionInMem(openTrade, nextTP.price, `${nextTP.name} Hit (OHLC)`);
          addOrderLog(`${nextTP.name}_OHLC`, 'Sell', nextTP.price, openTrade.amount, tradeRecord.profit_inr);
          mem.trades.last_closed_time = Date.now(); mem.trades.last_close_reason = 'tp';
          saveTradesAsync(); saveRiskStateAsync();
          return { ok: true, action: 'TP_HIT', trade: tradeRecord };
        }
      }
    } else { // SHORT
      if (candle.high >= openTrade.stop_loss) {
        const { tradeRecord } = await closeFullPositionInMem(openTrade, openTrade.stop_loss, 'SL Hit (OHLC)');
        addOrderLog('SL_HIT_OHLC', 'Buy', openTrade.stop_loss, openTrade.amount, tradeRecord.profit_inr);
        mem.trades.last_closed_time = Date.now(); mem.trades.last_close_reason = 'sl';
        saveTradesAsync(); saveRiskStateAsync();
        return { ok: true, action: 'SL_HIT', trade: tradeRecord };
      }
      const nextTP = openTrade.tp_levels?.find(t => !t.hit);
      if (nextTP && candle.low <= nextTP.price) {
        const { tradeRecord } = await closeFullPositionInMem(openTrade, nextTP.price, `${nextTP.name} Hit (OHLC)`);
        addOrderLog(`${nextTP.name}_OHLC`, 'Buy', nextTP.price, openTrade.amount, tradeRecord.profit_inr);
        mem.trades.last_closed_time = Date.now(); mem.trades.last_close_reason = 'tp';
        saveTradesAsync(); saveRiskStateAsync();
        return { ok: true, action: 'TP_HIT', trade: tradeRecord };
      }
    }
    // In trade — no new entries
    if (mem.trades.open_trade) return { ok: true, action: 'IN_TRADE' };
  }

  const result = await updateDemoTrade(signal, price, atr || 0, utbotStop || price);
  return { ok: true, action: result.action, ...result };
}

// ── Trading session scheduler ─────────────────────────────────
// Cron runs every minute. At session start (18:00) → immediate signal.
// At session end (23:00) → force close any open trade then stop.
// During session → server signal engine fires every 5 min.

let sessionActive     = false; // tracks whether we're inside trading hours
let signalCronHandle  = null;  // handle for the 5-min signal cron

function startTradingSession() {
  if (sessionActive) return;
  sessionActive = true;
  console.log('🟢 [SESSION] Trading session started (18:00 IST) — running immediate signal check');

  // Immediate signal check at session open
  serverSignalTick();

  // Schedule signal every 5 minutes during session
  // */5 * * * * = every 5 minutes
  signalCronHandle = cron.schedule('*/5 * * * *', () => {
    serverSignalTick();
  });
}

async function stopTradingSession() {
  if (!sessionActive) return;
  sessionActive = false;
  console.log('🔴 [SESSION] Trading session ended (23:00 IST) — force closing any open trade');

  // Stop 5-min signal cron
  if (signalCronHandle) {
    signalCronHandle.stop();
    signalCronHandle = null;
  }

  // Force close open trade at market price
  const openTrade = mem.trades.open_trade;
  if (openTrade) {
    const price = livePrice || await getCurrentPrice();
    if (price) {
      const closed = await forceClosePosition(price, 'Session End (23:00 IST)');
      console.log(`🔴 [SESSION] Force closed ${openTrade.type} @ $${price.toFixed(2)} | P/L:₹${closed?.profit_inr?.toFixed(2)}`);
    } else {
      console.error('[SESSION] Could not get price to force close — trade left open');
    }
  } else {
    console.log('[SESSION] No open trade to close');
  }
}

// Session watchdog — runs every minute, triggers start/stop at exact hours
cron.schedule('* * * * *', async () => {
  const state = getTradingStateFromMem();

  // Force start overrides session scheduler
  if (state.force_start || state.manual_pause) return;
  if (!state.enabled) return;

  const hour = getCurrentHourIST();
  const min  = getCurrentMinuteIST();

  // 18:00 → start session
  if (hour === state.start_hour && min === 0 && !sessionActive) {
    startTradingSession();
  }

  // 23:00 → stop session
  if (hour === state.end_hour && min === 0 && sessionActive) {
    await stopTradingSession();
  }

  // If we're mid-session (e.g. server restarted during trading hours)
  // and the signal cron isn't running, restart it
  if (hour >= state.start_hour && hour < state.end_hour && !sessionActive && !state.manual_pause) {
    console.log('[SESSION] Server restarted mid-session — resuming signal engine');
    startTradingSession();
  }
});

function getRiskStatus() {
  const config = getRiskConfigFromMem();
  const state  = getRiskStateFromMem();
  const limits = config.daily_limits;
  return {
    daily_stats: {
      trades: `${state.daily_trades}/${limits.max_daily_trades}`,
      loss:   `₹${state.daily_loss.toFixed(2)}/₹${limits.max_daily_loss.toFixed(2)}`,
      profit: `₹${state.daily_profit.toFixed(2)}`,
    },
    limits_usage: {
      trades_pct: (state.daily_trades / limits.max_daily_trades) * 100,
      loss_pct:   (state.daily_loss   / limits.max_daily_loss)   * 100,
    },
    config,
  };
}

// ════════════════════════════════════════════════════════════════
// EXPRESS ROUTES
// ════════════════════════════════════════════════════════════════
app.get('/ping', (req, res) => {
  const browserActive = (Date.now() - lastBrowserSignalAt) < BROWSER_TIMEOUT_MS;
  res.send(`pong | ws:${wsConnected ? wsSource : 'down'} | $${livePrice?.toFixed(2) || 'N/A'} | session:${sessionActive ? 'ACTIVE' : 'OFF'} | engine:${browserActive ? 'browser' : 'server'} | ${new Date().toISOString()}`);
});

app.get('/ws-status', (req, res) => {
  const browserActive = (Date.now() - lastBrowserSignalAt) < BROWSER_TIMEOUT_MS;
  res.json({
    connected: wsConnected, source: wsSource || null, live_price: livePrice,
    fail_count: wsFailCount, last_check: new Date(lastWsSync).toISOString(),
    session_active: sessionActive,
    engine_source: browserActive ? 'browser' : 'server',
    last_browser_signal: lastBrowserSignalAt ? new Date(lastBrowserSignalAt).toISOString() : null,
  });
});

app.get('/', (req, res) => res.sendFile(__dirname + '/index.html'));

// ── /signal — serves current state from RAM (zero Redis) ──────
// Browser calls this every 5s for live display.
// Signal computation is now done by browser → /compute-signal.
// This route only returns the current RAM state.
app.get('/signal', async (req, res) => {
  try {
    const { allowed, reason } = isTradingAllowed();
    const data      = mem.trades;
    const openTrade = data.open_trade;
    const price     = livePrice || (await getCurrentPrice()) || 0;
    const usdtInr   = getUSDTINRFromMem();
    const livePlUsdt = calculateLivePL(openTrade, price);
    const livePlInr  = livePlUsdt != null ? livePlUsdt * usdtInr : null;
    const riskStatus = getRiskStatus();

    res.json({
      price, signal: 'Hold', balance: data.balance,
      holding: !!openTrade, position_type: openTrade?.type || null,
      entry_price: openTrade?.entry_price || null,
      action: !allowed ? `⏸️ PAUSED: ${reason}` : (data.order_log?.slice(-1)[0]?.action || ''),
      latest_order: data.order_log?.slice(-1)[0] || null,
      live_pl_inr: livePlInr,
      stop_loss: openTrade?.stop_loss || null,
      tp_levels: openTrade?.tp_levels || [],
      position_size: openTrade?.amount || 0,
      atr: 0, // ATR now computed in browser
      risk_status: riskStatus,
      trading_allowed: allowed, pause_reason: reason || null,
      force_start: getTradingStateFromMem().force_start,
      cooldown: { active: false, message: null },
      ws_connected: wsConnected, ws_source: wsSource,
      strategy_info: { buy_strategy: 'UT Bot #2 (KV=2, ATR=300)', sell_strategy: 'UT Bot #1 (KV=2, ATR=1)' },
    });
  } catch (err) {
    console.error('/signal error:', err);
    res.status(500).json({ error: err.message });
  }
});

// ── /compute-signal — receives browser-computed signal ─────────
// Browser fetches klines, runs UT Bot logic, sends result here.
// Also updates lastBrowserSignalAt so server fallback knows
// browser is active and doesn't duplicate signal computation.
// Body: { signal, price, atr, utbot_stop, candle }
app.post('/compute-signal', async (req, res) => {
  try {
    const { signal, price, atr, utbot_stop, candle } = req.body;
    if (!signal || !price) return res.json({ ok: false, msg: 'Missing signal or price' });

    // Mark browser as active — suppresses server fallback for 10 min
    lastBrowserSignalAt = Date.now();

    console.log(`[SIGNAL-IN] Browser: ${signal} @ $${price} ATR:${atr?.toFixed(2)}`);

    const result = await processSignal(signal, price, atr || 0, utbot_stop || price, candle);
    return res.json(result);

  } catch (e) {
    console.error('/compute-signal error:', e);
    res.status(500).json({ ok: false, error: e.message });
  }
});

// ── Chart data (no Redis) ─────────────────────────────────────
app.get('/chart-data', async (req, res) => {
  try {
    const klines = await binanceRequest('/api/v3/klines', { symbol: 'BTCUSDT', interval: '5m', limit: 350 });
    if (!klines) return res.status(500).json({ error: 'No kline data' });
    const candles  = klines.map(k => ({ time: Math.floor(k[0] / 1000), open: parseFloat(k[1]), high: parseFloat(k[2]), low: parseFloat(k[3]), close: parseFloat(k[4]) }));
    const df1      = calcUtbot(klines, 2, 1);
    const stopLine = df1.stop.map((val, idx) => ({ time: candles[idx].time, value: val }));
    res.json({ candles, stop_line: stopLine });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ── History (from RAM — zero Redis) ───────────────────────────
app.get('/history', (req, res) => res.json(mem.trades.history || []));
app.get('/orders',  (req, res) => res.json((mem.trades.order_log || []).slice().reverse()));

app.get('/status', async (req, res) => {
  const price      = livePrice || (await getCurrentPrice());
  const usdtInr    = getUSDTINRFromMem();
  const livePlUsdt = calculateLivePL(mem.trades.open_trade, price);
  res.json({
    balance: mem.trades.balance, has_open_trade: !!mem.trades.open_trade,
    open_trade: mem.trades.open_trade, current_price: price,
    live_pl_inr: livePlUsdt != null ? livePlUsdt * usdtInr : null,
    last_signal: mem.trades.last_signal, total_trades: mem.trades.history?.length || 0,
    risk_status: getRiskStatus(),
    force_start: getTradingStateFromMem().force_start,
    ws_connected: wsConnected, ws_source: wsSource,
  });
});

// ── Risk config (from RAM) ────────────────────────────────────
app.get('/risk-config', (req, res) => res.json(mem.riskConfig));
app.post('/risk-config', (req, res) => {
  try {
    mem.riskConfig = req.body;
    saveRiskConfigAsync();
    res.json({ success: true });
  } catch (err) { res.status(400).json({ success: false, error: err.message }); }
});

app.get('/risk-status', (req, res) => res.json(getRiskStatus()));

// ── Trading control (from RAM) ────────────────────────────────
app.get('/trading-control', (req, res) => {
  const { allowed, reason } = isTradingAllowed();
  res.json({ state: getTradingStateFromMem(), trading_allowed: allowed, pause_reason: reason, current_time: new Date().toLocaleTimeString() });
});

app.post('/trading-control', async (req, res) => {
  try {
    const { action } = req.body;
    const state = mem.tradingState;
    switch (action) {
      case 'pause':       state.manual_pause = true;  state.force_start = false; saveTradingStateAsync(); return res.json({ success: true, message: 'Trading paused' });
      case 'resume':      state.manual_pause = false; state.force_start = false; saveTradingStateAsync(); return res.json({ success: true, message: 'Trading resumed' });
      case 'force_start': state.manual_pause = false; state.force_start = true;  saveTradingStateAsync(); return res.json({ success: true, message: 'Force start 24/7' });
      case 'force_stop': {
        const price = livePrice || await getCurrentPrice();
        if (price) {
          const closed = await forceClosePosition(price, 'Force Stop');
          state.manual_pause = true; state.force_start = false; saveTradingStateAsync();
          return res.json({ success: true, message: closed ? `Closed @ $${price.toFixed(2)} P/L:₹${closed.profit_inr.toFixed(2)}` : 'No open position' });
        }
        return res.json({ success: false, message: 'Could not get price' });
      }
      case 'update_hours':
        state.start_hour = req.body.start_hour ?? state.start_hour;
        state.end_hour   = req.body.end_hour   ?? state.end_hour;
        state.enabled    = req.body.enabled    ?? state.enabled;
        saveTradingStateAsync();
        return res.json({ success: true, message: 'Hours updated' });
      default: return res.status(400).json({ success: false, error: 'Invalid action' });
    }
  } catch (err) { res.status(400).json({ success: false, error: err.message }); }
});

// ── USDT/INR (from RAM) ───────────────────────────────────────
app.get('/usdt-inr-rate', (req, res) => res.json({ rate: getUSDTINRFromMem() }));
app.post('/usdt-inr-rate', (req, res) => {
  try {
    const { rate } = req.body;
    if (typeof rate !== 'number' || rate <= 0) throw new Error('Invalid rate');
    mem.usdtInr = rate;
    saveUSDTINRAsync();
    res.json({ success: true });
  } catch (err) { res.status(400).json({ success: false, error: err.message }); }
});

// ── Clear history ─────────────────────────────────────────────
app.post('/clear-history', async (req, res) => {
  try {
    mem.trades = makeDefaultTrades();
    mem.riskState = makeDefaultRiskState();
    saveTradesAsync();
    saveRiskStateAsync();
    res.json({ success: true, message: 'All trades cleared, balance reset.' });
  } catch (err) { res.status(500).json({ success: false, error: err.message }); }
});

// ── Export CSV ────────────────────────────────────────────────
app.get('/export-history', (req, res) => {
  const trades = mem.trades.history;
  if (!trades.length) return res.status(404).json({ error: 'No trades to export' });
  const headers = ['Type','Entry Price','Exit Price','Amount (BTC)','Stop Loss','TP1 Price','Profit (USDT)','Profit (INR)','Exit Reason','Opened At','Closed At','Duration (s)','Partial'];
  const rows = trades.map(t => [
    t.type, t.entry_price, t.exit_price, t.amount, t.stop_loss ?? 'N/A', t.tp1_price ?? 'N/A',
    t.profit_usdt, t.profit_inr, t.exit_reason,
    new Date(t.opened_at).toISOString(), new Date(t.closed_at).toISOString(),
    t.duration_ms ? (t.duration_ms / 1000).toFixed(1) : 'N/A', t.partial ? 'Yes' : 'No',
  ]);
  const csv = [headers, ...rows].map(r => r.join(',')).join('\n');
  res.setHeader('Content-Type', 'text/csv');
  res.setHeader('Content-Disposition', 'attachment; filename=trade_history.csv');
  res.send(csv);
});

// ════════════════════════════════════════════════════════════════
// BOOT
// ════════════════════════════════════════════════════════════════
app.listen(port, async () => {
  console.log(`✅ Server on port ${port}`);
  await loadAllFromRedis();       // load all state into RAM from Redis
  await resetDailyIfNeeded();     // check if daily reset needed
  connectBybitWS();               // start price feed

  // If we boot inside trading hours, start session immediately
  const state = getTradingStateFromMem();
  const hour  = getCurrentHourIST();
  if (!state.manual_pause && !state.force_start && state.enabled) {
    if (hour >= state.start_hour && hour < state.end_hour) {
      console.log(`🟢 [BOOT] Server started inside trading hours (${hour}:xx IST) — starting session`);
      startTradingSession();
    } else {
      console.log(`⏰ [BOOT] Outside trading hours (${hour}:xx IST) — waiting for ${state.start_hour}:00`);
    }
  } else if (state.force_start) {
    console.log('🔥 [BOOT] Force start active — starting session immediately');
    startTradingSession();
  }

  console.log(`🏓 Self-pinger: every 4min → ${SELF_URL}/ping`);
  console.log(`📊 Redis target: ~3,000 ops/day`);
  console.log(`⏰ Trading hours: ${state.start_hour}:00–${state.end_hour}:00 IST`);
  console.log(`🔄 Server fallback: kicks in after 10 min browser silence`);
});
