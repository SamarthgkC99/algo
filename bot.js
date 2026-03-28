// bot.js – UT Bot Trading System with Risk Management (Node.js + Upstash Redis REST)
// ✅ Fixed: All fetch() calls use AbortController for proper timeouts
// ✅ Fixed: breakeven_moved correctly set after TP1 partial close
// ✅ Fixed: Trailing stop fires correctly after breakeven is moved
// ✅ Fixed: TP1 = partial close (60% LONG / 100% SHORT) + breakeven move
// ✅ Fixed: tp_levels stores all levels (not sliced to 1)
// ✅ Fixed: /signal response includes cooldown object for frontend
// ✅ Fixed: Daily loss default tightened to ₹400 (4% of ₹10,000)
// ✅ Added: Condition-based cooldown after SL hit
// ✅ Added: Asymmetric exit — LONG: 60% at TP1 trail rest; SHORT: 100% at TP1
// ✅ Added: SL at 2.5×ATR; TP1 at 1.5×ATR, TP2 at 3×ATR
// ✅ NEW:   WebSocket price watcher — checks SL/TP on every tick (no more missed hits)
// ✅ NEW:   Built-in self-pinger — keeps Render server awake every 4 minutes
// ✅ NEW:   WebSocket fallback chain — Bybit WS (primary) → REST polling fallback

require('dotenv').config();
const express   = require('express');
const { Redis } = require('@upstash/redis');
const fetch     = require('node-fetch');
const cron      = require('node-cron');
const WebSocket = require('ws');

const app  = express();
const port = process.env.PORT || 5000;

// ------------------------------
// Upstash Redis REST Configuration
// ------------------------------
const HARDCODED_URL   = 'https://robust-kitten-78595.upstash.io';
const HARDCODED_TOKEN = 'gQAAAAAAATMDAAIncDEyZjJkNzQyMDQyN2Q0ODEwOTI1ZGY4MTczMWM4MGQzYnAxNzg1OTU';

const redisUrl   = process.env.UPSTASH_REDIS_REST_URL   || HARDCODED_URL;
const redisToken = process.env.UPSTASH_REDIS_REST_TOKEN || HARDCODED_TOKEN;

if (!redisUrl || !redisToken) {
  console.error('❌ Missing Upstash Redis REST URL or token.');
  process.exit(1);
}

const redis = new Redis({ url: redisUrl, token: redisToken });
redis.ping()
  .then(() => console.log('✅ Connected to Upstash Redis'))
  .catch(err => console.error('Redis error:', err));

// ------------------------------
// Constants
// ------------------------------
const START_BALANCE = 10000;

const TRADES_KEY        = 'demo_trades';
const RISK_STATE_KEY    = 'risk_state';
const RISK_CONFIG_KEY   = 'risk_config';
const TRADING_STATE_KEY = 'trading_state';
const USDT_INR_KEY      = 'usdt_inr_rate';

// ------------------------------
// WebSocket State
// Live price tracked by WebSocket — used for real-time SL/TP checks
// ------------------------------
let livePrice      = null;   // latest tick price from WebSocket
let wsConnected    = false;  // whether WebSocket is currently connected
let wsSource       = null;   // 'binance' | 'bybit'
let wsReconnectTimer = null;
let wsInstance     = null;
let wsFailCount    = 0;      // consecutive WS failures — triggers fallback source

// ------------------------------
// Utility: fetch with AbortController timeout
// ------------------------------
async function fetchWithTimeout(url, options = {}, timeoutMs = 7000) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const res = await fetch(url, { ...options, signal: controller.signal });
    clearTimeout(timer);
    return res;
  } catch (err) {
    clearTimeout(timer);
    throw err;
  }
}

// ------------------------------
// Helper: Load / Save Data from Redis
// ------------------------------
async function loadTrades() {
  const data = await redis.get(TRADES_KEY);
  if (!data) {
    const defaultData = {
      balance: START_BALANCE,
      open_trade: null,
      history: [],
      order_log: [],
      last_signal: null,
      last_closed_time: null,
      last_closed_side: null,
      last_closed_sl_price: null,
      last_close_reason: null,
      pending_opposite_side: null,
      pending_opposite_time: null
    };
    await redis.set(TRADES_KEY, JSON.stringify(defaultData));
    return defaultData;
  }
  return typeof data === 'string' ? JSON.parse(data) : data;
}

async function saveTrades(data) {
  await redis.set(TRADES_KEY, JSON.stringify(data));
}

// ------------------------------
// Risk State Management with IST date reset
// ------------------------------
async function loadRiskState() {
  const data = await redis.get(RISK_STATE_KEY);
  if (!data) return resetRiskState();
  let state = typeof data === 'string' ? JSON.parse(data) : data;
  if (typeof state.last_reset === 'number') {
    const istDate = new Date(state.last_reset).toLocaleString('en-US', { timeZone: 'Asia/Kolkata' });
    state.last_reset = new Date(istDate).toISOString().split('T')[0];
    await saveRiskState(state);
  }
  return state;
}

async function saveRiskState(state) {
  await redis.set(RISK_STATE_KEY, JSON.stringify(state));
}

async function resetRiskState() {
  const nowIST   = new Date().toLocaleString('en-US', { timeZone: 'Asia/Kolkata' });
  const todayStr = new Date(nowIST).toISOString().split('T')[0];
  const state = {
    daily_loss: 0, daily_profit: 0, daily_trades: 0,
    last_reset: todayStr, peak_balance: START_BALANCE
  };
  await saveRiskState(state);
  return state;
}

async function loadRiskConfig() {
  const data = await redis.get(RISK_CONFIG_KEY);
  if (!data) {
    const defaultConfig = {
      stop_loss: {
        enabled: true,
        type: 'hybrid',
        atr_multiplier: 2.5,
        max_loss_percentage: 3.0,
        trailing_enabled: true,
        trailing_atr_multiplier: 1.5
      },
      take_profit: {
        enabled: true,
        type: 'asymmetric',
        levels: [
          { percentage: 60, atr_multiplier: 1.5, name: 'TP1' },
          { percentage: 40, atr_multiplier: 3.0, name: 'TP2' }
        ]
      },
      position_sizing: {
        method: 'risk_fixed',
        value: 5.0,
        min_position_size: 0.0001,
        max_position_size: 0.01,
        risk_amount: 200
      },
      daily_limits: {
        enabled: true,
        max_daily_loss: 400.0,
        max_daily_trades: 20,
        reset_hour: 0
      },
      account_protection: {
        max_drawdown_percentage: 15.0,
        min_balance: 5000.0,
        emergency_stop: false
      },
      different_rules_for_position_type: {
        enabled: true,
        long:  { tp_atr_multipliers: [1.5, 3.0] },
        short: { tp_atr_multipliers: [1.5, 2.5] }
      },
      cooldown: {
        sl_condition_based: true,
        tp_cooldown_ms: 0,
        opposite_confirmation_ms: 30000,
        sl_fallback_ms: 120000
      }
    };
    await saveRiskConfig(defaultConfig);
    return defaultConfig;
  }
  return typeof data === 'string' ? JSON.parse(data) : data;
}

async function saveRiskConfig(config) {
  await redis.set(RISK_CONFIG_KEY, JSON.stringify(config));
}

async function loadTradingState() {
  const data = await redis.get(TRADING_STATE_KEY);
  if (!data) {
    const defaultState = { enabled: true, start_hour: 18, end_hour: 23, manual_pause: false, force_start: false };
    await redis.set(TRADING_STATE_KEY, JSON.stringify(defaultState));
    return defaultState;
  }
  return typeof data === 'string' ? JSON.parse(data) : data;
}

async function saveTradingState(state) {
  await redis.set(TRADING_STATE_KEY, JSON.stringify(state));
}

// ------------------------------
// USDT/INR Rate
// ------------------------------
async function getUSDTINR() {
  let rate = await redis.get(USDT_INR_KEY);
  if (!rate) { rate = 85; await redis.set(USDT_INR_KEY, rate); }
  return parseFloat(rate);
}

async function setUSDTINR(rate) {
  await redis.set(USDT_INR_KEY, parseFloat(rate));
}

// ------------------------------
// Helper: Current hour in IST
// ------------------------------
function getCurrentHourIST() {
  const istString = new Date().toLocaleString('en-US', { timeZone: 'Asia/Kolkata', hour: 'numeric', hour12: false });
  return parseInt(istString);
}

// ================================================================
// WEBSOCKET PRICE WATCHER
// Opens a persistent real-time stream from Binance or Bybit.
// Every price tick triggers a SL/TP check immediately.
// No more missed hits between cron pings.
// ================================================================

// --- Throttle: WebSocket SL/TP check runs at most once per second
// to avoid hammering Redis on every single tick
let lastWsCheckTime = 0;
const WS_CHECK_INTERVAL_MS = 1000; // 1 second throttle

// Lock to prevent concurrent SL/TP checks from overlapping
let wsCheckInProgress = false;

function onPriceTick(price) {
  livePrice = price;
  const now = Date.now();
  if (wsCheckInProgress) return;
  if (now - lastWsCheckTime < WS_CHECK_INTERVAL_MS) return;
  lastWsCheckTime = now;
  // Fire and forget — non-blocking
  checkSlTpOnTick(price).catch(err => console.error('WS SL/TP check error:', err));
}

async function checkSlTpOnTick(price) {
  wsCheckInProgress = true;
  try {
    const data = await loadTrades();
    const openTrade = data.open_trade;
    if (!openTrade) return; // nothing to check

    const posType  = openTrade.type;
    const sl       = openTrade.stop_loss;
    const nextTP   = openTrade.tp_levels?.find(t => !t.hit) || null;
    const tp1Price = nextTP?.price || openTrade.tp1_price || null;

    const slHit  = sl      && ((posType === 'LONG'  && price <= sl)       || (posType === 'SHORT' && price >= sl));
    const tp1Hit = tp1Price && ((posType === 'LONG'  && price >= tp1Price) || (posType === 'SHORT' && price <= tp1Price));

    if (!slHit && !tp1Hit) return; // nothing triggered

    const config = await loadRiskConfig();

    if (slHit) {
      console.log(`🛑 [WS] STOP-LOSS triggered @ $${price.toFixed(2)} | SL was $${sl.toFixed(2)}`);
      const { tradeRecord, side } = await closeFullPosition(data, openTrade, price, 'Stop-Loss Hit (WS)');
      const logEntry = {
        time: new Date().toISOString(), side: posType === 'LONG' ? 'Sell' : 'Buy',
        action: 'STOP_LOSS_WS', price, quantity: openTrade.amount,
        pl_inr: tradeRecord.profit_inr
      };
      if (!data.order_log) data.order_log = [];
      data.order_log.push(logEntry);
      if (data.order_log.length > 100) data.order_log.shift();
      data.last_closed_time     = Date.now();
      data.last_closed_side     = side;
      data.last_closed_sl_price = sl;
      data.last_close_reason    = 'sl';
      data.pending_opposite_side  = null;
      data.pending_opposite_time  = null;
      await saveTrades(data);
      console.log(`✅ [WS] SL closed. P/L: ₹${tradeRecord.profit_inr.toFixed(2)} | Balance: ₹${data.balance.toFixed(2)}`);

    } else if (tp1Hit) {
      if (posType === 'LONG') {
        // Partial close 60%, move SL to breakeven
        console.log(`✅ [WS] TP1 triggered (LONG) @ $${price.toFixed(2)} | TP was $${tp1Price.toFixed(2)}`);
        const { tradeRecord } = await closePartialPosition(data, openTrade, price, 0.6, 'TP1 Hit (WS)');
        const logEntry = {
          time: new Date().toISOString(), side: 'Sell',
          action: 'TP1_PARTIAL_LONG_WS', price, quantity: openTrade.amount * 0.6,
          pl_inr: tradeRecord.profit_inr
        };
        if (!data.order_log) data.order_log = [];
        data.order_log.push(logEntry);
        if (data.order_log.length > 100) data.order_log.shift();
        data.last_close_reason = 'tp';
        // open_trade still set (partial close updates it in place)
        await saveTrades(data);
        console.log(`✅ [WS] TP1 partial closed 60%. SL → breakeven $${openTrade.entry_price.toFixed(2)} | P/L so far: ₹${tradeRecord.profit_inr.toFixed(2)}`);
      } else {
        // SHORT: full exit at TP1
        console.log(`✅ [WS] TP1 triggered (SHORT) @ $${price.toFixed(2)} | TP was $${tp1Price.toFixed(2)}`);
        const { tradeRecord, side } = await closeFullPosition(data, openTrade, price, 'TP1 Hit - Full Exit (Short WS)');
        const logEntry = {
          time: new Date().toISOString(), side: 'Buy',
          action: 'TP1_FULL_SHORT_WS', price, quantity: openTrade.amount,
          pl_inr: tradeRecord.profit_inr
        };
        if (!data.order_log) data.order_log = [];
        data.order_log.push(logEntry);
        if (data.order_log.length > 100) data.order_log.shift();
        data.last_closed_time     = Date.now();
        data.last_closed_side     = side;
        data.last_closed_sl_price = null;
        data.last_close_reason    = 'tp';
        data.pending_opposite_side  = null;
        data.pending_opposite_time  = null;
        await saveTrades(data);
        console.log(`✅ [WS] SHORT TP1 full closed. P/L: ₹${tradeRecord.profit_inr.toFixed(2)} | Balance: ₹${data.balance.toFixed(2)}`);
      }
    }
  } finally {
    wsCheckInProgress = false;
  }
}

// --- Bybit WebSocket (primary — Binance is geo-blocked on Render with 451)
function connectBybitWS() {
  const url = 'wss://stream.bybit.com/v5/public/spot';
  console.log('🔌 Connecting to Bybit WebSocket...');
  const ws = new WebSocket(url);

  ws.on('open', () => {
    console.log('✅ Bybit WebSocket connected');
    wsConnected = true;
    wsSource    = 'bybit';
    wsFailCount = 0;
    wsInstance  = ws;
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

  ws.on('error', (err) => {
    console.warn(`⚠️ Bybit WS error: ${err.message}`);
    wsConnected = false;
  });

  ws.on('close', () => {
    wsConnected = false;
    wsInstance  = null;
    wsFailCount++;
    const delay = Math.min(5000 * wsFailCount, 30000); // backoff: 5s, 10s, 15s... max 30s
    console.warn(`⚠️ Bybit WS closed (failures: ${wsFailCount}). Reconnecting in ${delay/1000}s...`);
    wsReconnectTimer = setTimeout(connectBybitWS, delay);
  });

  // Bybit requires a ping every 20s to keep connection alive
  const pingInterval = setInterval(() => {
    if (ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify({ op: 'ping' }));
    } else {
      clearInterval(pingInterval);
    }
  }, 20000);

  return ws;
}

// --- Polling fallback: if WebSocket is down for > 30s, use REST price check every 10s
let pollingFallbackTimer = null;

function startPollingFallback() {
  if (pollingFallbackTimer) return;
  console.log('⚠️ WS down — starting REST polling fallback (every 10s)');
  pollingFallbackTimer = setInterval(async () => {
    if (wsConnected) {
      clearInterval(pollingFallbackTimer);
      pollingFallbackTimer = null;
      console.log('✅ WS reconnected — stopping REST fallback');
      return;
    }
    const price = await getCurrentPrice();
    if (price) onPriceTick(price);
  }, 10000);
}

// Monitor WebSocket health — if down for 30s, start polling fallback
setInterval(() => {
  if (!wsConnected) startPollingFallback();
}, 30000);

// Start WebSocket on boot — Bybit only (Binance returns 451 on Render)
connectBybitWS();

// ================================================================
// BUILT-IN RENDER KEEP-ALIVE PINGER
// Pings the server's own /ping endpoint every 4 minutes.
// Prevents Render free tier from spinning down between external pings.
// ================================================================
const SELF_URL = process.env.RENDER_EXTERNAL_URL || `http://localhost:${port}`;

async function selfPing() {
  try {
    const res = await fetchWithTimeout(`${SELF_URL}/ping`, {}, 10000);
    const text = await res.text();
    console.log(`🏓 Self-ping OK: ${text.trim()} | WS: ${wsConnected ? `✅ ${wsSource}` : '❌ down'} | Live price: $${livePrice?.toFixed(2) || 'N/A'}`);
  } catch (err) {
    console.warn(`⚠️ Self-ping failed: ${err.message}`);
  }
}

// Ping every 4 minutes (Render spins down after ~15 mins of inactivity)
setInterval(selfPing, 4 * 60 * 1000);
// First ping after 30s (let server fully start first)
setTimeout(selfPing, 30000);

// ================================================================
// Binance API with Multi-Proxy Fallback (REST — for klines/signals)
// ================================================================
const BINANCE_ENDPOINTS = [
  'https://data-api.binance.vision',
  'https://api.binance.us',
  'https://api1.binance.com',
  'https://api2.binance.com',
  'https://api3.binance.com',
  'https://api4.binance.com',
  'https://api.binance.com',
  'https://api-gcp.binance.com'
];

async function binanceRequest(path, params = {}) {
  for (const endpoint of BINANCE_ENDPOINTS) {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 6000);
    try {
      const url = new URL(path, endpoint);
      Object.entries(params).forEach(([k, v]) => url.searchParams.append(k, v));
      const response = await fetch(url.toString(), {
        headers: {
          'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
          'Accept': 'application/json', 'Cache-Control': 'no-cache'
        },
        signal: controller.signal
      });
      clearTimeout(timeout);
      if (response.status === 200) {
        console.log(`✅ Binance REST from: ${endpoint}`);
        return await response.json();
      }
      console.warn(`⚠️ ${endpoint} returned ${response.status}`);
    } catch (err) {
      clearTimeout(timeout);
      console.warn(`⚠️ ${endpoint} error: ${err.message}`);
    }
  }
  return null;
}

async function fetchPriceFromCoinGecko() {
  try {
    const response = await fetchWithTimeout('https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd', {}, 6000);
    const data = await response.json();
    if (data?.bitcoin?.usd) { console.log('✅ Price from CoinGecko'); return data.bitcoin.usd; }
  } catch (err) { console.warn('CoinGecko price error:', err.message); }
  return null;
}

async function fetchKlinesFromCryptoCompare(limit = 350) {
  try {
    const response = await fetchWithTimeout(
      `https://min-api.cryptocompare.com/data/v2/histominute?fsym=BTC&tsym=USD&limit=${limit}&aggregate=5`, {}, 8000);
    const data = await response.json();
    if (data?.Data?.Data?.length > 0) {
      console.log('✅ Klines from CryptoCompare');
      return data.Data.Data.map(c => [
        c.time * 1000, String(c.open), String(c.high), String(c.low), String(c.close),
        String(c.volumefrom), c.time * 1000 + 299999, String(c.volumeto), 0, '0', '0', '0'
      ]);
    }
  } catch (err) { console.warn('CryptoCompare klines error:', err.message); }
  return null;
}

async function fetchKlinesFromBybit(limit = 350) {
  try {
    const response = await fetchWithTimeout(
      `https://api.bybit.com/v5/market/kline?category=spot&symbol=BTCUSDT&interval=5&limit=${limit}`, {}, 8000);
    const data = await response.json();
    if (data?.result?.list?.length > 0) {
      console.log('✅ Klines from Bybit');
      return data.result.list.reverse().map(c => [
        parseInt(c[0]), c[1], c[2], c[3], c[4], c[5],
        parseInt(c[0]) + 299999, c[6], 0, '0', '0', '0'
      ]);
    }
  } catch (err) { console.warn('Bybit klines error:', err.message); }
  return null;
}

async function getCurrentPrice() {
  // Use live WebSocket price first if available
  if (wsConnected && livePrice) return livePrice;
  const binancePrice = await binanceRequest('/api/v3/ticker/price', { symbol: 'BTCUSDT' });
  if (binancePrice?.price) return parseFloat(binancePrice.price);
  try {
    const res = await fetchWithTimeout('https://api.bybit.com/v5/market/tickers?category=spot&symbol=BTCUSDT', {}, 6000);
    const data = await res.json();
    if (data?.result?.list?.[0]?.lastPrice) return parseFloat(data.result.list[0].lastPrice);
  } catch (err) { console.warn('Bybit price error:', err.message); }
  return await fetchPriceFromCoinGecko();
}

async function getKlines(symbol = 'BTCUSDT', interval = '5m', limit = 350) {
  const binanceKlines = await binanceRequest('/api/v3/klines', { symbol, interval, limit });
  if (binanceKlines?.length > 0) return binanceKlines;
  const bybitKlines = await fetchKlinesFromBybit(limit);
  if (bybitKlines?.length > 0) return bybitKlines;
  return await fetchKlinesFromCryptoCompare(limit);
}

// ------------------------------
// UT Bot Logic
// ------------------------------
function calcUtbot(klines, keyvalue, atrPeriod) {
  const close = klines.map(k => parseFloat(k[4]));
  const high  = klines.map(k => parseFloat(k[2]));
  const low   = klines.map(k => parseFloat(k[3]));
  const tr = [];
  for (let i = 0; i < high.length; i++) {
    if (i === 0) tr.push(high[i] - low[i]);
    else tr.push(Math.max(high[i] - low[i], Math.abs(high[i] - close[i-1]), Math.abs(low[i] - close[i-1])));
  }
  const atr = [];
  for (let i = 0; i < tr.length; i++) {
    if (i < atrPeriod - 1) atr.push(null);
    else {
      let sum = 0;
      for (let j = i - atrPeriod + 1; j <= i; j++) sum += tr[j];
      atr.push(sum / atrPeriod);
    }
  }
  const nLoss = atr.map(a => a === null ? null : keyvalue * a);
  const xATRTrailingStop = [close[0]];
  const pos = [0];
  for (let i = 1; i < close.length; i++) {
    const src = close[i], src1 = close[i-1], prevStop = xATRTrailingStop[i-1], nl = nLoss[i];
    let newStop;
    if (src > prevStop && src1 > prevStop)      newStop = Math.max(prevStop, src - nl);
    else if (src < prevStop && src1 < prevStop) newStop = Math.min(prevStop, src + nl);
    else                                         newStop = src > prevStop ? src - nl : src + nl;
    xATRTrailingStop.push(newStop);
    let newPos;
    if (src1 < prevStop && src > prevStop)      newPos = 1;
    else if (src1 > prevStop && src < prevStop) newPos = -1;
    else                                         newPos = pos[i-1];
    pos.push(newPos);
  }
  return { stop: xATRTrailingStop, pos, atr };
}

async function getUTBotSignal() {
  const klines = await getKlines();
  if (!klines) return { signal: 'No Data', price: 0, atr: 0, utbot_stop: 0 };
  const price   = parseFloat(klines[klines.length-1][4]);
  const df1     = calcUtbot(klines, 2, 1);
  const df2     = calcUtbot(klines, 2, 300);
  const signal1 = df1.pos[df1.pos.length-1];
  const signal2 = df2.pos[df2.pos.length-1];
  const stop1   = df1.stop[df1.stop.length-1];
  const stop2   = df2.stop[df2.stop.length-1];
  const atr     = df1.atr[df1.atr.length-1] || 0;
  let signal = 'Hold', utbotStop = null;
  if (signal2 === 1)  { signal = 'Buy';  utbotStop = stop2; }
  if (signal1 === -1) { signal = 'Sell'; utbotStop = stop1; }
  // Use live WebSocket price if available (more accurate than last candle close)
  const currentPrice = (wsConnected && livePrice) ? livePrice : price;
  return { signal, price: currentPrice, atr, utbot_stop: utbotStop || currentPrice };
}

// ------------------------------
// Risk Management
// ------------------------------
async function calculatePositionSize(balance, entryPrice, type, stopLoss, atr, config) {
  const sizing = config.position_sizing;
  let size;
  switch (sizing.method) {
    case 'percentage': {
      const btcPriceInr = entryPrice * (await getUSDTINR());
      size = (balance * (sizing.value / 100)) / btcPriceInr;
      break;
    }
    case 'fixed':
      size = sizing.value;
      break;
    case 'risk_fixed': {
      if (!stopLoss) {
        const btcPriceInr = entryPrice * (await getUSDTINR());
        size = (balance * 0.05) / btcPriceInr;
      } else {
        const usdtInr = await getUSDTINR();
        size = sizing.risk_amount / (Math.abs(entryPrice - stopLoss) * usdtInr);
      }
      break;
    }
    default: size = 0.001;
  }
  return parseFloat(Math.min(sizing.max_position_size, Math.max(sizing.min_position_size, size)).toFixed(6));
}

function calculateStopLoss(entry, type, atr, utbotStop, config) {
  const slConf = config.stop_loss;
  if (!slConf.enabled) return null;
  const atrStop   = type === 'LONG' ? entry - atr * slConf.atr_multiplier : entry + atr * slConf.atr_multiplier;
  const fixedStop = type === 'LONG' ? entry * (1 - slConf.max_loss_percentage / 100) : entry * (1 + slConf.max_loss_percentage / 100);
  let stop;
  switch (slConf.type) {
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
  const tpConf = config.take_profit;
  if (!tpConf.enabled) return [];
  const multipliers = config.different_rules_for_position_type?.enabled
    ? (type === 'LONG'
        ? config.different_rules_for_position_type.long.tp_atr_multipliers
        : config.different_rules_for_position_type.short.tp_atr_multipliers)
    : tpConf.levels.map(l => l.atr_multiplier);
  return multipliers.map((mult, i) => {
    const price = type === 'LONG' ? entry + atr * mult : entry - atr * mult;
    const levelInfo = tpConf.levels[i] || { percentage: Math.floor(100 / multipliers.length), name: `TP${i+1}` };
    return { price: parseFloat(price.toFixed(2)), percentage: levelInfo.percentage, name: levelInfo.name, hit: false };
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

async function checkDailyLimits() {
  const config = await loadRiskConfig();
  const state  = await loadRiskState();
  const limits = config.daily_limits;
  if (!limits.enabled) return { allowed: true, reason: null };
  if (state.daily_loss   >= limits.max_daily_loss)   return { allowed: false, reason: `Daily loss limit reached (₹${state.daily_loss.toFixed(2)} / ₹${limits.max_daily_loss})` };
  if (state.daily_trades >= limits.max_daily_trades) return { allowed: false, reason: `Daily trade limit reached (${state.daily_trades} / ${limits.max_daily_trades})` };
  return { allowed: true, reason: null };
}

async function checkAccountProtection(balance) {
  const config = await loadRiskConfig();
  const state  = await loadRiskState();
  const prot   = config.account_protection;
  if (prot.emergency_stop)    return { allowed: false, reason: 'Emergency stop activated' };
  if (balance < prot.min_balance) return { allowed: false, reason: `Balance below minimum (₹${balance.toFixed(2)} < ₹${prot.min_balance})` };
  if (state.peak_balance > 0) {
    const drawdownPct = ((state.peak_balance - balance) / state.peak_balance) * 100;
    if (drawdownPct >= prot.max_drawdown_percentage)
      return { allowed: false, reason: `Max drawdown exceeded (${drawdownPct.toFixed(2)}%)` };
  }
  if (balance > state.peak_balance) { state.peak_balance = balance; await saveRiskState(state); }
  return { allowed: true, reason: null };
}

async function canOpenTrade(balance) {
  const daily   = await checkDailyLimits();    if (!daily.allowed)   return daily;
  const account = await checkAccountProtection(balance); if (!account.allowed) return account;
  return { allowed: true, reason: null };
}

async function recordTradeResult(profitLoss) {
  const state = await loadRiskState();
  state.daily_trades++;
  if (profitLoss < 0) state.daily_loss   += Math.abs(profitLoss);
  else                state.daily_profit += profitLoss;
  await saveRiskState(state);
}

// ------------------------------
// Position Close Helpers
// ------------------------------
function calculateLivePL(openTrade, currentPrice) {
  if (!openTrade) return null;
  return openTrade.type === 'LONG'
    ? (currentPrice - openTrade.entry_price) * openTrade.amount
    : (openTrade.entry_price - currentPrice) * openTrade.amount;
}

async function closeFullPosition(data, openTrade, currentPrice, reason) {
  const usdtInr    = await getUSDTINR();
  const profitUsdt = openTrade.type === 'LONG'
    ? (currentPrice - openTrade.entry_price) * openTrade.amount
    : (openTrade.entry_price - currentPrice) * openTrade.amount;
  const profitInr  = profitUsdt * usdtInr;
  const balanceBefore = data.balance;
  data.balance += profitInr;
  const tradeRecord = {
    type: openTrade.type, entry_price: openTrade.entry_price, exit_price: currentPrice,
    amount: openTrade.amount,
    profit_usdt: parseFloat(profitUsdt.toFixed(2)), profit_inr: parseFloat(profitInr.toFixed(2)),
    balance_before: balanceBefore, balance_after: data.balance,
    closed_at: new Date().toISOString(), exit_reason: reason, partial: false,
    stop_loss: openTrade.stop_loss, tp1_price: openTrade.tp1_price,
    opened_at: openTrade.opened_at, duration_ms: new Date() - new Date(openTrade.opened_at)
  };
  data.history.push(tradeRecord);
  await recordTradeResult(profitInr);
  data.open_trade = null;
  return { tradeRecord, side: openTrade.type === 'LONG' ? 'Buy' : 'Sell' };
}

async function closePartialPosition(data, openTrade, currentPrice, partialPct, reason) {
  const usdtInr     = await getUSDTINR();
  const closeAmount = parseFloat((openTrade.amount * partialPct).toFixed(6));
  const profitUsdt  = openTrade.type === 'LONG'
    ? (currentPrice - openTrade.entry_price) * closeAmount
    : (openTrade.entry_price - currentPrice) * closeAmount;
  const profitInr   = profitUsdt * usdtInr;
  data.balance += profitInr;
  const remainingAmount = parseFloat((openTrade.amount - closeAmount).toFixed(6));
  const tradeRecord = {
    type: openTrade.type, entry_price: openTrade.entry_price, exit_price: currentPrice,
    amount: closeAmount,
    profit_usdt: parseFloat(profitUsdt.toFixed(2)), profit_inr: parseFloat(profitInr.toFixed(2)),
    balance_before: data.balance - profitInr, balance_after: data.balance,
    closed_at: new Date().toISOString(), exit_reason: reason + ` (partial ${Math.round(partialPct * 100)}%)`,
    partial: true, stop_loss: openTrade.stop_loss, tp1_price: openTrade.tp1_price,
    opened_at: openTrade.opened_at, duration_ms: new Date() - new Date(openTrade.opened_at)
  };
  data.history.push(tradeRecord);
  await recordTradeResult(profitInr);
  // Update open trade in place
  openTrade.amount          = remainingAmount;
  openTrade.stop_loss       = parseFloat(openTrade.entry_price.toFixed(2)); // move to breakeven
  openTrade.breakeven_moved = true;
  if (openTrade.tp_levels?.length > 0) openTrade.tp_levels[0].hit = true;
  return { tradeRecord };
}

// ------------------------------
// Condition-Based Cooldown Check
// ------------------------------
function checkCooldown(data, signal, currentPrice, cooldownConfig) {
  const now = Date.now();
  if (!data.last_closed_side) return { canOpen: true, reason: null };
  const sameSide = (data.last_closed_side === signal);

  if (sameSide) {
    if (data.last_close_reason === 'sl' && cooldownConfig.sl_condition_based) {
      const slPrice = data.last_closed_sl_price;
      const elapsed = now - data.last_closed_time;
      if (elapsed >= cooldownConfig.sl_fallback_ms) return { canOpen: true, reason: null };
      if (slPrice) {
        const priceCleared = signal === 'Buy' ? currentPrice > slPrice : currentPrice < slPrice;
        if (priceCleared) return { canOpen: true, reason: null };
        const remaining = Math.ceil((cooldownConfig.sl_fallback_ms - elapsed) / 1000);
        return { canOpen: false, reason: `⛔ SL cooldown: price must clear $${slPrice.toFixed(2)} for ${signal} (fallback in ${remaining}s)` };
      }
    }
    return { canOpen: true, reason: null };
  } else {
    if (data.pending_opposite_side === signal) {
      const elapsed = now - data.pending_opposite_time;
      if (elapsed >= cooldownConfig.opposite_confirmation_ms) return { canOpen: true, reason: null };
      const remaining = Math.ceil((cooldownConfig.opposite_confirmation_ms - elapsed) / 1000);
      return { canOpen: false, reason: `⏳ Opposite confirmation: ${remaining}s remaining for ${signal}` };
    } else {
      data.pending_opposite_side = signal;
      data.pending_opposite_time = now;
      const secs = Math.ceil(cooldownConfig.opposite_confirmation_ms / 1000);
      return { canOpen: false, reason: `⏳ Opposite signal ${signal} detected — confirming for ${secs}s` };
    }
  }
}

// ------------------------------
// Core Trade Logic (for /signal endpoint — handles new signals only)
// SL/TP is now handled by WebSocket in real time
// ------------------------------
async function updateDemoTrade(signal, price, atrValue, utbotStop) {
  signal = signal.charAt(0).toUpperCase() + signal.slice(1).toLowerCase();
  const data    = await loadTrades();
  const config  = await loadRiskConfig();
  let openTrade = data.open_trade;
  let actionMessage = '';

  const logEntry = { time: new Date().toISOString(), side: signal, price, quantity: 0 };

  // NOTE: SL/TP checks are intentionally NOT done here anymore.
  // The WebSocket watcher handles them in real time.
  // We only do trailing stop update here as a supplementary check.
  if (openTrade && openTrade.breakeven_moved && config.stop_loss.trailing_enabled) {
    const newStop = updateTrailingStop(price, openTrade.type, openTrade.stop_loss, atrValue, config);
    if (newStop) {
      openTrade.stop_loss = newStop;
      actionMessage = `📈 Trailing stop updated to $${newStop.toFixed(2)}`;
      logEntry.action = 'TRAILING_STOP_UPDATE';
    }
  }

  // ─── New trade / signal logic ──────────────────────────────────────────────
  if (signal === 'Hold') {
    if (!actionMessage) { actionMessage = 'Holding. Waiting for next signal.'; logEntry.action = 'HOLD'; }
    if (data.pending_opposite_side) { data.pending_opposite_side = null; data.pending_opposite_time = null; }

  } else {
    const canTrade = await canOpenTrade(data.balance);
    if (!canTrade.allowed) {
      actionMessage = `⚠️ Cannot open ${signal}: ${canTrade.reason}`;
      logEntry.action = 'BLOCKED';

    } else if (openTrade && openTrade.type === (signal === 'Buy' ? 'LONG' : 'SHORT')) {
      actionMessage = `Ignoring repeated "${signal}" — already in ${signal === 'Buy' ? 'LONG' : 'SHORT'}.`;
      logEntry.action = 'IGNORED';

    } else {
      // Close opposite position first
      if (openTrade) {
        const oppSide = openTrade.type === 'LONG' ? 'Sell' : 'Buy';
        if (oppSide === signal) {
          const { tradeRecord, side } = await closeFullPosition(data, openTrade, price, 'Opposite Signal');
          actionMessage = `CLOSED ${openTrade.type} @ $${price.toFixed(2)}, P/L: ₹${tradeRecord.profit_inr.toFixed(2)}. | `;
          logEntry.action = `CLOSE_${openTrade.type}`;
          openTrade = null;
          data.last_closed_time     = Date.now();
          data.last_closed_side     = side;
          data.last_closed_sl_price = null;
          data.last_close_reason    = 'signal';
          data.pending_opposite_side  = null;
          data.pending_opposite_time  = null;
        }
      }

      const cooldownCfg = config.cooldown || { sl_condition_based: true, tp_cooldown_ms: 0, opposite_confirmation_ms: 30000, sl_fallback_ms: 120000 };
      const { canOpen, reason: cooldownReason } = checkCooldown(data, signal, price, cooldownCfg);

      if (canOpen) {
        data.last_closed_side = null; data.last_closed_time = null;
        data.last_closed_sl_price = null; data.last_close_reason = null;
        data.pending_opposite_side = null; data.pending_opposite_time = null;

        const stopLoss     = calculateStopLoss(price, signal === 'Buy' ? 'LONG' : 'SHORT', atrValue, utbotStop, config);
        const positionSize = await calculatePositionSize(data.balance, price, signal === 'Buy' ? 'LONG' : 'SHORT', stopLoss, atrValue, config);
        const tpLevels     = calculateTakeProfitLevels(price, signal === 'Buy' ? 'LONG' : 'SHORT', atrValue, config);
        const tp1Price     = tpLevels[0]?.price || null;

        openTrade = {
          type: signal === 'Buy' ? 'LONG' : 'SHORT',
          entry_price: price, amount: positionSize, original_amount: positionSize,
          stop_loss: stopLoss, tp1_price: tp1Price, tp_levels: tpLevels,
          opened_at: new Date().toISOString(),
          strategy: signal === 'Buy' ? 'UT Bot #2 (KV=2, ATR=300)' : 'UT Bot #1 (KV=2, ATR=1)',
          atr_at_entry: atrValue, breakeven_moved: false
        };
        logEntry.quantity  = positionSize;
        logEntry.stop_loss = stopLoss;
        logEntry.tp1       = tp1Price;
        actionMessage += (signal === 'Buy' ? '🟢 OPENED LONG' : '🔴 OPENED SHORT') +
          ` @ $${price.toFixed(2)} | Size: ${positionSize} BTC | SL: $${stopLoss?.toFixed(2) || 'N/A'} | TP1: $${tp1Price?.toFixed(2) || 'N/A'}`;
        logEntry.action  = signal === 'Buy' ? 'OPEN_LONG' : 'OPEN_SHORT';
        data.last_signal = signal;
      } else {
        actionMessage = cooldownReason || 'Cooldown active';
        logEntry.action = 'COOLDOWN';
      }
    }
  }

  if (!data.order_log) data.order_log = [];
  data.order_log.push(logEntry);
  if (data.order_log.length > 100) data.order_log.shift();
  data.open_trade = openTrade;
  await saveTrades(data);

  const cooldownInfo = (logEntry.action === 'COOLDOWN')
    ? { active: true, message: actionMessage }
    : { active: false, message: null };

  return {
    balance: data.balance, holding: !!openTrade,
    position_type: openTrade?.type || null,
    action: actionMessage,
    stop_loss: openTrade?.stop_loss || null,
    tp_levels: openTrade?.tp_levels || [],
    position_size: openTrade?.amount || 0,
    cooldown: cooldownInfo
  };
}

async function forceClosePosition(currentPrice, reason) {
  const data      = await loadTrades();
  const openTrade = data.open_trade;
  if (!openTrade) return null;
  const { tradeRecord, side } = await closeFullPosition(data, openTrade, currentPrice, reason);
  data.order_log = data.order_log || [];
  data.order_log.push({
    time: new Date().toISOString(), side: 'CLOSE', action: 'FORCE_CLOSE',
    price: currentPrice, quantity: openTrade.amount, pl_inr: tradeRecord.profit_inr
  });
  data.last_closed_time = Date.now(); data.last_closed_side = side;
  data.last_closed_sl_price = null;   data.last_close_reason = 'force';
  data.pending_opposite_side = null;  data.pending_opposite_time = null;
  await saveTrades(data);
  return tradeRecord;
}

// ------------------------------
// Trading Hours & Control
// ------------------------------
function isWithinTradingHours(state) {
  if (state.force_start || !state.enabled) return true;
  const hour = getCurrentHourIST();
  return hour >= state.start_hour && hour < state.end_hour;
}

async function isTradingAllowed() {
  const state = await loadTradingState();
  if (state.force_start)  return { allowed: true,  reason: null };
  if (state.manual_pause) return { allowed: false, reason: 'Trading manually paused' };
  if (!isWithinTradingHours(state))
    return { allowed: false, reason: `Outside trading hours (${state.start_hour}:00–${state.end_hour}:00 IST). Current: ${getCurrentHourIST()}:00` };
  return { allowed: true, reason: null };
}

// ------------------------------
// Daily reset cron (IST midnight)
// ------------------------------
async function resetDailyIfNeeded() {
  const state    = await loadRiskState();
  const nowIST   = new Date().toLocaleString('en-US', { timeZone: 'Asia/Kolkata' });
  const todayStr = new Date(nowIST).toISOString().split('T')[0];
  if (state.last_reset !== todayStr) {
    await resetRiskState();
    console.log('🔄 Daily risk state reset at IST midnight');
  }
}

cron.schedule('0 * * * *', resetDailyIfNeeded);
resetDailyIfNeeded().catch(console.error);

// ------------------------------
// Express Routes
// ------------------------------
app.use(express.json());
app.use(express.static(__dirname));

// Keep-alive ping endpoint
app.get('/ping', (req, res) => {
  res.send(`pong | ws:${wsConnected ? wsSource : 'down'} | price:$${livePrice?.toFixed(2) || 'N/A'} | ${new Date().toISOString()}`);
});

// WebSocket status endpoint
app.get('/ws-status', (req, res) => {
  res.json({
    connected: wsConnected,
    source: wsSource || null,
    live_price: livePrice,
    fail_count: wsFailCount,
    last_check: new Date(lastWsCheckTime).toISOString()
  });
});

app.get('/', (req, res) => res.sendFile(__dirname + '/index.html'));

app.get('/signal', async (req, res) => {
  try {
    const tradingAllowed = await isTradingAllowed();
    const { signal, price, atr, utbot_stop } = await getUTBotSignal();
    if (signal === 'No Data' || price === 0)
      return res.status(500).json({ error: 'Could not generate signal' });

    const data      = await loadTrades();
    const openTrade = data.open_trade;
    const usdtInr   = await getUSDTINR();
    const livePlUsdt = calculateLivePL(openTrade, price);
    const livePlInr  = livePlUsdt != null ? livePlUsdt * usdtInr : null;

    if (!tradingAllowed.allowed) {
      const riskStatus = await getRiskStatus();
      return res.json({
        price, signal: 'Hold', balance: data.balance,
        holding: !!openTrade, position_type: openTrade?.type || null,
        entry_price: openTrade?.entry_price || null,
        action: `⏸️ PAUSED: ${tradingAllowed.reason}`,
        latest_order: data.order_log?.slice(-1)[0] || null,
        live_pl_inr: livePlInr,
        stop_loss: openTrade?.stop_loss || null, tp_levels: openTrade?.tp_levels || [],
        position_size: openTrade?.amount || 0, atr, risk_status: riskStatus,
        trading_allowed: false, pause_reason: tradingAllowed.reason,
        force_start: (await loadTradingState()).force_start,
        cooldown: { active: false, message: null },
        ws_connected: wsConnected, ws_source: wsSource,
        strategy_info: { buy_strategy: 'UT Bot #2 (KV=2, ATR=300)', sell_strategy: 'UT Bot #1 (KV=2, ATR=1)' }
      });
    }

    const { balance, holding, position_type, action, stop_loss, tp_levels, position_size, cooldown } =
      await updateDemoTrade(signal, price, atr, utbot_stop);

    const updatedData = await loadTrades();
    const riskStatus  = await getRiskStatus();

    res.json({
      price, signal, balance, holding, position_type,
      entry_price: updatedData.open_trade?.entry_price || null,
      action,
      latest_order: updatedData.order_log?.slice(-1)[0] || null,
      live_pl_inr: livePlInr,
      stop_loss, tp_levels, position_size, atr,
      risk_status: riskStatus,
      trading_allowed: true, pause_reason: null,
      force_start: (await loadTradingState()).force_start,
      cooldown,
      ws_connected: wsConnected, ws_source: wsSource,
      strategy_info: { buy_strategy: 'UT Bot #2 (KV=2, ATR=300)', sell_strategy: 'UT Bot #1 (KV=2, ATR=1)' }
    });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  }
});

app.get('/chart-data', async (req, res) => {
  try {
    const klines = await getKlines();
    if (!klines) return res.status(500).json({ error: 'No data' });
    const candles  = klines.map(k => ({ time: Math.floor(k[0] / 1000), open: parseFloat(k[1]), high: parseFloat(k[2]), low: parseFloat(k[3]), close: parseFloat(k[4]) }));
    const df1      = calcUtbot(klines, 2, 1);
    const stopLine = df1.stop.map((val, idx) => ({ time: candles[idx].time, value: val }));
    res.json({ candles, stop_line: stopLine });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/history',  async (req, res) => { const d = await loadTrades(); res.json(d.history || []); });
app.get('/orders',   async (req, res) => { const d = await loadTrades(); res.json((d.order_log || []).reverse()); });

app.get('/status', async (req, res) => {
  const data         = await loadTrades();
  const currentPrice = await getCurrentPrice();
  const usdtInr      = await getUSDTINR();
  const livePlUsdt   = calculateLivePL(data.open_trade, currentPrice);
  res.json({
    balance: data.balance, has_open_trade: !!data.open_trade, open_trade: data.open_trade,
    current_price: currentPrice,
    live_pl_inr: livePlUsdt != null ? livePlUsdt * usdtInr : null,
    last_signal: data.last_signal, total_trades: data.history?.length || 0,
    risk_status: await getRiskStatus(),
    force_start: (await loadTradingState()).force_start,
    ws_connected: wsConnected, ws_source: wsSource
  });
});

app.get('/risk-config',  async (req, res) => res.json(await loadRiskConfig()));
app.post('/risk-config', async (req, res) => {
  try { await saveRiskConfig(req.body); res.json({ success: true }); }
  catch (err) { res.status(400).json({ success: false, error: err.message }); }
});

app.get('/risk-status', async (req, res) => {
  try { res.json(await getRiskStatus()); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/trading-control', async (req, res) => {
  const state = await loadTradingState();
  const { allowed, reason } = await isTradingAllowed();
  res.json({ state, trading_allowed: allowed, pause_reason: reason, current_time: new Date().toLocaleTimeString() });
});

app.post('/trading-control', async (req, res) => {
  try {
    const { action } = req.body;
    const state = await loadTradingState();
    switch (action) {
      case 'pause':       state.manual_pause = true;  state.force_start = false; await saveTradingState(state); return res.json({ success: true, message: 'Trading paused' });
      case 'resume':      state.manual_pause = false; state.force_start = false; await saveTradingState(state); return res.json({ success: true, message: 'Trading resumed' });
      case 'force_start': state.manual_pause = false; state.force_start = true;  await saveTradingState(state); return res.json({ success: true, message: 'Force start — 24/7' });
      case 'force_stop': {
        const currentPrice = await getCurrentPrice();
        if (currentPrice) {
          const closedTrade = await forceClosePosition(currentPrice, 'Force Stop');
          state.manual_pause = true; state.force_start = false; await saveTradingState(state);
          return res.json({ success: true, message: closedTrade ? `Closed at $${currentPrice.toFixed(2)} | P/L: ₹${closedTrade.profit_inr.toFixed(2)}` : 'No open position' });
        }
        return res.json({ success: false, message: 'Could not get price' });
      }
      case 'update_hours':
        state.start_hour = req.body.start_hour ?? state.start_hour;
        state.end_hour   = req.body.end_hour   ?? state.end_hour;
        state.enabled    = req.body.enabled    ?? state.enabled;
        await saveTradingState(state);
        return res.json({ success: true, message: 'Trading hours updated' });
      default: return res.status(400).json({ success: false, error: 'Invalid action' });
    }
  } catch (err) { res.status(400).json({ success: false, error: err.message }); }
});

app.get('/usdt-inr-rate',  async (req, res) => res.json({ rate: await getUSDTINR() }));
app.post('/usdt-inr-rate', async (req, res) => {
  try {
    const { rate } = req.body;
    if (typeof rate !== 'number' || rate <= 0) throw new Error('Invalid rate');
    await setUSDTINR(rate); res.json({ success: true });
  } catch (err) { res.status(400).json({ success: false, error: err.message }); }
});

app.post('/clear-history', async (req, res) => {
  try {
    const data = await loadTrades();
    Object.assign(data, {
      history: [], order_log: [], balance: START_BALANCE, open_trade: null,
      last_closed_time: null, last_closed_side: null, last_closed_sl_price: null,
      last_close_reason: null, pending_opposite_side: null, pending_opposite_time: null
    });
    await saveTrades(data);
    await resetRiskState();
    res.json({ success: true, message: 'All trades cleared, balance reset.' });
  } catch (err) { res.status(500).json({ success: false, error: err.message }); }
});

app.get('/export-history', async (req, res) => {
  const data   = await loadTrades();
  const trades = data.history;
  if (!trades.length) return res.status(404).json({ error: 'No trades to export' });
  const headers = ['Type','Entry Price','Exit Price','Amount (BTC)','Stop Loss','TP1 Price','Profit (USDT)','Profit (INR)','Exit Reason','Opened At','Closed At','Duration (s)','Partial'];
  const rows = trades.map(t => [
    t.type, t.entry_price, t.exit_price, t.amount, t.stop_loss ?? 'N/A', t.tp1_price ?? 'N/A',
    t.profit_usdt, t.profit_inr, t.exit_reason,
    new Date(t.opened_at).toISOString(), new Date(t.closed_at).toISOString(),
    t.duration_ms ? (t.duration_ms / 1000).toFixed(1) : 'N/A', t.partial ? 'Yes' : 'No'
  ]);
  const csv = [headers, ...rows].map(r => r.join(',')).join('\n');
  res.setHeader('Content-Type', 'text/csv');
  res.setHeader('Content-Disposition', 'attachment; filename=trade_history.csv');
  res.send(csv);
});

async function getRiskStatus() {
  const config = await loadRiskConfig();
  const state  = await loadRiskState();
  const limits = config.daily_limits;
  return {
    daily_stats: {
      trades: `${state.daily_trades}/${limits.max_daily_trades}`,
      loss:   `₹${state.daily_loss.toFixed(2)}/₹${limits.max_daily_loss.toFixed(2)}`,
      profit: `₹${state.daily_profit.toFixed(2)}`
    },
    limits_usage: {
      trades_pct: (state.daily_trades / limits.max_daily_trades) * 100,
      loss_pct:   (state.daily_loss   / limits.max_daily_loss)   * 100
    },
    config
  };
}

app.listen(port, () => {
  console.log(`✅ Server running on port ${port}`);
  console.log(`📡 REST: Binance CDN → Bybit → CryptoCompare → CoinGecko`);
  console.log(`🔌 WebSocket: Bybit publicTrade (Binance skipped — geo-blocked on Render)`);
  console.log(`🏓 Self-pinger: every 4 min → ${SELF_URL}/ping`);
});
