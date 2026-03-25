// bot.js – UT Bot Trading System with Risk Management (Node.js + Upstash Redis REST)
// ✅ FIXED: Binance 451 geo-block issue with multi-proxy fallback + CryptoCompare kline fallback
// ✅ FIXED: Syntax error in BINANCE_ENDPOINTS definition
// ✅ ADDED: Cooldown after trade – wait for next signal before re-entering

require('dotenv').config();
const express = require('express');
const { Redis } = require('@upstash/redis');
const fetch = require('node-fetch');
const cron = require('node-cron');

const app = express();
const port = process.env.PORT || 5000;

// ------------------------------
// Upstash Redis REST Configuration
// ------------------------------
const HARDCODED_URL = 'https://robust-kitten-78595.upstash.io';
const HARDCODED_TOKEN = 'gQAAAAAAATMDAAIncDEyZjJkNzQyMDQyN2Q0ODEwOTI1ZGY4MTczMWM4MGQzYnAxNzg1OTU';

const redisUrl = process.env.UPSTASH_REDIS_REST_URL || HARDCODED_URL;
const redisToken = process.env.UPSTASH_REDIS_REST_TOKEN || HARDCODED_TOKEN;

if (!redisUrl || !redisToken) {
  console.error('❌ Missing Upstash Redis REST URL or token.');
  process.exit(1);
}

const redis = new Redis({
  url: redisUrl,
  token: redisToken,
});

redis.ping().then(() => console.log('✅ Connected to Upstash Redis')).catch(err => console.error('Redis error:', err));

// ------------------------------
// Constants
// ------------------------------
const START_BALANCE = 10000;
const COINS_PER_TRADE = 0.001;

const TRADES_KEY = 'demo_trades';
const RISK_STATE_KEY = 'risk_state';
const RISK_CONFIG_KEY = 'risk_config';
const TRADING_STATE_KEY = 'trading_state';
const USDT_INR_KEY = 'usdt_inr_rate';

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
      last_closed_signal: null   // NEW: stores the signal that caused the last close
    };
    await redis.set(TRADES_KEY, JSON.stringify(defaultData));
    return defaultData;
  }
  return typeof data === 'string' ? JSON.parse(data) : data;
}

async function saveTrades(data) {
  await redis.set(TRADES_KEY, JSON.stringify(data));
}

async function loadRiskState() {
  const data = await redis.get(RISK_STATE_KEY);
  if (!data) return resetRiskState();
  return typeof data === 'string' ? JSON.parse(data) : data;
}

async function saveRiskState(state) {
  await redis.set(RISK_STATE_KEY, JSON.stringify(state));
}

async function resetRiskState() {
  const state = {
    daily_loss: 0,
    daily_profit: 0,
    daily_trades: 0,
    consecutive_losses: 0,
    last_reset: Date.now(),
    peak_balance: START_BALANCE
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
        atr_multiplier: 2.0,
        max_loss_percentage: 3.0,
        trailing_enabled: true,
        trailing_atr_multiplier: 1.5
      },
      take_profit: {
        enabled: true,
        type: 'scaled_atr',
        levels: [
          { percentage: 50, atr_multiplier: 2.5, name: 'TP1' },
          { percentage: 30, atr_multiplier: 5.0, name: 'TP2' },
          { percentage: 20, atr_multiplier: 7.5, name: 'TP3' }
        ]
      },
      position_sizing: {
        method: 'percentage',
        value: 5.0,
        min_position_size: 0.0001,
        max_position_size: 0.01
      },
      daily_limits: {
        enabled: true,
        max_daily_loss: 1000.0,
        max_daily_trades: 20,
        max_consecutive_losses: 5,
        reset_hour: 0
      },
      account_protection: {
        max_drawdown_percentage: 20.0,
        min_balance: 5000.0,
        emergency_stop: false
      },
      different_rules_for_position_type: {
        enabled: true,
        long: { tp_atr_multipliers: [3.0, 6.0, 9.0] },
        short: { tp_atr_multipliers: [2.0, 4.0, 6.0] }
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
    const defaultState = {
      enabled: true,
      start_hour: 18,
      end_hour: 23,
      manual_pause: false,
      force_start: false
    };
    await redis.set(TRADING_STATE_KEY, JSON.stringify(defaultState));
    return defaultState;
  }
  return typeof data === 'string' ? JSON.parse(data) : data;
}

async function saveTradingState(state) {
  await redis.set(TRADING_STATE_KEY, JSON.stringify(state));
}

// ------------------------------
// USDT/INR Rate Management
// ------------------------------
async function getUSDTINR() {
  let rate = await redis.get(USDT_INR_KEY);
  if (!rate) {
    rate = 85;
    await redis.set(USDT_INR_KEY, rate);
  }
  return parseFloat(rate);
}

async function setUSDTINR(rate) {
  await redis.set(USDT_INR_KEY, parseFloat(rate));
}

// ------------------------------
// Helper: Current hour in IST (Asia/Kolkata)
// ------------------------------
function getCurrentHourIST() {
  const now = new Date();
  const istString = now.toLocaleString('en-US', { timeZone: 'Asia/Kolkata', hour: 'numeric', hour12: false });
  return parseInt(istString);
}

// ------------------------------
// Binance API with Multi-Proxy Fallback
// ------------------------------
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
          'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
          'Accept': 'application/json',
          'Accept-Language': 'en-US,en;q=0.9',
          'Cache-Control': 'no-cache'
        },
        signal: controller.signal
      });
      clearTimeout(timeout);
      if (response.status === 200) {
        console.log(`✅ Binance data from: ${endpoint}`);
        return await response.json();
      }
      console.warn(`⚠️ ${endpoint} returned ${response.status} — trying next...`);
    } catch (err) {
      clearTimeout(timeout);
      console.warn(`⚠️ ${endpoint} error: ${err.message} — trying next...`);
    }
  }
  console.error('❌ All Binance endpoints failed. Falling back to alternative sources.');
  return null;
}

async function fetchPriceFromCoinGecko() {
  try {
    const url = 'https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd';
    const response = await fetch(url, { timeout: 6000 });
    const data = await response.json();
    if (data && data.bitcoin && data.bitcoin.usd) {
      console.log('✅ Price from CoinGecko');
      return data.bitcoin.usd;
    }
  } catch (err) {
    console.warn('CoinGecko price error:', err.message);
  }
  return null;
}

async function fetchKlinesFromCryptoCompare(limit = 350) {
  try {
    const url = `https://min-api.cryptocompare.com/data/v2/histominute?fsym=BTC&tsym=USD&limit=${limit}&aggregate=5`;
    const response = await fetch(url, { timeout: 8000 });
    const data = await response.json();
    if (data && data.Data && data.Data.Data && data.Data.Data.length > 0) {
      console.log('✅ Klines from CryptoCompare (fallback)');
      return data.Data.Data.map(c => [
        c.time * 1000,
        String(c.open),
        String(c.high),
        String(c.low),
        String(c.close),
        String(c.volumefrom),
        c.time * 1000 + 299999,
        String(c.volumeto),
        0, '0', '0', '0'
      ]);
    }
  } catch (err) {
    console.warn('CryptoCompare klines error:', err.message);
  }
  return null;
}

async function fetchKlinesFromBybit(limit = 350) {
  try {
    const url = `https://api.bybit.com/v5/market/kline?category=spot&symbol=BTCUSDT&interval=5&limit=${limit}`;
    const response = await fetch(url, { timeout: 8000 });
    const data = await response.json();
    if (data && data.result && data.result.list && data.result.list.length > 0) {
      console.log('✅ Klines from Bybit (fallback)');
      return data.result.list.reverse().map(c => [
        parseInt(c[0]),
        c[1],
        c[2],
        c[3],
        c[4],
        c[5],
        parseInt(c[0]) + 299999,
        c[6],
        0, '0', '0', '0'
      ]);
    }
  } catch (err) {
    console.warn('Bybit klines error:', err.message);
  }
  return null;
}

async function getCurrentPrice() {
  const binancePrice = await binanceRequest('/api/v3/ticker/price', { symbol: 'BTCUSDT' });
  if (binancePrice && binancePrice.price) return parseFloat(binancePrice.price);
  try {
    const bybitUrl = 'https://api.bybit.com/v5/market/tickers?category=spot&symbol=BTCUSDT';
    const res = await fetch(bybitUrl, { timeout: 6000 });
    const data = await res.json();
    if (data?.result?.list?.[0]?.lastPrice) {
      console.log('✅ Price from Bybit');
      return parseFloat(data.result.list[0].lastPrice);
    }
  } catch (err) { console.warn('Bybit price error:', err.message); }
  const geckoPrice = await fetchPriceFromCoinGecko();
  if (geckoPrice) return geckoPrice;
  console.error('❌ All price sources failed');
  return null;
}

async function getKlines(symbol = 'BTCUSDT', interval = '5m', limit = 350) {
  const binanceKlines = await binanceRequest('/api/v3/klines', { symbol, interval, limit });
  if (binanceKlines && binanceKlines.length > 0) return binanceKlines;
  const bybitKlines = await fetchKlinesFromBybit(limit);
  if (bybitKlines && bybitKlines.length > 0) return bybitKlines;
  const ccKlines = await fetchKlinesFromCryptoCompare(limit);
  if (ccKlines && ccKlines.length > 0) return ccKlines;
  console.error('❌ All kline sources failed');
  return null;
}

// ------------------------------
// UT Bot Logic (unchanged)
// ------------------------------
function calcUtbot(klines, keyvalue, atrPeriod) {
  const close = klines.map(k => parseFloat(k[4]));
  const high = klines.map(k => parseFloat(k[2]));
  const low = klines.map(k => parseFloat(k[3]));
  const tr = [];
  for (let i = 0; i < high.length; i++) {
    if (i === 0) tr.push(high[i] - low[i]);
    else {
      const hl = high[i] - low[i];
      const hc = Math.abs(high[i] - close[i-1]);
      const lc = Math.abs(low[i] - close[i-1]);
      tr.push(Math.max(hl, hc, lc));
    }
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
    const src = close[i];
    const src1 = close[i-1];
    const prevStop = xATRTrailingStop[i-1];
    const nl = nLoss[i];
    let newStop;
    if (src > prevStop && src1 > prevStop) {
      newStop = Math.max(prevStop, src - nl);
    } else if (src < prevStop && src1 < prevStop) {
      newStop = Math.min(prevStop, src + nl);
    } else {
      newStop = src > prevStop ? src - nl : src + nl;
    }
    xATRTrailingStop.push(newStop);
    let newPos;
    if (src1 < prevStop && src > prevStop) newPos = 1;
    else if (src1 > prevStop && src < prevStop) newPos = -1;
    else newPos = pos[i-1];
    pos.push(newPos);
  }
  return { stop: xATRTrailingStop, pos, atr };
}

async function getUTBotSignal() {
  const klines = await getKlines();
  if (!klines) {
    console.error('No klines received');
    return { signal: 'No Data', price: 0, atr: 0, utbot_stop: 0 };
  }
  const price = parseFloat(klines[klines.length-1][4]);
  const df1 = calcUtbot(klines, 2, 1);
  const df2 = calcUtbot(klines, 2, 300);
  const signal1 = df1.pos[df1.pos.length-1];
  const signal2 = df2.pos[df2.pos.length-1];
  const stop1 = df1.stop[df1.stop.length-1];
  const stop2 = df2.stop[df2.stop.length-1];
  const atr = df1.atr[df1.atr.length-1] || 0;
  let signal = 'Hold';
  let utbotStop = null;
  if (signal2 === 1) {
    signal = 'Buy';
    utbotStop = stop2;
  }
  if (signal1 === -1) {
    signal = 'Sell';
    utbotStop = stop1;
  }
  return { signal, price, atr, utbot_stop: utbotStop || price };
}

// ------------------------------
// Risk Management Functions (unchanged except for dynamic USDT/INR)
// ------------------------------
async function calculatePositionSize(balance) {
  const config = await loadRiskConfig();
  const method = config.position_sizing.method;
  let size = config.position_sizing.value;
  if (method === 'percentage') {
    const price = await getCurrentPrice();
    const btcPriceInr = price * (await getUSDTINR());
    const positionValueInr = balance * (size / 100);
    size = positionValueInr / btcPriceInr;
  }
  const min = config.position_sizing.min_position_size;
  const max = config.position_sizing.max_position_size;
  size = Math.min(max, Math.max(min, size));
  return parseFloat(size.toFixed(6));
}

function calculateStopLoss(entry, type, atr, utbotStop, config) {
  const slConf = config.stop_loss;
  if (!slConf.enabled) return null;
  let atrStop = type === 'LONG'
    ? entry - atr * slConf.atr_multiplier
    : entry + atr * slConf.atr_multiplier;
  let fixedStop = type === 'LONG'
    ? entry * (1 - slConf.max_loss_percentage / 100)
    : entry * (1 + slConf.max_loss_percentage / 100);
  let stop;
  switch (slConf.type) {
    case 'atr': stop = atrStop; break;
    case 'percentage': stop = fixedStop; break;
    case 'utbot': stop = utbotStop || fixedStop; break;
    default: // hybrid
      stop = type === 'LONG'
        ? Math.max(atrStop, fixedStop)
        : Math.min(atrStop, fixedStop);
      if (utbotStop) {
        stop = type === 'LONG'
          ? Math.max(stop, utbotStop)
          : Math.min(stop, utbotStop);
      }
  }
  return parseFloat(stop.toFixed(2));
}

function calculateTakeProfitLevels(entry, type, atr, config) {
  const tpConf = config.take_profit;
  if (!tpConf.enabled) return [];
  const levels = [];
  let multipliers = [];
  if (config.different_rules_for_position_type.enabled) {
    multipliers = type === 'LONG'
      ? config.different_rules_for_position_type.long.tp_atr_multipliers
      : config.different_rules_for_position_type.short.tp_atr_multipliers;
  } else {
    multipliers = tpConf.levels.map(l => l.atr_multiplier);
  }
  for (let i = 0; i < multipliers.length; i++) {
    const mult = multipliers[i];
    const price = type === 'LONG'
      ? entry + atr * mult
      : entry - atr * mult;
    const levelInfo = tpConf.levels[i] || { percentage: 100 / multipliers.length, name: `TP${i+1}` };
    levels.push({
      price: parseFloat(price.toFixed(2)),
      percentage: levelInfo.percentage,
      name: levelInfo.name,
      hit: false
    });
  }
  return levels;
}

function updateTrailingStop(current, type, stopLoss, atr, config) {
  if (!config.stop_loss.trailing_enabled) return null;
  const mult = config.stop_loss.trailing_atr_multiplier;
  const trail = atr * mult;
  let newStop;
  if (type === 'LONG') {
    newStop = current - trail;
    if (newStop > stopLoss) return parseFloat(newStop.toFixed(2));
  } else {
    newStop = current + trail;
    if (newStop < stopLoss) return parseFloat(newStop.toFixed(2));
  }
  return null;
}

async function checkDailyLimits() {
  const config = await loadRiskConfig();
  const state = await loadRiskState();
  const limits = config.daily_limits;
  if (!limits.enabled) return { allowed: true, reason: null };
  if (state.daily_loss >= limits.max_daily_loss) {
    return { allowed: false, reason: `Daily loss limit reached (₹${state.daily_loss.toFixed(2)} / ₹${limits.max_daily_loss})` };
  }
  if (state.daily_trades >= limits.max_daily_trades) {
    return { allowed: false, reason: `Daily trade limit reached (${state.daily_trades} / ${limits.max_daily_trades})` };
  }
  if (state.consecutive_losses >= limits.max_consecutive_losses) {
    return { allowed: false, reason: `Max consecutive losses reached (${state.consecutive_losses})` };
  }
  return { allowed: true, reason: null };
}

async function checkAccountProtection(balance) {
  const config = await loadRiskConfig();
  const state = await loadRiskState();
  const prot = config.account_protection;
  if (prot.emergency_stop) return { allowed: false, reason: 'Emergency stop activated' };
  if (balance < prot.min_balance) return { allowed: false, reason: `Balance below minimum (₹${balance.toFixed(2)} < ₹${prot.min_balance})` };
  if (state.peak_balance > 0) {
    const drawdownPct = ((state.peak_balance - balance) / state.peak_balance) * 100;
    if (drawdownPct >= prot.max_drawdown_percentage) {
      return { allowed: false, reason: `Max drawdown exceeded (${drawdownPct.toFixed(2)}% >= ${prot.max_drawdown_percentage}%)` };
    }
  }
  if (balance > state.peak_balance) {
    state.peak_balance = balance;
    await saveRiskState(state);
  }
  return { allowed: true, reason: null };
}

async function canOpenTrade(balance) {
  const daily = await checkDailyLimits();
  if (!daily.allowed) return daily;
  const account = await checkAccountProtection(balance);
  if (!account.allowed) return account;
  return { allowed: true, reason: null };
}

async function recordTradeResult(profitLoss) {
  const state = await loadRiskState();
  state.daily_trades++;
  if (profitLoss < 0) {
    state.daily_loss += Math.abs(profitLoss);
    state.consecutive_losses++;
  } else {
    state.daily_profit += profitLoss;
    state.consecutive_losses = 0;
  }
  await saveRiskState(state);
}

// ------------------------------
// Demo Trader Logic with cooldown
// ------------------------------
function calculateLivePL(openTrade, currentPrice) {
  if (!openTrade) return null;
  const entry = openTrade.entry_price;
  const amount = openTrade.amount;
  const type = openTrade.type;
  let profitUsdt = 0;
  if (type === 'LONG') profitUsdt = (currentPrice - entry) * amount;
  else profitUsdt = (entry - currentPrice) * amount;
  return profitUsdt;
}

async function closeFullPosition(data, openTrade, currentPrice, reason) {
  const entry = openTrade.entry_price;
  const amount = openTrade.amount;
  const type = openTrade.type;
  const usdtInr = await getUSDTINR();

  let profitUsdt = type === 'LONG'
    ? (currentPrice - entry) * amount
    : (entry - currentPrice) * amount;
  const profitInr = profitUsdt * usdtInr;

  const balanceBefore = data.balance;
  data.balance += profitInr;

  const tradeRecord = {
    type,
    entry_price: entry,
    exit_price: currentPrice,
    amount,
    profit_usdt: parseFloat(profitUsdt.toFixed(2)),
    profit_inr: parseFloat(profitInr.toFixed(2)),
    balance_before: balanceBefore,
    balance_after: data.balance,
    closed_at: new Date().toISOString(),
    exit_reason: reason,
    partial: false,
    stop_loss: openTrade.stop_loss,
    tp1_price: openTrade.tp1_price,
    opened_at: openTrade.opened_at,
    duration_ms: new Date() - new Date(openTrade.opened_at)
  };

  data.history.push(tradeRecord);
  await recordTradeResult(profitInr);
  data.open_trade = null;
  return tradeRecord;
}

async function updateDemoTrade(signal, price, atrValue, utbotStop) {
  signal = signal.charAt(0).toUpperCase() + signal.slice(1).toLowerCase();
