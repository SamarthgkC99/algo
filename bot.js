// bot.js – UT Bot Trading System with Risk Management (Node.js + Upstash Redis REST)
// ✅ FIXED: Binance 451 geo-block issue with multi-proxy fallback + CryptoCompare kline fallback
// ✅ ADDED: Configurable USDT/INR rate, trading hours based on IST, export/clear trades, improved trade history

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
const USDT_INR_KEY = 'usdt_inr_rate';  // new key for exchange rate

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
      last_signal: null
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
    last_reset: Date.now(),        // store as timestamp (ms)
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
        reset_hour: 0          // midnight (IST)
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
    rate = 85; // default
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
// Binance API with Multi-Proxy Fallback (unchanged)
// ------------------------------
// (No changes here, omitted for brevity)
const BINANCE_ENDPOINTS = [ ... ]; // same as original

async function binanceRequest(path, params = {}) { ... } // unchanged

async function fetchPriceFromCoinGecko() { ... } // unchanged

async function fetchKlinesFromCryptoCompare(limit = 350) { ... } // unchanged

async function fetchKlinesFromBybit(limit = 350) { ... } // unchanged

async function getCurrentPrice() { ... } // unchanged

async function getKlines(symbol = 'BTCUSDT', interval = '5m', limit = 350) { ... } // unchanged

// ------------------------------
// UT Bot Logic (unchanged)
// ------------------------------
function calcUtbot(klines, keyvalue, atrPeriod) { ... } // unchanged

async function getUTBotSignal() { ... } // unchanged

// ------------------------------
// Risk Management Functions (modified to use dynamic USDT/INR)
// ------------------------------
async function calculatePositionSize(balance) {
  const config = await loadRiskConfig();
  const method = config.position_sizing.method;
  let size = config.position_sizing.value;

  if (method === 'percentage') {
    const price = await getCurrentPrice();
    const btcPriceInr = price * (await getUSDTINR()); // use dynamic rate
    const positionValueInr = balance * (size / 100);
    size = positionValueInr / btcPriceInr;
  }

  const min = config.position_sizing.min_position_size;
  const max = config.position_sizing.max_position_size;
  size = Math.min(max, Math.max(min, size));
  return parseFloat(size.toFixed(6));
}

// In closeFullPosition, we now record stop_loss and tp1_price
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
    // new fields for history table
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

function calculateLivePL(openTrade, currentPrice) {
  if (!openTrade) return null;
  const entry = openTrade.entry_price;
  const amount = openTrade.amount;
  const type = openTrade.type;
  let profitUsdt = 0;
  if (type === 'LONG') profitUsdt = (currentPrice - entry) * amount;
  else profitUsdt = (entry - currentPrice) * amount;
  // use dynamic rate via a helper – we'll call getUSDTINR() async; but this function is sync.
  // We'll make it async in the route handler. Keep it sync here, convert later.
  return profitUsdt;
}

// The rest of risk functions remain same, but we'll modify the daily reset to use IST
async function resetDailyIfNeeded() {
  const config = await loadRiskConfig();
  const state = await loadRiskState();
  const resetHour = config.daily_limits.reset_hour;

  const nowIST = new Date().toLocaleString('en-US', { timeZone: 'Asia/Kolkata' });
  const now = new Date(nowIST);
  const lastReset = new Date(state.last_reset);

  const resetDateToday = new Date(now);
  resetDateToday.setHours(resetHour, 0, 0, 0);

  if (now >= resetDateToday && lastReset < resetDateToday) {
    await resetRiskState();
    console.log('🔄 Daily risk state reset (IST midnight)');
  }
}

// Also update isWithinTradingHours to use IST
function isWithinTradingHours(state) {
  if (state.force_start) return true;
  if (!state.enabled) return true;
  const hour = getCurrentHourIST();
  return hour >= state.start_hour && hour < state.end_hour;
}

// ------------------------------
// New Endpoints
// ------------------------------
app.get('/usdt-inr-rate', async (req, res) => {
  const rate = await getUSDTINR();
  res.json({ rate });
});

app.post('/usdt-inr-rate', async (req, res) => {
  try {
    const { rate } = req.body;
    if (typeof rate !== 'number' || rate <= 0) throw new Error('Invalid rate');
    await setUSDTINR(rate);
    res.json({ success: true });
  } catch (err) {
    res.status(400).json({ success: false, error: err.message });
  }
});

app.post('/clear-history', async (req, res) => {
  try {
    const data = await loadTrades();
    data.history = [];
    data.order_log = [];
    data.balance = START_BALANCE;
    data.open_trade = null;
    await saveTrades(data);
    // Also reset risk state to fresh start
    await resetRiskState();
    res.json({ success: true, message: 'All trades cleared, balance reset.' });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

app.get('/export-history', async (req, res) => {
  const data = await loadTrades();
  const trades = data.history;
  if (!trades.length) {
    return res.status(404).json({ error: 'No trades to export' });
  }
  // Prepare CSV header
  const headers = [
    'Type', 'Entry Price', 'Exit Price', 'Amount (BTC)', 'Stop Loss', 'TP1 Price',
    'Profit (USDT)', 'Profit (INR)', 'Exit Reason', 'Opened At', 'Closed At', 'Duration (s)'
  ];
  const rows = trades.map(t => [
    t.type,
    t.entry_price,
    t.exit_price,
    t.amount,
    t.stop_loss ?? 'N/A',
    t.tp1_price ?? 'N/A',
    t.profit_usdt,
    t.profit_inr,
    t.exit_reason,
    new Date(t.opened_at).toISOString(),
    new Date(t.closed_at).toISOString(),
    t.duration_ms ? (t.duration_ms / 1000).toFixed(1) : 'N/A'
  ]);
  const csv = [headers, ...rows].map(row => row.join(',')).join('\n');
  res.setHeader('Content-Type', 'text/csv');
  res.setHeader('Content-Disposition', 'attachment; filename=trade_history.csv');
  res.send(csv);
});

// ------------------------------
// Existing Routes (modified slightly)
// ------------------------------
// In /signal endpoint, when returning live_pl_inr we need to multiply by dynamic rate.
// We'll adjust inside the route.
app.get('/signal', async (req, res) => {
  try {
    const tradingAllowed = await isTradingAllowed();
    const { signal, price, atr, utbot_stop } = await getUTBotSignal();

    if (signal === 'No Data' || price === 0) {
      return res.status(500).json({ error: 'Could not generate signal' });
    }

    const data = await loadTrades();
    const openTrade = data.open_trade;
    const usdtInr = await getUSDTINR();
    const livePlUsdt = calculateLivePL(openTrade, price);
    const livePlInr = livePlUsdt ? livePlUsdt * usdtInr : null;

    let response;
    if (!tradingAllowed.allowed) {
      const riskStatus = await getRiskStatus();
      response = {
        price,
        signal: 'Hold',
        balance: data.balance,
        holding: !!openTrade,
        position_type: openTrade?.type || null,
        entry_price: openTrade?.entry_price || null,
        action: `⏸️ PAUSED: ${tradingAllowed.reason}`,
        last_closed_trade: null,
        latest_order: data.order_log?.slice(-1)[0] || null,
        live_pl_inr: livePlInr,
        stop_loss: openTrade?.stop_loss || null,
        tp_levels: openTrade?.tp_levels || [],
        position_size: openTrade?.amount || 0,
        atr,
        risk_status: riskStatus,
        trading_allowed: false,
        pause_reason: tradingAllowed.reason,
        force_start: (await loadTradingState()).force_start,
        strategy_info: {
          buy_strategy: 'UT Bot #2 (KV=2, ATR=300)',
          sell_strategy: 'UT Bot #1 (KV=2, ATR=1)'
        }
      };
      return res.json(response);
    }

    const { balance, holding, position_type, action, stop_loss, tp_levels, position_size } =
      await updateDemoTrade(signal, price, atr, utbot_stop);

    const updatedData = await loadTrades();
    const riskStatus = await getRiskStatus();

    response = {
      price,
      signal,
      balance,
      holding,
      position_type,
      entry_price: updatedData.open_trade?.entry_price || null,
      action,
      last_closed_trade: null,
      latest_order: updatedData.order_log?.slice(-1)[0] || null,
      live_pl_inr: livePlInr,
      stop_loss,
      tp_levels,
      position_size,
      atr,
      risk_status: riskStatus,
      trading_allowed: true,
      pause_reason: null,
      force_start: (await loadTradingState()).force_start,
      strategy_info: {
        buy_strategy: 'UT Bot #2 (KV=2, ATR=300)',
        sell_strategy: 'UT Bot #1 (KV=2, ATR=1)'
      }
    };
    res.json(response);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  }
});

// In /status, adjust P/L calculation similarly (already handled in /signal, but keep)
app.get('/status', async (req, res) => {
  const data = await loadTrades();
  const currentPrice = await getCurrentPrice();
  const usdtInr = await getUSDTINR();
  const livePlUsdt = calculateLivePL(data.open_trade, currentPrice);
  const livePlInr = livePlUsdt ? livePlUsdt * usdtInr : null;
  const riskStatus = await getRiskStatus();
  res.json({
    balance: data.balance,
    has_open_trade: !!data.open_trade,
    open_trade: data.open_trade,
    current_price: currentPrice,
    live_pl_inr: livePlInr,
    last_signal: data.last_signal,
    total_trades: data.history?.length || 0,
    risk_status: riskStatus,
    force_start: (await loadTradingState()).force_start
  });
});

// All other routes remain same (risk-config, trading-control, etc.)
// ...

// Cron job for daily reset (use improved function)
cron.schedule('0 * * * *', resetDailyIfNeeded);

// Start server
app.listen(port, () => {
  console.log(`✅ Server running on port ${port}`);
  console.log(`📡 Data source priority: Binance CDN → Bybit → CryptoCompare → CoinGecko`);
});      position_sizing: {
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
// ✅ Binance API with Multi-Proxy Fallback (fixes 451 geo-block)
// ------------------------------
// Priority order:
// 1. data-api.binance.vision  → Binance's public CDN (least geo-blocked)
// 2. api.binance.us           → US-region Binance (different IP range)
// 3. Standard Binance APIs    → Last resort
const BINANCE_ENDPOINTS = [
  'https://data-api.binance.vision',  // ✅ Best: Binance public market data CDN
  'https://api.binance.us',           // ✅ US endpoint (different geo-block rules)
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
    const timeout = setTimeout(() => controller.abort(), 6000); // 6s per endpoint

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

// ------------------------------
// ✅ CoinGecko Fallback — Price only
// ------------------------------
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

// ------------------------------
// ✅ CryptoCompare Fallback — Klines (OHLCV candle data)
// Converts CryptoCompare minute data to Binance kline format:
// [openTime, open, high, low, close, volume, ...]
// ------------------------------
async function fetchKlinesFromCryptoCompare(limit = 350) {
  try {
    const url = `https://min-api.cryptocompare.com/data/v2/histominute?fsym=BTC&tsym=USD&limit=${limit}&aggregate=5`;
    const response = await fetch(url, { timeout: 8000 });
    const data = await response.json();

    if (data && data.Data && data.Data.Data && data.Data.Data.length > 0) {
      console.log('✅ Klines from CryptoCompare (fallback)');
      // Map to Binance kline format: [openTime, open, high, low, close, volume]
      return data.Data.Data.map(c => [
        c.time * 1000,           // openTime (ms)
        String(c.open),          // open
        String(c.high),          // high
        String(c.low),           // low
        String(c.close),         // close
        String(c.volumefrom),    // volume
        c.time * 1000 + 299999,  // closeTime
        String(c.volumeto),      // quoteAssetVolume
        0, '0', '0', '0'         // filler fields
      ]);
    }
  } catch (err) {
    console.warn('CryptoCompare klines error:', err.message);
  }
  return null;
}

// ------------------------------
// ✅ Bybit Fallback — Klines (very permissive geo-access)
// ------------------------------
async function fetchKlinesFromBybit(limit = 350) {
  try {
    // Bybit uses different interval format: '5' = 5 minutes
    const url = `https://api.bybit.com/v5/market/kline?category=spot&symbol=BTCUSDT&interval=5&limit=${limit}`;
    const response = await fetch(url, { timeout: 8000 });
    const data = await response.json();

    if (data && data.result && data.result.list && data.result.list.length > 0) {
      console.log('✅ Klines from Bybit (fallback)');
      // Bybit format: [startTime, open, high, low, close, volume, turnover]
      // Reverse because Bybit returns newest first
      return data.result.list.reverse().map(c => [
        parseInt(c[0]),  // openTime (ms)
        c[1],            // open
        c[2],            // high
        c[3],            // low
        c[4],            // close
        c[5],            // volume
        parseInt(c[0]) + 299999, // closeTime
        c[6],            // quoteVolume
        0, '0', '0', '0'
      ]);
    }
  } catch (err) {
    console.warn('Bybit klines error:', err.message);
  }
  return null;
}

// ------------------------------
// Main Price & Kline Fetchers
// ------------------------------
async function getCurrentPrice() {
  // 1. Try Binance (all proxies)
  const binancePrice = await binanceRequest('/api/v3/ticker/price', { symbol: 'BTCUSDT' });
  if (binancePrice && binancePrice.price) return parseFloat(binancePrice.price);

  // 2. Try Bybit price
  try {
    const bybitUrl = 'https://api.bybit.com/v5/market/tickers?category=spot&symbol=BTCUSDT';
    const res = await fetch(bybitUrl, { timeout: 6000 });
    const data = await res.json();
    if (data?.result?.list?.[0]?.lastPrice) {
      console.log('✅ Price from Bybit');
      return parseFloat(data.result.list[0].lastPrice);
    }
  } catch (err) {
    console.warn('Bybit price error:', err.message);
  }

  // 3. Fallback to CoinGecko
  const geckoPrice = await fetchPriceFromCoinGecko();
  if (geckoPrice) return geckoPrice;

  console.error('❌ All price sources failed');
  return null;
}

async function getKlines(symbol = 'BTCUSDT', interval = '5m', limit = 350) {
  // 1. Try Binance (all proxies)
  const binanceKlines = await binanceRequest('/api/v3/klines', { symbol, interval, limit });
  if (binanceKlines && binanceKlines.length > 0) return binanceKlines;

  // 2. Try Bybit
  const bybitKlines = await fetchKlinesFromBybit(limit);
  if (bybitKlines && bybitKlines.length > 0) return bybitKlines;

  // 3. Try CryptoCompare
  const ccKlines = await fetchKlinesFromCryptoCompare(limit);
  if (ccKlines && ccKlines.length > 0) return ccKlines;

  console.error('❌ All kline sources failed');
  return null;
}

// ------------------------------
// UT Bot Logic (pure JS)
// ------------------------------
function calcUtbot(klines, keyvalue, atrPeriod) {
  const close = klines.map(k => parseFloat(k[4]));
  const high = klines.map(k => parseFloat(k[2]));
  const low = klines.map(k => parseFloat(k[3]));

  // Calculate TR
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

  // ATR (simple moving average)
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
// Risk Management Functions
// ------------------------------
async function calculatePositionSize(balance) {
  const config = await loadRiskConfig();
  const method = config.position_sizing.method;
  let size = config.position_sizing.value;

  if (method === 'percentage') {
    const price = await getCurrentPrice();
    const btcPriceInr = price * BTC_USDT_RATE;
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
    case 'atr':
      stop = atrStop;
      break;
    case 'percentage':
      stop = fixedStop;
      break;
    case 'utbot':
      stop = utbotStop || fixedStop;
      break;
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
// Demo Trader Logic
// ------------------------------
function calculateLivePL(openTrade, currentPrice) {
  if (!openTrade) return null;
  const entry = openTrade.entry_price;
  const amount = openTrade.amount;
  const type = openTrade.type;
  let profitUsdt = 0;
  if (type === 'LONG') profitUsdt = (currentPrice - entry) * amount;
  else profitUsdt = (entry - currentPrice) * amount;
  return parseFloat((profitUsdt * BTC_USDT_RATE).toFixed(2));
}

async function closeFullPosition(data, openTrade, currentPrice, reason) {
  const entry = openTrade.entry_price;
  const amount = openTrade.amount;
  const type = openTrade.type;

  let profitUsdt = type === 'LONG'
    ? (currentPrice - entry) * amount
    : (entry - currentPrice) * amount;
  const profitInr = profitUsdt * BTC_USDT_RATE;

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
    partial: false
  };

  data.history.push(tradeRecord);
  await recordTradeResult(profitInr);
  data.open_trade = null;
  return tradeRecord;
}

async function updateDemoTrade(signal, price, atrValue, utbotStop) {
  signal = signal.charAt(0).toUpperCase() + signal.slice(1).toLowerCase();
  const data = await loadTrades();
  const config = await loadRiskConfig();
  let openTrade = data.open_trade;
  let actionMessage = '';
  let lastClosedTrade = null;

  const logEntry = {
    time: new Date().toISOString(),
    side: signal,
    price,
    quantity: COINS_PER_TRADE
  };

  if (openTrade) {
    const hitType = (() => {
      const posType = openTrade.type;
      const sl = openTrade.stop_loss;
      const tp1 = openTrade.tp1_price;
      if (sl && ((posType === 'LONG' && price <= sl) || (posType === 'SHORT' && price >= sl))) return 'SL';
      if (tp1 && ((posType === 'LONG' && price >= tp1) || (posType === 'SHORT' && price <= tp1))) return 'TP1';
      return null;
    })();

    if (hitType === 'SL') {
      lastClosedTrade = await closeFullPosition(data, openTrade, price, 'Stop-Loss Hit');
      actionMessage = `🛑 STOP-LOSS HIT @ $${price.toFixed(2)} | P/L: ₹${lastClosedTrade.profit_inr.toFixed(2)}`;
      logEntry.action = 'STOP_LOSS';
      logEntry.pl_inr = lastClosedTrade.profit_inr;
      openTrade = null;
    } else if (hitType === 'TP1') {
      lastClosedTrade = await closeFullPosition(data, openTrade, price, 'TP1 Hit - Full Exit');
      actionMessage = `✅ TP1 HIT @ $${price.toFixed(2)} | FULL EXIT | P/L: ₹${lastClosedTrade.profit_inr.toFixed(2)}`;
      logEntry.action = 'TP1_FULL_EXIT';
      logEntry.pl_inr = lastClosedTrade.profit_inr;
      openTrade = null;
    } else if (openTrade && openTrade.breakeven_moved && config.stop_loss.trailing_enabled) {
      const newStop = updateTrailingStop(price, openTrade.type, openTrade.stop_loss, atrValue, config);
      if (newStop) {
        openTrade.stop_loss = newStop;
        actionMessage = `📈 Trailing stop updated to $${newStop.toFixed(2)}`;
        logEntry.action = 'TRAILING_STOP_UPDATE';
      }
    }
  }

  if (signal === 'Hold') {
    if (!actionMessage) {
      actionMessage = 'Holding position. Waiting for next signal.';
      logEntry.action = 'HOLD';
    }
  } else if (signal === 'Buy') {
    const canTrade = await canOpenTrade(data.balance);
    if (!canTrade.allowed) {
      actionMessage = `⚠️ Cannot open BUY: ${canTrade.reason}`;
      logEntry.action = 'BLOCKED';
    } else if (openTrade && openTrade.type === 'LONG') {
      actionMessage = 'Ignoring repeated "Buy" signal. Already in LONG position.';
      logEntry.action = 'IGNORED';
    } else {
      if (openTrade && openTrade.type === 'SHORT') {
        lastClosedTrade = await closeFullPosition(data, openTrade, price, 'Opposite Signal');
        actionMessage = `CLOSED SHORT @ $${price.toFixed(2)}, P/L: ₹${lastClosedTrade.profit_inr.toFixed(2)}. | `;
        logEntry.action = 'CLOSE_SHORT';
        openTrade = null;
      }

      const positionSize = await calculatePositionSize(data.balance);
      const stopLoss = calculateStopLoss(price, 'LONG', atrValue, utbotStop, config);
      const tpLevels = calculateTakeProfitLevels(price, 'LONG', atrValue, config);
      const tp1Price = tpLevels[0]?.price || null;

      openTrade = {
        type: 'LONG',
        entry_price: price,
        amount: positionSize,
        original_amount: positionSize,
        stop_loss: stopLoss,
        tp1_price: tp1Price,
        tp_levels: tpLevels.slice(0,1),
        opened_at: new Date().toISOString(),
        strategy: 'UT Bot #2 (KV=2, ATR=300)',
        atr_at_entry: atrValue,
        breakeven_moved: false
      };
      actionMessage += `🟢 OPENED LONG @ $${price.toFixed(2)} | Size: ${positionSize} BTC | SL: $${stopLoss.toFixed(2)} | TP1: $${tp1Price?.toFixed(2) || 'N/A'}`;
      logEntry.action = 'OPEN_LONG';
      logEntry.stop_loss = stopLoss;
      logEntry.tp1 = tp1Price;
      data.last_signal = 'Buy';
    }
  } else if (signal === 'Sell') {
    const canTrade = await canOpenTrade(data.balance);
    if (!canTrade.allowed) {
      actionMessage = `⚠️ Cannot open SELL: ${canTrade.reason}`;
      logEntry.action = 'BLOCKED';
    } else if (openTrade && openTrade.type === 'SHORT') {
      actionMessage = 'Ignoring repeated "Sell" signal. Already in SHORT position.';
      logEntry.action = 'IGNORED';
    } else {
      if (openTrade && openTrade.type === 'LONG') {
        lastClosedTrade = await closeFullPosition(data, openTrade, price, 'Opposite Signal');
        actionMessage = `CLOSED LONG @ $${price.toFixed(2)}, P/L: ₹${lastClosedTrade.profit_inr.toFixed(2)}. | `;
        logEntry.action = 'CLOSE_LONG';
        openTrade = null;
      }

      const positionSize = await calculatePositionSize(data.balance);
      const stopLoss = calculateStopLoss(price, 'SHORT', atrValue, utbotStop, config);
      const tpLevels = calculateTakeProfitLevels(price, 'SHORT', atrValue, config);
      const tp1Price = tpLevels[0]?.price || null;

      openTrade = {
        type: 'SHORT',
        entry_price: price,
        amount: positionSize,
        original_amount: positionSize,
        stop_loss: stopLoss,
        tp1_price: tp1Price,
        tp_levels: tpLevels.slice(0,1),
        opened_at: new Date().toISOString(),
        strategy: 'UT Bot #1 (KV=2, ATR=1)',
        atr_at_entry: atrValue,
        breakeven_moved: false
      };
      actionMessage += `🔴 OPENED SHORT @ $${price.toFixed(2)} | Size: ${positionSize} BTC | SL: $${stopLoss.toFixed(2)} | TP1: $${tp1Price?.toFixed(2) || 'N/A'}`;
      logEntry.action = 'OPEN_SHORT';
      logEntry.stop_loss = stopLoss;
      logEntry.tp1 = tp1Price;
      data.last_signal = 'Sell';
    }
  }

  if (!data.order_log) data.order_log = [];
  data.order_log.push(logEntry);
  if (data.order_log.length > 100) data.order_log.shift();

  data.open_trade = openTrade;
  await saveTrades(data);

  return {
    balance: data.balance,
    holding: !!openTrade,
    position_type: openTrade?.type || null,
    action: actionMessage,
    stop_loss: openTrade?.stop_loss || null,
    tp_levels: openTrade?.tp_levels || [],
    position_size: openTrade?.amount || 0
  };
}

async function forceClosePosition(currentPrice, reason) {
  const data = await loadTrades();
  const openTrade = data.open_trade;
  if (!openTrade) return null;

  const closedTrade = await closeFullPosition(data, openTrade, currentPrice, reason);
  const logEntry = {
    time: new Date().toISOString(),
    side: 'CLOSE',
    action: 'FORCE_CLOSE',
    price: currentPrice,
    quantity: openTrade.amount,
    pl_inr: closedTrade.profit_inr
  };
  data.order_log.push(logEntry);
  await saveTrades(data);
  return closedTrade;
}

// ------------------------------
// Trading Hours & Control
// ------------------------------
function isWithinTradingHours(state) {
  if (state.force_start) return true;
  if (!state.enabled) return true;
  const now = new Date();
  const hour = now.getHours();
  return hour >= state.start_hour && hour < state.end_hour;
}

async function isTradingAllowed() {
  const state = await loadTradingState();
  if (state.force_start) return { allowed: true, reason: null };
  if (state.manual_pause) return { allowed: false, reason: 'Trading manually paused' };
  if (!isWithinTradingHours(state)) {
    const now = new Date();
    return { allowed: false, reason: `Outside trading hours (${state.start_hour}:00 - ${state.end_hour}:00). Current: ${now.getHours()}:00` };
  }
  return { allowed: true, reason: null };
}

// ------------------------------
// Express Routes
// ------------------------------
app.use(express.json());
app.use(express.static(__dirname));

app.get('/', (req, res) => {
  res.sendFile(__dirname + '/index.html');
});

app.get('/signal', async (req, res) => {
  try {
    const tradingAllowed = await isTradingAllowed();
    const { signal, price, atr, utbot_stop } = await getUTBotSignal();

    if (signal === 'No Data' || price === 0) {
      return res.status(500).json({ error: 'Could not generate signal' });
    }

    const data = await loadTrades();
    const openTrade = data.open_trade;

    let response;
    if (!tradingAllowed.allowed) {
      const livePl = calculateLivePL(openTrade, price);
      const riskStatus = await getRiskStatus();
      response = {
        price,
        signal: 'Hold',
        balance: data.balance,
        holding: !!openTrade,
        position_type: openTrade?.type || null,
        entry_price: openTrade?.entry_price || null,
        action: `⏸️ PAUSED: ${tradingAllowed.reason}`,
        last_closed_trade: null,
        latest_order: data.order_log?.slice(-1)[0] || null,
        live_pl_inr: livePl,
        stop_loss: openTrade?.stop_loss || null,
        tp_levels: openTrade?.tp_levels || [],
        position_size: openTrade?.amount || 0,
        atr,
        risk_status: riskStatus,
        trading_allowed: false,
        pause_reason: tradingAllowed.reason,
        force_start: (await loadTradingState()).force_start,
        strategy_info: {
          buy_strategy: 'UT Bot #2 (KV=2, ATR=300)',
          sell_strategy: 'UT Bot #1 (KV=2, ATR=1)'
        }
      };
      return res.json(response);
    }

    const { balance, holding, position_type, action, stop_loss, tp_levels, position_size } =
      await updateDemoTrade(signal, price, atr, utbot_stop);

    const updatedData = await loadTrades();
    const livePl = calculateLivePL(updatedData.open_trade, price);
    const riskStatus = await getRiskStatus();

    response = {
      price,
      signal,
      balance,
      holding,
      position_type,
      entry_price: updatedData.open_trade?.entry_price || null,
      action,
      last_closed_trade: null,
      latest_order: updatedData.order_log?.slice(-1)[0] || null,
      live_pl_inr: livePl,
      stop_loss,
      tp_levels,
      position_size,
      atr,
      risk_status: riskStatus,
      trading_allowed: true,
      pause_reason: null,
      force_start: (await loadTradingState()).force_start,
      strategy_info: {
        buy_strategy: 'UT Bot #2 (KV=2, ATR=300)',
        sell_strategy: 'UT Bot #1 (KV=2, ATR=1)'
      }
    };
    res.json(response);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  }
});

app.get('/chart-data', async (req, res) => {
  try {
    const klines = await getKlines();
    if (!klines) return res.status(500).json({ error: 'No data' });

    const candles = klines.map(k => ({
      time: Math.floor(k[0] / 1000),
      open: parseFloat(k[1]),
      high: parseFloat(k[2]),
      low: parseFloat(k[3]),
      close: parseFloat(k[4])
    }));

    const df1 = calcUtbot(klines, 2, 1);
    const stopLine = df1.stop.map((val, idx) => ({
      time: candles[idx].time,
      value: val
    }));

    res.json({ candles, stop_line: stopLine });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  }
});

app.get('/history', async (req, res) => {
  const data = await loadTrades();
  res.json(data.history || []);
});

app.get('/orders', async (req, res) => {
  const data = await loadTrades();
  res.json((data.order_log || []).reverse());
});

app.get('/status', async (req, res) => {
  const data = await loadTrades();
  const currentPrice = await getCurrentPrice();
  const livePl = calculateLivePL(data.open_trade, currentPrice);
  const riskStatus = await getRiskStatus();
  res.json({
    balance: data.balance,
    has_open_trade: !!data.open_trade,
    open_trade: data.open_trade,
    current_price: currentPrice,
    live_pl_inr: livePl,
    last_signal: data.last_signal,
    total_trades: data.history?.length || 0,
    risk_status: riskStatus,
    force_start: (await loadTradingState()).force_start
  });
});

app.get('/risk-config', async (req, res) => {
  const config = await loadRiskConfig();
  res.json(config);
});

app.post('/risk-config', async (req, res) => {
  try {
    const newConfig = req.body;
    await saveRiskConfig(newConfig);
    res.json({ success: true, message: 'Risk configuration updated' });
  } catch (err) {
    res.status(400).json({ success: false, error: err.message });
  }
});

app.get('/risk-status', async (req, res) => {
  try {
    const status = await getRiskStatus();
    res.json(status);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.get('/trading-control', async (req, res) => {
  const state = await loadTradingState();
  const { allowed, reason } = await isTradingAllowed();
  res.json({
    state,
    trading_allowed: allowed,
    pause_reason: reason,
    current_time: new Date().toLocaleTimeString()
  });
});

app.post('/trading-control', async (req, res) => {
  try {
    const { action } = req.body;
    const state = await loadTradingState();

    switch (action) {
      case 'pause':
        state.manual_pause = true;
        state.force_start = false;
        await saveTradingState(state);
        return res.json({ success: true, message: 'Trading paused manually' });
      case 'resume':
        state.manual_pause = false;
        state.force_start = false;
        await saveTradingState(state);
        return res.json({ success: true, message: 'Trading resumed' });
      case 'force_start':
        state.manual_pause = false;
        state.force_start = true;
        await saveTradingState(state);
        return res.json({ success: true, message: 'Force start activated - Trading 24/7' });
      case 'force_stop':
        const currentPrice = await getCurrentPrice();
        if (currentPrice) {
          const closedTrade = await forceClosePosition(currentPrice, 'Force Stop');
          const message = closedTrade
            ? `Position closed at $${currentPrice.toFixed(2)} | P/L: ₹${closedTrade.profit_inr.toFixed(2)}`
            : 'No open position to close';
          state.manual_pause = true;
          state.force_start = false;
          await saveTradingState(state);
          return res.json({ success: true, message });
        } else {
          return res.json({ success: false, message: 'Could not get current price to close position' });
        }
      case 'update_hours':
        state.start_hour = req.body.start_hour ?? state.start_hour;
        state.end_hour = req.body.end_hour ?? state.end_hour;
        state.enabled = req.body.enabled ?? state.enabled;
        await saveTradingState(state);
        return res.json({ success: true, message: 'Trading hours updated' });
      default:
        return res.status(400).json({ success: false, error: 'Invalid action' });
    }
  } catch (err) {
    res.status(400).json({ success: false, error: err.message });
  }
});

// ------------------------------
// Helper for getRiskStatus (used in routes)
// ------------------------------
async function getRiskStatus() {
  const config = await loadRiskConfig();
  const state = await loadRiskState();
  const limits = config.daily_limits;

  return {
    daily_stats: {
      trades: `${state.daily_trades}/${limits.max_daily_trades}`,
      loss: `₹${state.daily_loss.toFixed(2)}/₹${limits.max_daily_loss.toFixed(2)}`,
      profit: `₹${state.daily_profit.toFixed(2)}`,
      consecutive_losses: `${state.consecutive_losses}/${limits.max_consecutive_losses}`
    },
    limits_usage: {
      trades_pct: (state.daily_trades / limits.max_daily_trades) * 100,
      loss_pct: (state.daily_loss / limits.max_daily_loss) * 100
    },
    config
  };
}

// ------------------------------
// Cron Job: Reset Daily Risk State
// ------------------------------
async function resetDailyIfNeeded() {
  const config = await loadRiskConfig();
  const state = await loadRiskState();
  const resetHour = config.daily_limits.reset_hour;

  const now = new Date();
  const lastReset = new Date(state.last_reset);
  if (now.getHours() >= resetHour && lastReset.getHours() < resetHour &&
      (now.getDate() !== lastReset.getDate() || now.getMonth() !== lastReset.getMonth())) {
    await resetRiskState();
    console.log('🔄 Daily risk state reset');
  }
}

cron.schedule('0 * * * *', resetDailyIfNeeded);

// ------------------------------
// Start Server
// ------------------------------
app.listen(port, () => {
  console.log(`✅ Server running on port ${port}`);
  console.log(`📡 Data source priority: Binance CDN → Bybit → CryptoCompare → CoinGecko`);
});
