'use strict';

/**
 * ═══════════════════════════════════════════════════════════════
 *  CAPUNGA TRADE — BTCUSDT Signal Engine v2.0
 *  Estratégia: multi-timeframe + ATR + cooldown dinâmico
 *  Gale máximo: 1
 * ═══════════════════════════════════════════════════════════════
 */

const express = require('express');
const cors = require('cors');
const axios = require('axios');
const { Pool } = require('pg');

const app = express();
app.use(cors());
app.use(express.json({ limit: '256kb' }));

// ─── Config ────────────────────────────────────────────────────────────────

const PORT = Number(process.env.PORT || 3000);
const DATABASE_URL = process.env.DATABASE_URL;
const BOT_TOKEN = (process.env.BOT_TOKEN || '').trim();
const CHAT_ID = (process.env.CHAT_ID || '').trim();

const SYMBOL = (process.env.SYMBOL || 'BTCUSDT').toUpperCase();
const INTERVALO_ANALISE_MS = Math.max(20000, Number(process.env.INTERVALO_ANALISE_MS || 45000));
const SINAL_EXPIRA_MS = Math.max(180000, Number(process.env.SINAL_EXPIRA_MS || 720000)); // 12 min
const MIN_CONFLUENCIA = Math.min(95, Math.max(60, Number(process.env.MIN_CONFLUENCIA || 80)));

const MAX_GALE = 1;
const TP_ATR_MULT = 1.8;
const SL_ATR_MULT_GALE0 = 1.0;
const SL_ATR_MULT_GALE1 = 2.5;

// Cooldown dinâmico por score do ÚLTIMO sinal
const COOLDOWN_TIERS = [
  { min: 92, ms: 90 * 1000 },    // premium  → 1.5 min
  { min: 88, ms: 150 * 1000 },   // excelente → 2.5 min
  { min: 84, ms: 240 * 1000 },   // forte    → 4 min
  { min: 80, ms: 360 * 1000 },   // bom      → 6 min
  { min: 0,  ms: 600 * 1000 }    // fraco    → 10 min
];

if (!DATABASE_URL) {
  console.error('[FATAL] DATABASE_URL em falta.');
  process.exit(1);
}

const pool = new Pool({
  connectionString: DATABASE_URL,
  ssl: process.env.DATABASE_SSL === 'false' ? false : { rejectUnauthorized: false },
  max: 8,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 12000
});

// ─── Estado ────────────────────────────────────────────────────────────────

let analisando = false;
let ultimaAnalise = null;
let lastError = null;
let totalSinais = 0;
let timerAnalise = null;

// ─── Utils ─────────────────────────────────────────────────────────────────

const agoraISO = () => new Date().toISOString();
const sleep = (ms) => new Promise(r => setTimeout(r, ms));
const clamp = (n, min, max) => Math.max(min, Math.min(max, n));
const num = (v, fb = 0) => { const n = Number(v); return Number.isFinite(n) ? n : fb; };
const fmtPreco = (n) => Number(n).toLocaleString('pt-PT', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
const fmtPct = (n) => (n >= 0 ? '+' : '') + Number(n).toFixed(3) + '%';

// ─── Telegram ──────────────────────────────────────────────────────────────

async function telegram(texto) {
  if (!BOT_TOKEN || !CHAT_ID) return false;
  try {
    const { data } = await axios.post(
      `https://api.telegram.org/bot${BOT_TOKEN}/sendMessage`,
      { chat_id: CHAT_ID, text: texto, disable_web_page_preview: true, parse_mode: 'HTML' },
      { timeout: 12000 }
    );
    return data?.ok === true;
  } catch (err) {
    console.error('[TG]', err.response?.data || err.message);
    lastError = `telegram: ${err.message}`;
    return false;
  }
}

// ─── Schema ────────────────────────────────────────────────────────────────

async function criarTabelas() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS trade_sinais (
      id BIGSERIAL PRIMARY KEY,
      symbol TEXT NOT NULL,
      direcao TEXT NOT NULL CHECK (direcao IN ('COMPRA','VENDA')),
      preco_entrada NUMERIC NOT NULL,
      tp NUMERIC,
      sl NUMERIC,
      confluencia INTEGER NOT NULL,
      indicadores JSONB,
      estado TEXT NOT NULL DEFAULT 'PENDENTE'
        CHECK (estado IN ('PENDENTE','GREEN','G1','LOSS','CANCELADO','EXPIRADO')),
      gale SMALLINT NOT NULL DEFAULT 0 CHECK (gale BETWEEN 0 AND 1),
      preco_saida NUMERIC,
      pnl_pct NUMERIC,
      criado_em TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      resolvido_em TIMESTAMPTZ,
      ultima_vela_avaliada TIMESTAMPTZ,
      mensagem_entrada TEXT
    );
  `);
  // Compatibilidade com instalações antigas
  await pool.query(`ALTER TABLE trade_sinais ADD COLUMN IF NOT EXISTS tp NUMERIC;`);
  await pool.query(`ALTER TABLE trade_sinais ADD COLUMN IF NOT EXISTS sl NUMERIC;`);
  await pool.query(`ALTER TABLE trade_sinais ADD COLUMN IF NOT EXISTS ultima_vela_avaliada TIMESTAMPTZ;`);

  await pool.query(`CREATE INDEX IF NOT EXISTS idx_sinais_estado ON trade_sinais (estado, criado_em DESC);`);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS trade_eventos (
      id BIGSERIAL PRIMARY KEY,
      sinal_id BIGINT,
      tipo TEXT NOT NULL,
      detalhe TEXT,
      preco NUMERIC,
      ocorrido_em TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);
  await pool.query(`CREATE INDEX IF NOT EXISTS idx_eventos_sinal ON trade_eventos (sinal_id, ocorrido_em DESC);`);

  // Limpeza opcional da tabela antiga (não usada)
  await pool.query(`DROP TABLE IF EXISTS trade_candles;`);

  console.log('[DB] Schema Capunga Trade v2 verificado.');
}

// ─── Market data (multi-fonte) ─────────────────────────────────────────────

const cacheMarket = new Map();
const cacheGet = (k) => { const v = cacheMarket.get(k); if (!v) return null; if (Date.now() > v.exp) { cacheMarket.delete(k); return null; } return v.val; };
const cacheSet = (k, val, ttl) => cacheMarket.set(k, { val, exp: Date.now() + ttl });

const OKX_TF   = { '1m': '1m', '5m': '5m', '15m': '15m' };
const KUCOIN_TF= { '1m': '1min', '5m': '5min', '15m': '15min' };
const BYBIT_TF = { '1m': '1', '5m': '5', '15m': '15' };

async function fetchKlines(interval = '1m', limit = 120) {
  const cacheKey = `${SYMBOL}:${interval}:${limit}`;
  const cached = cacheGet(cacheKey);
  if (cached) return cached;

  const headers = { 'User-Agent': 'CapungaTrade/2.0', Accept: 'application/json' };
  const errors = [];

  // 1) OKX
  try {
    const instId = SYMBOL.replace('USDT', '-USDT');
    const url = `https://www.okx.com/api/v5/market/candles?instId=${instId}&bar=${OKX_TF[interval] || '1m'}&limit=${limit}`;
    const { data } = await axios.get(url, { timeout: 9000, headers });
    if (data?.code === '0' && data.data?.length) {
      const out = data.data.slice().reverse().map(k => ({
        openTime: Number(k[0]), open: num(k[1]), high: num(k[2]), low: num(k[3]), close: num(k[4]),
        volume: num(k[5]), closeTime: Number(k[0]) + 60000
      }));
      cacheSet(cacheKey, out, interval === '1m' ? 30000 : 120000);
      return out;
    }
    errors.push('okx: empty');
  } catch (e) { errors.push(`okx: ${e.message}`); }

  // 2) KuCoin
  try {
    const sym = SYMBOL.replace('USDT', '-USDT');
    const url = `https://api.kucoin.com/api/v1/market/candles?type=${KUCOIN_TF[interval] || '1min'}&symbol=${sym}`;
    const { data } = await axios.get(url, { timeout: 9000, headers });
    if (data?.code === '200000' && data.data?.length) {
      const rows = data.data.slice(0, limit).reverse();
      const out = rows.map(k => {
        const ts = Number(k[0]) * (String(k[0]).length === 10 ? 1000 : 1);
        return { openTime: ts, open: num(k[1]), high: num(k[3]), low: num(k[4]), close: num(k[2]),
          volume: num(k[5]), closeTime: ts + 60000 };
      });
      cacheSet(cacheKey, out, interval === '1m' ? 30000 : 120000);
      return out;
    }
    errors.push('kucoin: empty');
  } catch (e) { errors.push(`kucoin: ${e.message}`); }

  // 3) Bybit
  try {
    const url = `https://api.bybit.com/v5/market/kline?category=linear&symbol=${SYMBOL}&interval=${BYBIT_TF[interval] || '1'}&limit=${limit}`;
    const { data } = await axios.get(url, { timeout: 9000, headers });
    const list = data?.result?.list || [];
    if (list.length) {
      const out = list.slice().reverse().map(k => ({
        openTime: Number(k[0]), open: num(k[1]), high: num(k[2]), low: num(k[3]), close: num(k[4]),
        volume: num(k[5]), closeTime: Number(k[0]) + 60000
      }));
      cacheSet(cacheKey, out, interval === '1m' ? 30000 : 120000);
      return out;
    }
    errors.push('bybit: empty');
  } catch (e) { errors.push(`bybit: ${e.message}`); }

  // 4) Binance (último recurso)
  try {
    const url = `https://api.binance.com/api/v3/klines?symbol=${SYMBOL}&interval=${interval}&limit=${limit}`;
    const { data } = await axios.get(url, { timeout: 9000, headers });
    if (Array.isArray(data) && data.length) {
      const out = data.map(k => ({
        openTime: Number(k[0]), open: num(k[1]), high: num(k[2]), low: num(k[3]), close: num(k[4]),
        volume: num(k[5]), closeTime: Number(k[6])
      }));
      cacheSet(cacheKey, out, interval === '1m' ? 30000 : 120000);
      return out;
    }
    errors.push('binance: empty');
  } catch (e) { errors.push(`binance: ${e.message}`); }

  throw new Error(`market_data: todas as fontes falharam → ${errors.join(' | ')}`);
}

// ─── Indicadores ───────────────────────────────────────────────────────────

function ema(values, period) {
  if (values.length < period) return [];
  const k = 2 / (period + 1);
  const out = [];
  let prev = values.slice(0, period).reduce((a, b) => a + b, 0) / period;
  out.push(prev);
  for (let i = period; i < values.length; i++) { prev = values[i] * k + prev * (1 - k); out.push(prev); }
  return out;
}

function sma(values, period) {
  if (values.length < period) return [];
  const out = []; let sum = 0;
  for (let i = 0; i < values.length; i++) {
    sum += values[i];
    if (i >= period) sum -= values[i - period];
    if (i >= period - 1) out.push(sum / period);
  }
  return out;
}

function rsi(closes, period = 14) {
  if (closes.length < period + 1) return [];
  const changes = [];
  for (let i = 1; i < closes.length; i++) changes.push(closes[i] - closes[i - 1]);
  let avgGain = 0, avgLoss = 0;
  for (let i = 0; i < period; i++) { if (changes[i] >= 0) avgGain += changes[i]; else avgLoss -= changes[i]; }
  avgGain /= period; avgLoss /= period;
  const out = [avgLoss === 0 ? 100 : 100 - 100 / (1 + avgGain / avgLoss)];
  for (let i = period; i < changes.length; i++) {
    const g = changes[i] > 0 ? changes[i] : 0;
    const l = changes[i] < 0 ? -changes[i] : 0;
    avgGain = (avgGain * (period - 1) + g) / period;
    avgLoss = (avgLoss * (period - 1) + l) / period;
    out.push(avgLoss === 0 ? 100 : 100 - 100 / (1 + avgGain / avgLoss));
  }
  return out;
}

function macd(closes, fast = 12, slow = 26, signal = 9) {
  const emaFast = ema(closes, fast);
  const emaSlow = ema(closes, slow);
  if (!emaFast.length || !emaSlow.length) return { macdLine: [], signalLine: [], hist: [] };
  const offset = slow - fast;
  const macdLine = [];
  for (let i = 0; i < emaSlow.length; i++) macdLine.push(emaFast[i + offset] - emaSlow[i]);
  const signalLine = ema(macdLine, signal);
  const hist = [];
  const sigOffset = macdLine.length - signalLine.length;
  for (let i = 0; i < signalLine.length; i++) hist.push(macdLine[i + sigOffset] - signalLine[i]);
  return { macdLine, signalLine, hist };
}

function atr(highs, lows, closes, period = 14) {
  if (highs.length < period + 1) return [];
  const trs = [];
  for (let i = 1; i < highs.length; i++) {
    trs.push(Math.max(highs[i] - lows[i], Math.abs(highs[i] - closes[i - 1]), Math.abs(lows[i] - closes[i - 1])));
  }
  return sma(trs, period);
}

function bollinger(closes, period = 20, mult = 2) {
  const mid = sma(closes, period);
  if (!mid.length) return { mid: [], upper: [], lower: [] };
  const upper = [], lower = [];
  for (let i = 0; i < mid.length; i++) {
    const idx = i + period - 1;
    const slice = closes.slice(idx - period + 1, idx + 1);
    const mean = mid[i];
    const variance = slice.reduce((a, v) => a + (v - mean) ** 2, 0) / period;
    const sd = Math.sqrt(variance);
    upper.push(mean + sd * mult);
    lower.push(mean - sd * mult);
  }
  return { mid, upper, lower };
}

function stochRsi(closes, period = 14, kPeriod = 3, dPeriod = 3) {
  const r = rsi(closes, period);
  if (r.length < period) return [];
  const stoch = [];
  for (let i = period - 1; i < r.length; i++) {
    const slice = r.slice(i - period + 1, i + 1);
    const lo = Math.min(...slice);
    const hi = Math.max(...slice);
    stoch.push(hi === lo ? 50 : ((r[i] - lo) / (hi - lo)) * 100);
  }
  const k = sma(stoch, kPeriod);
  return k.length ? k : stoch;
}

function tendenciaDe(candles) {
  if (!candles || candles.length < 30) return 'NEUTRO';
  const closes = candles.map(c => c.close);
  const e9 = ema(closes, 9);
  const e21 = ema(closes, 21);
  if (!e9.length || !e21.length) return 'NEUTRO';
  const e9last = e9[e9.length - 1];
  const e21last = e21[e21.length - 1];
  const e9prev = e9[Math.max(0, e9.length - 4)] || e9last;
  const slope = e9last - e9prev;
  if (e9last > e21last && slope > 0) return 'ALTA';
  if (e9last < e21last && slope < 0) return 'BAIXA';
  return 'NEUTRO';
}

function detectarDivergencia(closes, rsiVals, lookback = 14) {
  if (closes.length < lookback || rsiVals.length < lookback) return null;
  const n = closes.length;
  const pr = closes.slice(n - lookback);
  const rs = rsiVals.slice(rsiVals.length - lookback);

  // Procura mínimos/máximos locais nas últimas 3 velas vs. pico oposto na janela
  const lastPr = pr[pr.length - 1];
  const lastRs = rs[rs.length - 1];

  const minIdx = pr.indexOf(Math.min(...pr.slice(0, -2)));
  const maxIdx = pr.indexOf(Math.max(...pr.slice(0, -2)));
  const rsiAtMin = rs[minIdx];
  const rsiAtMax = rs[maxIdx];

  // Bullish: preço fez novo mínimo, mas RSI mais alto
  if (lastPr <= pr[minIdx] && lastRs > rsiAtMin + 3) {
    return { tipo: 'BULLISH', detalhe: `RSI ${rsiAtMin.toFixed(1)}→${lastRs.toFixed(1)}` };
  }
  // Bearish: preço fez novo máximo, mas RSI mais baixo
  if (lastPr >= pr[maxIdx] && lastRs < rsiAtMax - 3) {
    return { tipo: 'BEARISH', detalhe: `RSI ${rsiAtMax.toFixed(1)}→${lastRs.toFixed(1)}` };
  }
  return null;
}

function detectarPadraoVelas(candles) {
  if (candles.length < 3) return null;
  const ant = candles[candles.length - 2];
  const atual = candles[candles.length - 1];
  const corpoAtual = Math.abs(atual.close - atual.open);
  const corpoAnt = Math.abs(ant.close - ant.open);

  if (corpoAtual < 1e-9 || corpoAnt < 1e-9) return null;

  // Engolfo
  if (ant.close < ant.open && atual.close > atual.open &&
      atual.close > ant.open && atual.open < ant.close && corpoAtual > corpoAnt * 1.1) {
    return { tipo: 'ENGOLFO_ALTA', peso: 'forte' };
  }
  if (ant.close > ant.open && atual.close < atual.open &&
      atual.close < ant.open && atual.open > ant.close && corpoAtual > corpoAnt * 1.1) {
    return { tipo: 'ENGOLFO_BAIXA', peso: 'forte' };
  }

  const sombraSup = atual.high - Math.max(atual.open, atual.close);
  const sombraInf = Math.min(atual.open, atual.close) - atual.low;

  if (sombraInf > corpoAtual * 2 && sombraSup < corpoAtual * 0.6) return { tipo: 'MARTELO', peso: 'media' };
  if (sombraSup > corpoAtual * 2 && sombraInf < corpoAtual * 0.6) return { tipo: 'ESTRELA_CADENTE', peso: 'media' };

  return null;
}

// ─── Motor de confluência multi-timeframe ──────────────────────────────────

function analisarConfluencia(c1m, c5m, c15m) {
  if (!c1m || c1m.length < 80) return { ok: false, motivo: 'amostra_1m_insuficiente' };
  if (!c5m || c5m.length < 40) return { ok: false, motivo: 'amostra_5m_insuficiente' };
  if (!c15m || c15m.length < 30) return { ok: false, motivo: 'amostra_15m_insuficiente' };

  const closes = c1m.map(c => c.close);
  const highs  = c1m.map(c => c.high);
  const lows   = c1m.map(c => c.low);
  const vols   = c1m.map(c => c.volume);

  const e9 = ema(closes, 9), e21 = ema(closes, 21), e50 = ema(closes, 50);
  const r = rsi(closes, 14);
  const { hist } = macd(closes);
  const a = atr(highs, lows, closes, 14);
  const bb = bollinger(closes, 20, 2);
  const stoch = stochRsi(closes);
  const volSma = sma(vols, 20);

  if (!e9.length || !e21.length || !e50.length || !r.length || !hist.length ||
      !a.length || !bb.mid.length || !stoch.length || !volSma.length) {
    return { ok: false, motivo: 'indicadores_insuficientes' };
  }

  const i = closes.length - 1;
  const price = closes[i];
  const e9v = e9[e9.length - 1], e21v = e21[e21.length - 1], e50v = e50[e50.length - 1];
  const rv = r[r.length - 1];
  const hv = hist[hist.length - 1], hvPrev = hist[hist.length - 2] || 0;
  const av = a[a.length - 1];
  const bbU = bb.upper[bb.upper.length - 1];
  const bbL = bb.lower[bb.lower.length - 1];
  const bbM = bb.mid[bb.mid.length - 1];
  const stv = stoch[stoch.length - 1];
  const vol = vols[i];
  const volMed = volSma[volSma.length - 1];

  const t1 = tendenciaDe(c1m);
  const t5 = tendenciaDe(c5m);
  const t15 = tendenciaDe(c15m);

  let scoreC = 0, scoreV = 0;
  const fatores = [];

  // 1) Tendência multi-TF — até 25 pts
  if (t5 === 'ALTA' && t15 === 'ALTA') { scoreC += 15; fatores.push('5m+15m em ALTA'); }
  else if (t5 === 'BAIXA' && t15 === 'BAIXA') { scoreV += 15; fatores.push('5m+15m em BAIXA'); }
  if (t1 === 'ALTA') scoreC += 10;
  else if (t1 === 'BAIXA') scoreV += 10;

  // 2) Alinhamento EMA (9/21/50) — até 18 pts
  if (e9v > e21v && e21v > e50v) { scoreC += 18; fatores.push('EMA9>EMA21>EMA50'); }
  else if (e9v < e21v && e21v < e50v) { scoreV += 18; fatores.push('EMA9<EMA21<EMA50'); }
  else if (e9v > e21v) { scoreC += 8; }
  else if (e9v < e21v) { scoreV += 8; }

  // 3) RSI — até 20 pts (extremos pesam mais)
  if (rv < 27) { scoreC += 20; fatores.push(`RSI oversold ${rv.toFixed(1)}`); }
  else if (rv < 35) { scoreC += 12; fatores.push(`RSI baixo ${rv.toFixed(1)}`); }
  else if (rv > 73) { scoreV += 20; fatores.push(`RSI overbought ${rv.toFixed(1)}`); }
  else if (rv > 65) { scoreV += 12; fatores.push(`RSI alto ${rv.toFixed(1)}`); }
  else if (rv < 45) scoreC += 5;
  else if (rv > 55) scoreV += 5;

  // 4) MACD histograma expansivo — 14 pts
  if (hv > 0 && hv > hvPrev) { scoreC += 14; fatores.push('MACD hist +'); }
  else if (hv < 0 && hv < hvPrev) { scoreV += 14; fatores.push('MACD hist -'); }
  else if (hv > 0) scoreC += 5;
  else scoreV += 5;

  // 5) Volume relativo — 10 pts
  if (volMed > 0 && vol > volMed * 1.4) {
    if (price > closes[i - 1]) { scoreC += 10; fatores.push(`Volume ${(vol / volMed).toFixed(1)}× + verde`); }
    else { scoreV += 10; fatores.push(`Volume ${(vol / volMed).toFixed(1)}× + vermelho`); }
  }

  // 6) Bollinger — posição relativa — 8 pts
  const bbPos = bbU > bbL ? (price - bbL) / (bbU - bbL) : 0.5;
  if (bbPos < 0.1) { scoreC += 8; fatores.push('Preço no fundo da BB'); }
  else if (bbPos > 0.9) { scoreV += 8; fatores.push('Preço no topo da BB'); }

  // 7) StochRSI — 8 pts (filtro de timing)
  if (stv < 15) { scoreC += 8; fatores.push(`StochRSI ${stv.toFixed(0)}`); }
  else if (stv > 85) { scoreV += 8; fatores.push(`StochRSI ${stv.toFixed(0)}`); }

  // 8) Divergência — até 15 pts
  const div = detectarDivergencia(closes, r, 14);
  if (div?.tipo === 'BULLISH') { scoreC += 15; fatores.push(`Divergência ${div.detalhe}`); }
  else if (div?.tipo === 'BEARISH') { scoreV += 15; fatores.push(`Divergência ${div.detalhe}`); }

  // 9) Padrão de velas — até 10 pts
  const padrao = detectarPadraoVelas(c1m);
  if (padrao) {
    const pts = padrao.peso === 'forte' ? 10 : 6;
    if (padrao.tipo.includes('ALTA') || padrao.tipo === 'MARTELO') { scoreC += pts; fatores.push(padrao.tipo); }
    else { scoreV += pts; fatores.push(padrao.tipo); }
  }

  // 10) Bónus reversal: RSI extremo contra tendência — 10 pts
  if (rv < 30 && t5 === 'BAIXA') { scoreC += 10; fatores.push('Reversão: oversold vs. 5m baixa'); }
  if (rv > 70 && t5 === 'ALTA') { scoreV += 10; fatores.push('Reversão: overbought vs. 5m alta'); }

  // 11) Volatilidade muito baixa → penalidade
  const atrPct = (av / price) * 100;
  if (atrPct < 0.02) { scoreC = Math.floor(scoreC * 0.85); scoreV = Math.floor(scoreV * 0.85); fatores.push('ATR baixo'); }

  scoreC = clamp(scoreC, 0, 100);
  scoreV = clamp(scoreV, 0, 100);

  let direcao = null, score = 0;
  const margem = 6;
  if (scoreC >= MIN_CONFLUENCIA && scoreC >= scoreV + margem) { direcao = 'COMPRA'; score = scoreC; }
  else if (scoreV >= MIN_CONFLUENCIA && scoreV >= scoreC + margem) { direcao = 'VENDA'; score = scoreV; }

  const { tp, sl } = direcao ? calcularTPSL(direcao, price, av, 0) : { tp: 0, sl: 0 };

  return {
    ok: Boolean(direcao), direcao, score, scoreCompra: scoreC, scoreVenda: scoreV,
    price, atr: av, atrPct: Number(atrPct.toFixed(4)), tp, sl,
    rsi: Number(rv.toFixed(2)), trend1: t1, trend5: t5, trend15: t15,
    stochRsi: Number(stv.toFixed(1)), bbPos: Number(bbPos.toFixed(3)),
    fatores, ema: { e9: Number(e9v.toFixed(2)), e21: Number(e21v.toFixed(2)), e50: Number(e50v.toFixed(2)) },
    motivo: direcao ? 'confluencia_ok' : 'sem_confluencia_forte'
  };
}

function calcularTPSL(direcao, entry, atr, gale) {
  const slMult = gale === 0 ? SL_ATR_MULT_GALE0 : SL_ATR_MULT_GALE1;
  if (direcao === 'COMPRA') {
    return {
      tp: Number((entry + atr * TP_ATR_MULT).toFixed(2)),
      sl: Number((entry - atr * slMult).toFixed(2))
    };
  }
  return {
    tp: Number((entry - atr * TP_ATR_MULT).toFixed(2)),
    sl: Number((entry + atr * slMult).toFixed(2))
  };
}

// ─── Mensagens Telegram (painel HTML) ──────────────────────────────────────

function montarMsgEntrada(sinal, indicadores) {
  const emoji = sinal.direcao === 'COMPRA' ? '🟢' : '🔴';
  const pnlTp = ((sinal.tp - sinal.preco_entrada) / sinal.preco_entrada) * 100;
  const pnlSl = ((sinal.sl - sinal.preco_entrada) / sinal.preco_entrada) * 100;
  const fatores = (indicadores.fatores || []).slice(0, 6).map(f => `  • ${f}`).join('\n');
  return [
    `${emoji} <b>SINAL · ${sinal.direcao}</b>  ·  ${SYMBOL}`,
    `━━━━━━━━━━━━━━━━━━━━━`,
    `💵 <b>Entrada:</b>   <code>${fmtPreco(sinal.preco_entrada)}</code>`,
    `🎯 <b>Alvo:</b>      <code>${fmtPreco(sinal.tp)}</code>  (${fmtPct(pnlTp)})`,
    `🛑 <b>Stop:</b>      <code>${fmtPreco(sinal.sl)}</code>  (${fmtPct(pnlSl)})`,
    `📊 <b>Confluência:</b> ${sinal.confluencia}/100`,
    `⚡ <b>RSI:</b> ${indicadores.rsi}  ·  <b>ATR:</b> ${indicadores.atrPct}%`,
    `📈 <b>Tendência:</b> 1m ${indicadores.trend1} · 5m ${indicadores.trend5} · 15m ${indicadores.trend15}`,
    ``,
    `<b>🔍 Fatores de entrada</b>`,
    fatores || '  • —',
    ``,
    `<i>⏱ Válido ${Math.round(SINAL_EXPIRA_MS / 60000)} min · Gale máx ${MAX_GALE}</i>`,
    `<i>⚠️ Experimental · não é conselho financeiro</i>`
  ].join('\n');
}

function montarMsgGale(sinal, precoAtual) {
  return [
    `🐔 <b>GALE 1</b>  ·  ${SYMBOL}`,
    `━━━━━━━━━━━━━━━━━━━━━`,
    `ID #${sinal.id}  ·  ${sinal.direcao}`,
    `Preço atual: <code>${fmtPreco(precoAtual)}</code>`,
    `Entrada orig: <code>${fmtPreco(sinal.preco_entrada)}</code>`,
    `Novo stop:  <code>${fmtPreco(sinal.sl)}</code>`,
    ``,
    `<i>Recuperação em curso — aguardar alvo</i>`
  ].join('\n');
}

function montarMsgResultado(sinal, estado, precoSaida, pnl) {
  const emoji = estado === 'LOSS' ? '🔴' : '🟢';
  const titulo = estado === 'LOSS' ? 'LOSS' : (estado === 'G1' ? 'GREEN · G1' : 'GREEN');
  return [
    `${emoji} <b>${titulo}</b>  ·  ${SYMBOL}`,
    `━━━━━━━━━━━━━━━━━━━━━`,
    `ID #${sinal.id}  ·  ${sinal.direcao}`,
    `Entrada: <code>${fmtPreco(sinal.preco_entrada)}</code>`,
    `Saída:   <code>${fmtPreco(precoSaida)}</code>`,
    `PnL:     <b>${fmtPct(pnl)}</b>`,
    `Gale:    ${sinal.gale}`
  ].join('\n');
}

function montarMsgExpirado(sinal) {
  return [
    `⏱ <b>SINAL EXPIRADO</b>  ·  ${SYMBOL}`,
    `━━━━━━━━━━━━━━━━━━━━━`,
    `ID #${sinal.id}  ·  ${sinal.direcao}`,
    `Entrada: <code>${fmtPreco(sinal.preco_entrada)}</code>`,
    ``,
    `<i>Sem alvo nem stop atingido no período.</i>`
  ].join('\n');
}

// ─── Persistência / helpers ────────────────────────────────────────────────

async function obterSinalPendente() {
  const { rows } = await pool.query(`SELECT * FROM trade_sinais WHERE estado = 'PENDENTE' ORDER BY id ASC LIMIT 1`);
  return rows[0] || null;
}

async function logEvento(sinalId, tipo, detalhe, preco = null) {
  await pool.query(`INSERT INTO trade_eventos (sinal_id, tipo, detalhe, preco) VALUES ($1,$2,$3,$4)`,
    [sinalId, tipo, detalhe, preco]);
}

async function expirarSinais() {
  const { rows } = await pool.query(`
    UPDATE trade_sinais
    SET estado = 'EXPIRADO', resolvido_em = NOW()
    WHERE estado = 'PENDENTE'
      AND criado_em < NOW() - ($1::text || ' milliseconds')::interval
    RETURNING id, direcao, preco_entrada
  `, [String(SINAL_EXPIRA_MS)]);

  for (const s of rows) {
    await logEvento(s.id, 'EXPIRADO', 'timeout sem resolução');
    await telegram(montarMsgExpirado(s));
    console.log(`[EXPIRADO] #${s.id} ${s.direcao}`);
  }
}

// ─── Resolução por TP/SL ───────────────────────────────────────────────────

async function resolverSinal() {
  const sinal = await obterSinalPendente();
  if (!sinal) return;

  // Sinal legado sem TP/SL → espera expirar
  if (sinal.tp == null || sinal.sl == null) return;

  const candles = await fetchKlines('1m', 60);
  if (!candles.length) return;

  const tp = Number(sinal.tp);
  const sl = Number(sinal.sl);
  const direcao = sinal.direcao;
  const gale = Number(sinal.gale);
  const precoEntrada = Number(sinal.preco_entrada);
  const criadoEm = new Date(sinal.criado_em).getTime();
  const ultimaAvaliada = sinal.ultima_vela_avaliada ? new Date(sinal.ultima_vela_avaliada).getTime() : criadoEm;

  const novas = candles.filter(c => c.openTime > ultimaAvaliada);
  if (!novas.length) return;

  const agora = Date.now();
  let hit = null, hitPrice = null;

  // Conservador: checa SL primeiro (mesmo candle com wick duplo → LOSS)
  for (const c of novas) {
    if (direcao === 'COMPRA') {
      if (c.low  <= sl) { hit = 'SL'; hitPrice = sl; break; }
      if (c.high >= tp) { hit = 'TP'; hitPrice = tp; break; }
    } else {
      if (c.high >= sl) { hit = 'SL'; hitPrice = sl; break; }
      if (c.low  <= tp) { hit = 'TP'; hitPrice = tp; break; }
    }
  }

  // Avança marcador até a última vela FECHADA
  const fechadas = novas.filter(c => c.closeTime <= agora);
  if (fechadas.length) {
    const ultima = fechadas[fechadas.length - 1];
    await pool.query(
      `UPDATE trade_sinais SET ultima_vela_avaliada = to_timestamp($2/1000.0) WHERE id = $1`,
      [sinal.id, ultima.openTime]
    );
  }

  if (hit === 'TP') {
    const estado = gale === 0 ? 'GREEN' : 'G1';
    const pnl = ((hitPrice - precoEntrada) / precoEntrada) * 100;
    const { rowCount } = await pool.query(
      `UPDATE trade_sinais SET estado=$2, preco_saida=$3, pnl_pct=$4, resolvido_em=NOW()
       WHERE id=$1 AND estado='PENDENTE'`,
      [sinal.id, estado, hitPrice, Number(pnl.toFixed(4))]
    );
    if (!rowCount) return;
    await logEvento(sinal.id, estado, `TP ${hitPrice}`, hitPrice);
    await telegram(montarMsgResultado(sinal, estado, hitPrice, pnl));
    console.log(`[${estado}] #${sinal.id} @ ${hitPrice}  pnl=${pnl.toFixed(3)}%`);
    return;
  }

  if (hit === 'SL') {
    if (gale === 0) {
      const atrVal = Number(sinal.indicadores?.atr || 0);
      const newSl = direcao === 'COMPRA'
        ? (atrVal > 0 ? precoEntrada - atrVal * SL_ATR_MULT_GALE1 : precoEntrada * 0.995)
        : (atrVal > 0 ? precoEntrada + atrVal * SL_ATR_MULT_GALE1 : precoEntrada * 1.005);

      await pool.query(`UPDATE trade_sinais SET gale=1, sl=$2 WHERE id=$1 AND estado='PENDENTE'`,
        [sinal.id, Number(newSl.toFixed(2))]);
      await logEvento(sinal.id, 'GALE1', `SL ${hitPrice} → novo SL ${newSl.toFixed(2)}`, hitPrice);
      await telegram(montarMsgGale({ ...sinal, sl: Number(newSl.toFixed(2)) }, hitPrice));
      console.log(`[GALE1] #${sinal.id} novo SL ${newSl.toFixed(2)}`);
      return;
    }

    const pnl = ((hitPrice - precoEntrada) / precoEntrada) * 100;
    const { rowCount } = await pool.query(
      `UPDATE trade_sinais SET estado='LOSS', preco_saida=$2, pnl_pct=$3, resolvido_em=NOW()
       WHERE id=$1 AND estado='PENDENTE'`,
      [sinal.id, hitPrice, Number(pnl.toFixed(4))]
    );
    if (!rowCount) return;
    await logEvento(sinal.id, 'LOSS', `SL final ${hitPrice}`, hitPrice);
    await telegram(montarMsgResultado(sinal, 'LOSS', hitPrice, pnl));
    console.log(`[LOSS] #${sinal.id} @ ${hitPrice}  pnl=${pnl.toFixed(3)}%`);
  }
}

// ─── Cooldown dinâmico ─────────────────────────────────────────────────────

async function podeCriarNovo() {
  if (await obterSinalPendente()) return false;

  const { rows } = await pool.query(
    `SELECT confluencia, estado, criado_em, resolvido_em
     FROM trade_sinais ORDER BY id DESC LIMIT 1`
  );
  if (!rows.length) return true;

  const ultimo = rows[0];
  const score = Number(ultimo.confluencia) || 0;
  const estado = ultimo.estado;

  const tier = COOLDOWN_TIERS.find(t => score >= t.min) || COOLDOWN_TIERS[COOLDOWN_TIERS.length - 1];
  let cooldownMs = tier.ms;

  if (estado === 'LOSS') cooldownMs *= 2;
  else if (estado === 'GREEN' || estado === 'G1') cooldownMs *= 0.75;
  else if (estado === 'EXPIRADO') cooldownMs *= 1.25;

  const refTs = ultimo.resolvido_em
    ? new Date(ultimo.resolvido_em).getTime()
    : new Date(ultimo.criado_em).getTime();

  return Date.now() - refTs >= cooldownMs;
}

// ─── Criação de sinal ──────────────────────────────────────────────────────

async function criarSinal(analise) {
  if (!await podeCriarNovo()) return null;

  const indicadores = {
    rsi: analise.rsi,
    atr: analise.atr,
    atrPct: analise.atrPct,
    stochRsi: analise.stochRsi,
    bbPos: analise.bbPos,
    trend1: analise.trend1,
    trend5: analise.trend5,
    trend15: analise.trend15,
    ema: analise.ema,
    fatores: analise.fatores
  };

  const msg = montarMsgEntrada(
    { direcao: analise.direcao, preco_entrada: analise.price, tp: analise.tp, sl: analise.sl,
      confluencia: analise.score },
    indicadores
  );

  const { rows } = await pool.query(
    `INSERT INTO trade_sinais
       (symbol, direcao, preco_entrada, tp, sl, confluencia, indicadores, mensagem_entrada)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
     RETURNING id`,
    [SYMBOL, analise.direcao, analise.price, analise.tp, analise.sl,
     analise.score, JSON.stringify(indicadores), msg]
  );

  const id = rows[0]?.id;
  if (!id) return null;

  totalSinais += 1;
  await logEvento(id, 'ENTRADA', `${analise.direcao} score ${analise.score}`, analise.price);
  await telegram(msg);
  console.log(`[SINAL] #${id} ${analise.direcao} @ ${analise.price} score=${analise.score} tp=${analise.tp} sl=${analise.sl}`);
  return id;
}

// ─── Ciclo principal ───────────────────────────────────────────────────────

async function ciclo() {
  if (analisando) return;
  analisando = true;
  try {
    await expirarSinais();
    await resolverSinal();

    const [c1m, c5m, c15m] = await Promise.all([
      fetchKlines('1m', 120),
      fetchKlines('5m', 60),
      fetchKlines('15m', 60)
    ]);

    const analise = analisarConfluencia(c1m, c5m, c15m);
    ultimaAnalise = { ...analise, ts: agoraISO() };

    if (analise.ok) {
      await criarSinal(analise);
    } else {
      console.log(`[ANALISE] ${analise.motivo} | C=${analise.scoreCompra} V=${analise.scoreVenda} (min ${MIN_CONFLUENCIA})`);
    }

    lastError = null;
  } catch (err) {
    lastError = err.message;
    console.error('[CICLO]', err.message);
  } finally {
    analisando = false;
  }
}

// ─── HTTP ──────────────────────────────────────────────────────────────────

app.get('/', (_req, res) => {
  res.json({
    nome: 'CAPUNGA TRADE',
    versao: '2.0.0',
    simbolo: SYMBOL,
    estado: 'online',
    gale_maximo: MAX_GALE,
    min_confluencia: MIN_CONFLUENCIA,
    intervalo_analise_ms: INTERVALO_ANALISE_MS,
    cooldown_dinamico: COOLDOWN_TIERS.map(t => ({ min_score: t.min, cooldown_min: t.ms / 60000 })),
    aviso: 'Sinais experimentais. Não constituem aconselhamento financeiro.'
  });
});

app.get('/health', async (_req, res) => {
  try {
    await pool.query('SELECT 1');
    res.json({
      ok: true,
      database: 'connected',
      ultimaAnalise: ultimaAnalise?.ts || null,
      lastError,
      totalSinais,
      telegram: Boolean(BOT_TOKEN && CHAT_ID)
    });
  } catch (err) {
    res.status(503).json({ ok: false, erro: err.message });
  }
});

app.get('/status', async (_req, res) => {
  try {
    const { rows: totais } = await pool.query(`
      SELECT
        COUNT(*) FILTER (WHERE estado = 'GREEN')::int    AS green,
        COUNT(*) FILTER (WHERE estado = 'G1')::int       AS g1,
        COUNT(*) FILTER (WHERE estado = 'LOSS')::int     AS loss,
        COUNT(*) FILTER (WHERE estado = 'PENDENTE')::int AS pendente,
        COUNT(*) FILTER (WHERE estado = 'EXPIRADO')::int AS expirado,
        COUNT(*)::int AS total
      FROM trade_sinais
    `);
    const t = totais[0] || {};
    const settled = (t.green || 0) + (t.g1 || 0) + (t.loss || 0);
    const wins = (t.green || 0) + (t.g1 || 0);
    const winrate = settled ? Number(((wins * 100) / settled).toFixed(2)) : null;

    const { rows: pnlRows } = await pool.query(`
      SELECT COALESCE(SUM(pnl_pct), 0)::float AS pnl_total,
             COALESCE(AVG(pnl_pct), 0)::float AS pnl_medio,
             COALESCE(AVG(confluencia), 0)::float AS conf_media
      FROM trade_sinais WHERE estado IN ('GREEN','G1','LOSS')
    `);
    const pnl = pnlRows[0] || {};

    const pendente = await obterSinalPendente();

    res.json({
      ok: true,
      simbolo: SYMBOL,
      estatisticas: {
        ...t, settled, wins,
        winrate_pct: winrate,
        pnl_total_pct: Number((pnl.pnl_total || 0).toFixed(3)),
        pnl_medio_pct: Number((pnl.pnl_medio || 0).toFixed(4)),
        conf_media: Number((pnl.conf_media || 0).toFixed(1))
      },
      sinal_pendente: pendente ? {
        id: pendente.id,
        direcao: pendente.direcao,
        preco_entrada: Number(pendente.preco_entrada),
        tp: Number(pendente.tp),
        sl: Number(pendente.sl),
        confluencia: pendente.confluencia,
        gale: Number(pendente.gale),
        criado_em: pendente.criado_em
      } : null,
      ultima_analise: ultimaAnalise,
      ultimo_erro: lastError
    });
  } catch (err) {
    res.status(500).json({ ok: false, erro: err.message });
  }
});

app.get('/sinais', async (req, res) => {
  try {
    const limit = Math.min(100, Math.max(1, Number(req.query.limit) || 30));
    const { rows } = await pool.query(
      `SELECT id, symbol, direcao, preco_entrada, tp, sl, confluencia, estado, gale,
              preco_saida, pnl_pct, criado_em, resolvido_em
       FROM trade_sinais ORDER BY id DESC LIMIT $1`, [limit]);
    res.json({