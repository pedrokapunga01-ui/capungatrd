'use strict';

/**
 * CAPUNGA TRADE — BTC Signal Engine v3 (Bybit)
 *
 * - Dados da Bybit (BTCUSDT perp), fallback OKX
 * - Só usa velas FECHADAS (sem repaint)
 * - Só sinaliza com 5m + 15m alinhados com a direção + pullback + momentum
 * - TP/SL dinâmicos por ATR, resolvidos por high/low das velas (não perde picos)
 * - Gale máx. 1 (re-entrada real, com novo TP/SL)
 * - 100% automático: loop + keep-alive (Render free) + pausa após LOSS
 * - /backtest mede a taxa de acerto REAL da estratégia nos dados recentes
 *
 * Env obrigatórias: DATABASE_URL · BOT_TOKEN · CHAT_ID
 * Env opcionais:
 *   PORT · SYMBOL (BTCUSDT) · BYBIT_CATEGORY (linear)
 *   MIN_CONFLUENCIA (75) · LOOP_MS (20000)
 *   TP_ATR (1.0) · SL_ATR (2.0) · TP_MIN_PCT (0.10) · SL_MIN_PCT (0.20)
 *   SINAL_EXPIRA_MS (1200000) · ANTI_SPAM_MS (180000) · LOSS_PAUSE_MS (900000)
 *   KEEPALIVE_URL (ou RENDER_EXTERNAL_URL automático no Render)
 *   DATABASE_SSL=false para desligar SSL
 */

const express = require('express');
const cors = require('cors');
const axios = require('axios');
const { Pool } = require('pg');

const app = express();
app.use(cors());
app.use(express.json({ limit: '256kb' }));

// ── Config ──────────────────────────────────────────────────────────────────

const envNum = (k, d) => {
  const n = Number(process.env[k]);
  return Number.isFinite(n) && process.env[k] !== '' && process.env[k] != null ? n : d;
};

const PORT = envNum('PORT', 3000);
const DATABASE_URL = process.env.DATABASE_URL;
const BOT_TOKEN = (process.env.BOT_TOKEN || '').trim();
const CHAT_ID = (process.env.CHAT_ID || '').trim();
const SYMBOL = (process.env.SYMBOL || 'BTCUSDT').toUpperCase();
const CATEGORY = process.env.BYBIT_CATEGORY || 'linear';

const MIN_SCORE = Math.min(95, Math.max(55, envNum('MIN_CONFLUENCIA', 75)));
const LOOP_MS = Math.max(15000, envNum('LOOP_MS', 20000));
const SINAL_EXPIRA_MS = Math.max(300000, envNum('SINAL_EXPIRA_MS', 1200000));
const ANTI_SPAM_MS = Math.max(60000, envNum('ANTI_SPAM_MS', 180000));
const LOSS_PAUSE_MS = Math.max(0, envNum('LOSS_PAUSE_MS', 900000));
const MAX_GALE = 1;

const TP_ATR = envNum('TP_ATR', 1.0);
const SL_ATR = envNum('SL_ATR', 2.0);
const TP_MIN_PCT = envNum('TP_MIN_PCT', 0.10);
const SL_MIN_PCT = envNum('SL_MIN_PCT', 0.20);
const ATR_MIN_PCT = envNum('ATR_MIN_PCT', 0.02);
const ATR_MAX_PCT = envNum('ATR_MAX_PCT', 0.6);

const SELF_URL = (process.env.KEEPALIVE_URL || process.env.RENDER_EXTERNAL_URL || '').replace(/\/$/, '');

if (!DATABASE_URL) {
  console.error('[FATAL] DATABASE_URL em falta');
  process.exit(1);
}

const pool = new Pool({
  connectionString: DATABASE_URL,
  ssl: process.env.DATABASE_SSL === 'false' ? false : { rejectUnauthorized: false },
  max: 6,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 12000
});

let busy = false;
let lastSignalAt = 0;
let lastLossAt = 0;
let lastAnalysis = null;
let lastError = null;
let lastSource = null;
let signalsSent = 0;
let loopTimer = null;
let keepTimer = null;

// ── Utils ───────────────────────────────────────────────────────────────────

const nowISO = () => new Date().toISOString();
const clamp = (n, a, b) => Math.max(a, Math.min(b, n));
const num = (v, fb = 0) => {
  const n = Number(v);
  return Number.isFinite(n) ? n : fb;
};
const fmt = (n) => Number(n).toLocaleString('en-US', { minimumFractionDigits: 1, maximumFractionDigits: 1 });

const TF = {
  '1m': { bybit: '1', okx: '1m', ms: 60000 },
  '5m': { bybit: '5', okx: '5m', ms: 300000 },
  '15m': { bybit: '15', okx: '15m', ms: 900000 }
};

function levels(atrPct) {
  return {
    tp: Number(Math.max(TP_MIN_PCT, TP_ATR * atrPct).toFixed(3)),
    sl: Number(Math.max(SL_MIN_PCT, SL_ATR * atrPct).toFixed(3))
  };
}

// ── Telegram ────────────────────────────────────────────────────────────────

async function tg(text) {
  if (!BOT_TOKEN || !CHAT_ID) return false;
  try {
    const { data } = await axios.post(
      `https://api.telegram.org/bot${BOT_TOKEN}/sendMessage`,
      { chat_id: CHAT_ID, text, disable_web_page_preview: true, parse_mode: 'HTML' },
      { timeout: 12000 }
    );
    return data && data.ok === true;
  } catch (e) {
    lastError = 'tg: ' + e.message;
    console.error('[TG]', (e.response && e.response.data) || e.message);
    return false;
  }
}

// ── DB ──────────────────────────────────────────────────────────────────────

async function initDB() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS trade_sinais (
      id BIGSERIAL PRIMARY KEY,
      symbol TEXT NOT NULL,
      direcao TEXT NOT NULL,
      preco_entrada NUMERIC NOT NULL,
      score INTEGER DEFAULT 0,
      setup TEXT,
      estado TEXT NOT NULL DEFAULT 'PENDENTE',
      gale SMALLINT NOT NULL DEFAULT 0,
      preco_saida NUMERIC,
      pnl_pct NUMERIC,
      criado_em TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      resolvido_em TIMESTAMPTZ
    );
  `);
  await pool.query(`ALTER TABLE trade_sinais ADD COLUMN IF NOT EXISTS score INTEGER DEFAULT 0`);
  await pool.query(`ALTER TABLE trade_sinais ADD COLUMN IF NOT EXISTS setup TEXT`);
  await pool.query(`ALTER TABLE trade_sinais ADD COLUMN IF NOT EXISTS preco_saida NUMERIC`);
  await pool.query(`ALTER TABLE trade_sinais ADD COLUMN IF NOT EXISTS pnl_pct NUMERIC`);
  await pool.query(`ALTER TABLE trade_sinais ADD COLUMN IF NOT EXISTS tp_pct NUMERIC`);
  await pool.query(`ALTER TABLE trade_sinais ADD COLUMN IF NOT EXISTS sl_pct NUMERIC`);
  await pool.query(`ALTER TABLE trade_sinais ADD COLUMN IF NOT EXISTS gale_preco NUMERIC`);
  await pool.query(`ALTER TABLE trade_sinais ADD COLUMN IF NOT EXISTS ref_em TIMESTAMPTZ`);
  // Tabela legada (v1/Bac Bo) pode ter colunas NOT NULL antigas, ex.: confluencia → deixa de bloquear o INSERT
  await pool.query(`
    DO $$
    DECLARE r RECORD;
    BEGIN
      FOR r IN
        SELECT column_name FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name = 'trade_sinais'
          AND is_nullable = 'NO'
          AND column_default IS NULL
          AND column_name NOT IN ('id','symbol','direcao','preco_entrada')
      LOOP
        EXECUTE format('ALTER TABLE trade_sinais ALTER COLUMN %I DROP NOT NULL', r.column_name);
      END LOOP;
    END $$;
  `);
  await pool.query(`CREATE INDEX IF NOT EXISTS idx_ts_estado ON trade_sinais (estado, id DESC)`);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS trade_eventos (
      id BIGSERIAL PRIMARY KEY,
      sinal_id BIGINT,
      tipo TEXT NOT NULL,
      detalhe TEXT,
      preco NUMERIC,
      em TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);
  console.log('[DB] OK');
}

async function pendingSignal() {
  const { rows } = await pool.query(
    `SELECT * FROM trade_sinais WHERE estado='PENDENTE' ORDER BY id ASC LIMIT 1`
  );
  return rows[0] || null;
}

async function logEv(sinalId, tipo, detalhe, preco = null) {
  try {
    await pool.query(
      `INSERT INTO trade_eventos (sinal_id, tipo, detalhe, preco) VALUES ($1,$2,$3,$4)`,
      [sinalId, tipo, detalhe, preco]
    );
  } catch (e) { console.error('[EV]', e.message); }
}

// ── Market data (Bybit primeiro, OKX fallback) ──────────────────────────────

const BYBIT_HOSTS = ['https://api.bybit.com', 'https://api.bytick.com'];
const HEADERS = { 'User-Agent': 'CapungaTrade/3.0', Accept: 'application/json' };

async function fetchKlines(interval, limit) {
  const tf = TF[interval];
  const errs = [];

  for (const host of BYBIT_HOSTS) {
    try {
      const { data } = await axios.get(
        `${host}/v5/market/kline?category=${CATEGORY}&symbol=${SYMBOL}&interval=${tf.bybit}&limit=${Math.min(limit, 1000)}`,
        { timeout: 9000, headers: HEADERS }
      );
      const list = data && data.retCode === 0 && data.result && data.result.list;
      if (list && list.length) {
        lastSource = 'bybit';
        return list.slice().reverse().map(function (k) {
          return { t: Number(k[0]), o: num(k[1]), h: num(k[2]), l: num(k[3]), c: num(k[4]), v: num(k[5]) };
        });
      }
      errs.push('bybit:' + ((data && data.retMsg) || 'vazio'));
    } catch (e) {
      errs.push('bybit:' + ((e.response && e.response.status) || e.message));
    }
  }

  try {
    const inst = SYMBOL.replace('USDT', '-USDT');
    const { data } = await axios.get(
      `https://www.okx.com/api/v5/market/candles?instId=${inst}&bar=${tf.okx}&limit=${Math.min(limit, 300)}`,
      { timeout: 9000, headers: HEADERS }
    );
    if (data && data.code === '0' && data.data && data.data.length) {
      lastSource = 'okx';
      return data.data.slice().reverse().map(function (k) {
        return { t: Number(k[0]), o: num(k[1]), h: num(k[2]), l: num(k[3]), c: num(k[4]), v: num(k[5]) };
      });
    }
    errs.push('okx');
  } catch (e) { errs.push('okx:' + e.message); }

  throw new Error('market_data fail: ' + errs.join(','));
}

async function fetchPrice(fallback) {
  for (const host of BYBIT_HOSTS) {
    try {
      const { data } = await axios.get(
        `${host}/v5/market/tickers?category=${CATEGORY}&symbol=${SYMBOL}`,
        { timeout: 7000, headers: HEADERS }
      );
      const p = num(data && data.result && data.result.list && data.result.list[0] && data.result.list[0].lastPrice, 0);
      if (p > 0) return p;
    } catch (e) { /* tenta próximo */ }
  }
  return fallback;
}

// só velas já fechadas (a vela em formação distorce os indicadores)
function closedOnly(candles, ms, at) {
  const now = at == null ? Date.now() : at;
  return candles.filter(function (k) { return k.t + ms <= now; });
}

// ── Indicators ──────────────────────────────────────────────────────────────

function ema(arr, p) {
  if (arr.length < p) return [];
  const k = 2 / (p + 1);
  let prev = arr.slice(0, p).reduce(function (a, b) { return a + b; }, 0) / p;
  const out = [prev];
  for (let i = p; i < arr.length; i++) {
    prev = arr[i] * k + prev * (1 - k);
    out.push(prev);
  }
  return out;
}

function sma(arr, p) {
  if (arr.length < p) return [];
  const out = [];
  let s = 0;
  for (let i = 0; i < arr.length; i++) {
    s += arr[i];
    if (i >= p) s -= arr[i - p];
    if (i >= p - 1) out.push(s / p);
  }
  return out;
}

function rsi(closes, p) {
  p = p || 14;
  if (closes.length < p + 1) return [];
  const ch = [];
  for (let i = 1; i < closes.length; i++) ch.push(closes[i] - closes[i - 1]);
  let ag = 0, al = 0;
  for (let i = 0; i < p; i++) {
    if (ch[i] >= 0) ag += ch[i];
    else al -= ch[i];
  }
  ag /= p; al /= p;
  const out = [al === 0 ? 100 : 100 - 100 / (1 + ag / al)];
  for (let i = p; i < ch.length; i++) {
    const g = ch[i] > 0 ? ch[i] : 0;
    const l = ch[i] < 0 ? -ch[i] : 0;
    ag = (ag * (p - 1) + g) / p;
    al = (al * (p - 1) + l) / p;
    out.push(al === 0 ? 100 : 100 - 100 / (1 + ag / al));
  }
  return out;
}

function macdHist(closes) {
  const ef = ema(closes, 12);
  const es = ema(closes, 26);
  const off = 26 - 12;
  const line = [];
  for (let i = 0; i < es.length; i++) line.push(ef[i + off] - es[i]);
  const sig = ema(line, 9);
  const hist = [];
  const so = line.length - sig.length;
  for (let i = 0; i < sig.length; i++) hist.push(line[i + so] - sig[i]);
  return hist;
}

function atr(h, l, c, p) {
  p = p || 14;
  if (h.length < p + 1) return [];
  const tr = [];
  for (let i = 1; i < h.length; i++) {
    tr.push(Math.max(h[i] - l[i], Math.abs(h[i] - c[i - 1]), Math.abs(l[i] - c[i - 1])));
  }
  return sma(tr, p);
}

function trendOf(candles) {
  const closes = candles.map(function (x) { return x.c; });
  const e9 = ema(closes, 9);
  const e21 = ema(closes, 21);
  if (!e9.length || !e21.length) return 'NEUTRO';
  const a = e9[e9.length - 1], b = e21[e21.length - 1], px = closes[closes.length - 1];
  if (a > b && px > b) return 'ALTA';
  if (a < b && px < b) return 'BAIXA';
  return 'NEUTRO';
}

// ── Strategy (velas FECHADAS) ───────────────────────────────────────────────
// Regras duras: 5m e 15m têm de concordar com a direção.
// Pontuação: tendência (40) + stack EMA (12) + pullback (15) + RSI (10) + MACD (12) + volume (8)

function analyze(c1, c5, c15) {
  if (!c1 || c1.length < 60) return { ok: false, reason: 'dados_1m', buy: 0, sell: 0 };
  if (!c5 || c5.length < 30) return { ok: false, reason: 'dados_5m', buy: 0, sell: 0 };
  if (!c15 || c15.length < 30) return { ok: false, reason: 'dados_15m', buy: 0, sell: 0 };

  const closes = c1.map(function (x) { return x.c; });
  const highs = c1.map(function (x) { return x.h; });
  const lows = c1.map(function (x) { return x.l; });
  const vols = c1.map(function (x) { return x.v; });

  const e9 = ema(closes, 9);
  const e21 = ema(closes, 21);
  const e50 = ema(closes, 50);
  const r14 = rsi(closes, 14);
  const mh = macdHist(closes);
  const a14 = atr(highs, lows, closes, 14);
  const vSma = sma(vols, 20);

  if (!e9.length || !e21.length || !e50.length || r14.length < 2 || mh.length < 2 || !a14.length) {
    return { ok: false, reason: 'indicadores', buy: 0, sell: 0 };
  }

  const i = c1.length - 1;
  const k = c1[i];
  const price = k.c;
  const E9 = e9[e9.length - 1];
  const E21 = e21[e21.length - 1];
  const E50 = e50[e50.length - 1];
  const R = r14[r14.length - 1];
  const Rp = r14[r14.length - 2];
  const H = mh[mh.length - 1];
  const Hp = mh[mh.length - 2];
  const A = a14[a14.length - 1];
  const volM = vSma[vSma.length - 1] || k.v;
  const atrPct = (A / price) * 100;
  const trend5 = trendOf(c5);
  const trend15 = trendOf(c15);

  const minLow4 = Math.min.apply(null, lows.slice(-4));
  const maxHigh4 = Math.max.apply(null, highs.slice(-4));

  function scoreSide(up) {
    let sc = 0;
    const tg = [];
    const want = up ? 'ALTA' : 'BAIXA';
    const t15ok = trend15 === want;
    const t5ok = trend5 === want;
    if (t15ok) { sc += 20; tg.push('15m ' + trend15); }
    if (t5ok) { sc += 20; tg.push('5m ' + trend5); }

    const stack = up ? (E9 > E21 && E21 > E50) : (E9 < E21 && E21 < E50);
    if (stack) { sc += 12; tg.push('EMA 9/21/50 alinhadas'); }

    const pull = up ? (minLow4 <= E21 && price > E9) : (maxHigh4 >= E21 && price < E9);
    if (pull) { sc += 15; tg.push('Pullback EMA21 retomado'); }

    if (up) {
      if (R >= 40 && R <= 62 && R > Rp) { sc += 10; tg.push('RSI ' + R.toFixed(0) + ' a subir'); }
      if (R > 75) sc -= 15;
    } else {
      if (R >= 38 && R <= 60 && R < Rp) { sc += 10; tg.push('RSI ' + R.toFixed(0) + ' a descer'); }
      if (R < 25) sc -= 15;
    }

    if (up ? (H > 0 && H > Hp) : (H < 0 && H < Hp)) { sc += 12; tg.push(up ? 'MACD a reforçar' : 'MACD a enfraquecer'); }

    if (k.v > volM * 1.1 && (up ? price > k.o : price < k.o)) { sc += 8; tg.push('Volume confirma'); }

    return { sc: clamp(sc, 0, 100), tags: tg, aligned: t15ok && t5ok };
  }

  const B = scoreSide(true);
  const S = scoreSide(false);

  const base = {
    buy: B.sc, sell: S.sc, price: price, rsi: Number(R.toFixed(1)),
    trend5: trend5, trend15: trend15, atrPct: Number(atrPct.toFixed(3)), candleT: k.t
  };

  if (atrPct < ATR_MIN_PCT) return Object.assign(base, { ok: false, reason: 'mercado_parado' });
  if (atrPct > ATR_MAX_PCT) return Object.assign(base, { ok: false, reason: 'volatilidade_extrema' });
  if ((k.h - k.l) > 3 * A) return Object.assign(base, { ok: false, reason: 'vela_espigada' });

  let side = null, pick = null;
  if (B.aligned && B.sc >= MIN_SCORE && B.sc >= S.sc + 10) { side = 'COMPRA'; pick = B; }
  else if (S.aligned && S.sc >= MIN_SCORE && S.sc >= B.sc + 10) { side = 'VENDA'; pick = S; }

  if (!side) return Object.assign(base, { ok: false, reason: 'sem_setup' });

  const lv = levels(atrPct);
  return Object.assign(base, {
    ok: true, side: side, score: pick.sc, tags: pick.tags,
    tpPct: lv.tp, slPct: lv.sl, reason: 'setup_ok'
  });
}

// ── Lifecycle ───────────────────────────────────────────────────────────────

async function expireOld() {
  const { rows } = await pool.query(
    `UPDATE trade_sinais
     SET estado='EXPIRADO', resolvido_em=NOW()
     WHERE estado='PENDENTE'
       AND COALESCE(ref_em, criado_em) < NOW() - ($1::int * INTERVAL '1 millisecond')
     RETURNING id, direcao, preco_entrada`,
    [SINAL_EXPIRA_MS]
  );
  for (const s of rows) {
    await logEv(s.id, 'EXPIRADO', 'timeout');
    await tg(
      '⏱ <b>CAPUNGA TRADE</b>\n' +
      'Sinal #' + s.id + ' expirado sem TP/SL\n' +
      s.direcao + ' @ ' + fmt(s.preco_entrada) + '\n' +
      'Considere fechado, sem resultado.'
    );
  }
}

async function resolvePending(raw1, px) {
  const s = await pendingSignal();
  if (!s) return;

  const stage = Number(s.gale);
  const long = s.direcao === 'COMPRA';
  const ref = stage ? num(s.gale_preco) : num(s.preco_entrada);
  const refMs = new Date(s.ref_em || s.criado_em).getTime();
  const tpP = num(s.tp_pct, TP_MIN_PCT);
  const slP = num(s.sl_pct, SL_MIN_PCT);

  const tpPrice = long ? ref * (1 + tpP / 100) : ref * (1 - tpP / 100);
  const slPrice = long ? ref * (1 - slP / 100) : ref * (1 + slP / 100);

  // velas que começaram depois da entrada (+ preço atual) → apanha picos entre ticks
  const after = raw1.filter(function (k) { return k.t >= refMs; });
  let hi = px, lo = px;
  after.forEach(function (k) { if (k.h > hi) hi = k.h; if (k.l < lo) lo = k.l; });

  const hitSL = long ? lo <= slPrice : hi >= slPrice;
  const hitTP = long ? hi >= tpPrice : lo <= tpPrice;

  // se ambos no mesmo intervalo, assume SL primeiro (pessimista)
  if (hitSL) {
    if (stage < MAX_GALE) {
      await pool.query(
        `UPDATE trade_sinais SET gale=1, gale_preco=$2, ref_em=NOW() WHERE id=$1 AND estado='PENDENTE'`,
        [s.id, px]
      );
      await logEv(s.id, 'GALE1', 'SL tocado', px);
      await tg(
        '🔁 <b>GALE 1 · CAPUNGA TRADE</b>\n' +
        '----------------\n' +
        '#' + s.id + '  re-entrar ' + s.direcao + '\n' +
        'Entrada ' + fmt(px) + '\n' +
        'TP ' + fmt(long ? px * (1 + tpP / 100) : px * (1 - tpP / 100)) + '\n' +
        'SL ' + fmt(long ? px * (1 - slP / 100) : px * (1 + slP / 100)) + '\n' +
        'Último gale — se falhar, é LOSS.'
      );
      return;
    }
    const pnl = Number(((long ? slPrice - ref : ref - slPrice) / ref * 100).toFixed(4));
    await pool.query(
      `UPDATE trade_sinais SET estado='LOSS', preco_saida=$2, pnl_pct=$3, resolvido_em=NOW()
       WHERE id=$1 AND estado='PENDENTE'`,
      [s.id, slPrice, pnl]
    );
    lastLossAt = Date.now();
    await logEv(s.id, 'LOSS', 'max gale', slPrice);
    await tg(
      '❌ <b>LOSS · CAPUNGA TRADE</b>\n' +
      '----------------\n' +
      '#' + s.id + '  ' + s.direcao + ' (gale ' + stage + ')\n' +
      'In  ' + fmt(ref) + '\n' +
      'Out ' + fmt(slPrice) + '\n' +
      'Pausa de ' + Math.round(LOSS_PAUSE_MS / 60000) + ' min nos sinais.'
    );
    return;
  }

  if (hitTP) {
    const st = stage === 0 ? 'GREEN' : 'G1';
    const pnl = Number(((long ? tpPrice - ref : ref - tpPrice) / ref * 100).toFixed(4));
    await pool.query(
      `UPDATE trade_sinais SET estado=$2, preco_saida=$3, pnl_pct=$4, resolvido_em=NOW()
       WHERE id=$1 AND estado='PENDENTE'`,
      [s.id, st, tpPrice, pnl]
    );
    await logEv(s.id, 'FECHO', st, tpPrice);
    await tg(
      '✅ <b>' + (st === 'G1' ? 'GREEN G1' : 'GREEN') + ' · CAPUNGA TRADE</b>\n' +
      '----------------\n' +
      '#' + s.id + '  ' + s.direcao + '\n' +
      'In  ' + fmt(ref) + '\n' +
      'Out ' + fmt(tpPrice) + '\n' +
      'PnL +' + pnl.toFixed(3) + '%'
    );
  }
}

async function openSignal(a) {
  if (await pendingSignal()) return null;
  if (Date.now() - lastSignalAt < ANTI_SPAM_MS) return null;
  if (Date.now() - lastLossAt < LOSS_PAUSE_MS) return null;

  const long = a.side === 'COMPRA';
  const tpPrice = long ? a.price * (1 + a.tpPct / 100) : a.price * (1 - a.tpPct / 100);
  const slPrice = long ? a.price * (1 - a.slPct / 100) : a.price * (1 + a.slPct / 100);

  const { rows } = await pool.query(
    `INSERT INTO trade_sinais (symbol, direcao, preco_entrada, score, setup, tp_pct, sl_pct, ref_em)
     VALUES ($1,$2,$3,$4,$5,$6,$7,NOW()) RETURNING id`,
    [SYMBOL, a.side, a.price, a.score, a.tags.join(' | '), a.tpPct, a.slPct]
  );
  const id = rows[0] && rows[0].id;
  if (!id) return null;

  lastSignalAt = Date.now();
  signalsSent += 1;
  await logEv(id, 'ENTRADA', a.side, a.price);

  await tg(
    (long ? '🟢' : '🔴') + ' <b>CAPUNGA TRADE · ' + a.side + '</b>\n' +
    '----------------\n' +
    'Bybit · ' + SYMBOL + ' · #' + id + '\n' +
    'Entrada ' + fmt(a.price) + '\n' +
    'TP ' + fmt(tpPrice) + '  (+' + a.tpPct + '%)\n' +
    'SL ' + fmt(slPrice) + '  (-' + a.slPct + '%)\n' +
    'Score ' + a.score + '/100 · RSI ' + a.rsi + '\n' +
    'Gale max ' + MAX_GALE + ' (se bater SL)\n' +
    '----------------\n' +
    a.tags.slice(0, 5).map(function (t) { return '- ' + t; }).join('\n') +
    '\n\nSinal experimental · risco de perda'
  );
  console.log('[SINAL] #' + id + ' ' + a.side + ' @ ' + a.price + ' score=' + a.score);
  return id;
}

// ── Loop ────────────────────────────────────────────────────────────────────

async function tick() {
  if (busy) return;
  busy = true;
  try {
    await expireOld();

    const res = await Promise.all([
      fetchKlines('1m', 200),
      fetchKlines('5m', 120),
      fetchKlines('15m', 100)
    ]);
    const raw1 = res[0];
    const px = await fetchPrice(raw1[raw1.length - 1].c);

    await resolvePending(raw1, px);

    const c1 = closedOnly(raw1, TF['1m'].ms);
    const c5 = closedOnly(res[1], TF['5m'].ms);
    const c15 = closedOnly(res[2], TF['15m'].ms);

    const a = analyze(c1, c5, c15);
    lastAnalysis = Object.assign({}, a, { ts: nowISO(), livePrice: px, source: lastSource });

    if (a.ok) await openSignal(a);
    else console.log('[SCAN] C=' + a.buy + ' V=' + a.sell + ' min=' + MIN_SCORE + ' · ' + a.reason);

    lastError = null;
  } catch (e) {
    lastError = e.message;
    console.error('[TICK]', e.message);
  } finally {
    busy = false;
  }
}

// ── Backtest (taxa de acerto real nos dados recentes) ───────────────────────

function simulate(k1, i, side, price, atrPct) {
  const long = side === 'COMPRA';
  const lv = levels(atrPct);
  const maxJ = Math.min(k1.length - 1, i + Math.ceil(SINAL_EXPIRA_MS / 60000));
  let ref = price;
  let stage = 0;
  for (let j = i + 1; j <= maxJ; j++) {
    const k = k1[j];
    const sl = long ? ref * (1 - lv.sl / 100) : ref * (1 + lv.sl / 100);
    const tp = long ? ref * (1 + lv.tp / 100) : ref * (1 - lv.tp / 100);
    const hitSL = long ? k.l <= sl : k.h >= sl;
    const hitTP = long ? k.h >= tp : k.l <= tp;
    if (hitSL) {
      if (stage === 0) { stage = 1; ref = k.c; continue; }
      return { res: 'LOSS', end: j };
    }
    if (hitTP) return { res: stage ? 'G1' : 'GREEN', end: j };
  }
  return { res: 'EXP', end: maxJ };
}

async function backtest() {
  const res = await Promise.all([
    fetchKlines('1m', 1000),
    fetchKlines('5m', 1000),
    fetchKlines('15m', 1000)
  ]);
  const k1 = closedOnly(res[0], TF['1m'].ms);
  const k5 = res[1];
  const k15 = res[2];

  const out = { GREEN: 0, G1: 0, LOSS: 0, EXP: 0 };
  let signals = 0;
  let i = 120;
  while (i < k1.length - 2) {
    const tClose = k1[i].t + TF['1m'].ms;
    const s1 = k1.slice(i - 119, i + 1);
    const s5 = closedOnly(k5, TF['5m'].ms, tClose).slice(-120);
    const s15 = closedOnly(k15, TF['15m'].ms, tClose).slice(-100);
    const a = analyze(s1, s5, s15);
    if (a.ok) {
      signals += 1;
      const r = simulate(k1, i, a.side, a.price, a.atrPct);
      out[r.res] += 1;
      i = Math.max(i + 1, r.end + Math.ceil(ANTI_SPAM_MS / 60000));
      if (r.res === 'LOSS') i += Math.ceil(LOSS_PAUSE_MS / 60000);
    } else {
      i += 1;
    }
  }
  const settled = out.GREEN + out.G1 + out.LOSS;
  const wins = out.GREEN + out.G1;
  return {
    candles1m: k1.length,
    horas: Number((k1.length / 60).toFixed(1)),
    signals: signals,
    results: out,
    winrate: settled ? Number(((wins * 100) / settled).toFixed(1)) : null,
    winrateSemGale: settled ? Number(((out.GREEN * 100) / settled).toFixed(1)) : null,
    nota: 'Sem taxas/slippage. Amostra curta (~16h de 1m): use como indicação, não como garantia.'
  };
}

// ── HTTP ────────────────────────────────────────────────────────────────────

app.get('/', function (_req, res) {
  res.json({
    name: 'CAPUNGA TRADE', symbol: SYMBOL, exchange: 'bybit', status: 'online',
    version: '3.0', gale: MAX_GALE, mode: 'opportunity-based',
    disclaimer: 'Experimental. Not financial advice.'
  });
});

app.get('/health', async function (_req, res) {
  try {
    await pool.query('SELECT 1');
    res.json({
      ok: true, db: 'up',
      lastAnalysis: (lastAnalysis && lastAnalysis.ts) || null,
      source: lastSource, lastError: lastError, signalsSent: signalsSent,
      telegram: Boolean(BOT_TOKEN && CHAT_ID)
    });
  } catch (e) {
    res.status(503).json({ ok: false, error: e.message });
  }
});

app.get('/status', async function (_req, res) {
  try {
    const { rows } = await pool.query(`
      SELECT
        COUNT(*) FILTER (WHERE estado='GREEN')::int AS green,
        COUNT(*) FILTER (WHERE estado='G1')::int AS g1,
        COUNT(*) FILTER (WHERE estado='LOSS')::int AS loss,
        COUNT(*) FILTER (WHERE estado='PENDENTE')::int AS pending,
        COUNT(*) FILTER (WHERE estado='EXPIRADO')::int AS expired
      FROM trade_sinais
    `);
    const t = rows[0] || {};
    const settled = (t.green || 0) + (t.g1 || 0) + (t.loss || 0);
    const wins = (t.green || 0) + (t.g1 || 0);
    const winrate = settled ? Number(((wins * 100) / settled).toFixed(2)) : null;
    const pend = await pendingSignal();

    res.json({
      ok: true, symbol: SYMBOL, source: lastSource,
      config: {
        minScore: MIN_SCORE, gale: MAX_GALE, tpAtr: TP_ATR, slAtr: SL_ATR,
        tpMinPct: TP_MIN_PCT, slMinPct: SL_MIN_PCT, expiraMs: SINAL_EXPIRA_MS,
        antiSpamMs: ANTI_SPAM_MS, lossPauseMs: LOSS_PAUSE_MS
      },
      stats: Object.assign({}, t, { settled: settled, winrate: winrate }),
      pending: pend ? {
        id: pend.id, side: pend.direcao, entry: pend.preco_entrada,
        gale: pend.gale, galeEntry: pend.gale_preco, at: pend.criado_em
      } : null,
      lastAnalysis: lastAnalysis, lastError: lastError
    });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

app.get('/sinais', async function (req, res) {
  try {
    const limit = Math.min(50, Math.max(1, Number(req.query.limit) || 20));
    const { rows } = await pool.query(
      `SELECT id, symbol, direcao, preco_entrada, score, estado, gale, tp_pct, sl_pct,
              preco_saida, pnl_pct, criado_em, resolvido_em
       FROM trade_sinais ORDER BY id DESC LIMIT $1`,
      [limit]
    );
    res.json({ ok: true, items: rows });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

app.post('/scan', async function (_req, res) {
  try {
    await tick();
    res.json({ ok: true, lastAnalysis: lastAnalysis });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

app.get('/backtest', async function (_req, res) {
  try {
    res.json({ ok: true, config: { minScore: MIN_SCORE, tpAtr: TP_ATR, slAtr: SL_ATR }, result: await backtest() });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

// ── Boot ────────────────────────────────────────────────────────────────────

async function main() {
  await initDB();
  const server = app.listen(PORT, '0.0.0.0', function () {
    console.log('[HTTP] CAPUNGA TRADE v3 · Bybit · :' + PORT + ' · ' + SYMBOL);
  });

  await tick();
  loopTimer = setInterval(function () {
    tick().catch(function (e) { console.error(e.message); });
  }, LOOP_MS);

  // Render free adormece sem tráfego: auto-ping a cada 8 min
  if (SELF_URL) {
    keepTimer = setInterval(function () {
      axios.get(SELF_URL + '/health', { timeout: 10000 }).catch(function () {});
    }, 8 * 60 * 1000);
    console.log('[KEEPALIVE] ' + SELF_URL);
  } else {
    console.log('[KEEPALIVE] sem URL (defina KEEPALIVE_URL ou use um ping externo em /health)');
  }

  function stop() {
    if (loopTimer) clearInterval(loopTimer);
    if (keepTimer) clearInterval(keepTimer);
    server.close(async function () {
      try { await pool.end(); } finally { process.exit(0); }
    });
    setTimeout(function () { process.exit(1); }, 6000).unref();
  }
  process.on('SIGTERM', stop);
  process.on('SIGINT', stop);
}

main().catch(function (e) {
  console.error('[FATAL]', e);
  process.exit(1);
});
