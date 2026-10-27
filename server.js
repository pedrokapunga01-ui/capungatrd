'use strict';

/**
 * CAPUNGA TRADE — BTC Signal Engine v3 (Bybit)
 *
 * - Dados da Bybit (BTCUSDT perp), fallback OKX
 * - Só usa velas FECHADAS (sem repaint)
 * - Só sinaliza com 5m + 15m alinhados com a direção + pullback + momentum
 * - TP/SL dinâmicos por ATR, resolvidos por high/low das velas (não perde picos)
 * - Gale máx. 1 (re-entrada real, com novo TP/SL)
 * - Sem sinais expirados: ao acordar, apura GREEN/LOSS pelo histórico de velas
 * - 100% automático: loop + keep-alive (Render free) + pausa após LOSS
 * - /backtest mede a taxa de acerto REAL da estratégia nos dados recentes
 *
 * Env obrigatórias: DATABASE_URL · BOT_TOKEN · CHAT_ID
 * Env opcionais:
 *   PORT · SYMBOL (BTCUSDT) · BYBIT_CATEGORY (linear)
 *   MIN_CONFLUENCIA (75) · LOOP_MS (20000)
 *   TP_ATR (1.0) · SL_ATR (2.0) · TP_MIN_PCT (0.10) · SL_MIN_PCT (0.20)
 *   SINAL_EXPIRA_MS (1200000, janela máx. antes de fechar por preço) · ANTI_SPAM_MS (180000) · LOSS_PAUSE_MS (900000)
 *   KEEPALIVE_URL (ou RENDER_EXTERNAL_URL automático no Render)
 *   TIMEZONE (Africa/Luanda) · DATABASE_SSL=false para desligar SSL
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
const TIMEZONE = process.env.TIMEZONE || 'Africa/Luanda';

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
const pos = (v, fb) => { const n = Number(v); return v != null && Number.isFinite(n) && n > 0 ? n : fb; };
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

// ── Mensagens (estilo SMS), placar e histórico ──────────────────────────────

const LINE = '━━━━━━━━━━━━━━';
const fTime = (ms) => new Date(ms).toLocaleTimeString('pt-PT', { hour: '2-digit', minute: '2-digit', hour12: false, timeZone: TIMEZONE });
const fDate = (ms) => new Date(ms).toLocaleDateString('pt-PT', { day: '2-digit', month: '2-digit', timeZone: TIMEZONE });
const dayKey = (ms) => new Date(ms).toLocaleDateString('en-CA', { timeZone: TIMEZONE });
const sideIcon = (side) => (side === 'COMPRA' ? '🟢' : '🔴');
const stateIcon = (st) => ({ GREEN: '✅', G1: '🔁', LOSS: '❌', PENDENTE: '⏳', EXPIRADO: '⌛' }[st] || '•');
const pctTxt = (v) => (v == null ? '' : (Number(v) >= 0 ? '+' : '') + Number(v).toFixed(2) + '%');

// contagens: hoje (fuso configurado) ou total
async function counts(daily) {
  const where = daily
    ? `WHERE criado_em >= (date_trunc('day', NOW() AT TIME ZONE $1::text) AT TIME ZONE $1::text)`
    : `WHERE $1::text IS NOT NULL`;
  const { rows } = await pool.query(
    `SELECT COUNT(*) FILTER (WHERE estado='GREEN')::int AS g,
            COUNT(*) FILTER (WHERE estado='G1')::int AS g1,
            COUNT(*) FILTER (WHERE estado='LOSS')::int AS l,
            COUNT(*) FILTER (WHERE estado='PENDENTE')::int AS p
     FROM trade_sinais ${where}`,
    [TIMEZONE]
  );
  return rows[0] || { g: 0, g1: 0, l: 0, p: 0 };
}

function boardLine(c) {
  const settled = c.g + c.g1 + c.l;
  const wr = settled ? Math.round(((c.g + c.g1) * 100) / settled) + '%' : '—';
  return '✅ ' + c.g + '  🔁 ' + c.g1 + '  ❌ ' + c.l + '  🎯 ' + wr;
}

async function historyText(limit, daily) {
  const cond = daily
    ? `WHERE criado_em >= (date_trunc('day', NOW() AT TIME ZONE $2::text) AT TIME ZONE $2::text)`
    : `WHERE $2::text IS NOT NULL`;
  const q = await pool.query(
    `SELECT id, direcao, estado, preco_entrada, preco_saida, pnl_pct, criado_em
     FROM trade_sinais ${cond} ORDER BY id DESC LIMIT $1`,
    [limit, TIMEZONE]
  );
  const d = await counts(true);
  const t = await counts(false);
  const head = daily
    ? '🌙 <b>RESUMO DO DIA · ' + fDate(Date.now()) + '</b>'
    : '📋 <b>HISTÓRICO · CAPUNGA TRADE</b>';
  const rows = q.rows.map(function (r) {
    const when = fTime(new Date(r.criado_em).getTime());
    const px = Number(r.preco_entrada);
    const out = r.preco_saida != null ? ' → ' + fmt(r.preco_saida) : '';
    const res = r.pnl_pct != null ? ' · ' + pctTxt(r.pnl_pct) : '';
    return stateIcon(r.estado) + ' #' + r.id + ' ' + sideIcon(r.direcao) + ' ' + r.direcao + ' · ' + fmt(px) + out + res + ' · ' + when;
  });
  return [
    head,
    LINE,
    '📅 Hoje   ' + boardLine(d) + (d.p ? '  ⏳ ' + d.p : ''),
    '🏆 Total  ' + boardLine(t),
    LINE,
    rows.length ? rows.join('\n') : 'Ainda sem sinais.',
    LINE,
    '✅ GREEN  🔁 GREEN no gale  ❌ LOSS  ⏳ em aberto'
  ].join('\n');
}

function msgSignal(a, id, tpPrice, slPrice, d) {
  const long = a.side === 'COMPRA';
  const arr = function (t) { return t === 'ALTA' ? '▲' : t === 'BAIXA' ? '▼' : '►'; };
  return [
    sideIcon(a.side) + ' <b>' + a.side + ' · ' + SYMBOL + '</b>',
    '🏦 Bybit · Perp · ordem a mercado',
    '🕒 ' + fDate(Date.now()) + ' · ' + fTime(Date.now()),
    LINE,
    '🎯 <b>Entrada</b>  <code>' + fmt(a.price) + '</code>',
    '✅ <b>TP</b>  <code>' + fmt(tpPrice) + '</code>  (+' + a.tpPct + '% lucro)',
    '🛑 <b>SL</b>  <code>' + fmt(slPrice) + '</code>  (-' + a.slPct + '% risco)',
    LINE,
    '🔁 <b>Gale 1:</b> se o SL for tocado, re-entrar ' + a.side + ' a mercado (envio novo TP/SL)',
    '⏱ Fecho por preço até ' + fTime(Date.now() + WINDOW_MS),
    LINE,
    '📊 Força <b>' + a.score + '/100</b> · RSI ' + a.rsi,
    '📈 Tendência 15m ' + arr(a.trend15) + ' · 5m ' + arr(a.trend5),
    '🧠 ' + a.tags.slice(0, 4).join(' · '),
    LINE,
    '🆔 #' + id + '  📅 Hoje ' + boardLine(d)
  ].join('\n');
}

function msgGale(s, g, tpP, slP, late) {
  const long = s.direcao === 'COMPRA';
  return [
    '🔁 <b>GALE 1 · #' + s.id + '</b>',
    '⚠️ SL tocado — re-entrar ' + sideIcon(s.direcao) + ' <b>' + s.direcao + '</b> a mercado',
    LINE,
    '🎯 <b>Entrada</b>  <code>' + fmt(g) + '</code>',
    '✅ <b>TP</b>  <code>' + fmt(long ? g * (1 + tpP / 100) : g * (1 - tpP / 100)) + '</code>',
    '🛑 <b>SL</b>  <code>' + fmt(long ? g * (1 - slP / 100) : g * (1 + slP / 100)) + '</code>',
    LINE,
    '⚠️ Último gale: se falhar, é LOSS.' + (late ? '\n🕒 Apurado pelo histórico (bot estava inativo)' : '')
  ].join('\n');
}

function msgResult(s, r, d, late) {
  const win = r.state !== 'LOSS';
  const title = r.state === 'GREEN' ? '✅ <b>GREEN</b>' : r.state === 'G1' ? '🔁 <b>GREEN no GALE 1</b>' : '❌ <b>LOSS</b>';
  const lines = [
    title + ' · #' + s.id,
    sideIcon(s.direcao) + ' ' + s.direcao + ' · ' + SYMBOL,
    LINE,
    '🎯 Entrada  <code>' + fmt(r.ref) + '</code>',
    '🏁 Saída  <code>' + fmt(r.exit) + '</code>',
    (win ? '💰 Resultado  <b>' : '📉 Resultado  <b>') + pctTxt(r.pnl) + '</b>',
    '🕒 ' + fTime(r.at)
  ];
  if (r.timeout) lines.push('ℹ️ Fechado no fim da janela, a favor da entrada');
  if (r.state === 'LOSS') lines.push('⏸ Pausa de ' + Math.round(LOSS_PAUSE_MS / 60000) + ' min nos sinais');
  if (late) lines.push('🕒 Apurado pelo histórico (bot estava inativo)');
  lines.push(LINE, '📅 Hoje ' + boardLine(d));
  return lines.join('\n');
}

// ── Comandos do Telegram (/historico /placar /ajuda) ────────────────────────

let tgOffset = null;

async function tgCommands() {
  if (!BOT_TOKEN || !CHAT_ID) return;
  try {
    const base = `https://api.telegram.org/bot${BOT_TOKEN}/getUpdates`;
    if (tgOffset === null) {
      // primeira vez: ignora mensagens antigas
      const first = await axios.get(base, { params: { offset: -1, timeout: 0 }, timeout: 10000 });
      const l = (first.data && first.data.result) || [];
      tgOffset = l.length ? l[l.length - 1].update_id + 1 : 0;
      return;
    }
    const { data } = await axios.get(base, {
      params: { offset: tgOffset, timeout: 0, allowed_updates: JSON.stringify(['message', 'channel_post']) },
      timeout: 10000
    });
    for (const u of (data && data.result) || []) {
      tgOffset = u.update_id + 1;
      const m = u.message || u.channel_post;
      if (!m || !m.text || !m.chat || String(m.chat.id) !== CHAT_ID) continue;
      const cmd = m.text.trim().split(/[\s@]/)[0].toLowerCase();
      if (cmd === '/historico' || cmd === '/histórico') await tg(await historyText(10, false));
      else if (cmd === '/placar') {
        const d = await counts(true);
        const t = await counts(false);
        await tg('📊 <b>PLACAR</b>\n' + LINE + '\n📅 Hoje   ' + boardLine(d) + '\n🏆 Total  ' + boardLine(t));
      } else if (cmd === '/ajuda' || cmd === '/start') {
        await tg('🤖 <b>CAPUNGA TRADE</b>\n' + LINE + '\n/historico · últimos 10 sinais\n/placar · acertos de hoje e total\n/ajuda · esta mensagem');
      }
    }
  } catch (e) { /* webhook ativo ou rede: ignora */ }
}

// resumo automático no fim do dia (23:55 no fuso configurado)
let lastSummaryDay = null;
async function dailySummary() {
  const now = Date.now();
  const hm = fTime(now);
  const today = dayKey(now);
  if (hm >= '23:55' && lastSummaryDay !== today) {
    lastSummaryDay = today;
    const d = await counts(true);
    if (d.g + d.g1 + d.l + d.p > 0) await tg(await historyText(30, true));
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
  // Limpeza: resultados falsos com saída = entrada (bug de TP/SL nulos em sinais da versão antiga)
  const bad = await pool.query(
    `SELECT id FROM trade_sinais
     WHERE estado IN ('GREEN','G1','LOSS') AND preco_saida IS NOT NULL AND preco_saida = preco_entrada`
  );
  if (bad.rows.length) {
    const ids = bad.rows.map(function (r) { return r.id; });
    await pool.query(`DELETE FROM trade_eventos WHERE sinal_id = ANY($1::bigint[])`, [ids]);
    await pool.query(`DELETE FROM trade_sinais WHERE id = ANY($1::bigint[])`, [ids]);
    console.log('[DB] removidos ' + ids.length + ' resultados falsos: #' + ids.join(', #'));
  }
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
// Sem "expirado": todo sinal fecha como GREEN / G1 / LOSS.
// Se o bot esteve a dormir, ao acordar reconstrói o que aconteceu a partir das
// velas de 1m desde a entrada (replay cronológico) e atualiza o histórico.
// Fim da janela (SINAL_EXPIRA_MS) sem TP/SL: fecha pelo preço — a favor = GREEN, contra = gale/LOSS.

const WINDOW_MS = SINAL_EXPIRA_MS;

// Velas de 1m desde startMs até agora (paginado, 1000 por pedido)
async function fetchRange(startMs) {
  const endAll = Date.now();
  let cursor = Math.max(startMs, endAll - 13 * 24 * 3600 * 1000);
  const out = [];
  for (let n = 0; n < 20 && cursor < endAll; n++) {
    const end = Math.min(endAll, cursor + 1000 * 60000 - 1);
    let list = null;
    for (const host of BYBIT_HOSTS) {
      try {
        const { data } = await axios.get(
          `${host}/v5/market/kline?category=${CATEGORY}&symbol=${SYMBOL}&interval=1&start=${cursor}&end=${end}&limit=1000`,
          { timeout: 12000, headers: HEADERS }
        );
        const l = data && data.retCode === 0 && data.result && data.result.list;
        if (l) { list = l; break; }
      } catch (e) { /* tenta próximo host */ }
    }
    if (!list) return null;
    list.forEach(function (k) {
      out.push({ t: Number(k[0]), o: num(k[1]), h: num(k[2]), l: num(k[3]), c: num(k[4]), v: num(k[5]) });
    });
    cursor = end + 1;
  }
  out.sort(function (a, b) { return a.t - b.t; });
  return out;
}

// Replay cronológico de um sinal pendente sobre as velas (SL tem prioridade se ambos na mesma vela)
function replaySignal(s, candles, nowMs, px) {
  const long = s.direcao === 'COMPRA';
  const tpP = pos(s.tp_pct, TP_MIN_PCT);
  const slP = pos(s.sl_pct, SL_MIN_PCT);
  let stage = Number(s.gale);
  let ref = stage ? num(s.gale_preco) : num(s.preco_entrada);
  let refMs = new Date(s.ref_em || s.criado_em).getTime();
  let galeEntry = stage ? ref : null;

  const pnlOf = function (exit) {
    return Number(((long ? exit - ref : ref - exit) / ref * 100).toFixed(4));
  };
  const done = function (state, exit, at, extra) {
    return Object.assign({
      state: state, stage: stage, ref: ref, galeEntry: galeEntry,
      exit: exit, pnl: pnlOf(exit), at: at
    }, extra || {});
  };

  for (const k of candles) {
    if (k.t < refMs) continue;
    const endMs = k.t + 60000;
    const closed = endMs <= nowMs;
    const tpPrice = long ? ref * (1 + tpP / 100) : ref * (1 - tpP / 100);
    const slPrice = long ? ref * (1 - slP / 100) : ref * (1 + slP / 100);
    const hitSL = long ? k.l <= slPrice : k.h >= slPrice;
    const hitTP = long ? k.h >= tpPrice : k.l <= tpPrice;
    const at = Math.min(endMs, nowMs);

    let loss = null;
    if (hitSL) {
      loss = { exit: slPrice, entry: closed ? k.c : px };
    } else if (hitTP) {
      return done(stage ? 'G1' : 'GREEN', tpPrice, at);
    } else if (closed && endMs >= refMs + WINDOW_MS) {
      const fav = long ? k.c > ref : k.c < ref;
      if (fav) return done(stage ? 'G1' : 'GREEN', k.c, at, { timeout: true });
      loss = { exit: k.c, entry: k.c };
    }

    if (loss) {
      if (stage >= MAX_GALE) return done('LOSS', loss.exit, at);
      stage = 1;
      ref = loss.entry;
      galeEntry = ref;
      refMs = at;
    }
  }
  return { state: 'PENDENTE', stage: stage, ref: ref, galeEntry: galeEntry, refMs: refMs };
}

async function resolvePending(raw1, px) {
  const s = await pendingSignal();
  if (!s) return;

  const nowMs = Date.now();
  const refMs0 = new Date(s.ref_em || s.criado_em).getTime();
  let candles = raw1;
  if (raw1.length && refMs0 < raw1[0].t) {
    // bot esteve inativo mais que as velas já carregadas → busca o histórico em falta
    const rng = await fetchRange(refMs0);
    if (rng && rng.length) candles = rng;
  }

  const r = replaySignal(s, candles, nowMs, px);
  const tpP = pos(s.tp_pct, TP_MIN_PCT);
  const slP = pos(s.sl_pct, SL_MIN_PCT);

  if (r.state === 'PENDENTE') {
    if (r.stage > Number(s.gale)) {
      await pool.query(
        `UPDATE trade_sinais SET gale=1, gale_preco=$2, ref_em=to_timestamp($3::double precision / 1000.0)
         WHERE id=$1 AND estado='PENDENTE'`,
        [s.id, r.galeEntry, r.refMs]
      );
      await logEv(s.id, 'GALE1', 'SL tocado', r.galeEntry);
      await tg(msgGale(s, r.galeEntry, tpP, slP, nowMs - r.refMs > 180000));
    }
    return;
  }

  await pool.query(
    `UPDATE trade_sinais
     SET estado=$2, gale=$3, gale_preco=$4, preco_saida=$5, pnl_pct=$6,
         resolvido_em=to_timestamp($7::double precision / 1000.0)
     WHERE id=$1 AND estado='PENDENTE'`,
    [s.id, r.state, r.stage, r.galeEntry, r.exit, r.pnl, r.at]
  );
  await logEv(s.id, 'FECHO', r.state + (r.timeout ? ' (fim da janela)' : ''), r.exit);
  if (r.state === 'LOSS') lastLossAt = r.at;

  const d = await counts(true);
  await tg(msgResult(s, r, d, nowMs - r.at > 180000));
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

  const d = await counts(true);
  await tg(msgSignal(a, id, tpPrice, slPrice, d));
  console.log('[SINAL] #' + id + ' ' + a.side + ' @ ' + a.price + ' score=' + a.score);
  return id;
}

// ── Instância única (evita sinais/mensagens duplicados em deploys sobrepostos) ──

let lockClient = null;
let isLeader = false;

async function ensureLeader() {
  if (isLeader && lockClient) return true;
  try {
    if (!lockClient) {
      const c = await pool.connect();
      lockClient = c;
      c.on('error', function () {
        isLeader = false;
        lockClient = null;
        try { c.release(true); } catch (e) { /* ignora */ }
      });
    }
    const { rows } = await lockClient.query('SELECT pg_try_advisory_lock(727301) AS ok');
    isLeader = rows[0].ok === true;
    if (isLeader) console.log('[LOCK] instância líder');
    else {
      console.log('[LOCK] outra instância ativa, em espera');
      lockClient.release();
      lockClient = null;
    }
    return isLeader;
  } catch (e) {
    isLeader = false;
    if (lockClient) { try { lockClient.release(true); } catch (x) { /* ignora */ } lockClient = null; }
    return false;
  }
}

// ── Loop ────────────────────────────────────────────────────────────────────

async function tick() {
  if (busy) return;
  busy = true;
  try {
    if (!(await ensureLeader())) return;
    await tgCommands();
    await dailySummary();

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
  const W = Math.ceil(WINDOW_MS / 60000);
  let ref = price;
  let stage = 0;
  let startJ = i;
  for (let j = i + 1; j < k1.length; j++) {
    const k = k1[j];
    const sl = long ? ref * (1 - lv.sl / 100) : ref * (1 + lv.sl / 100);
    const tp = long ? ref * (1 + lv.tp / 100) : ref * (1 - lv.tp / 100);
    const hitSL = long ? k.l <= sl : k.h >= sl;
    const hitTP = long ? k.h >= tp : k.l <= tp;
    let lost = false;
    if (hitSL) lost = true;
    else if (hitTP) return { res: stage ? 'G1' : 'GREEN', end: j };
    else if (j - startJ >= W) {
      if (long ? k.c > ref : k.c < ref) return { res: stage ? 'G1' : 'GREEN', end: j };
      lost = true;
    }
    if (lost) {
      if (stage === 0) { stage = 1; ref = k.c; startJ = j; }
      else return { res: 'LOSS', end: j };
    }
  }
  return { res: 'ABERTO', end: k1.length };
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

  const out = { GREEN: 0, G1: 0, LOSS: 0, ABERTO: 0 };
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
    if (lockClient) { try { lockClient.release(true); } catch (e) { /* ignora */ } }
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
