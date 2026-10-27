'use strict';

/**
 * CAPUNGA TRADE — BTC Signal Engine v2
 *
 * Sinais COMPRA / VENDA por oportunidade (sem intervalo fixo de 4 min).
 * Gale máximo: 1
 *
 * Env obrigatórias:
 *   DATABASE_URL · BOT_TOKEN · CHAT_ID
 *
 * Env opcionais:
 *   PORT · SYMBOL · MIN_CONFLUENCIA (default 62)
 *   SINAL_EXPIRA_MS (default 8 min) · ANTI_SPAM_MS (default 75s)
 *   DATABASE_SSL
 */

const express = require('express');
const cors = require('cors');
const axios = require('axios');
const { Pool } = require('pg');

const app = express();
app.use(cors());
app.use(express.json({ limit: '256kb' }));

// ── Config ──────────────────────────────────────────────────────────────────

const PORT = Number(process.env.PORT || 3000);
const DATABASE_URL = process.env.DATABASE_URL;
const BOT_TOKEN = (process.env.BOT_TOKEN || '').trim();
const CHAT_ID = (process.env.CHAT_ID || '').trim();
const SYMBOL = (process.env.SYMBOL || 'BTCUSDT').toUpperCase();

const MIN_CONFLUENCIA = Math.min(90, Math.max(50, Number(process.env.MIN_CONFLUENCIA || 62)));
const SINAL_EXPIRA_MS = Math.max(180000, Number(process.env.SINAL_EXPIRA_MS || 480000));
const ANTI_SPAM_MS = Math.max(45000, Number(process.env.ANTI_SPAM_MS || 75000));
const LOOP_MS = Math.max(20000, Number(process.env.LOOP_MS || 45000));
const MAX_GALE = 1;
const TP_PCT = 0.12;
const SL_PCT = 0.10;

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
let lastAnalysis = null;
let lastError = null;
let signalsSent = 0;
let loopTimer = null;

// ── Utils ───────────────────────────────────────────────────────────────────

const nowISO = () => new Date().toISOString();
const clamp = (n, a, b) => Math.max(a, Math.min(b, n));
const num = (v, fb = 0) => {
  const n = Number(v);
  return Number.isFinite(n) ? n : fb;
};

// ── Telegram ────────────────────────────────────────────────────────────────

async function tg(text) {
  if (!BOT_TOKEN || !CHAT_ID) return false;
  try {
    const { data } = await axios.post(
      `https://api.telegram.org/bot${BOT_TOKEN}/sendMessage`,
      { chat_id: CHAT_ID, text, disable_web_page_preview: true, parse_mode: 'HTML' },
      { timeout: 12000 }
    );
    return data?.ok === true;
  } catch (e) {
    lastError = 'tg: ' + e.message;
    console.error('[TG]', e.response?.data || e.message);
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
  // Migração da v1 (confluencia) → v2 (score)
  await pool.query(`ALTER TABLE trade_sinais ADD COLUMN IF NOT EXISTS score INTEGER DEFAULT 0`);
  await pool.query(`ALTER TABLE trade_sinais ADD COLUMN IF NOT EXISTS setup TEXT`);
  await pool.query(`ALTER TABLE trade_sinais ADD COLUMN IF NOT EXISTS preco_saida NUMERIC`);
  await pool.query(`ALTER TABLE trade_sinais ADD COLUMN IF NOT EXISTS pnl_pct NUMERIC`);
  // Se existir coluna antiga confluencia, copia valores
  await pool.query(`
    DO $$ BEGIN
      IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name='trade_sinais' AND column_name='confluencia'
      ) THEN
        UPDATE trade_sinais SET score = COALESCE(score, confluencia, 0) WHERE score IS NULL OR score = 0;
      END IF;
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
  await pool.query(
    `INSERT INTO trade_eventos (sinal_id, tipo, detalhe, preco) VALUES ($1,$2,$3,$4)`,
    [sinalId, tipo, detalhe, preco]
  );
}

// ── Market data ─────────────────────────────────────────────────────────────

async function fetchKlines(interval = '1m', limit = 100) {
  const headers = { 'User-Agent': 'CapungaTrade/2.0', Accept: 'application/json' };
  const errs = [];

  try {
    const inst = SYMBOL.replace('USDT', '-USDT');
    const bar = interval === '5m' ? '5m' : interval === '15m' ? '15m' : '1m';
    const { data } = await axios.get(
      `https://www.okx.com/api/v5/market/candles?instId=${inst}&bar=${bar}&limit=${limit}`,
      { timeout: 9000, headers }
    );
    if (data && data.code === '0' && data.data && data.data.length) {
      return data.data.slice().reverse().map(function (k) {
        return {
          t: Number(k[0]), o: num(k[1]), h: num(k[2]), l: num(k[3]), c: num(k[4]), v: num(k[5])
        };
      });
    }
    errs.push('okx');
  } catch (e) { errs.push('okx:' + e.message); }

  try {
    const sym = SYMBOL.replace('USDT', '-USDT');
    const type = interval === '5m' ? '5min' : interval === '15m' ? '15min' : '1min';
    const { data } = await axios.get(
      `https://api.kucoin.com/api/v1/market/candles?type=${type}&symbol=${sym}`,
      { timeout: 9000, headers }
    );
    if (data && data.code === '200000' && data.data && data.data.length) {
      return data.data.slice(0, limit).reverse().map(function (k) {
        return {
          t: Number(k[0]) * (String(k[0]).length === 10 ? 1000 : 1),
          o: num(k[1]), c: num(k[2]), h: num(k[3]), l: num(k[4]), v: num(k[5])
        };
      });
    }
    errs.push('kucoin');
  } catch (e) { errs.push('kucoin:' + e.message); }

  try {
    const iv = interval === '5m' ? '5' : interval === '15m' ? '15' : '1';
    const { data } = await axios.get(
      `https://api.bybit.com/v5/market/kline?category=linear&symbol=${SYMBOL}&interval=${iv}&limit=${limit}`,
      { timeout: 9000, headers }
    );
    const list = (data && data.result && data.result.list) || [];
    if (list.length) {
      return list.slice().reverse().map(function (k) {
        return {
          t: Number(k[0]), o: num(k[1]), h: num(k[2]), l: num(k[3]), c: num(k[4]), v: num(k[5])
        };
      });
    }
    errs.push('bybit');
  } catch (e) { errs.push('bybit:' + e.message); }

  throw new Error('market_data fail: ' + errs.join(','));
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

// ── Strategy ────────────────────────────────────────────────────────────────

function analyze(c1, c5) {
  if (!c1 || c1.length < 60) return { ok: false, reason: 'dados_1m', buy: 0, sell: 0 };
  if (!c5 || c5.length < 30) return { ok: false, reason: 'dados_5m', buy: 0, sell: 0 };

  const closes = c1.map(function (x) { return x.c; });
  const highs = c1.map(function (x) { return x.h; });
  const lows = c1.map(function (x) { return x.l; });
  const vols = c1.map(function (x) { return x.v; });
  const c5c = c5.map(function (x) { return x.c; });

  const e9 = ema(closes, 9);
  const e21 = ema(closes, 21);
  const e50 = ema(closes, 50);
  const r14 = rsi(closes, 14);
  const mh = macdHist(closes);
  const a14 = atr(highs, lows, closes, 14);
  const vSma = sma(vols, 20);
  const e9_5 = ema(c5c, 9);
  const e21_5 = ema(c5c, 21);

  if (!e9.length || !e21.length || !e50.length || !r14.length || !mh.length || !a14.length) {
    return { ok: false, reason: 'indicadores', buy: 0, sell: 0 };
  }

  const i = closes.length - 1;
  const price = closes[i];
  const prev = closes[i - 1];
  const E9 = e9[e9.length - 1];
  const E21 = e21[e21.length - 1];
  const E50 = e50[e50.length - 1];
  const R = r14[r14.length - 1];
  const H = mh[mh.length - 1];
  const Hp = mh.length > 1 ? mh[mh.length - 2] : 0;
  const A = a14[a14.length - 1];
  const vol = vols[i];
  const volM = vSma[vSma.length - 1] || vol;
  const trend5 = e9_5.length && e21_5.length
    ? (e9_5[e9_5.length - 1] > e21_5[e21_5.length - 1] ? 'ALTA' : 'BAIXA')
    : 'NEUTRO';

  const up3 = closes[i] > closes[i - 1] && closes[i - 1] > closes[i - 2];
  const dn3 = closes[i] < closes[i - 1] && closes[i - 1] < closes[i - 2];

  let buy = 0;
  let sell = 0;
  const tags = [];

  if (E9 > E21 && E21 > E50) { buy += 18; tags.push('EMA stack up'); }
  else if (E9 < E21 && E21 < E50) { sell += 18; tags.push('EMA stack down'); }
  else if (E9 > E21) { buy += 8; }
  else { sell += 8; }

  if (R < 28) { buy += 22; tags.push('RSI ' + R.toFixed(0) + ' oversold'); }
  else if (R < 38) { buy += 12; tags.push('RSI ' + R.toFixed(0)); }
  else if (R > 72) { sell += 22; tags.push('RSI ' + R.toFixed(0) + ' overbought'); }
  else if (R > 62) { sell += 12; tags.push('RSI ' + R.toFixed(0)); }
  else if (R < 48) buy += 5;
  else if (R > 52) sell += 5;

  if (H > 0 && H > Hp) { buy += 14; tags.push('MACD+'); }
  else if (H < 0 && H < Hp) { sell += 14; tags.push('MACD-'); }
  else if (H > 0) buy += 5;
  else sell += 5;

  if (vol > volM * 1.3) {
    if (price > prev) { buy += 12; tags.push('Vol up green'); }
    else { sell += 12; tags.push('Vol up red'); }
  }

  if (trend5 === 'ALTA') { buy += 10; tags.push('5m ALTA'); }
  else if (trend5 === 'BAIXA') { sell += 10; tags.push('5m BAIXA'); }

  if (up3) { buy += 10; tags.push('3 up'); }
  if (dn3) { sell += 10; tags.push('3 down'); }

  if (price > E50) buy += 6;
  else sell += 6;

  if (R < 30 && trend5 === 'BAIXA') { buy += 14; tags.push('Rev oversold'); }
  if (R > 70 && trend5 === 'ALTA') { sell += 14; tags.push('Rev overbought'); }

  const atrPct = (A / price) * 100;
  if (atrPct < 0.02) {
    buy = Math.floor(buy * 0.9);
    sell = Math.floor(sell * 0.9);
    tags.push('low vol');
  }

  buy = clamp(buy, 0, 100);
  sell = clamp(sell, 0, 100);

  let side = null;
  let score = 0;
  if (buy >= MIN_CONFLUENCIA && buy >= sell + 4) {
    side = 'COMPRA';
    score = buy;
  } else if (sell >= MIN_CONFLUENCIA && sell >= buy + 4) {
    side = 'VENDA';
    score = sell;
  }

  return {
    ok: Boolean(side),
    side: side,
    score: score,
    buy: buy,
    sell: sell,
    price: price,
    rsi: Number(R.toFixed(1)),
    trend5: trend5,
    atrPct: Number(atrPct.toFixed(3)),
    tags: tags,
    reason: side ? 'setup_ok' : 'sem_setup'
  };
}

// ── Lifecycle ───────────────────────────────────────────────────────────────

async function expireOld() {
  const { rows } = await pool.query(
    `UPDATE trade_sinais
     SET estado='EXPIRADO', resolvido_em=NOW()
     WHERE estado='PENDENTE'
       AND criado_em < NOW() - ($1::text || ' milliseconds')::interval
     RETURNING id, direcao, preco_entrada`,
    [String(SINAL_EXPIRA_MS)]
  );
  for (const s of rows) {
    await logEv(s.id, 'EXPIRADO', 'timeout');
    await tg(
      'CAPUNGA TRADE\n' +
      'Sinal #' + s.id + ' expirado\n' +
      s.direcao + ' @ ' + Number(s.preco_entrada).toFixed(1) + '\n' +
      'Sem resultado'
    );
  }
}

async function resolveWithPrice(px) {
  const s = await pendingSignal();
  if (!s) return;

  const entry = Number(s.preco_entrada);
  const dir = s.direcao;
  const gale = Number(s.gale);
  const pct = ((px - entry) / entry) * 100;

  const win =
    (dir === 'COMPRA' && pct >= TP_PCT) ||
    (dir === 'VENDA' && pct <= -TP_PCT);

  const lose =
    (dir === 'COMPRA' && pct <= -SL_PCT) ||
    (dir === 'VENDA' && pct >= SL_PCT);

  if (win) {
    const st = gale === 0 ? 'GREEN' : 'G1';
    await pool.query(
      `UPDATE trade_sinais SET estado=$2, preco_saida=$3, pnl_pct=$4, resolvido_em=NOW()
       WHERE id=$1 AND estado='PENDENTE'`,
      [s.id, st, px, Number(pct.toFixed(4))]
    );
    await logEv(s.id, 'FECHO', st, px);
    await tg(
      'GREEN' + (st === 'G1' ? ' G1' : '') + ' · CAPUNGA TRADE\n' +
      '----------------\n' +
      '#' + s.id + '  ' + dir + '\n' +
      'In  ' + entry.toFixed(1) + '\n' +
      'Out ' + px.toFixed(1) + '\n' +
      'PnL ' + (pct >= 0 ? '+' : '') + pct.toFixed(3) + '%\n' +
      'Gale ' + gale
    );
    return;
  }

  if (lose) {
    if (gale < MAX_GALE) {
      await pool.query(`UPDATE trade_sinais SET gale=1 WHERE id=$1 AND estado='PENDENTE'`, [s.id]);
      await logEv(s.id, 'GALE1', String(px), px);
      await tg(
        'GALE 1 · CAPUNGA TRADE\n' +
        '----------------\n' +
        '#' + s.id + '  manter ' + dir + '\n' +
        'Agora ' + px.toFixed(1) + '\n' +
        'Entrada ' + entry.toFixed(1)
      );
      return;
    }
    await pool.query(
      `UPDATE trade_sinais SET estado='LOSS', preco_saida=$2, pnl_pct=$3, resolvido_em=NOW()
       WHERE id=$1 AND estado='PENDENTE'`,
      [s.id, px, Number(pct.toFixed(4))]
    );
    await logEv(s.id, 'LOSS', 'max gale', px);
    await tg(
      'LOSS · CAPUNGA TRADE\n' +
      '----------------\n' +
      '#' + s.id + '  ' + dir + '\n' +
      'In  ' + entry.toFixed(1) + '\n' +
      'Out ' + px.toFixed(1) + '\n' +
      'PnL ' + pct.toFixed(3) + '%'
    );
  }
}

async function openSignal(a) {
  if (await pendingSignal()) return null;
  if (Date.now() - lastSignalAt < ANTI_SPAM_MS) return null;

  const arrow = a.side === 'COMPRA' ? 'COMPRA' : 'VENDA';
  const msg =
    'CAPUNGA TRADE\n' +
    '----------------\n' +
    arrow + '\n' +
    SYMBOL + '  ' + a.price.toFixed(1) + '\n' +
    'Score ' + a.score + '/100\n' +
    'RSI ' + a.rsi + ' · ' + a.trend5 + '\n' +
    'Gale max ' + MAX_GALE + '\n' +
    '----------------\n' +
    a.tags.slice(0, 4).map(function (t) { return '- ' + t; }).join('\n') +
    '\n\nSinal experimental · risco de perda';

  const { rows } = await pool.query(
    `INSERT INTO trade_sinais (symbol, direcao, preco_entrada, score, setup)
     VALUES ($1,$2,$3,$4,$5) RETURNING id`,
    [SYMBOL, a.side, a.price, a.score, a.tags.join(' | ')]
  );
  const id = rows[0] && rows[0].id;
  if (!id) return null;

  lastSignalAt = Date.now();
  signalsSent += 1;
  await logEv(id, 'ENTRADA', a.side, a.price);
  await tg(msg);
  console.log('[SINAL] #' + id + ' ' + a.side + ' @ ' + a.price + ' score=' + a.score);
  return id;
}

// ── Loop ────────────────────────────────────────────────────────────────────

async function tick() {
  if (busy) return;
  busy = true;
  try {
    await expireOld();
    const pair = await Promise.all([
      fetchKlines('1m', 100),
      fetchKlines('5m', 50)
    ]);
    const c1 = pair[0];
    const c5 = pair[1];
    const px = c1[c1.length - 1] && c1[c1.length - 1].c;
    if (px) await resolveWithPrice(px);

    const a = analyze(c1, c5);
    lastAnalysis = Object.assign({}, a, { ts: nowISO(), price: px });

    if (a.ok) await openSignal(a);
    else console.log('[SCAN] C=' + a.buy + ' V=' + a.sell + ' min=' + MIN_CONFLUENCIA + ' · ' + a.reason);

    lastError = null;
  } catch (e) {
    lastError = e.message;
    console.error('[TICK]', e.message);
  } finally {
    busy = false;
  }
}

// ── HTTP ────────────────────────────────────────────────────────────────────

app.get('/', function (_req, res) {
  res.json({
    name: 'CAPUNGA TRADE',
    symbol: SYMBOL,
    status: 'online',
    version: '2.0',
    gale: MAX_GALE,
    mode: 'opportunity-based',
    disclaimer: 'Experimental. Not financial advice.'
  });
});

app.get('/health', async function (_req, res) {
  try {
    await pool.query('SELECT 1');
    res.json({
      ok: true,
      db: 'up',
      lastAnalysis: (lastAnalysis && lastAnalysis.ts) || null,
      lastError: lastError,
      signalsSent: signalsSent,
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
      ok: true,
      symbol: SYMBOL,
      config: { minScore: MIN_CONFLUENCIA, gale: MAX_GALE, tpPct: TP_PCT, slPct: SL_PCT },
      stats: Object.assign({}, t, { settled: settled, winrate: winrate }),
      pending: pend ? {
        id: pend.id, side: pend.direcao, entry: pend.preco_entrada,
        gale: pend.gale, at: pend.criado_em
      } : null,
      lastAnalysis: lastAnalysis,
      lastError: lastError
    });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

app.get('/sinais', async function (req, res) {
  try {
    const limit = Math.min(50, Math.max(1, Number(req.query.limit) || 20));
    const { rows } = await pool.query(
      `SELECT id, symbol, direcao, preco_entrada, score, estado, gale,
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

// ── Boot ────────────────────────────────────────────────────────────────────

async function main() {
  await initDB();
  const server = app.listen(PORT, '0.0.0.0', function () {
    console.log('[HTTP] CAPUNGA TRADE v2 · :' + PORT + ' · ' + SYMBOL);
  });

  await tick();
  loopTimer = setInterval(function () {
    tick().catch(function (e) { console.error(e.message); });
  }, LOOP_MS);

  function stop() {
    if (loopTimer) clearInterval(loopTimer);
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
