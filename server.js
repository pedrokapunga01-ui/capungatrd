'use strict';

/**
 * ═══════════════════════════════════════════════════════════════
 *  CAPUNGA TRADE — BTCUSDT Signal Engine (Bybit / Binance style)
 * ═══════════════════════════════════════════════════════════════
 *
 * Sinais de COMPRA 🟢 / VENDA 🔴 a cada ~4 minutos (mínimo).
 * Gale máximo: 1
 * Lógica: confluência multi-indicador (EMA + RSI + MACD + Volume + ATR + tendência)
 *
 * Variáveis obrigatórias (Secrets / Env):
 *   DATABASE_URL
 *   BOT_TOKEN
 *   CHAT_ID
 *
 * Opcionais:
 *   PORT                  (default 3000)
 *   SYMBOL                (default BTCUSDT)
 *   INTERVALO_ANALISE_MS  (default 60000 = 1 min)
 *   MIN_INTERVAL_SINAL_MS (default 240000 = 4 min)
 *   SINAL_EXPIRA_MS       (default 600000 = 10 min)
 *   MIN_CONFLUENCIA       (default 72)  // 0-100
 *   DATABASE_SSL          (default true)
 *
 * Aviso legal: Sinais experimentais. Não constituem aconselhamento financeiro.
 * Trading envolve risco elevado de perda de capital. Use por sua conta e risco.
 */

const express = require('express');
const cors = require('cors');
const axios = require('axios');
const { Pool } = require('pg');

const app = express();
app.use(cors());
app.use(express.json({ limit: '512kb' }));

// ─── Config ────────────────────────────────────────────────────────────────

const PORT = Number(process.env.PORT || 3000);
const DATABASE_URL = process.env.DATABASE_URL;
const BOT_TOKEN = (process.env.BOT_TOKEN || '').trim();
const CHAT_ID = (process.env.CHAT_ID || '').trim();

const SYMBOL = (process.env.SYMBOL || 'BTCUSDT').toUpperCase();
const INTERVALO_ANALISE_MS = Math.max(30000, Number(process.env.INTERVALO_ANALISE_MS || 60000));
const MIN_INTERVAL_SINAL_MS = Math.max(120000, Number(process.env.MIN_INTERVAL_SINAL_MS || 240000)); // 4 min
const SINAL_EXPIRA_MS = Math.max(180000, Number(process.env.SINAL_EXPIRA_MS || 600000));
const MIN_CONFLUENCIA = Math.min(95, Math.max(50, Number(process.env.MIN_CONFLUENCIA || 72)));
const MAX_GALE = 1;
const HORIZONTE_VELAS = 4; // avalia resultado nas próximas ~4 velas de 1m

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

// ─── Runtime state ─────────────────────────────────────────────────────────

let analisando = false;
let ultimoSinalTs = 0;
let ultimaAnalise = null;
let lastError = null;
let totalSinais = 0;
let timerAnalise = null;

// ─── Utils ─────────────────────────────────────────────────────────────────

function agoraISO() {
  return new Date().toISOString();
}

function sleep(ms) {
  return new Promise(r => setTimeout(r, ms));
}

function clamp(n, min, max) {
  return Math.max(min, Math.min(max, n));
}

function num(v, fb = 0) {
  const n = Number(v);
  return Number.isFinite(n) ? n : fb;
}

// ─── Telegram (estilo SMS Capunga Trade) ───────────────────────────────────

async function telegram(texto) {
  if (!BOT_TOKEN || !CHAT_ID) {
    console.warn('[TG] BOT_TOKEN ou CHAT_ID ausente');
    return false;
  }
  try {
    const { data } = await axios.post(
      `https://api.telegram.org/bot${BOT_TOKEN}/sendMessage`,
      {
        chat_id: CHAT_ID,
        text: texto,
        disable_web_page_preview: true,
        parse_mode: 'HTML'
      },
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
    CREATE TABLE IF NOT EXISTS trade_candles (
      id BIGSERIAL PRIMARY KEY,
      symbol TEXT NOT NULL,
      interval TEXT NOT NULL,
      open_time TIMESTAMPTZ NOT NULL,
      open NUMERIC NOT NULL,
      high NUMERIC NOT NULL,
      low NUMERIC NOT NULL,
      close NUMERIC NOT NULL,
      volume NUMERIC NOT NULL,
      UNIQUE(symbol, interval, open_time)
    );
  `);
  await pool.query(`CREATE INDEX IF NOT EXISTS idx_candles_time ON trade_candles (symbol, interval, open_time DESC);`);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS trade_sinais (
      id BIGSERIAL PRIMARY KEY,
      symbol TEXT NOT NULL,
      direcao TEXT NOT NULL CHECK (direcao IN ('COMPRA', 'VENDA')),
      preco_entrada NUMERIC NOT NULL,
      confluencia INTEGER NOT NULL,
      indicadores JSONB,
      estado TEXT NOT NULL DEFAULT 'PENDENTE'
        CHECK (estado IN ('PENDENTE', 'GREEN', 'G1', 'LOSS', 'CANCELADO', 'EXPIRADO')),
      gale SMALLINT NOT NULL DEFAULT 0 CHECK (gale BETWEEN 0 AND 1),
      preco_saida NUMERIC,
      pnl_pct NUMERIC,
      criado_em TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      resolvido_em TIMESTAMPTZ,
      mensagem_entrada TEXT
    );
  `);
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

  console.log('[DB] Tabelas Capunga Trade verificadas.');
}

// ─── Market data (multi-fonte para evitar bloqueio 451 na Render) ───────────

function mapIntervalOkx(interval) {
  if (interval === '1m') return '1m';
  if (interval === '5m') return '5m';
  if (interval === '15m') return '15m';
  return '1m';
}

function mapIntervalKucoin(interval) {
  if (interval === '1m') return '1min';
  if (interval === '5m') return '5min';
  if (interval === '15m') return '15min';
  return '1min';
}

function mapIntervalBybit(interval) {
  if (interval === '1m') return '1';
  if (interval === '5m') return '5';
  if (interval === '15m') return '15';
  return '1';
}

async function fetchKlines(interval = '1m', limit = 120) {
  const headers = { 'User-Agent': 'CapungaTrade/1.1', Accept: 'application/json' };
  const errors = [];

  // 1) OKX (geralmente acessível a partir da Render)
  try {
    const instId = SYMBOL.replace('USDT', '-USDT'); // BTCUSDT → BTC-USDT
    const bar = mapIntervalOkx(interval);
    const url = `https://www.okx.com/api/v5/market/candles?instId=${instId}&bar=${bar}&limit=${limit}`;
    const { data } = await axios.get(url, { timeout: 10000, headers });
    if (data?.code === '0' && Array.isArray(data.data) && data.data.length) {
      // OKX devolve [ts, o, h, l, c, vol, volCcy, volCcyQuote, confirm] — mais recente primeiro
      return data.data.slice().reverse().map(k => ({
        openTime: Number(k[0]),
        open: num(k[1]),
        high: num(k[2]),
        low: num(k[3]),
        close: num(k[4]),
        volume: num(k[5]),
        closeTime: Number(k[0]) + 60000
      }));
    }
    errors.push('okx: empty');
  } catch (e) {
    errors.push(`okx: ${e.message}`);
  }

  // 2) KuCoin
  try {
    const sym = SYMBOL.replace('USDT', '-USDT');
    const type = mapIntervalKucoin(interval);
    const url = `https://api.kucoin.com/api/v1/market/candles?type=${type}&symbol=${sym}`;
    const { data } = await axios.get(url, { timeout: 10000, headers });
    if (data?.code === '200000' && Array.isArray(data.data) && data.data.length) {
      // KuCoin: [time, open, close, high, low, volume, turnover] — mais recente primeiro
      const rows = data.data.slice(0, limit).reverse();
      return rows.map(k => ({
        openTime: Number(k[0]) * (String(k[0]).length === 10 ? 1000 : 1),
        open: num(k[1]),
        high: num(k[3]),
        low: num(k[4]),
        close: num(k[2]),
        volume: num(k[5]),
        closeTime: Number(k[0]) * (String(k[0]).length === 10 ? 1000 : 1) + 60000
      }));
    }
    errors.push('kucoin: empty');
  } catch (e) {
    errors.push(`kucoin: ${e.message}`);
  }

  // 3) Bybit
  try {
    const bybitInterval = mapIntervalBybit(interval);
    const url = `https://api.bybit.com/v5/market/kline?category=linear&symbol=${SYMBOL}&interval=${bybitInterval}&limit=${limit}`;
    const { data } = await axios.get(url, { timeout: 10000, headers });
    const list = data?.result?.list || [];
    if (list.length) {
      return list.slice().reverse().map(k => ({
        openTime: Number(k[0]),
        open: num(k[1]),
        high: num(k[2]),
        low: num(k[3]),
        close: num(k[4]),
        volume: num(k[5]),
        closeTime: Number(k[0]) + 60000
      }));
    }
    errors.push('bybit: empty');
  } catch (e) {
    errors.push(`bybit: ${e.message}`);
  }

  // 4) Binance (último recurso)
  try {
    const url = `https://api.binance.com/api/v3/klines?symbol=${SYMBOL}&interval=${interval}&limit=${limit}`;
    const { data } = await axios.get(url, { timeout: 10000, headers });
    if (Array.isArray(data) && data.length) {
      return data.map(k => ({
        openTime: Number(k[0]),
        open: num(k[1]),
        high: num(k[2]),
        low: num(k[3]),
        close: num(k[4]),
        volume: num(k[5]),
        closeTime: Number(k[6])
      }));
    }
    errors.push('binance: empty');
  } catch (e) {
    errors.push(`binance: ${e.message}`);
  }

  throw new Error(`market_data: todas as fontes falharam → ${errors.join(' | ')}`);
}

// ─── Indicadores técnicos ──────────────────────────────────────────────────

function ema(values, period) {
  if (values.length < period) return [];
  const k = 2 / (period + 1);
  const out = [];
  let prev = values.slice(0, period).reduce((a, b) => a + b, 0) / period;
  out.push(prev);
  for (let i = period; i < values.length; i++) {
    prev = values[i] * k + prev * (1 - k);
    out.push(prev);
  }
  return out;
}

function sma(values, period) {
  if (values.length < period) return [];
  const out = [];
  let sum = 0;
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
  for (let i = 0; i < period; i++) {
    if (changes[i] >= 0) avgGain += changes[i];
    else avgLoss -= changes[i];
  }
  avgGain /= period;
  avgLoss /= period;
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
  const offset = slow - fast;
  const macdLine = [];
  for (let i = 0; i < emaSlow.length; i++) {
    macdLine.push(emaFast[i + offset] - emaSlow[i]);
  }
  const signalLine = ema(macdLine, signal);
  const hist = [];
  const sigOffset = macdLine.length - signalLine.length;
  for (let i = 0; i < signalLine.length; i++) {
    hist.push(macdLine[i + sigOffset] - signalLine[i]);
  }
  return { macdLine, signalLine, hist };
}

function atr(highs, lows, closes, period = 14) {
  if (highs.length < period + 1) return [];
  const trs = [];
  for (let i = 1; i < highs.length; i++) {
    const tr = Math.max(
      highs[i] - lows[i],
      Math.abs(highs[i] - closes[i - 1]),
      Math.abs(lows[i] - closes[i - 1])
    );
    trs.push(tr);
  }
  return sma(trs, period);
}

// ─── Motor de confluência (lógica avançada) ────────────────────────────────

function analisarConfluencia(candles1m, candles5m) {
  if (!candles1m || candles1m.length < 80) {
    return { ok: false, motivo: 'amostra_1m_insuficiente' };
  }
  if (!candles5m || candles5m.length < 40) {
    return { ok: false, motivo: 'amostra_5m_insuficiente' };
  }

  const c1 = candles1m.map(c => c.close);
  const h1 = candles1m.map(c => c.high);
  const l1 = candles1m.map(c => c.low);
  const v1 = candles1m.map(c => c.volume);
  const c5 = candles5m.map(c => c.close);

  const ema9 = ema(c1, 9);
  const ema21 = ema(c1, 21);
  const ema50 = ema(c1, 50);
  const rsi14 = rsi(c1, 14);
  const { macdLine, signalLine, hist } = macd(c1);
  const atr14 = atr(h1, l1, c1, 14);
  const volSma = sma(v1, 20);

  if (!ema9.length || !ema21.length || !ema50.length || !rsi14.length || !hist.length || !atr14.length || !volSma.length) {
    return { ok: false, motivo: 'indicadores_insuficientes' };
  }

  const i = c1.length - 1;
  const price = c1[i];
  const e9 = ema9[ema9.length - 1];
  const e21 = ema21[ema21.length - 1];
  const e50 = ema50[ema50.length - 1];
  const r = rsi14[rsi14.length - 1];
  const h = hist[hist.length - 1];
  const hPrev = hist.length > 1 ? hist[hist.length - 2] : 0;
  const a = atr14[atr14.length - 1];
  const vol = v1[i];
  const volMed = volSma[volSma.length - 1];

  // Tendência 5m
  const ema9_5 = ema(c5, 9);
  const ema21_5 = ema(c5, 21);
  const trend5 = ema9_5.length && ema21_5.length
    ? (ema9_5[ema9_5.length - 1] > ema21_5[ema21_5.length - 1] ? 'ALTA' : 'BAIXA')
    : 'NEUTRO';

  // Score de confluência 0-100
  let scoreCompra = 0;
  let scoreVenda = 0;
  const fatores = [];

  // 1. Alinhamento de EMAs (peso alto)
  if (e9 > e21 && e21 > e50) {
    scoreCompra += 22;
    fatores.push('EMA alinhada ALTA');
  } else if (e9 < e21 && e21 < e50) {
    scoreVenda += 22;
    fatores.push('EMA alinhada BAIXA');
  } else if (e9 > e21) {
    scoreCompra += 10;
    fatores.push('EMA9>EMA21');
  } else {
    scoreVenda += 10;
    fatores.push('EMA9<EMA21');
  }

  // 2. RSI
  if (r < 32) {
    scoreCompra += 18;
    fatores.push(`RSI oversold ${r.toFixed(1)}`);
  } else if (r > 68) {
    scoreVenda += 18;
    fatores.push(`RSI overbought ${r.toFixed(1)}`);
  } else if (r < 45) {
    scoreCompra += 8;
  } else if (r > 55) {
    scoreVenda += 8;
  }

  // 3. MACD histograma (momentum)
  if (h > 0 && h > hPrev) {
    scoreCompra += 16;
    fatores.push('MACD hist expansivo +');
  } else if (h < 0 && h < hPrev) {
    scoreVenda += 16;
    fatores.push('MACD hist expansivo -');
  } else if (h > 0) {
    scoreCompra += 7;
  } else {
    scoreVenda += 7;
  }

  // 4. Volume acima da média
  if (vol > volMed * 1.35) {
    if (price > candles1m[i - 1].close) {
      scoreCompra += 12;
      fatores.push('Volume alto + candle verde');
    } else {
      scoreVenda += 12;
      fatores.push('Volume alto + candle vermelho');
    }
  }

  // 5. Tendência 5m (filtro de regime)
  if (trend5 === 'ALTA') {
    scoreCompra += 14;
    fatores.push('Tendência 5m ALTA');
  } else if (trend5 === 'BAIXA') {
    scoreVenda += 14;
    fatores.push('Tendência 5m BAIXA');
  }

  // 6. Volatilidade (ATR) — evita mercados mortos
  const atrPct = (a / price) * 100;
  if (atrPct < 0.04) {
    // mercado muito calmo → reduz confiança
    scoreCompra = Math.floor(scoreCompra * 0.7);
    scoreVenda = Math.floor(scoreVenda * 0.7);
    fatores.push('ATR baixo (mercado calmo)');
  }

  // 7. Preço vs EMA50 (posição estrutural)
  if (price > e50) scoreCompra += 8;
  else scoreVenda += 8;

  scoreCompra = clamp(scoreCompra, 0, 100);
  scoreVenda = clamp(scoreVenda, 0, 100);

  let direcao = null;
  let score = 0;
  if (scoreCompra >= MIN_CONFLUENCIA && scoreCompra > scoreVenda + 8) {
    direcao = 'COMPRA';
    score = scoreCompra;
  } else if (scoreVenda >= MIN_CONFLUENCIA && scoreVenda > scoreCompra + 8) {
    direcao = 'VENDA';
    score = scoreVenda;
  }

  return {
    ok: Boolean(direcao),
    direcao,
    score,
    price,
    atr: a,
    atrPct: Number(atrPct.toFixed(4)),
    rsi: Number(r.toFixed(2)),
    trend5,
    fatores,
    ema: { e9: Number(e9.toFixed(2)), e21: Number(e21.toFixed(2)), e50: Number(e50.toFixed(2)) },
    motivo: direcao ? 'confluencia_ok' : 'sem_confluencia_forte'
  };
}

// ─── Gestão de sinais ──────────────────────────────────────────────────────

async function obterSinalPendente() {
  const { rows } = await pool.query(`
    SELECT * FROM trade_sinais
    WHERE estado = 'PENDENTE'
    ORDER BY id ASC
    LIMIT 1
  `);
  return rows[0] || null;
}

async function logEvento(sinalId, tipo, detalhe, preco = null) {
  await pool.query(
    `INSERT INTO trade_eventos (sinal_id, tipo, detalhe, preco) VALUES ($1,$2,$3,$4)`,
    [sinalId, tipo, detalhe, preco]
  );
}

async function expirarSinais() {
  const { rows } = await pool.query(
    `UPDATE trade_sinais
     SET estado = 'EXPIRADO', resolvido_em = NOW()
     WHERE estado = 'PENDENTE'
       AND criado_em < NOW() - ($1::text || ' milliseconds')::interval
     RETURNING id, direcao, preco_entrada`,
    [String(SINAL_EXPIRA_MS)]
  );
  for (const s of rows) {
    await logEvento(s.id, 'EXPIRADO', 'timeout sem resolução');
    await telegram(
      `⏱ <b>CAPUNGA TRADE</b> · SINAL EXPIRADO\n` +
      `━━━━━━━━━━━━━━━━\n` +
      `ID #${s.id} · ${s.direcao}\n` +
      `Entrada: ${Number(s.preco_entrada).toFixed(2)}\n` +
      `Não contado como Green/Loss.`
    );
  }
}

async function resolverSinalComPreco(precoAtual) {
  const sinal = await obterSinalPendente();
  if (!sinal) return;

  const entrada = Number(sinal.preco_entrada);
  const direcao = sinal.direcao;
  const gale = Number(sinal.gale);

  // Critério simples de resultado (horizonte curto)
  // COMPRA ganha se preço subiu > 0.08% ; VENDA se desceu > 0.08%
  // Caso contrário e já passou tempo suficiente → gale ou loss
  const pct = ((precoAtual - entrada) / entrada) * 100;
  const threshold = 0.08; // 0.08%

  let ganhou = false;
  if (direcao === 'COMPRA' && pct >= threshold) ganhou = true;
  if (direcao === 'VENDA' && pct <= -threshold) ganhou = true;

  const perdeuClaro = (direcao === 'COMPRA' && pct <= -threshold) ||
                      (direcao === 'VENDA' && pct >= threshold);

  if (ganhou) {
    const estado = gale === 0 ? 'GREEN' : 'G1';
    await pool.query(
      `UPDATE trade_sinais
       SET estado = $2, preco_saida = $3, pnl_pct = $4, resolvido_em = NOW()
       WHERE id = $1 AND estado = 'PENDENTE'`,
      [sinal.id, estado, precoAtual, Number(pct.toFixed(4))]
    );
    await logEvento(sinal.id, 'FECHO', estado, precoAtual);
    await telegram(
      `🟢 <b>GREEN ${estado === 'G1' ? 'G1' : ''}</b> · CAPUNGA TRADE\n` +
      `━━━━━━━━━━━━━━━━\n` +
      `ID #${sinal.id} · ${direcao}\n` +
      `Entrada: ${entrada.toFixed(2)}\n` +
      `Saída:   ${precoAtual.toFixed(2)}\n` +
      `PnL:     ${pct >= 0 ? '+' : ''}${pct.toFixed(3)}%\n` +
      `Gale:    ${gale}`
    );
    return;
  }

  if (perdeuClaro) {
    if (gale < MAX_GALE) {
      // Dispara Gale 1
      await pool.query(
        `UPDATE trade_sinais SET gale = 1 WHERE id = $1 AND estado = 'PENDENTE'`,
        [sinal.id]
      );
      await logEvento(sinal.id, 'GALE1', `preço ${precoAtual}`, precoAtual);
      await telegram(
        `🐔 <b>GALE 1</b> · CAPUNGA TRADE\n` +
        `━━━━━━━━━━━━━━━━\n` +
        `ID #${sinal.id}\n` +
        `Manter ${direcao}\n` +
        `Preço atual: ${precoAtual.toFixed(2)}\n` +
        `Entrada original: ${entrada.toFixed(2)}\n` +
        `Recuperação em curso...`
      );
      return;
    }

    // Loss final
    await pool.query(
      `UPDATE trade_sinais
       SET estado = 'LOSS', preco_saida = $2, pnl_pct = $3, resolvido_em = NOW()
       WHERE id = $1 AND estado = 'PENDENTE'`,
      [sinal.id, precoAtual, Number(pct.toFixed(4))]
    );
    await logEvento(sinal.id, 'LOSS', 'gale máximo', precoAtual);
    await telegram(
      `🔴 <b>LOSS</b> · CAPUNGA TRADE\n` +
      `━━━━━━━━━━━━━━━━\n` +
      `ID #${sinal.id} · ${direcao}\n` +
      `Entrada: ${entrada.toFixed(2)}\n` +
      `Saída:   ${precoAtual.toFixed(2)}\n` +
      `PnL:     ${pct.toFixed(3)}%\n` +
      `Gale:    ${gale}`
    );
  }
}

async function criarSinal(analise) {
  if (await obterSinalPendente()) return null;
  if (Date.now() - ultimoSinalTs < MIN_INTERVAL_SINAL_MS) return null;

  const msg =
    `📡 <b>CAPUNGA TRADE</b> · BTC\n` +
    `━━━━━━━━━━━━━━━━\n` +
    `${analise.direcao === 'COMPRA' ? '🟢 COMPRA' : '🔴 VENDA'}\n` +
    `Par: ${SYMBOL}\n` +
    `Preço: ${analise.price.toFixed(2)}\n` +
    `Confluência: ${analise.score}/100\n` +
    `RSI: ${analise.rsi} · 5m: ${analise.trend5}\n` +
    `Gale máx: ${MAX_GALE}\n` +
    `━━━━━━━━━━━━━━━━\n` +
    `Fatores:\n` +
    analise.fatores.slice(0, 5).map(f => `• ${f}`).join('\n') +
    `\n\n⚠️ Sinal experimental · não é conselho financeiro`;

  const { rows } = await pool.query(
    `INSERT INTO trade_sinais
       (symbol, direcao, preco_entrada, confluencia, indicadores, mensagem_entrada)
     VALUES ($1,$2,$3,$4,$5,$6)
     RETURNING id`,
    [
      SYMBOL,
      analise.direcao,
      analise.price,
      analise.score,
      JSON.stringify({
        rsi: analise.rsi,
        atrPct: analise.atrPct,
        trend5: analise.trend5,
        ema: analise.ema,
        fatores: analise.fatores
      }),
      msg
    ]
  );

  const id = rows[0]?.id;
  if (!id) return null;

  ultimoSinalTs = Date.now();
  totalSinais += 1;
  await logEvento(id, 'ENTRADA', analise.direcao, analise.price);
  await telegram(msg);
  console.log(`[SINAL] #${id} ${analise.direcao} @ ${analise.price} score=${analise.score}`);
  return id;
}

// ─── Ciclo principal ───────────────────────────────────────────────────────

async function ciclo() {
  if (analisando) return;
  analisando = true;
  try {
    await expirarSinais();

    const [c1m, c5m] = await Promise.all([
      fetchKlines('1m', 120),
      fetchKlines('5m', 60)
    ]);

    const preco = c1m[c1m.length - 1]?.close;
    if (preco) {
      await resolverSinalComPreco(preco);
    }

    const analise = analisarConfluencia(c1m, c5m);
    ultimaAnalise = {
      ...analise,
      ts: agoraISO(),
      preco
    };

    if (analise.ok) {
      await criarSinal(analise);
    } else {
      console.log(`[ANALISE] ${analise.motivo} | scoreC/V não atingiu ${MIN_CONFLUENCIA}`);
    }

    lastError = null;
  } catch (err) {
    lastError = err.message;
    console.error('[CICLO]', err.message);
  } finally {
    analisando = false;
  }
}

// ─── Rotas HTTP ────────────────────────────────────────────────────────────

app.get('/', (_req, res) => {
  res.json({
    nome: 'CAPUNGA TRADE',
    simbolo: SYMBOL,
    estado: 'online',
    gale_maximo: MAX_GALE,
    intervalo_min_sinal_min: MIN_INTERVAL_SINAL_MS / 60000,
    versao: '1.0.0',
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
        COUNT(*) FILTER (WHERE estado = 'GREEN')::int AS green,
        COUNT(*) FILTER (WHERE estado = 'G1')::int AS g1,
        COUNT(*) FILTER (WHERE estado = 'LOSS')::int AS loss,
        COUNT(*) FILTER (WHERE estado = 'PENDENTE')::int AS pendente,
        COUNT(*) FILTER (WHERE estado IN ('CANCELADO','EXPIRADO'))::int AS anulados
      FROM trade_sinais
    `);
    const t = totais[0] || {};
    const settled = (t.green || 0) + (t.g1 || 0) + (t.loss || 0);
    const wins = (t.green || 0) + (t.g1 || 0);
    const winrate = settled ? Number(((wins * 100) / settled).toFixed(2)) : null;

    const pendente = await obterSinalPendente();

    res.json({
      ok: true,
      simbolo: SYMBOL,
      criterios: {
        min_confluencia: MIN_CONFLUENCIA,
        intervalo_min_sinal_ms: MIN_INTERVAL_SINAL_MS,
        gale_maximo: MAX_GALE
      },
      estatisticas: { ...t, settled, winrate },
      sinal_pendente: pendente ? {
        id: pendente.id,
        direcao: pendente.direcao,
        preco: pendente.preco_entrada,
        gale: pendente.gale,
        criado_em: pendente.criado_em
      } : null,
      ultima_analise: ultimaAnalise,
      lastError
    });
  } catch (err) {
    res.status(500).json({ ok: false, erro: err.message });
  }
});

app.get('/sinais', async (req, res) => {
  try {
    const limit = Math.min(100, Math.max(1, Number(req.query.limit) || 30));
    const { rows } = await pool.query(
      `SELECT id, symbol, direcao, preco_entrada, confluencia, estado, gale,
              preco_saida, pnl_pct, criado_em, resolvido_em
       FROM trade_sinais
       ORDER BY id DESC
       LIMIT $1`,
      [limit]
    );
    res.json({ ok: true, items: rows });
  } catch (err) {
    res.status(500).json({ ok: false, erro: err.message });
  }
});

app.post('/analisar', async (_req, res) => {
  try {
    await ciclo();
    res.json({ ok: true, ultimaAnalise });
  } catch (err) {
    res.status(500).json({ ok: false, erro: err.message });
  }
});

// ─── Boot ──────────────────────────────────────────────────────────────────

async function iniciar() {
  await criarTabelas();

  const server = app.listen(PORT, '0.0.0.0', () => {
    console.log(`[HTTP] CAPUNGA TRADE online · porta ${PORT} · ${SYMBOL}`);
  });

  // Primeira análise imediata + loop
  await ciclo();
  timerAnalise = setInterval(() => {
    ciclo().catch(e => console.error('[TIMER]', e.message));
  }, INTERVALO_ANALISE_MS);

  let shutting = false;
  async function shutdown() {
    if (shutting) return;
    shutting = true;
    console.log('[SHUTDOWN]');
    if (timerAnalise) clearInterval(timerAnalise);
    server.close(async () => {
      try { await pool.end(); } finally { process.exit(0); }
    });
    setTimeout(() => process.exit(1), 8000).unref();
  }
  process.on('SIGTERM', shutdown);
  process.on('SIGINT', shutdown);
}

iniciar().catch(err => {
  console.error('[FATAL]', err);
  process.exit(1);
});
