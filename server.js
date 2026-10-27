'use strict';
require('dotenv').config();
const express = require('express');
const cors = require('cors');
const axios = require('axios');
const { Pool } = require('pg');

/* ==================== PROTEÇÃO CONTRA CRASHES ==================== */
process.on('unhandledRejection', (e) => {
  console.error('[unhandledRejection]', e?.message || e);
});
process.on('uncaughtException', (e) => {
  console.error('[uncaughtException]', e?.message || e);
});

const app = express();
app.disable('x-powered-by');
app.use(cors());
app.use(express.json({ limit: '64kb' }));

/* ============================ UTIL ============================ */
function numEnv(name, fallback, min, max) {
  const n = Number(process.env[name] ?? fallback);
  return Number.isFinite(n) ? Math.min(max, Math.max(min, n)) : fallback;
}
function boolEnv(name, def = false) {
  return String(process.env[name] ?? def).toLowerCase() === 'true';
}
function round(n, places = 2) {
  const p = 10 ** places;
  return Math.round((Number(n) + Number.EPSILON) * p) / p;
}
function errText(e) {
  return String(e?.message || e || 'Unknown error').slice(0, 400);
}

/* ============================ CONFIG ============================ */
const CFG = Object.freeze({
  port: numEnv('PORT', 3000, 1, 65535),
  apiKey: process.env.API_KEY || '',
  webhookSecret: process.env.WEBHOOK_SECRET || '',

  botToken: process.env.BOT_TOKEN || '',
  chatId: process.env.CHAT_ID || '',
  telegramPollMs: numEnv('TELEGRAM_POLL_MS', 4000, 1500, 30000),
  allowTelegramCommands: boolEnv('TELEGRAM_COMMANDS', true),

  keepaliveUrl: process.env.KEEPALIVE_URL || '',
  databaseUrl: process.env.DATABASE_URL || '',
  databaseSsl: boolEnv('DATABASE_SSL', true),

  minConfidence: numEnv('MIN_CONFIDENCE', 65, 50, 99),
  minSamples: numEnv('MIN_SAMPLES', 15, 5, 100),
  signalExpirySec: numEnv('SIGNAL_EXPIRY_SEC', 120, 20, 600),
  antiSpamSec: numEnv('ANTI_SPAM_SEC', 15, 5, 300),
  historyLimit: numEnv('HISTORY_LIMIT', 500, 50, 5000),
  martingaleMax: numEnv('MARTINGALE_MAX_LEVEL', 4, 0, 10),
  baseUnit: numEnv('BASE_UNIT', 1, 0.1, 1000),
  allowDrawSignals: boolEnv('ALLOW_DRAW_SIGNALS', false),

  strategies: {
    streak: boolEnv('STRAT_STREAK', true),
    trend: boolEnv('STRAT_TREND', true),
    pattern: boolEnv('STRAT_PATTERN', true),
    momentum: boolEnv('STRAT_MOMENTUM', true)
  },

  // Fonte única opcional. Se vazia, o servidor opera só com webhook.
  genericApiUrl: process.env.GENERIC_API_URL || '',
  genericApiKey: process.env.GENERIC_API_KEY || '',
  sourcePollMs: numEnv('SOURCE_POLL_MS', 20000, 10000, 120000)
});

/* ============================ DB ============================ */
const pool = CFG.databaseUrl
  ? new Pool({
      connectionString: CFG.databaseUrl,
      ssl: CFG.databaseSsl ? { rejectUnauthorized: false } : false,
      max: 6,
      idleTimeoutMillis: 30000,
      connectionTimeoutMillis: 10000
    })
  : null;

/* ============================ STATE ============================ */
let server;
let keepaliveTimer, telegramTimer, sourceTimer;
let shuttingDown = false;
let scanning = false;
let polling = false;
let lastError = null;
let telegramOffset = 0;
const sourceHealth = {};

/* ============================ LOG ============================ */
async function logEvent(tipo, mensagem, dados = {}) {
  if (!pool) return;
  try {
    await pool.query(
      'INSERT INTO football_eventos(tipo,mensagem,dados) VALUES($1,$2,$3)',
      [tipo, mensagem, JSON.stringify(dados)]
    );
  } catch (e) {
    console.error('[DB event]', errText(e));
  }
}

/* ============================ DB INIT ============================ */
async function initDB() {
  if (!pool) {
    throw new Error(
      'DATABASE_URL não configurado. No Render: crie um PostgreSQL e anexe ao serviço, ' +
      'ou defina DATABASE_URL manualmente nas variáveis de ambiente.'
    );
  }
  await pool.query(`
    CREATE TABLE IF NOT EXISTS football_resultados (
      id BIGSERIAL PRIMARY KEY,
      round_id TEXT,
      home_card TEXT NOT NULL,
      away_card TEXT NOT NULL,
      resultado TEXT NOT NULL,
      hash TEXT,
      origem TEXT NOT NULL DEFAULT 'webhook',
      criado_em TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    CREATE UNIQUE INDEX IF NOT EXISTS football_resultados_round_idx
      ON football_resultados(round_id) WHERE round_id IS NOT NULL;
    CREATE INDEX IF NOT EXISTS football_resultados_created_idx
      ON football_resultados(criado_em DESC);

    CREATE TABLE IF NOT EXISTS football_sinais (
      id BIGSERIAL PRIMARY KEY,
      direcao TEXT NOT NULL,
      confianca INTEGER NOT NULL,
      estado TEXT NOT NULL DEFAULT 'PENDENTE',
      martingale_nivel INTEGER NOT NULL DEFAULT 0,
      stake_sugerida NUMERIC NOT NULL,
      estrategias TEXT[] NOT NULL DEFAULT '{}',
      resultado_id BIGINT,
      resultado_final TEXT,
      criado_em TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      resolvido_em TIMESTAMPTZ,
      expiracao_em TIMESTAMPTZ NOT NULL,
      metadados JSONB NOT NULL DEFAULT '{}'::jsonb
    );
    CREATE INDEX IF NOT EXISTS football_sinais_estado_idx
      ON football_sinais(estado, criado_em DESC);
    CREATE INDEX IF NOT EXISTS football_sinais_criado_idx
      ON football_sinais(criado_em DESC);

    CREATE TABLE IF NOT EXISTS football_eventos (
      id BIGSERIAL PRIMARY KEY,
      tipo TEXT NOT NULL,
      mensagem TEXT NOT NULL,
      dados JSONB NOT NULL DEFAULT '{}'::jsonb,
      criado_em TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    CREATE INDEX IF NOT EXISTS football_eventos_created_idx
      ON football_eventos(criado_em DESC);

    CREATE TABLE IF NOT EXISTS football_config (
      chave TEXT PRIMARY KEY,
      valor TEXT NOT NULL,
      atualizado_em TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);
  await pool.query(`
    INSERT INTO football_config(chave,valor)
    VALUES('martingale_nivel','0')
    ON CONFLICT(chave) DO NOTHING
  `);
}

/* ============================ MARTINGALE ============================ */
async function getMartingaleLevel() {
  const { rows } = await pool.query(
    "SELECT valor FROM football_config WHERE chave='martingale_nivel'"
  );
  return Number(rows[0]?.valor || 0);
}
async function setMartingaleLevel(level) {
  await pool.query(
    `INSERT INTO football_config(chave,valor,atualizado_em)
     VALUES('martingale_nivel',$1,NOW())
     ON CONFLICT(chave) DO UPDATE SET valor=EXCLUDED.valor, atualizado_em=NOW()`,
    [String(level)]
  );
}
function stakeForLevel(level) {
  return round(CFG.baseUnit * Math.pow(2, level), 2);
}

/* ============================ TELEGRAM ============================ */
async function telegramSend(text) {
  if (!CFG.botToken || !CFG.chatId) return { skipped: true };
  try {
    const { data } = await axios.post(
      `https://api.telegram.org/bot${CFG.botToken}/sendMessage`,
      { chat_id: CFG.chatId, text, disable_web_page_preview: true },
      { timeout: 12000 }
    );
    if (!data?.ok) throw new Error(data?.description || 'Telegram rejected');
    return { sent: true };
  } catch (e) {
    await logEvent('telegram_error', errText(e));
    console.error('[TG]', errText(e));
    return { sent: false, error: errText(e) };
  }
}

/* ============================ ANÁLISE ============================ */
function streakSignal(history) {
  if (history.length < 3) return null;
  const last = history[0].resultado;
  if (last === 'DRAW') return null;
  let streak = 0;
  for (const h of history) {
    if (h.resultado === last) streak++;
    else break;
  }
  if (streak >= 5) {
    const dir = last === 'HOME' ? 'AWAY' : 'HOME';
    return {
      direction: dir,
      confidence: Math.min(85, 60 + (streak - 5) * 6),
      strategy: 'reversal',
      detail: `Reversão após ${streak}× ${last}`
    };
  }
  if (streak >= 3) {
    return {
      direction: last,
      confidence: Math.min(78, 55 + (streak - 3) * 6),
      strategy: 'streak',
      detail: `${streak}× ${last} consecutivos`
    };
  }
  return null;
}

function trendSignal(history) {
  if (history.length < 10) return null;
  const recent = history.slice(0, 20);
  const home = recent.filter(h => h.resultado === 'HOME').length;
  const away = recent.filter(h => h.resultado === 'AWAY').length;
  const total = home + away;
  if (total < 10) return null;
  const bias = Math.max(home, away) / total;
  if (bias < 0.63) return null;
  const dir = home > away ? 'HOME' : 'AWAY';
  return {
    direction: dir,
    confidence: Math.min(80, 55 + (bias - 0.63) * 100),
    strategy: 'trend',
    detail: `${round(bias * 100, 1)}% ${dir} em ${total} rodadas`
  };
}

function patternSignal(history) {
  if (history.length < 40) return null;
  const patLen = 3;
  const current = history.slice(0, patLen).map(h => h.resultado).join('|');
  const after = { HOME: 0, AWAY: 0, DRAW: 0 };
  for (let i = patLen + 1; i < history.length - patLen; i++) {
    const seq = history.slice(i, i + patLen).map(h => h.resultado).join('|');
    if (seq === current) after[history[i - 1].resultado]++;
  }
  const total = after.HOME + after.AWAY + after.DRAW;
  if (total < 6) return null;
  let best = 'HOME';
  for (const d of ['AWAY', 'DRAW']) if (after[d] > after[best]) best = d;
  const rate = after[best] / total;
  if (rate < 0.55) return null;
  if (best === 'DRAW' && !CFG.allowDrawSignals) return null;
  return {
    direction: best,
    confidence: Math.min(82, 50 + rate * 45),
    strategy: 'pattern',
    detail: `Padrão ${current.replace(/\|/g, '-')} → ${best}`
  };
}

function momentumSignal(history) {
  if (history.length < 10) return null;
  const last5 = history.slice(0, 5);
  const prev5 = history.slice(5, 10);
  const lh = last5.filter(h => h.resultado === 'HOME').length;
  const la = last5.filter(h => h.resultado === 'AWAY').length;
  const ph = prev5.filter(h => h.resultado === 'HOME').length;
  const pa = prev5.filter(h => h.resultado === 'AWAY').length;
  if (lh - ph >= 2) {
    return {
      direction: 'HOME',
      confidence: Math.min(76, 55 + (lh - ph) * 5),
      strategy: 'momentum',
      detail: `Momentum HOME ↑ (${lh} vs ${ph})`
    };
  }
  if (la - pa >= 2) {
    return {
      direction: 'AWAY',
      confidence: Math.min(76, 55 + (la - pa) * 5),
      strategy: 'momentum',
      detail: `Momentum AWAY ↑ (${la} vs ${pa})`
    };
  }
  return null;
}

function combineSignals(history) {
  const raw = [];
  if (CFG.strategies.streak)   { const s = streakSignal(history);   if (s) raw.push(s); }
  if (CFG.strategies.trend)    { const s = trendSignal(history);    if (s) raw.push(s); }
  if (CFG.strategies.pattern)  { const s = patternSignal(history);  if (s) raw.push(s); }
  if (CFG.strategies.momentum) { const s = momentumSignal(history); if (s) raw.push(s); }

  if (!raw.length) {
    return { direction: null, confidence: 0, reasons: ['Sem padrão detectado'], strategies: [] };
  }

  const groups = { HOME: [], AWAY: [], DRAW: [] };
  for (const s of raw) groups[s.direction].push(s);

  let best = null;
  for (const dir of ['HOME', 'AWAY', 'DRAW']) {
    const g = groups[dir];
    if (!g.length) continue;
    const baseAvg = g.reduce((a, s) => a + s.confidence, 0) / g.length;
    const agreementBoost = Math.max(0, g.length - 1) * 6;
    const finalConf = Math.min(95, baseAvg + agreementBoost);
    if (!best || finalConf > best.confidence) {
      best = {
        direction: dir,
        confidence: Math.round(finalConf),
        strategies: g.map(s => s.strategy),
        reasons: g.map(s => `${s.strategy}: ${s.detail}`)
      };
    }
  }
  return best;
}

/* ============================ SIGNAL FLOW ============================ */
async function pendingSignal(client = pool) {
  const { rows } = await client.query(
    "SELECT * FROM football_sinais WHERE estado='PENDENTE' ORDER BY id DESC LIMIT 1"
  );
  return rows[0] || null;
}

async function resolvePendingByResult(resultado) {
  const sig = await pendingSignal();
  if (!sig) return null;
  const isGreen = sig.direcao === resultado;
  const novoEstado = isGreen ? 'GREEN' : 'LOSS';

  const upd = await pool.query(
    `UPDATE football_sinais
     SET estado=$2, resultado_final=$3, resolvido_em=NOW()
     WHERE id=$1 AND estado='PENDENTE'
     RETURNING id, direcao, martingale_nivel, stake_sugerida, confianca`,
    [sig.id, novoEstado, resultado]
  );
  if (!upd.rowCount) return null;
  const s = upd.rows[0];

  let level = Number(s.martingale_nivel);
  if (isGreen) level = 0;
  else level = Math.min(CFG.martingaleMax + 1, level + 1);
  await setMartingaleLevel(level);

  await logEvent('signal_resolved', `Sinal ${novoEstado}`, {
    id: s.id, direcao: s.direcao, resultado, martingale: level
  });

  const icon = isGreen ? '🟢 GREEN' : '🔴 LOSS';
  const nextLevelMsg = isGreen
    ? 'Martingale resetado para nível 0.'
    : level > CFG.martingaleMax
      ? `⚠️ Martingale atingiu nível máximo (${CFG.martingaleMax}). Parar e reavaliar.`
      : `Próximo stake (nível ${level}): ${stakeForLevel(level)} un.`;

  await telegramSend(
    `${icon} — FOOTBALL STUDIO\n` +
    `Sinal #${s.id} · ${s.direcao}\n` +
    `Resultado: ${resultado}\n` +
    `Confiança: ${s.confianca}%\n` +
    `${nextLevelMsg}`
  );

  return { id: s.id, estado: novoEstado, resultado, level };
}

async function generateSignal(history) {
  if (history.length < CFG.minSamples) {
    return { created: false, reason: 'insufficient_history', samples: history.length };
  }
  const analysis = combineSignals(history);
  if (!analysis.direction) return { created: false, reason: 'no_pattern' };
  if (analysis.confidence < CFG.minConfidence) {
    return { created: false, reason: 'below_min_confidence', confidence: analysis.confidence };
  }
  if (analysis.direction === 'DRAW' && !CFG.allowDrawSignals) {
    return { created: false, reason: 'draw_disabled' };
  }

  const { rows: recent } = await pool.query(
    "SELECT criado_em FROM football_sinais ORDER BY id DESC LIMIT 1"
  );
  if (
    recent.rowCount &&
    Date.now() - new Date(recent.rows[0].criado_em).getTime() < CFG.antiSpamSec * 1000
  ) {
    return { created: false, reason: 'anti_spam' };
  }

  const client = await pool.connect();
  let newId;
  try {
    await client.query('BEGIN');
    await client.query("SELECT pg_advisory_xact_lock(hashtext('football_signal_engine'))");
    const pending = await pendingSignal(client);
    if (pending) {
      await client.query('COMMIT');
      return { created: false, reason: 'pending_exists', id: pending.id };
    }

    const level = await getMartingaleLevel();
    const stake = stakeForLevel(level);
    const expira = new Date(Date.now() + CFG.signalExpirySec * 1000);

    const ins = await client.query(
      `INSERT INTO football_sinais
       (direcao, confianca, estado, martingale_nivel, stake_sugerida, estrategias, expiracao_em, metadados)
       VALUES($1,$2,'PENDENTE',$3,$4,$5,$6,$7::jsonb)
       RETURNING id`,
      [
        analysis.direction,
        analysis.confidence,
        level,
        stake,
        analysis.strategies,
        expira,
        JSON.stringify({ reasons: analysis.reasons, history_size: history.length })
      ]
    );
    newId = ins.rows[0].id;
    await client.query('COMMIT');
  } catch (e) {
    try { await client.query('ROLLBACK'); } catch (_) {}
    throw e;
  } finally {
    client.release();
  }

  await logEvent('signal_opened', 'Novo sinal', {
    id: newId, direction: analysis.direction, confidence: analysis.confidence
  });

  const lvl = await getMartingaleLevel();
  const stake = stakeForLevel(lvl);
  const emoji = analysis.direction === 'HOME' ? '🔴' : analysis.direction === 'AWAY' ? '🔵' : '⚪';
  await telegramSend(
    `⚽ NOVO SINAL — FOOTBALL STUDIO\n\n` +
    `${emoji} DIREÇÃO: ${analysis.direction}\n` +
    `🎯 Confiança: ${analysis.confidence}%\n` +
    `💰 Stake sugerida: ${stake} un. (nível ${lvl})\n` +
    `🧠 Estratégias: ${analysis.strategies.join(', ')}\n` +
    `📊 ${analysis.reasons.join(' | ')}\n` +
    `⏱️ Expira em ${CFG.signalExpirySec}s\n` +
    `🆔 Sinal #${newId}`
  );

  return {
    created: true,
    id: newId,
    direction: analysis.direction,
    confidence: analysis.confidence,
    stake,
    level: lvl
  };
}

/* ============================ RESULT PIPELINE ============================ */
const CARD_RANK = {
  'A': 1, '2': 2, '3': 3, '4': 4, '5': 5, '6': 6, '7': 7,
  '8': 8, '9': 9, '10': 10, 'J': 11, 'Q': 12, 'K': 13
};
function cardRank(c) { return CARD_RANK[String(c).toUpperCase().trim()] ?? null; }
function computeResult(h, a) {
  const rh = cardRank(h), ra = cardRank(a);
  if (rh == null || ra == null) return null;
  return rh > ra ? 'HOME' : rh < ra ? 'AWAY' : 'DRAW';
}

async function processNewResult(payload) {
  const { roundId, homeCard, awayCard, resultado, hash, origem } = payload;

  let result = resultado;
  if (!result) result = computeResult(homeCard, awayCard);
  if (!['HOME', 'AWAY', 'DRAW'].includes(result)) {
    throw new Error('resultado inválido (HOME|AWAY|DRAW)');
  }
  if (!homeCard || !awayCard) throw new Error('homeCard e awayCard são obrigatórios');

  let inserted = null;
  try {
    const { rows } = await pool.query(
      `INSERT INTO football_resultados(round_id,home_card,away_card,resultado,hash,origem)
       VALUES($1,$2,$3,$4,$5,$6)
       ON CONFLICT (round_id) WHERE round_id IS NOT NULL DO NOTHING
       RETURNING *`,
      [roundId || null, String(homeCard), String(awayCard), result, hash || null, origem || 'webhook']
    );
    inserted = rows[0] || null;
  } catch (e) {
    throw new Error('Falha ao gravar resultado: ' + errText(e));
  }

  if (!inserted) return { duplicate: true, roundId };

  const resolution = await resolvePendingByResult(result);

  const { rows: hist } = await pool.query(
    `SELECT resultado FROM football_resultados ORDER BY id DESC LIMIT $1`,
    [CFG.historyLimit]
  );
  const nextSignal = await generateSignal(hist);

  await logEvent('result_processed', 'Novo resultado', {
    roundId, resultado: result, resolution, nextSignal
  });

  return { inserted, resolution, nextSignal };
}

/* ============================ GENERIC POLLING (opcional) ============================ */
async function fetchGeneric() {
  if (!CFG.genericApiUrl) throw new Error('GENERIC_API_URL não configurada');
  const headers = {
    'Accept': 'application/json',
    'User-Agent': 'Mozilla/5.0 (compatible; FootballSignals/1.0)'
  };
  if (CFG.genericApiKey) headers['Authorization'] = `Bearer ${CFG.genericApiKey}`;

  const { data } = await axios.get(CFG.genericApiUrl, { headers, timeout: 15000 });
  const list = Array.isArray(data) ? data : data?.results ?? data?.data ?? [];
  if (!Array.isArray(list) || !list.length) throw new Error('generic: sem rodadas válidas');
  return list;
}

async function pollSources() {
  if (shuttingDown || polling) return { skipped: true };
  if (!CFG.genericApiUrl) return { skipped: true, reason: 'no_source_configured' };

  polling = true;
  try {
    const rounds = await fetchGeneric();
    let newCount = 0;
    for (const r of rounds) {
      try {
        const out = await processNewResult({
          roundId: r.roundId ?? r.round_id ?? r.id ?? null,
          homeCard: r.homeCard ?? r.home_card ?? r.home,
          awayCard: r.awayCard ?? r.away_card ?? r.away,
          resultado: r.resultado ?? null,
          hash: r.hash ?? null,
          origem: 'generic'
        });
        if (out?.inserted) newCount++;
      } catch (_) { /* duplicado ou inválido */ }
    }
    sourceHealth.generic = {
      ok: true,
      lastOk: Date.now(),
      lastCount: rounds.length,
      lastNew: newCount
    };
    if (newCount > 0) {
      await logEvent('source_ok', `Generic: +${newCount} rodadas`, {
        total: rounds.length, new: newCount
      });
    }
    return { ok: true, total: rounds.length, newRounds: newCount };
  } catch (e) {
    sourceHealth.generic = { ok: false, lastError: errText(e) };
    await logEvent('source_fail', 'Generic falhou', { error: errText(e) });
    console.error('[poll:generic]', errText(e));
    return { ok: false, error: errText(e) };
  } finally {
    polling = false;
  }
}

function startSourcePolling() {
  if (!CFG.genericApiUrl) {
    console.log('[SOURCES] Nenhuma fonte configurada — operando apenas via webhook.');
    return;
  }
  if (sourceTimer) clearInterval(sourceTimer);
  pollSources().catch(() => {});
  sourceTimer = setInterval(() => pollSources().catch(() => {}), CFG.sourcePollMs);
  console.log(`[SOURCES] Polling Generic a cada ${CFG.sourcePollMs}ms`);
}

/* ============================ MIDDLEWARES ============================ */
function auth(req, res, next) {
  if (!CFG.apiKey) return res.status(503).json({ ok: false, error: 'API_KEY não configurada' });
  const key = req.get('x-api-key') || '';
  if (key.length !== CFG.apiKey.length || key !== CFG.apiKey) {
    return res.status(401).json({ ok: false, error: 'API key inválida' });
  }
  next();
}
function webhookAuth(req, res, next) {
  if (!CFG.webhookSecret) return next();
  const key = req.get('x-webhook-secret') || req.query.secret || req.body?.secret || '';
  if (key !== CFG.webhookSecret) {
    return res.status(401).json({ ok: false, error: 'Webhook secret inválido' });
  }
  next();
}
function asyncRoute(fn) {
  return (req, res, next) => Promise.resolve(fn(req, res, next)).catch(next);
}

/* ============================ ROUTES ============================ */
app.get('/', (_req, res) => res.json({
  name: 'Football Studio Signal Server',
  version: '2.0.0',
  mode: CFG.genericApiUrl ? 'polling + webhook' : 'webhook-only',
  strategies: Object.keys(CFG.strategies).filter(k => CFG.strategies[k])
}));

app.get('/health', asyncRoute(async (_req, res) => {
  let db = 'up';
  try { await pool.query('SELECT 1'); } catch { db = 'down'; }
  const ok = db === 'up' && !shuttingDown;
  res.status(ok ? 200 : 503).json({
    ok, db, uptime: Math.floor(process.uptime()), lastError, shuttingDown
  });
}));

app.get('/status', auth, asyncRoute(async (_req, res) => {
  const stats = await pool.query(`
    SELECT
      COUNT(*) FILTER (WHERE estado='GREEN')::int AS green,
      COUNT(*) FILTER (WHERE estado='LOSS')::int AS loss,
      COUNT(*) FILTER (WHERE estado='PENDENTE')::int AS pending,
      COUNT(*)::int AS total
    FROM football_sinais
  `);
  const totalResults = await pool.query('SELECT COUNT(*)::int AS n FROM football_resultados');
  const martingale = await getMartingaleLevel();
  const lastSig = await pool.query('SELECT * FROM football_sinais ORDER BY id DESC LIMIT 1');
  const lastRes = await pool.query('SELECT * FROM football_resultados ORDER BY id DESC LIMIT 10');

  const s = stats.rows[0];
  const settled = s.green + s.loss;
  res.json({
    ok: true,
    config: {
      minConfidence: CFG.minConfidence,
      minSamples: CFG.minSamples,
      signalExpirySec: CFG.signalExpirySec,
      martingaleMax: CFG.martingaleMax,
      baseUnit: CFG.baseUnit,
      strategies: CFG.strategies,
      sourceMode: CFG.genericApiUrl ? 'polling+webhook' : 'webhook-only'
    },
    stats: { ...s, settled, winrate: settled ? round((s.green / settled) * 100, 2) : null },
    martingale_level: martingale,
    next_stake: stakeForLevel(martingale),
    total_results: totalResults.rows[0].n,
    last_signal: lastSig.rows[0] || null,
    last_results: lastRes.rows
  });
}));

app.get('/resultados', asyncRoute(async (req, res) => {
  const limit = Math.min(200, Math.max(1, Number(req.query.limit) || 50));
  const { rows } = await pool.query(
    'SELECT * FROM football_resultados ORDER BY id DESC LIMIT $1', [limit]
  );
  res.json({ ok: true, count: rows.length, items: rows });
}));

app.get('/sinais', asyncRoute(async (req, res) => {
  const limit = Math.min(200, Math.max(1, Number(req.query.limit) || 30));
  const { rows } = await pool.query(
    'SELECT * FROM football_sinais ORDER BY id DESC LIMIT $1', [limit]
  );
  res.json({ ok: true, count: rows.length, items: rows });
}));

app.get('/placar', asyncRoute(async (_req, res) => {
  const { rows } = await pool.query(`
    SELECT
      COUNT(*) FILTER (WHERE estado='GREEN')::int AS green,
      COUNT(*) FILTER (WHERE estado='LOSS')::int AS loss,
      COUNT(*) FILTER (WHERE estado='PENDENTE')::int AS pending,
      COUNT(*) FILTER (WHERE estado='EXPIRADO')::int AS expirado
    FROM football_sinais
    WHERE criado_em::date = CURRENT_DATE
  `);
  const s = rows[0];
  const settled = s.green + s.loss;
  res.json({
    ok: true,
    date: new Date().toISOString().slice(0, 10),
    ...s,
    settled,
    winrate: settled ? round((s.green / settled) * 100, 2) : null
  });
}));

app.get('/eventos', auth, asyncRoute(async (req, res) => {
  const limit = Math.min(200, Math.max(1, Number(req.query.limit) || 50));
  const { rows } = await pool.query(
    'SELECT * FROM football_eventos ORDER BY id DESC LIMIT $1', [limit]
  );
  res.json({ ok: true, count: rows.length, items: rows });
}));

app.get('/sources', asyncRoute(async (_req, res) => {
  res.json({
    ok: true,
    mode: CFG.genericApiUrl ? 'polling+webhook' : 'webhook-only',
    pollMs: CFG.sourcePollMs,
    generic: sourceHealth.generic || { ok: null, note: 'não configurado' }
  });
}));

/* ============ RECEPÇÃO DE RESULTADOS ============ */
app.post('/webhook/resultado', webhookAuth, asyncRoute(async (req, res) => {
  const b = req.body || {};
  if (!b.homeCard || !b.awayCard) {
    return res.status(400).json({ ok: false, error: 'homeCard e awayCard são obrigatórios' });
  }
  const out = await processNewResult({
    roundId: b.roundId || b.round_id || null,
    homeCard: b.homeCard,
    awayCard: b.awayCard,
    resultado: b.resultado || null,
    hash: b.hash || null,
    origem: b.origem || 'webhook'
  });
  res.json({ ok: true, ...out });
}));

app.post('/poll', auth, asyncRoute(async (_req, res) => {
  const r = await pollSources();
  res.json({ ok: true, ...r });
}));

app.post('/scan', auth, asyncRoute(async (_req, res) => {
  if (scanning) return res.json({ ok: false, reason: 'scan_in_progress' });
  scanning = true;
  try {
    const { rows: hist } = await pool.query(
      'SELECT resultado FROM football_resultados ORDER BY id DESC LIMIT $1',
      [CFG.historyLimit]
    );
    const sig = await generateSignal(hist);
    res.json({ ok: true, result: sig });
  } finally {
    scanning = false;
  }
}));

app.post('/sinais/:id/resolver', auth, asyncRoute(async (req, res) => {
  const id = Number(req.params.id);
  const { resultado } = req.body || {};
  if (!Number.isSafeInteger(id) || id < 1) {
    return res.status(400).json({ ok: false, error: 'id inválido' });
  }
  if (!['HOME', 'AWAY', 'DRAW'].includes(resultado)) {
    return res.status(400).json({ ok: false, error: 'resultado inválido' });
  }
  const r = await resolvePendingByResult(resultado);
  res.json({ ok: true, resolution: r });
}));

app.post('/martingale/reset', auth, asyncRoute(async (_req, res) => {
  await setMartingaleLevel(0);
  res.json({ ok: true, level: 0 });
}));

/* ============ ERRO GLOBAL ============ */
app.use((err, _req, res, _next) => {
  lastError = errText(err);
  console.error('[HTTP]', lastError);
  res.status(500).json({ ok: false, error: lastError });
});

/* ============================ TELEGRAM COMMANDS ============================ */
async function telegramCommand(text, chatId) {
  if (!CFG.botToken || !CFG.allowTelegramCommands) return;
  if (String(chatId) !== String(CFG.chatId)) return;
  const cmd = String(text || '').trim().split(/\s+/)[0].split('@')[0].toLowerCase();
  const send = msg => axios.post(
    `https://api.telegram.org/bot${CFG.botToken}/sendMessage`,
    { chat_id: chatId, text: msg, disable_web_page_preview: true },
    { timeout: 10000 }
  );

  if (['/start', '/ajuda', '/help'].includes(cmd)) {
    return send(
      '⚽ Football Studio Signal Server\n\n' +
      '/status — estado + Martingale\n' +
      '/placar — placar do dia\n' +
      '/historico — últimos 10 sinais\n' +
      '/ultimos — últimos 10 resultados\n' +
      '/sources — status da fonte\n' +
      '/scan — gerar sinal agora\n' +
      '/reset — resetar Martingale'
    );
  }

  if (cmd === '/status') {
    const { rows } = await pool.query(`
      SELECT
        COUNT(*) FILTER (WHERE estado='GREEN')::int g,
        COUNT(*) FILTER (WHERE estado='LOSS')::int l,
        COUNT(*) FILTER (WHERE estado='PENDENTE')::int p
      FROM football_sinais`);
    const s = rows[0];
    const settled = s.g + s.l;
    const lvl = await getMartingaleLevel();
    return send(
      `⚽ STATUS\n` +
      `🟢 ${s.g} · 🔴 ${s.l} · ⏳ ${s.p}\n` +
      `Winrate: ${settled ? round((s.g / settled) * 100, 1) : '—'}%\n` +
      `Martingale: nível ${lvl} · próximo stake: ${stakeForLevel(lvl)} un.`
    );
  }

  if (cmd === '/placar') {
    const { rows } = await pool.query(`
      SELECT
        COUNT(*) FILTER (WHERE estado='GREEN')::int g,
        COUNT(*) FILTER (WHERE estado='LOSS')::int l
      FROM football_sinais WHERE criado_em::date = CURRENT_DATE`);
    const s = rows[0];
    const n = s.g + s.l;
    return send(
      `⚽ PLACAR DO DIA\n🟢 ${s.g}\n🔴 ${s.l}\n` +
      `Precisão: ${n ? round((s.g / n) * 100, 1) : '—'}%`
    );
  }

  if (cmd === '/historico') {
    const { rows } = await pool.query(
      'SELECT id,direcao,confianca,estado,resultado_final FROM football_sinais ORDER BY id DESC LIMIT 10'
    );
    if (!rows.length) return send('Sem sinais ainda.');
    const lines = rows.map(s =>
      `#${s.id} ${s.direcao} (${s.confianca}%) → ${s.estado}` +
      (s.resultado_final ? ` [${s.resultado_final}]` : '')
    );
    return send('⚽ ÚLTIMOS SINAIS\n' + lines.join('\n'));
  }

  if (cmd === '/ultimos') {
    const { rows } = await pool.query(
      'SELECT home_card,away_card,resultado FROM football_resultados ORDER BY id DESC LIMIT 10'
    );
    if (!rows.length) return send('Sem resultados ainda.');
    const lines = rows.map(r => `${r.home_card}×${r.away_card} → ${r.resultado}`);
    return send('⚽ ÚLTIMOS RESULTADOS\n' + lines.join('\n'));
  }

  if (cmd === '/sources') {
    if (!CFG.genericApiUrl) return send('🔌 Modo: webhook-only (sem polling)');
    const h = sourceHealth.generic || {};
    return send(
      `🔌 Generic: ${h.ok ? '🟢 OK' : '🔴 Falha'}\n` +
      (h.lastCount ? `Rodadas: ${h.lastCount}\n` : '') +
      (h.lastError ? `Erro: ${h.lastError.slice(0, 100)}` : '')
    );
  }

  if (cmd === '/scan') {
    await send('🔍 Analisando…');
    const { rows: hist } = await pool.query(
      'SELECT resultado FROM football_resultados ORDER BY id DESC LIMIT $1',
      [CFG.historyLimit]
    );
    const sig = await generateSignal(hist);
    return send(sig.created
      ? `⚽ Sinal #${sig.id} → ${sig.direction} (${sig.confidence}%) · stake ${sig.stake} un.`
      : `Sem sinal: ${sig.reason || 'sem padrão'}`);
  }

  if (cmd === '/reset') {
    await setMartingaleLevel(0);
    return send('🔄 Martingale resetado para nível 0.');
  }
}

async function pollTelegram() {
  if (!CFG.botToken || !CFG.allowTelegramCommands || shuttingDown) return;
  try {
    const { data } = await axios.get(
      `https://api.telegram.org/bot${CFG.botToken}/getUpdates`,
      {
        params: { offset: telegramOffset, timeout: 0, allowed_updates: ['message'] },
        timeout: 15000
      }
    );
    if (!data?.ok || !Array.isArray(data.result)) return;
    for (const u of data.result) {
      telegramOffset = Math.max(telegramOffset, Number(u.update_id) + 1);
      const msg = u.message;
      if (msg?.text) await telegramCommand(msg.text, msg.chat?.id);
    }
  } catch (e) {
    console.error('[TG poll]', errText(e));
  }
}

/* ============================ SHUTDOWN ============================ */
async function shutdown() {
  if (shuttingDown) return;
  shuttingDown = true;
  for (const t of [keepaliveTimer, telegramTimer, sourceTimer]) {
    try { clearInterval(t); } catch (_) {}
  }
  if (server) await new Promise(r => server.close(r));
  if (pool) await pool.end();
  process.exit(0);
}

/* ============================ MAIN ============================ */
async function main() {
  await initDB();
  server = app.listen(CFG.port, '0.0.0.0', () => {
    console.log(`[HTTP] Football Studio Signal Server v2.0 · :${CFG.port}`);
    console.log(`[MODE] ${CFG.genericApiUrl ? 'polling+webhook' : 'webhook-only'}`);
  });

  if (CFG.keepaliveUrl) {
    keepaliveTimer = setInterval(() =>
      axios.get(CFG.keepaliveUrl.replace(/\/$/, '') + '/health', { timeout: 10000 }).catch(() => {}),
      4 * 60000
    );
  }

  if (CFG.botToken && CFG.allowTelegramCommands) {
    telegramTimer = setInterval(() => pollTelegram().catch(() => {}), CFG.telegramPollMs);
    pollTelegram().catch(() => {});
  }

  startSourcePolling();

  process.on('SIGTERM', shutdown);
  process.on('SIGINT', shutdown);
}

main().catch(e => {
  console.error('[FATAL]', errText(e));
  process.exit(1);
});