'use strict';

/* =============================================================
   CAPUNGA v3.2 — Bac Bo Brasileiro
   Webhook / API polling / scraper (experimental) -> sinais no Telegram
   ============================================================= */

process.env.NTBA_FIX_319 = process.env.NTBA_FIX_319 || '1';
process.env.NTBA_FIX_350 = process.env.NTBA_FIX_350 || '1';

try { require('dotenv').config(); } catch { /* dotenv é opcional */ }

const crypto      = require('crypto');
const fs          = require('fs');
const path        = require('path');
const express     = require('express');
const TelegramBot = require('node-telegram-bot-api');

/* =============================================================
   CONFIGURAÇÃO (tudo vem de variáveis de ambiente)
   ============================================================= */
const env = (key, def = '') => {
  const v = process.env[key];
  return v === undefined || String(v).trim() === '' ? def : String(v).trim();
};
const envNum = (key, def) => {
  const v = process.env[key];
  if (v === undefined || String(v).trim() === '') return def;
  const n = Number(v);
  return Number.isFinite(n) ? n : def;
};
const envBool = (key, def = false) => {
  const v = process.env[key];
  if (v === undefined || String(v).trim() === '') return def;
  return ['1', 'true', 'yes', 'sim', 'on'].includes(String(v).trim().toLowerCase());
};

const DEFAULT_DISCLAIMER =
  '⚠️ Jogo de azar: não há garantia de resultado. Aposte com responsabilidade. +18';
const rawDisclaimer = process.env.DISCLAIMER;

const CONFIG = {
  TELEGRAM_TOKEN:   env('TELEGRAM_TOKEN'),
  TELEGRAM_CHAT_ID: env('TELEGRAM_CHAT_ID'),
  ADMIN_IDS: new Set(
    env('ADMIN_ID').split(',').map(s => s.trim()).filter(s => s && s !== '0')
  ),

  SOURCE:           env('SOURCE', 'webhook').toLowerCase(), // webhook | api | scraper
  HISTORY_URL:      env('HISTORY_URL'),
  HISTORY_NEWEST_FIRST: envBool('HISTORY_NEWEST_FIRST', true),
  API_KEY:          env('API_KEY'),
  SCRAPER_URL:      env('SCRAPER_URL', 'https://www.betano.bet.br/cassino/ao-vivo/bac-bo-brasileiro'),
  SCRAPER_NEWEST_FIRST: envBool('SCRAPER_NEWEST_FIRST', true),
  POLL_MS:          envNum('POLL_MS', 8000),
  SCRAPE_MS:        envNum('SCRAPE_MS', 15000),

  PORT:             envNum('PORT', 3000),
  WEBHOOK_SECRET:   env('WEBHOOK_SECRET'),

  MIN_CONFIDENCE:   envNum('MIN_CONFIDENCE', 60), // pontuação mínima (0-100); acima de ~70 quase nunca dispara
  MIN_AGREEMENT:    envNum('MIN_AGREEMENT', 3),
  COOLDOWN_MS:      envNum('COOLDOWN_MS', 0),
  PENDING_TTL_MS:   envNum('PENDING_TTL_MS', 10 * 60 * 1000),

  DATA_DIR:         env('DATA_DIR', __dirname),
  ANNOUNCE_ONLINE:  envBool('ANNOUNCE_ONLINE', false), // aviso "online" no canal
  KEEP_ALIVE:       envBool('KEEP_ALIVE', false),      // auto-ping (Render free)
  KEEP_ALIVE_URL:   env('KEEP_ALIVE_URL'),
  DISCLAIMER:
    rawDisclaimer === undefined
      ? DEFAULT_DISCLAIMER
      : (String(rawDisclaimer).trim().toLowerCase() === 'off' ? '' : String(rawDisclaimer).trim()),
};

const BRAND = 'CAPUNGA';
const VERSION = '3.2';
const MAX_HISTORY = 120;
const MAX_PROCESSED = 800;

/* =============================================================
   LOGS
   ============================================================= */
const ts = () => new Date().toISOString();
const log = {
  info:  (...a) => console.log(`[${ts()}] [INFO]`, ...a),
  warn:  (...a) => console.warn(`[${ts()}] [WARN]`, ...a),
  error: (...a) => console.error(`[${ts()}] [ERROR]`, ...a),
};

/* =============================================================
   VALIDAÇÃO INICIAL
   ============================================================= */
(function validateConfig() {
  const problems = [];
  if (!CONFIG.TELEGRAM_TOKEN || !CONFIG.TELEGRAM_TOKEN.includes(':')) {
    problems.push('TELEGRAM_TOKEN ausente ou inválido');
  }
  if (!CONFIG.TELEGRAM_CHAT_ID) {
    problems.push('TELEGRAM_CHAT_ID ausente');
  }
  if (!CONFIG.WEBHOOK_SECRET) {
    problems.push('WEBHOOK_SECRET ausente (defina um segredo longo e aleatório)');
  }
  if (!['webhook', 'api', 'scraper'].includes(CONFIG.SOURCE)) {
    problems.push(`SOURCE inválido: "${CONFIG.SOURCE}" (use webhook, api ou scraper)`);
  }
  if (problems.length) {
    console.error('❌ Configuração inválida:');
    for (const p of problems) console.error('   - ' + p);
    console.error('Defina as variáveis em Environment no Render (ou no ficheiro .env local).');
    process.exit(1);
  }
})();

/* =============================================================
   DATA (fuso de Angola)
   ============================================================= */
function getAngolaDateKey() {
  try {
    return new Intl.DateTimeFormat('en-CA', {
      timeZone: 'Africa/Luanda',
      year: 'numeric',
      month: '2-digit',
      day: '2-digit',
    }).format(new Date());
  } catch {
    // Angola = UTC+1, sem horário de verão
    return new Date(Date.now() + 3600000).toISOString().slice(0, 10);
  }
}

/* =============================================================
   ESTADO (persistido em disco, escrita atómica)
   ============================================================= */
const BASE_WEIGHTS = {
  streak: 1.3, zigzag: 1.1, doublePair: 1.0, triplePattern: 1.0,
  markov: 1.4, ngram: 1.2, regression: 1.0, swing: 0.9,
};

const ANALYZER_NAMES = Object.keys(BASE_WEIGHTS);

function defaultState() {
  const analyzerStats = {};
  for (const n of ANALYZER_NAMES) analyzerStats[n] = { wins: 0, losses: 0 };
  return {
    history: [],
    processedRounds: [],
    wins: 0,
    losses: 0,
    totalSignals: 0,
    pendingSignal: null,
    stopped: false,
    lastSignalTs: 0,
    dayKey: getAngolaDateKey(),
    analyzerStats,
    regime: { entropy: 1.5, sampleSize: 0 },
  };
}

const STATE_FILE = path.join(CONFIG.DATA_DIR, 'capunga-state.json');

function loadState() {
  const fresh = defaultState();
  try {
    fs.mkdirSync(CONFIG.DATA_DIR, { recursive: true });
    if (fs.existsSync(STATE_FILE)) {
      const data = JSON.parse(fs.readFileSync(STATE_FILE, 'utf8'));
      const s = {
        ...fresh,
        ...data,
        analyzerStats: { ...fresh.analyzerStats, ...(data.analyzerStats || {}) },
        regime: { ...fresh.regime, ...(data.regime || {}) },
      };
      if (!Array.isArray(s.history)) s.history = [];
      if (!Array.isArray(s.processedRounds)) s.processedRounds = [];
      for (const n of ANALYZER_NAMES) {
        const a = s.analyzerStats[n];
        if (!a || typeof a.wins !== 'number' || typeof a.losses !== 'number') {
          s.analyzerStats[n] = { wins: 0, losses: 0 };
        }
      }
      log.info(`Estado carregado de ${STATE_FILE}`);
      return s;
    }
  } catch (e) {
    log.error('Erro ao carregar estado (a iniciar vazio):', e.message);
    try { fs.renameSync(STATE_FILE, `${STATE_FILE}.corrupt-${Date.now()}`); } catch { /* ignora */ }
  }
  return fresh;
}

const state = loadState();

function saveState() {
  try {
    const tmp = `${STATE_FILE}.tmp`;
    fs.writeFileSync(tmp, JSON.stringify(state));
    fs.renameSync(tmp, STATE_FILE);
  } catch (e) {
    log.error('Erro ao salvar estado:', e.message);
  }
}

function isProcessed(round) {
  return round != null && round !== '' && state.processedRounds.includes(String(round));
}

function markProcessed(round) {
  if (round == null || round === '') return;
  state.processedRounds.push(String(round));
  if (state.processedRounds.length > MAX_PROCESSED) state.processedRounds.shift();
}

/* =============================================================
   TELEGRAM
   ============================================================= */
const bot = new TelegramBot(CONFIG.TELEGRAM_TOKEN, { polling: false });

const sleep = (ms) => new Promise(r => setTimeout(r, ms));
const esc = (s) => String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');

async function sendWithRetry(chatId, text, attempt = 0) {
  try {
    await bot.sendMessage(chatId, text, { parse_mode: 'HTML', disable_web_page_preview: true });
  } catch (e) {
    const retryAfter = e && e.response && e.response.body && e.response.body.parameters
      ? e.response.body.parameters.retry_after
      : null;
    if (retryAfter && attempt < 2) {
      await sleep((retryAfter + 1) * 1000);
      return sendWithRetry(chatId, text, attempt + 1);
    }
    log.error('Erro ao enviar mensagem Telegram:', e.message);
  }
}

// Fila de envio: mantém a ordem das mensagens e nunca bloqueia o processamento
let sendChain = Promise.resolve();
function notify(text, chatId = CONFIG.TELEGRAM_CHAT_ID) {
  sendChain = sendChain.then(() => sendWithRetry(chatId, text)).catch(() => {});
  return sendChain;
}

function tgLabel(side) {
  if (side === 'B') return 'BANKER';
  if (side === 'P') return 'PLAYER';
  return 'EMPATE';
}

function isAdmin(msg) {
  return CONFIG.ADMIN_IDS.size > 0 && !!msg.from && CONFIG.ADMIN_IDS.has(String(msg.from.id));
}

function fmtRecord(wins, losses) {
  const total = wins + losses;
  const pct = total ? Math.round((wins / total) * 100) : 0;
  return `${wins}W / ${losses}L (${pct}%)`;
}

/* =============================================================
   ANALISADORES
   ============================================================= */
function entropy(sides) {
  if (!sides.length) return 0;
  const count = { B: 0, P: 0, T: 0 };
  for (const s of sides) count[s]++;
  let h = 0;
  for (const k of ['B', 'P', 'T']) {
    if (count[k]) {
      const p = count[k] / sides.length;
      h -= p * Math.log2(p);
    }
  }
  return h;
}

function updateRegime() {
  const recent = state.history.slice(-20).map(x => x.side);
  state.regime = {
    entropy: recent.length >= 10 ? entropy(recent) : 1.5,
    sampleSize: recent.length,
  };
}

function streakAnalyzer(s) {
  if (s.length < 3) return null;
  const last = s[s.length - 1];
  if (last === 'T') return null;
  let len = 0;
  for (let i = s.length - 1; i >= 0; i--) {
    if (s[i] === last) len++;
    else break;
  }
  if (len < 3) return null;
  return { bet: last === 'B' ? 'P' : 'B', confidence: Math.min(95, 60 + (len - 3) * 8) };
}

function zigzagAnalyzer(s) {
  if (s.length < 4) return null;
  const last = s[s.length - 1];
  if (last === 'T') return null;
  let changes = 0;
  for (let i = s.length - 1; i > 0; i--) {
    if (s[i] === 'T' || s[i - 1] === 'T' || s[i] === s[i - 1]) break;
    changes++;
  }
  if (changes < 3) return null;
  return { bet: last === 'B' ? 'P' : 'B', confidence: Math.min(92, 60 + (changes - 3) * 8) };
}

function doublePairAnalyzer(s) {
  if (s.length < 4) return null;
  const last4 = s.slice(-4);
  if (last4.includes('T')) return null;
  if (last4[0] === last4[1] && last4[2] === last4[3] && last4[0] !== last4[2]) {
    return { bet: last4[0], confidence: 72 };
  }
  return null;
}

function triplePatternAnalyzer(s) {
  if (s.length < 6) return null;
  const last6 = s.slice(-6);
  if (last6.includes('T')) return null;
  if (last6.slice(0, 3).every(x => x === last6[0]) &&
      last6.slice(3).every(x => x === last6[3]) &&
      last6[0] !== last6[3]) {
    return { bet: last6[0], confidence: 76 };
  }
  if (last6.slice(0, 3).join('') === last6.slice(3).join('')) {
    return { bet: last6[0], confidence: 79 };
  }
  return null;
}

function markovAnalyzer(s) {
  if (s.length < 10) return null;
  const transitions = {
    B: { B: 0, P: 0, T: 0 },
    P: { B: 0, P: 0, T: 0 },
    T: { B: 0, P: 0, T: 0 },
  };
  for (let i = 1; i < s.length; i++) transitions[s[i - 1]][s[i]]++;
  const last = s[s.length - 1];
  const row = transitions[last];
  const total = row.B + row.P + row.T;
  if (total < 5) return null;
  const pB = row.B / total;
  const pP = row.P / total;
  if (pB > 0.62 && pB > pP * 1.3) return { bet: 'B', confidence: Math.min(92, 50 + pB * 50) };
  if (pP > 0.62 && pP > pB * 1.3) return { bet: 'P', confidence: Math.min(92, 50 + pP * 50) };
  return null;
}

function ngramAnalyzer(s) {
  if (s.length < 8) return null;
  for (const k of [4, 3]) {
    if (s.length < k + 3) continue;
    const tail = s.slice(-k).join('');
    const freq = { B: 0, P: 0, T: 0 };
    for (let i = 0; i < s.length - k; i++) {
      if (s.slice(i, i + k).join('') === tail) freq[s[i + k]]++;
    }
    const total = freq.B + freq.P + freq.T;
    if (total < 3) continue;
    const pB = freq.B / total;
    const pP = freq.P / total;
    if (pB >= 0.67) return { bet: 'B', confidence: Math.min(90, 55 + pB * 40) };
    if (pP >= 0.67) return { bet: 'P', confidence: Math.min(90, 55 + pP * 40) };
  }
  return null;
}

function regressionAnalyzer(s) {
  if (s.length < 10) return null;
  const last10 = s.slice(-10);
  const b = last10.filter(x => x === 'B').length;
  const p = last10.filter(x => x === 'P').length;
  if (b <= 2 && p >= 6) return { bet: 'B', confidence: 68 };
  if (p <= 2 && b >= 6) return { bet: 'P', confidence: 68 };
  return null;
}

function swingAnalyzer(s) {
  if (s.length < 8) return null;
  const first = s.slice(-8, -4);
  const second = s.slice(-4);
  const bFirst = first.filter(x => x === 'B').length;
  const bSecond = second.filter(x => x === 'B').length;
  if (bFirst >= 3 && bSecond <= 1) return { bet: 'B', confidence: 63 };
  if (bFirst <= 1 && bSecond >= 3) return { bet: 'P', confidence: 63 };
  return null;
}

const ANALYZERS = {
  streak: streakAnalyzer,
  zigzag: zigzagAnalyzer,
  doublePair: doublePairAnalyzer,
  triplePattern: triplePatternAnalyzer,
  markov: markovAnalyzer,
  ngram: ngramAnalyzer,
  regression: regressionAnalyzer,
  swing: swingAnalyzer,
};

function getWeight(name) {
  const base = BASE_WEIGHTS[name] || 1;
  const st = state.analyzerStats[name];
  if (!st) return base;
  const total = st.wins + st.losses;
  if (total < 5) return base;
  return base * (0.5 + st.wins / total);
}

function runAnalyzers() {
  const sequence = state.history.map(r => r.side);
  const found = [];
  for (const [name, fn] of Object.entries(ANALYZERS)) {
    let result = null;
    try { result = fn(sequence); } catch (e) { log.warn(`Analisador ${name} falhou:`, e.message); }
    if (result) found.push({ name, ...result, weight: getWeight(name) });
  }
  return found;
}

function computeConsensus(analyzers) {
  if (!analyzers.length) return null;

  let weightB = 0;
  let weightP = 0;
  const agreeingB = [];
  const agreeingP = [];

  for (const a of analyzers) {
    if (a.bet === 'B') { weightB += a.weight; agreeingB.push(a); }
    else if (a.bet === 'P') { weightP += a.weight; agreeingP.push(a); }
  }

  const total = weightB + weightP;
  if (!total) return null;

  const ratioB = weightB / total;
  const ratioP = weightP / total;

  if (ratioB >= ratioP) {
    return { bet: 'B', ratio: ratioB, agreeing: agreeingB, disagreeing: agreeingP };
  }
  return { bet: 'P', ratio: ratioP, agreeing: agreeingP, disagreeing: agreeingB };
}

// Pontuação interna 0-100 (heurística). NÃO é uma probabilidade de acerto.
function computeScore(consensus) {
  if (consensus.agreeing.length < CONFIG.MIN_AGREEMENT) return 0;

  let score = 0;
  score += consensus.ratio * 25;
  score += Math.min(consensus.agreeing.length / 5, 1) * 15;

  const avgInd = consensus.agreeing.reduce((sum, a) => sum + a.confidence, 0) / consensus.agreeing.length;
  score += (avgInd / 100) * 20;

  let historicalAccuracy = 0;
  for (const a of consensus.agreeing) {
    const st = state.analyzerStats[a.name];
    const total = st ? st.wins + st.losses : 0;
    historicalAccuracy += total >= 3 ? st.wins / total : 0.5;
  }
  historicalAccuracy /= consensus.agreeing.length;
  score += historicalAccuracy * 20;

  const e = state.regime.entropy;
  score += Math.max(0, Math.min(1, (1.6 - e) / 0.6)) * 10;

  score += Math.min(state.history.length / 40, 1) * 5;

  const totalGames = state.wins + state.losses;
  if (totalGames >= 10) {
    score += Math.max(0, Math.min(5, (state.wins / totalGames - 0.5) * 20));
  }

  score -= consensus.disagreeing.length * 2.5;

  return Math.max(0, Math.min(100, Math.round(score)));
}

/* =============================================================
   RESET DIÁRIO
   ============================================================= */
function checkNewDay() {
  const today = getAngolaDateKey();
  if (today === state.dayKey) return false;

  const prev = { wins: state.wins, losses: state.losses };
  state.wins = 0;
  state.losses = 0;
  state.totalSignals = 0;
  state.lastSignalTs = 0;
  state.dayKey = today;
  saveState();

  notify(
    `🔄 <b>${BRAND} — NOVO DIA</b>\n` +
    `📅 ${today} (Angola)\n` +
    `📊 Anterior: ${fmtRecord(prev.wins, prev.losses)}`
  );
  return true;
}

/* =============================================================
   PROCESSAMENTO DE RESULTADO (síncrono => sem condições de corrida)
   ============================================================= */
function mapSide(raw) {
  if (raw == null) return null;
  const s = String(raw).trim().toLowerCase();
  // No Bac Bo: dados azuis = Player, dados vermelhos = Banker
  if (['b', 'banker', 'banca', 'banco', 'red', 'vermelho', '1'].includes(s)) return 'B';
  if (['p', 'player', 'jogador', 'blue', 'azul', '2'].includes(s)) return 'P';
  if (['t', 'tie', 'empate', 'yellow', 'amarelo', 'green', 'verde', '0'].includes(s)) return 'T';
  return null;
}

function processResult({ round, side, silent = false }) {
  const info = { resolved: null, signal: null };
  if (!side) return info;

  checkNewDay();

  if (!silent) {
    // Sinal pendente antigo demais (ex.: instância esteve parada): descarta sem contar
    if (state.pendingSignal && Date.now() - state.pendingSignal.ts > CONFIG.PENDING_TTL_MS) {
      log.warn('Sinal pendente expirou; descartado sem contar.');
      state.pendingSignal = null;
    }

    // Resolve sinal pendente
    if (state.pendingSignal) {
      const pending = state.pendingSignal;
      const won = side === pending.bet;

      for (const name of pending.analyzers) {
        const st = state.analyzerStats[name];
        if (st) {
          if (won) st.wins++;
          else st.losses++;
        }
      }

      if (won) state.wins++;
      else state.losses++;

      state.pendingSignal = null;
      info.resolved = won ? 'win' : 'loss';

      notify(
        won
          ? `✅ <b>${BRAND} — WIN</b>\n🎯 ${tgLabel(pending.bet)}\n📊 Hoje: ${fmtRecord(state.wins, state.losses)}`
          : `❌ <b>${BRAND} — LOSS</b>\n🎯 ${tgLabel(pending.bet)} (saiu ${tgLabel(side)})\n📊 Hoje: ${fmtRecord(state.wins, state.losses)}`
      );
    }
  }

  // Histórico
  state.history.push({ round: round == null ? null : String(round), side, ts: Date.now() });
  if (state.history.length > MAX_HISTORY) state.history.shift();
  updateRegime();

  // Novo sinal
  if (!silent && !state.stopped && !state.pendingSignal) {
    const consensus = computeConsensus(runAnalyzers());
    if (consensus) {
      const cooled = CONFIG.COOLDOWN_MS === 0 || Date.now() - state.lastSignalTs >= CONFIG.COOLDOWN_MS;
      if (cooled) {
        const score = computeScore(consensus);
        if (score >= CONFIG.MIN_CONFIDENCE) {
          state.pendingSignal = {
            bet: consensus.bet,
            score,
            analyzers: consensus.agreeing.map(a => a.name),
            ts: Date.now(),
          };
          state.lastSignalTs = Date.now();
          state.totalSignals++;
          info.signal = { bet: consensus.bet, score };

          notify(
            `🚨 <b>${BRAND}</b> 🚨\n` +
            `🎯 <b>SINAL BAC BO</b>\n` +
            `➡️ Entrada: <b>${tgLabel(consensus.bet)}</b>\n` +
            `💯 Força do sinal: <b>${score}/100</b>\n` +
            `📊 Hoje: ${fmtRecord(state.wins, state.losses)}` +
            (CONFIG.DISCLAIMER ? `\n\n${esc(CONFIG.DISCLAIMER)}` : '')
          );
        }
      }
    }
  }

  saveState();
  return info;
}

// Entrada comum a todas as fontes: deduplica e processa
function ingest({ round, side, silent = false }) {
  const key = round == null || round === '' ? null : String(round);
  if (key && isProcessed(key)) return { ok: true, skipped: 'already_processed' };
  if (key) markProcessed(key);
  const info = processResult({ round: key, side, silent });
  return { ok: true, side, ...info };
}

/* =============================================================
   SERVIDOR HTTP + WEBHOOK
   ============================================================= */
const app = express();
app.disable('x-powered-by');
app.use(express.json({ limit: '64kb' }));

function safeEqual(a, b) {
  const ba = Buffer.from(String(a));
  const bb = Buffer.from(String(b));
  if (ba.length !== bb.length) return false;
  return crypto.timingSafeEqual(ba, bb);
}

function isAuthorized(req) {
  const provided =
    req.get('x-secret') ||
    (req.get('authorization') || '').replace(/^Bearer\s+/i, '') ||
    (typeof req.query.secret === 'string' ? req.query.secret : '');
  return !!provided && safeEqual(provided, CONFIG.WEBHOOK_SECRET);
}

app.get('/', (_req, res) => res.type('text/plain').send(`${BRAND} v${VERSION} online`));

app.get('/health', (_req, res) => {
  res.json({
    brand: BRAND,
    version: VERSION,
    ok: true,
    source: CONFIG.SOURCE,
    stopped: state.stopped,
    wins: state.wins,
    losses: state.losses,
    pending: !!state.pendingSignal,
    historyLength: state.history.length,
    dayKey: state.dayKey,
  });
});

app.post('/webhook/result', (req, res) => {
  try {
    if (!isAuthorized(req)) return res.status(401).json({ error: 'unauthorized' });

    const body = req.body || {};
    const side = mapSide(body.result ?? body.side ?? body.outcome ?? body.winner);
    if (!side) return res.status(400).json({ error: 'invalid result' });

    const round = body.round ?? body.id ?? body.round_id ?? body.roundId ?? null;
    return res.json(ingest({ round, side }));
  } catch (e) {
    log.error('Erro no webhook:', e.message);
    return res.status(500).json({ error: 'internal_error' });
  }
});

// eslint-disable-next-line no-unused-vars
app.use((err, _req, res, _next) => {
  if (err && err.type === 'entity.parse.failed') {
    return res.status(400).json({ error: 'invalid_json' });
  }
  log.error('Erro HTTP:', err && err.message);
  return res.status(500).json({ error: 'internal_error' });
});

/* =============================================================
   FONTE: API (polling)
   ============================================================= */
function normalizeHistory(payload) {
  if (!payload) return [];
  const list = Array.isArray(payload)
    ? payload
    : (payload.data || payload.results || payload.history || []);
  if (!Array.isArray(list)) return [];

  return list
    .map(item => ({
      round: String(item.id ?? item.round ?? item.round_id ?? item.roundId ?? ''),
      side: mapSide(item.outcome ?? item.result ?? item.winner ?? item.color ?? item.side),
    }))
    .filter(x => x.side && x.round);
}

let pollTimer = null;
let apiPolling = false;
let apiSeeded = false;

async function pollHistory() {
  if (apiPolling || !CONFIG.HISTORY_URL) return;
  apiPolling = true;
  try {
    const headers = { Accept: 'application/json' };
    if (CONFIG.API_KEY) headers['x-api-key'] = CONFIG.API_KEY;

    const response = await fetch(CONFIG.HISTORY_URL, { headers, signal: AbortSignal.timeout(10000) });
    if (!response.ok) {
      log.warn('pollHistory status:', response.status);
      return;
    }

    const results = normalizeHistory(await response.json());
    if (CONFIG.HISTORY_NEWEST_FIRST) results.reverse(); // processa do mais antigo para o mais novo

    const fresh = results.filter(r => !isProcessed(r.round));
    // Primeira leitura: só carrega o histórico, sem gerar sinais/resultados antigos
    for (const r of fresh) ingest({ round: r.round, side: r.side, silent: !apiSeeded });

    if (!apiSeeded) {
      apiSeeded = true;
      if (fresh.length) { state.pendingSignal = null; saveState(); }
      log.info(`API: histórico inicial carregado (${fresh.length} resultados)`);
    }
  } catch (e) {
    log.warn('pollHistory erro:', e.message);
  } finally {
    apiPolling = false;
  }
}

/* =============================================================
   FONTE: SCRAPER (experimental — requer puppeteer + Chrome)
   ============================================================= */
let browser = null;
let scraperTimer = null;
let scraping = false;
let scraperSeeded = false;
let scraperWarnedNoIds = false;

const SCRAPER_SELECTORS = [
  '[data-round]', '.bacbo-round', '.round-result', '.history-item',
  '.result-item', '[class*="result"]', '[class*="history"]',
];

function sideFromScrape(cls, txt) {
  const hay = `${cls} ${txt}`;
  const found = new Set();
  if (/\b(banker|banco|red|vermelho)\b/.test(hay)) found.add('B');
  if (/\b(player|jogador|blue|azul)\b/.test(hay)) found.add('P');
  if (/\b(tie|empate|green|verde|yellow|amarelo)\b/.test(hay)) found.add('T');
  return found.size === 1 ? [...found][0] : null;
}

async function startScraper() {
  log.info('Iniciando scraper (experimental)...');

  let puppeteer;
  let StealthPlugin;
  try {
    puppeteer = require('puppeteer-extra');
    StealthPlugin = require('puppeteer-extra-plugin-stealth');
  } catch (e) {
    log.error('Scraper indisponível — instale puppeteer, puppeteer-extra e puppeteer-extra-plugin-stealth:', e.message);
    log.warn('Continuando sem scraper. Use webhook ou API.');
    return;
  }

  try {
    puppeteer.use(StealthPlugin());
    browser = await puppeteer.launch({
      headless: true,
      args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage'],
    });

    const page = await browser.newPage();
    await page.setUserAgent(
      'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    );
    await page.goto(CONFIG.SCRAPER_URL, { waitUntil: 'networkidle2', timeout: 90000 });
    log.info('Página do scraper carregada');

    scraperTimer = setInterval(async () => {
      if (scraping) return;
      scraping = true;
      try {
        let items = await page.evaluate((selectors) => {
          let els = [];
          for (const sel of selectors) {
            els = Array.from(document.querySelectorAll(sel));
            if (els.length > 2) break;
          }
          // Só aceita elementos com ID estável: IDs por posição quebrariam a deduplicação
          return els.map(el => ({
            id: el.getAttribute('data-round') || el.getAttribute('data-id') || el.getAttribute('data-game-id') || '',
            cls: String(el.className || '').toLowerCase(),
            txt: String(el.textContent || '').toLowerCase().slice(0, 80),
          })).filter(x => x.id);
        }, SCRAPER_SELECTORS);

        if (!items.length && !scraperWarnedNoIds) {
          scraperWarnedNoIds = true;
          log.warn('Scraper: nenhum elemento com ID estável (data-round/data-id) encontrado na página.');
        }

        if (CONFIG.SCRAPER_NEWEST_FIRST) items = items.reverse();

        const fresh = [];
        for (const it of items) {
          const side = sideFromScrape(it.cls, it.txt);
          if (side && !isProcessed(it.id)) fresh.push({ round: it.id, side });
        }

        for (const r of fresh) {
          ingest({ round: r.round, side: r.side, silent: !scraperSeeded });
          if (scraperSeeded) log.info(`Scraper → ${r.round} = ${r.side}`);
        }
        if (!scraperSeeded) {
          scraperSeeded = true;
          if (fresh.length) { state.pendingSignal = null; saveState(); }
        }
      } catch (e) {
        log.warn('Erro no loop do scraper:', e.message);
      } finally {
        scraping = false;
      }
    }, CONFIG.SCRAPE_MS);
  } catch (e) {
    log.error('Falha ao iniciar scraper:', e.message);
    log.warn('Continuando sem scraper. Use webhook ou API.');
  }
}

/* =============================================================
   COMANDOS DO BOT
   ============================================================= */
const reply = (msg, text) => notify(text, msg.chat.id);

// Qualquer pessoa: descobrir IDs (útil para configurar ADMIN_ID / TELEGRAM_CHAT_ID)
bot.onText(/^\/id(?:@\w+)?$/i, (msg) => {
  reply(
    msg,
    `🆔 Seu ID: <code>${msg.from ? msg.from.id : '?'}</code>\nChat ID: <code>${msg.chat.id}</code>`
  );
});

bot.onText(/^\/status(?:@\w+)?$/i, (msg) => {
  if (!isAdmin(msg)) return;

  const analyzerLines = Object.entries(state.analyzerStats)
    .map(([name, st]) => {
      const t = st.wins + st.losses;
      return `  ${esc(name)}: ${t ? Math.round((st.wins / t) * 100) : 0}% (${t})`;
    })
    .join('\n');

  reply(
    msg,
    `📊 <b>${BRAND} — STATUS</b>\n` +
    `📅 ${state.dayKey}\n` +
    `🎯 Hoje: ${fmtRecord(state.wins, state.losses)}\n` +
    `📡 Sinais: ${state.totalSignals}\n` +
    `⏳ Pendente: ${state.pendingSignal ? tgLabel(state.pendingSignal.bet) : 'não'}\n` +
    `🌡 Entropia: ${state.regime.entropy.toFixed(2)}\n` +
    `📜 Histórico: ${state.history.length} rodadas\n` +
    `⏸ Pausado: ${state.stopped ? 'Sim' : 'Não'}\n\n` +
    `<b>Analisadores:</b>\n${analyzerLines}`
  );
});

bot.onText(/^\/stop(?:@\w+)?$/i, (msg) => {
  if (!isAdmin(msg)) return;
  state.stopped = true;
  saveState();
  reply(msg, `🛑 ${BRAND} pausado.`);
});

bot.onText(/^\/start(?:@\w+)?$/i, (msg) => {
  if (!isAdmin(msg)) return;
  state.stopped = false;
  saveState();
  reply(msg, `▶️ ${BRAND} retomado.`);
});

bot.onText(/^\/reset(?:@\w+)?$/i, (msg) => {
  if (!isAdmin(msg)) return;
  state.history = [];
  state.wins = 0;
  state.losses = 0;
  state.totalSignals = 0;
  state.pendingSignal = null;
  state.stopped = false;
  updateRegime();
  saveState();
  reply(msg, `♻️ ${BRAND} resetado.`);
});

const pollingErrorLog = new Map();
bot.on('polling_error', (error) => {
  const code = error && error.code ? error.code : 'UNKNOWN';
  const now = Date.now();
  if (now - (pollingErrorLog.get(code) || 0) < 60000) return; // evita flood de logs
  pollingErrorLog.set(code, now);

  const message = error && error.message ? error.message : String(error);
  if (/409/.test(message)) {
    log.warn('Telegram 409: outra instância do bot está a fazer polling (normal durante um redeploy).');
  } else if (/401/.test(message)) {
    log.error('Telegram 401: TELEGRAM_TOKEN inválido ou revogado. Atualize a variável de ambiente.');
  } else {
    log.error('Telegram polling error:', message);
  }
});

/* =============================================================
   INICIALIZAÇÃO
   ============================================================= */
const server = app.listen(CONFIG.PORT, '0.0.0.0', () => {
  log.info(`${BRAND} v${VERSION} — Bac Bo Brasileiro`);
  log.info(`Fonte: ${CONFIG.SOURCE}`);
  log.info(`Porta: ${CONFIG.PORT}`);
  log.info('Webhook: POST /webhook/result (header x-secret)');
  log.info('Health:  GET  /health');
  log.info(`Admins configurados: ${CONFIG.ADMIN_IDS.size}`);
});

server.on('error', (e) => {
  log.error('Erro ao iniciar servidor HTTP:', e.message);
  process.exit(1);
});

bot.startPolling();
bot.getMe()
  .then(me => log.info(`Telegram conectado como @${me.username}`))
  .catch(e => log.error('Falha ao validar token do Telegram:', e.message));

if (CONFIG.SOURCE === 'scraper') {
  startScraper().catch(e => log.error('Erro ao iniciar scraper:', e.message));
} else if (CONFIG.SOURCE === 'api') {
  if (CONFIG.HISTORY_URL) {
    log.info(`Iniciando polling da API a cada ${CONFIG.POLL_MS}ms`);
    pollTimer = setInterval(pollHistory, CONFIG.POLL_MS);
    pollHistory();
  } else {
    log.warn('SOURCE=api sem HISTORY_URL — a aguardar apenas webhooks.');
  }
} else {
  log.info('Modo webhook ativo — aguardando POST /webhook/result');
}

// Verifica reset diário a cada 30 segundos
const dayTimer = setInterval(() => {
  try { checkNewDay(); } catch (e) { log.error('Erro no reset diário:', e.message); }
}, 30000);

// Keep-alive opcional (evita o spin-down do plano grátis do Render)
let keepAliveTimer = null;
if (CONFIG.KEEP_ALIVE) {
  const base = env('RENDER_EXTERNAL_URL');
  const url = CONFIG.KEEP_ALIVE_URL || (base ? `${base.replace(/\/$/, '')}/health` : '');
  if (url) {
    log.info(`Keep-alive ativo: ${url} a cada 10 min`);
    keepAliveTimer = setInterval(() => {
      fetch(url, { signal: AbortSignal.timeout(10000) }).catch(e => log.warn('keep-alive:', e.message));
    }, 10 * 60 * 1000);
  } else {
    log.warn('KEEP_ALIVE=true mas sem RENDER_EXTERNAL_URL/KEEP_ALIVE_URL.');
  }
}

// Avisos de arranque
const onlineText = `🚀 <b>${BRAND}</b> v${VERSION} online — Bac Bo (${esc(CONFIG.SOURCE)})`;
for (const id of CONFIG.ADMIN_IDS) notify(onlineText, id);
if (CONFIG.ANNOUNCE_ONLINE) notify(onlineText);

/* =============================================================
   SHUTDOWN GRACEFUL
   ============================================================= */
let shuttingDown = false;
async function shutdown(signal, code = 0) {
  if (shuttingDown) return;
  shuttingDown = true;
  log.info(`Recebido ${signal}, encerrando...`);
  setTimeout(() => process.exit(code), 5000).unref();

  saveState();
  if (pollTimer) clearInterval(pollTimer);
  if (scraperTimer) clearInterval(scraperTimer);
  if (keepAliveTimer) clearInterval(keepAliveTimer);
  clearInterval(dayTimer);

  try { await sendChain; } catch { /* ignora */ }
  if (browser) { try { await browser.close(); } catch { /* ignora */ } }
  try { await bot.stopPolling(); } catch { /* ignora */ }
  try { server.close(); } catch { /* ignora */ }

  process.exit(code);
}

process.on('SIGINT', () => shutdown('SIGINT'));
process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('unhandledRejection', (reason) => {
  log.error('Unhandled Rejection:', reason && reason.message ? reason.message : reason);
});
process.on('uncaughtException', (error) => {
  log.error('Uncaught Exception:', error && error.stack ? error.stack : error);
  shutdown('uncaughtException', 1); // o Render reinicia o serviço
});
