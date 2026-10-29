'use strict';

require('dotenv').config();

const express     = require('express');
const TelegramBot = require('node-telegram-bot-api');
const fs          = require('fs');
const path        = require('path');

/* =============================================================
   CONFIGURAÇÃO
   ============================================================= */
const CONFIG = {
  TELEGRAM_TOKEN:   process.env.TELEGRAM_TOKEN || '8810847298:AAGv3AUCeEpT397e1esOia0QaaTX8DPRSGw',
  TELEGRAM_CHAT_ID: process.env.TELEGRAM_CHAT_ID || '-1004321782898',
  ADMIN_ID:         process.env.ADMIN_ID || '0',

  SOURCE:           process.env.SOURCE || 'webhook', // webhook | api | scraper
  HISTORY_URL:      process.env.HISTORY_URL || '',
  API_KEY:          process.env.API_KEY || '',
  SCRAPER_URL:      process.env.SCRAPER_URL || 'https://www.betano.bet.br/cassino/ao-vivo/bac-bo-brasileiro',
  POLL_MS:          Number(process.env.POLL_MS) || 8000,
  SCRAPE_MS:        Number(process.env.SCRAPE_MS) || 15000,

  PORT:             Number(process.env.PORT) || 3000,
  WEBHOOK_SECRET:   process.env.WEBHOOK_SECRET || 'capunga',

  MIN_CONFIDENCE:   Number(process.env.MIN_CONFIDENCE) || 94,
  MIN_AGREEMENT:    Number(process.env.MIN_AGREEMENT) || 3,
  COOLDOWN_MS:      Number(process.env.COOLDOWN_MS) || 0,
};

const BRAND = 'CAPUNGA';
const STATE_FILE = path.join(__dirname, 'capunga-state.json');

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
if (!CONFIG.TELEGRAM_TOKEN || CONFIG.TELEGRAM_TOKEN.includes('COLOQUE')) {
  console.error('❌ TELEGRAM_TOKEN inválido');
  process.exit(1);
}
if (!CONFIG.TELEGRAM_CHAT_ID) {
  console.error('❌ TELEGRAM_CHAT_ID inválido');
  process.exit(1);
}

/* =============================================================
   ESTADO
   ============================================================= */
const BASE_WEIGHTS = {
  streak: 1.3, zigzag: 1.1, doublePair: 1.0, triplePattern: 1.0,
  markov: 1.4, ngram: 1.2, regression: 1.0, swing: 0.9,
};

const DEFAULT_STATE = {
  history: [],
  processedRounds: [],
  wins: 0,
  losses: 0,
  totalSignals: 0,
  pendingSignal: null,
  stopped: false,
  lastSignalTs: 0,
  dayKey: getAngolaDateKey(),
  analyzerStats: {
    streak: { wins: 0, losses: 0 },
    zigzag: { wins: 0, losses: 0 },
    doublePair: { wins: 0, losses: 0 },
    triplePattern: { wins: 0, losses: 0 },
    markov: { wins: 0, losses: 0 },
    ngram: { wins: 0, losses: 0 },
    regression: { wins: 0, losses: 0 },
    swing: { wins: 0, losses: 0 },
  },
  regime: { entropy: 1.5, sampleSize: 0 },
};

function getAngolaDateKey() {
  try {
    return new Intl.DateTimeFormat('en-CA', {
      timeZone: 'Africa/Luanda',
      year: 'numeric',
      month: '2-digit',
      day: '2-digit',
    }).format(new Date());
  } catch {
    return new Date(Date.now() + 3600000).toISOString().slice(0, 10);
  }
}

function loadState() {
  try {
    if (fs.existsSync(STATE_FILE)) {
      const data = JSON.parse(fs.readFileSync(STATE_FILE, 'utf8'));
      return {
        ...DEFAULT_STATE,
        ...data,
        analyzerStats: { ...DEFAULT_STATE.analyzerStats, ...(data.analyzerStats || {}) },
      };
    }
  } catch (e) {
    log.error('Erro ao carregar estado:', e.message);
  }
  return JSON.parse(JSON.stringify(DEFAULT_STATE));
}

function saveState() {
  try {
    fs.writeFileSync(STATE_FILE, JSON.stringify(state, null, 2));
  } catch (e) {
    log.error('Erro ao salvar estado:', e.message);
  }
}

const state = loadState();
const MAX_HISTORY = 120;
const MAX_PROCESSED = 800;

function isProcessed(round) {
  return round != null && state.processedRounds.includes(String(round));
}

function markProcessed(round) {
  if (round == null) return;
  state.processedRounds.push(String(round));
  if (state.processedRounds.length > MAX_PROCESSED) {
    state.processedRounds.shift();
  }
}

/* =============================================================
   TELEGRAM
   ============================================================= */
const bot = new TelegramBot(CONFIG.TELEGRAM_TOKEN, {
  polling: {
    autoStart: true,
    params: { timeout: 10 },
  },
});

async function tgSend(text) {
  try {
    await bot.sendMessage(CONFIG.TELEGRAM_CHAT_ID, text, { parse_mode: 'Markdown' });
  } catch (e) {
    log.error('Erro ao enviar mensagem Telegram:', e.message);
  }
}

function tgLabel(side) {
  if (side === 'B') return 'BANKER';
  if (side === 'P') return 'PLAYER';
  return 'EMPATE';
}

function isAdmin(msg) {
  if (!CONFIG.ADMIN_ID || CONFIG.ADMIN_ID === '0') return false;
  const admin = String(CONFIG.ADMIN_ID);
  return String(msg.chat?.id) === admin || String(msg.from?.id) === admin;
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
  let changes = 0;
  for (let i = s.length - 1; i > 0; i--) {
    if (s[i] === 'T' || s[i - 1] === 'T') return null;
    if (s[i] !== s[i - 1]) changes++;
    else break;
  }
  if (changes < 3) return null;
  const last = s[s.length - 1];
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
  for (let i = 1; i < s.length; i++) {
    transitions[s[i - 1]][s[i]]++;
  }
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
      if (s.slice(i, i + k).join('') === tail) {
        freq[s[i + k]]++;
      }
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
  const analyzers = {
    streak: streakAnalyzer,
    zigzag: zigzagAnalyzer,
    doublePair: doublePairAnalyzer,
    triplePattern: triplePatternAnalyzer,
    markov: markovAnalyzer,
    ngram: ngramAnalyzer,
    regression: regressionAnalyzer,
    swing: swingAnalyzer,
  };

  for (const [name, fn] of Object.entries(analyzers)) {
    const result = fn(sequence);
    if (result) {
      found.push({ name, ...result, weight: getWeight(name) });
    }
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
    if (a.bet === 'B') {
      weightB += a.weight;
      agreeingB.push(a);
    } else if (a.bet === 'P') {
      weightP += a.weight;
      agreeingP.push(a);
    }
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

function computeFinalConfidence(consensus) {
  if (consensus.agreeing.length < CONFIG.MIN_AGREEMENT) return 0;

  let conf = 0;
  conf += consensus.ratio * 25;
  conf += Math.min(consensus.agreeing.length / 5, 1) * 15;

  const avgInd = consensus.agreeing.reduce((sum, a) => sum + a.confidence, 0) / consensus.agreeing.length;
  conf += (avgInd / 100) * 20;

  let historicalAccuracy = 0;
  for (const a of consensus.agreeing) {
    const st = state.analyzerStats[a.name];
    const total = st ? st.wins + st.losses : 0;
    historicalAccuracy += total >= 3 ? st.wins / total : 0.5;
  }
  historicalAccuracy /= consensus.agreeing.length;
  conf += historicalAccuracy * 20;

  const e = state.regime.entropy;
  const regimeScore = Math.max(0, Math.min(1, (1.6 - e) / 0.6));
  conf += regimeScore * 10;

  conf += Math.min(state.history.length / 40, 1) * 5;

  const totalGames = state.wins + state.losses;
  if (totalGames >= 10) {
    conf += Math.max(0, Math.min(5, (state.wins / totalGames - 0.5) * 20));
  }

  conf -= consensus.disagreeing.length * 2.5;

  return Math.max(0, Math.min(100, Math.round(conf)));
}

/* =============================================================
   PROCESSAMENTO DE RESULTADO
   ============================================================= */
function mapSide(raw) {
  if (raw == null) return null;
  const s = String(raw).trim().toLowerCase();
  if (['b', 'banker', 'banco', '1', 'blue', 'azul'].includes(s)) return 'B';
  if (['p', 'player', 'jogador', '2', 'red', 'vermelho'].includes(s)) return 'P';
  if (['t', 'tie', 'empate', '0', 'green', 'verde'].includes(s)) return 'T';
  return null;
}

async function processResult({ round, side }) {
  if (!side) return;

  // Reset diário
  if (getAngolaDateKey() !== state.dayKey) {
    await resetForNewDay();
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

    const total = state.wins + state.losses;
    const accuracy = total ? Math.round((state.wins / total) * 100) : 0;

    await tgSend(
      won
        ? `✅ *${BRAND} — WIN*\n🎯 ${tgLabel(pending.bet)}\n📊 Hoje: ${state.wins}W / \( {state.losses}L ( \){accuracy}%)`
        : `❌ *${BRAND} — LOSS*\n🎯 ${tgLabel(pending.bet)} (saiu ${tgLabel(side)})\n📊 Hoje: ${state.wins}W / \( {state.losses}L ( \){accuracy}%)`
    );
  }

  // Adiciona ao histórico
  state.history.push({ round, side, ts: Date.now() });
  if (state.history.length > MAX_HISTORY) {
    state.history.shift();
  }
  updateRegime();

  // Gera novo sinal
  if (!state.stopped && !state.pendingSignal) {
    const analyzers = runAnalyzers();
    const consensus = computeConsensus(analyzers);

    if (consensus) {
      const canSignal = CONFIG.COOLDOWN_MS === 0 || (Date.now() - state.lastSignalTs >= CONFIG.COOLDOWN_MS);
      if (canSignal) {
        const confidence = computeFinalConfidence(consensus);
        if (confidence >= CONFIG.MIN_CONFIDENCE) {
          state.pendingSignal = {
            bet: consensus.bet,
            confidence,
            analyzers: consensus.agreeing.map(a => a.name),
            ts: Date.now(),
          };
          state.lastSignalTs = Date.now();
          state.totalSignals++;

          await tgSend(
            `🚨 *\( {BRAND}* 🚨\n🎯 *SINAL BAC BO*\n➡️ Entrada: * \){tgLabel(consensus.bet)}*\n💯 Confiança: *${confidence}%*\n📊 Hoje: ${state.wins}W / ${state.losses}L`
          );
        }
      }
    }
  }

  saveState();
}

/* =============================================================
   RESET DIÁRIO
   ============================================================= */
async function resetForNewDay() {
  const previous = {
    wins: state.wins,
    losses: state.losses,
    signals: state.totalSignals,
  };

  state.history = [];
  state.wins = 0;
  state.losses = 0;
  state.totalSignals = 0;
  state.pendingSignal = null;
  state.lastSignalTs = 0;
  state.dayKey = getAngolaDateKey();
  saveState();

  const total = previous.wins + previous.losses;
  const accuracy = total ? Math.round((previous.wins / total) * 100) : 0;

  await tgSend(
    `🔄 *${BRAND} — NOVO DIA*\n📅 00:00 Angola\n📊 Anterior: ${previous.wins}W / \( {previous.losses}L ( \){accuracy}%)`
  );
}

/* =============================================================
   FONTES DE DADOS
   ============================================================= */

// --- Webhook (principal) ---
const app = express();
app.use(express.json({ limit: '64kb' }));

app.get('/health', (_req, res) => {
  res.json({
    brand: BRAND,
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

app.post('/webhook/result', async (req, res) => {
  try {
    if (CONFIG.WEBHOOK_SECRET && req.headers['x-secret'] !== CONFIG.WEBHOOK_SECRET) {
      return res.status(401).json({ error: 'unauthorized' });
    }

    const { round, result, side: rawSide } = req.body || {};
    const side = mapSide(result || rawSide);

    if (!side) {
      return res.status(400).json({ error: 'invalid result' });
    }

    if (isProcessed(round)) {
      return res.json({ ok: true, skipped: 'already_processed' });
    }

    markProcessed(round);
    await processResult({ round, side });

    res.json({ ok: true, side });
  } catch (e) {
    log.error('Erro no webhook:', e.message);
    res.status(500).json({ error: 'internal_error' });
  }
});

// --- API Polling ---
function normalizeHistory(payload) {
  if (!payload) return [];
  const list = Array.isArray(payload)
    ? payload
    : (payload.data || payload.results || payload.history || []);

  return list
    .map(item => ({
      round: item.id || item.round || item.round_id || item.roundId,
      side: mapSide(item.outcome || item.result || item.winner || item.color || item.side),
    }))
    .filter(x => x.side && x.round);
}

async function pollHistory() {
  if (!CONFIG.HISTORY_URL) return;

  try {
    const headers = { Accept: 'application/json' };
    if (CONFIG.API_KEY) headers['x-api-key'] = CONFIG.API_KEY;

    const response = await fetch(CONFIG.HISTORY_URL, { headers });
    if (!response.ok) {
      log.warn('pollHistory status:', response.status);
      return;
    }

    const results = normalizeHistory(await response.json());
    results.reverse(); // processa do mais antigo para o mais novo

    for (const r of results) {
      if (!isProcessed(r.round)) {
        markProcessed(r.round);
        await processResult(r);
      }
    }
  } catch (e) {
    log.warn('pollHistory erro:', e.message);
  }
}

// --- Scraper (opcional e isolado) ---
let browser = null;
let scraperTimer = null;

async function startScraper() {
  log.info('Iniciando scraper (experimental)...');

  try {
    const puppeteer = require('puppeteer-extra');
    const StealthPlugin = require('puppeteer-extra-plugin-stealth');
    puppeteer.use(StealthPlugin());

    browser = await puppeteer.launch({
      headless: 'new',
      args: [
        '--no-sandbox',
        '--disable-setuid-sandbox',
        '--disable-dev-shm-usage',
        '--disable-blink-features=AutomationControlled',
      ],
    });

    const page = await browser.newPage();
    await page.setUserAgent(
      'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    );
    await page.goto(CONFIG.SCRAPER_URL, { waitUntil: 'networkidle2', timeout: 90000 });
    log.info('Página do scraper carregada');

    const seen = new Set();

    scraperTimer = setInterval(async () => {
      try {
        const rounds = await page.evaluate(() => {
          const out = [];
          const selectors = [
            '[data-round]', '.bacbo-round', '.round-result', '.history-item',
            '.result-item', '[class*="result"]', '[class*="history"]',
          ];

          let elements = [];
          for (const sel of selectors) {
            elements = document.querySelectorAll(sel);
            if (elements.length > 2) break;
          }

          elements.forEach((el, i) => {
            const round =
              el.getAttribute('data-round') ||
              el.getAttribute('data-id') ||
              el.getAttribute('data-game-id') ||
              `idx-${i}`;

            let side = null;
            const cls = (el.className || '').toLowerCase();
            const txt = (el.textContent || '').toLowerCase();

            if (cls.includes('blue') || cls.includes('banker') || cls.includes('azul') ||
                txt.includes('banker') || txt.includes('azul')) {
              side = 'B';
            } else if (cls.includes('red') || cls.includes('player') || cls.includes('vermelho') ||
                       txt.includes('player') || txt.includes('vermelho')) {
              side = 'P';
            } else if (cls.includes('green') || cls.includes('tie') || cls.includes('empate') ||
                       txt.includes('tie') || txt.includes('empate')) {
              side = 'T';
            }

            if (side) out.push({ round: String(round), side });
          });

          return out;
        });

        for (const r of rounds) {
          if (!seen.has(r.round) && !isProcessed(r.round)) {
            seen.add(r.round);
            markProcessed(r.round);
            await processResult(r);
            log.info(`Scraper → ${r.round} = ${r.side}`);
          }
        }
      } catch (e) {
        log.warn('Erro no loop do scraper:', e.message);
      }
    }, CONFIG.SCRAPE_MS);
  } catch (e) {
    log.error('Falha ao iniciar scraper:', e.message);
    log.warn('Continuando sem scraper. Use webhook ou API.');
  }
}

/* =============================================================
   COMANDOS ADMIN
   ============================================================= */
bot.onText(/\/status/, async (msg) => {
  if (!isAdmin(msg)) return;

  const total = state.wins + state.losses;
  const accuracy = total ? Math.round((state.wins / total) * 100) : 0;

  const analyzerLines = Object.entries(state.analyzerStats)
    .map(([name, st]) => {
      const t = st.wins + st.losses;
      return `  ${name}: \( {t ? Math.round((st.wins / t) * 100) : 0}% ( \){t})`;
    })
    .join('\n');

  await bot.sendMessage(
    msg.chat.id,
    `📊 *${BRAND} — STATUS*\n` +
    `📅 ${state.dayKey}\n` +
    `🎯 Hoje: ${state.wins}W / \( {state.losses}L ( \){accuracy}%)\n` +
    `📡 Sinais: ${state.totalSignals}\n` +
    `🌡 Entropia: ${state.regime.entropy.toFixed(2)}\n` +
    `⏸ Pausado: ${state.stopped ? 'Sim' : 'Não'}\n\n` +
    `*Analisadores:*\n${analyzerLines}`,
    { parse_mode: 'Markdown' }
  );
});

bot.onText(/\/stop/, async (msg) => {
  if (!isAdmin(msg)) return;
  state.stopped = true;
  saveState();
  await bot.sendMessage(msg.chat.id, `🛑 ${BRAND} pausado.`);
});

bot.onText(/\/start/, async (msg) => {
  if (!isAdmin(msg)) return;
  state.stopped = false;
  saveState();
  await bot.sendMessage(msg.chat.id, `▶️ ${BRAND} retomado.`);
});

bot.onText(/\/reset/, async (msg) => {
  if (!isAdmin(msg)) return;
  state.history = [];
  state.wins = 0;
  state.losses = 0;
  state.totalSignals = 0;
  state.pendingSignal = null;
  state.stopped = false;
  saveState();
  await bot.sendMessage(msg.chat.id, `♻️ ${BRAND} resetado.`);
});

bot.on('polling_error', (error) => {
  log.error('Telegram polling error:', error.message);
});

/* =============================================================
   INICIALIZAÇÃO
   ============================================================= */
let pollTimer = null;

app.listen(CONFIG.PORT, () => {
  log.info(`${BRAND} v3.1 — Bac Bo Brasileiro`);
  log.info(`Fonte: ${CONFIG.SOURCE}`);
  log.info(`Porta: ${CONFIG.PORT}`);
  log.info(`Webhook: POST http://localhost:${CONFIG.PORT}/webhook/result`);
  log.info(`Health:  GET  http://localhost:${CONFIG.PORT}/health`);
});

// Inicia a fonte de dados escolhida
if (CONFIG.SOURCE === 'scraper') {
  startScraper().catch(e => log.error('Erro ao iniciar scraper:', e.message));
} else if (CONFIG.SOURCE === 'api' && CONFIG.HISTORY_URL) {
  log.info(`Iniciando polling da API a cada ${CONFIG.POLL_MS}ms`);
  pollTimer = setInterval(pollHistory, CONFIG.POLL_MS);
  pollHistory();
} else {
  log.info('Modo webhook ativo — aguardando POST /webhook/result');
}

// Verifica reset diário a cada 30 segundos
setInterval(() => {
  if (getAngolaDateKey() !== state.dayKey) {
    resetForNewDay().catch(e => log.error('Erro no reset diário:', e.message));
  }
}, 30000);

// Mensagem de online
tgSend(`🚀 *\( {BRAND}* online — Bac Bo ( \){CONFIG.SOURCE})`).catch(() => {});

/* =============================================================
   SHUTDOWN GRACEFUL
   ============================================================= */
async function shutdown(signal) {
  log.info(`Recebido ${signal}, encerrando...`);
  saveState();

  if (pollTimer) clearInterval(pollTimer);
  if (scraperTimer) clearInterval(scraperTimer);

  if (browser) {
    try { await browser.close(); } catch {}
  }

  try { await bot.stopPolling(); } catch {}

  process.exit(0);
}

process.on('SIGINT', () => shutdown('SIGINT'));
process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('unhandledRejection', (reason) => {
  log.error('Unhandled Rejection:', reason?.message || reason);
});
process.on('uncaughtException', (error) => {
  log.error('Uncaught Exception:', error.message);
});