'use strict';

/**
 * CAPUNGA DADOS — BAC BO · v12 (SEQUENCIAL + NUMÉRICO)
 *
 * - Fonte exclusiva: API Casino.org (Evolution)
 * - Frequência \~5 min, mas SÓ gera sinal DEPOIS da rodada terminar + buffer
 * - NUNCA mais "NÃO APURADO" → timeout força LOSS
 * - Estratégias prioritárias: sequências + padrões numéricos
 * - Gale 1 | Empate vale GREEN (configurável)
 *
 * ENV obrigatórias: BOT_TOKEN, CHAT_ID
 * ENV opcionais:
 *   SINAIS_ATIVOS=true
 *   EMPATE_VALE_GREEN=true
 *   MIN_AMOSTRA=60
 *   SYNC_MS=2000
 *   MIN_INTERVAL_MS=285000   (4m45s)
 *   MAX_INTERVAL_MS=330000   (5m30s)
 *   ATRASO_RODADA_MS=35000   (35s após última rodada)
 *   APURA_MAX_MS=900000      (15 min)
 *   KEEPALIVE_MS=60000
 *   SINAL_INICIAL=1
 *   ADMIN_TOKEN
 *   DATABASE_URL
 *   SELF_URL / RENDER_EXTERNAL_URL
 */

const fs = require('fs');
const crypto = require('crypto');
const express = require('express');
const cors = require('cors');
const axios = require('axios');
const { Pool } = require('pg');

/* ============================ CONFIG ============================ */

const num = (name, def, min, max) => {
  const n = Number.parseInt(process.env[name], 10);
  return Number.isFinite(n) ? Math.max(min, Math.min(max, n)) : def;
};
const bool = (v, def) =>
  v === undefined || v === '' ? def : ['true', '1', 'yes', 'sim', 'on'].includes(String(v).trim().toLowerCase());

const PORT = Number(process.env.PORT || 10000);
const BOT_TOKEN = process.env.BOT_TOKEN || '';
const CHAT_ID = process.env.CHAT_ID || '';
const ADMIN_TOKEN = process.env.ADMIN_TOKEN || '';
const DATABASE_URL = process.env.DATABASE_URL || '';

const API_BACBO =
  'https://api-cs.casino.org/svc-evolution-game-events/api/bacbo?page=0&size=50&sort=data.settledAt,desc&duration=120&wheelResults=PlayerWon,BankerWon,Tie';

const MAX_GALE = 1;

const CFG = {
  ativos: bool(process.env.SINAIS_ATIVOS, true),
  empateGreen: bool(process.env.EMPATE_VALE_GREEN, true),
  minAmostra: num('MIN_AMOSTRA', 60, 30, 5000),
  syncMs: num('SYNC_MS', 2000, 1000, 10000),
  sinalInicial: num('SINAL_INICIAL', 1, 1, 99999999),
  maxHist: 4000,
  gapMs: 7 * 60 * 1000,
  keepAliveMs: num('KEEPALIVE_MS', 60000, 30000, 300000),
  apuraMaxMs: num('APURA_MAX_MS', 900000, 300000, 3600000), // 15 min
  minIntervalMs: num('MIN_INTERVAL_MS', 285000, 240000, 600000), // 4m45s
  maxIntervalMs: num('MAX_INTERVAL_MS', 330000, 270000, 720000), // 5m30s
  atrasoRodadaMs: num('ATRASO_RODADA_MS', 35000, 20000, 90000)  // 35s após rodada
};

const J = 'Jogador';
const B = 'Banca';
const E = 'Empate';
const isCol = (x) => x === J || x === B;
const opp = (c) => (c === J ? B : c === B ? J : null);
const FECHADOS = new Set(['GREEN', 'G1', 'EMPATE', 'LOSS']);

/* ============================ DEPLOY ID ============================ */

function calcDeployId() {
  if (process.env.DEPLOY_ID) return String(process.env.DEPLOY_ID).trim();
  try {
    return crypto.createHash('sha256').update(fs.readFileSync(__filename)).digest('hex').slice(0, 16);
  } catch {
    return 'unknown';
  }
}
const DEPLOY_ID = calcDeployId();

/* ============================ ESTADO ============================ */

const S = {
  hist: [],
  seen: new Set(),
  signals: [],
  nextId: CFG.sinalInicial,
  lastSignalAt: 0,
  stats: null,
  statsAtLen: -1
};

const R = {
  iniciadoEm: new Date().toISOString(),
  deployId: DEPLOY_ID,
  ultimaSync: null,
  ultimaRodadaNova: null,
  ciclosSemNovos: 0,
  erroAPI: null,
  erroTelegram: null,
  ultimoMotivo: null,
  ultimoSinal: null,
  wakes: 0,
  ultimoWake: null,
  ultimoKeepAlive: null
};

let pool = null;
let server = null;
let tickTimer = null;
let keepTimer = null;
let busy = false;
let stopping = false;
let lastTickMs = Date.now();

/* ============================ UTILS ============================ */

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
function log(tag, data) {
  console.log(`[\( {new Date().toISOString()}] [ \){tag}] ${typeof data === 'string' ? data : JSON.stringify(data)}`);
}

const fmtHora = new Intl.DateTimeFormat('pt-PT', {
  timeZone: 'Africa/Luanda', hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false
});
const hora = (ms = Date.now()) => fmtHora.format(new Date(ms));

const corTxt = (c) => (c === B ? '🔴 <b>BANCA</b>' : c === J ? '🔵 <b>JOGADOR</b>' : '🟡 <b>EMPATE</b>');
const barra = (p) => {
  const f = Math.max(0, Math.min(10, Math.round(p / 10)));
  return '▰'.repeat(f) + '▱'.repeat(10 - f);
};

/* ============================ API ============================ */

function normalizeOutcome(v) {
  if (v && typeof v === 'object') v = v.outcome ?? v.resultado ?? v.winner ?? v.name;
  const s = String(v ?? '')
    .normalize('NFD').replace(/[\u0300-\u036f]/g, '')
    .trim().toLowerCase().replace(/[\s_-]+/g, '');
  if (['player', 'jogador', 'playerwin', 'playerwins', 'playerwon', 'blue'].includes(s)) return J;
  if (['banker', 'banca', 'bankerwin', 'bankerwins', 'bankerwon', 'red', 'vermelho'].includes(s)) return B;
  if (['tie', 'draw', 'empate', 'equal', 'push', 'tied'].includes(s)) return E;
  return null;
}

function validMs(v) {
  if (!v) return null;
  const t = new Date(v).getTime();
  return Number.isNaN(t) ? null : t;
}

function extractItems(body) {
  if (Array.isArray(body)) return body;
  if (!body || typeof body !== 'object') return [];
  const candidates = [
    body.data, body.results, body.items, body.resultados, body.rounds, body.events,
    body.data?.results, body.data?.items, body.data?.resultados, body.data?.rounds,
    body.data?.events, body.data?.data
  ];
  return candidates.find(Array.isArray) || [];
}

function normalizeRound(item) {
  const d = item?.data || item?.round || item || {};
  const r = (d.result && typeof d.result === 'object') ? d.result :
            (item?.result && typeof item.result === 'object') ? item.result : {};

  const t = validMs(d.settledAt ?? d.endTime ?? d.endedAt ?? d.finishedAt ?? d.fim ?? item?.settledAt ?? item?.fim);
  const raw = r.outcome ?? r.resultado ?? r.winner ?? d.outcome ?? d.resultado ?? d.winner ??
              item?.outcome ?? item?.resultado ?? item?.winner ??
              (typeof d.result === 'string' ? d.result : null) ??
              (typeof item?.result === 'string' ? item.result : null);

  const resultado = normalizeOutcome(raw);
  const id = String(d.id ?? d.roundId ?? d.round_id ?? d.eventId ?? item?.id ?? item?.roundId ?? item?.round_id ?? item?.eventId ?? '').trim();

  if (!id || !resultado || !t) return null;

  const pj = Number(r.playerDice?.score ?? d.playerDice?.score ?? d.playerScore ?? d.player_score ?? d.pontos_jogador);
  const pb = Number(r.bankerDice?.score ?? d.bankerDice?.score ?? d.bankerScore ?? d.banker_score ?? d.pontos_banca);

  return {
    id,
    resultado,
    t,
    pj: Number.isFinite(pj) ? pj : null,
    pb: Number.isFinite(pb) ? pb : null
  };
}

async function fetchRounds() {
  let lastErr = null;
  for (let i = 1; i <= 3; i++) {
    try {
      const resp = await axios.get(API_BACBO, {
        timeout: 14000,
        headers: { Accept: 'application/json', 'User-Agent': 'CapungaDados/12.0' }
      });
      const map = new Map();
      for (const it of extractItems(resp.data)) {
        const rd = normalizeRound(it);
        if (rd) map.set(rd.id, rd);
      }
      R.erroAPI = null;
      return [...map.values()].sort((a, b) => a.t - b.t || (a.id < b.id ? -1 : 1));
    } catch (e) {
      lastErr = e;
      R.erroAPI = e.message;
      if (i < 3) await sleep(500 * i);
    }
  }
  throw lastErr || new Error('Falha API');
}

/* ============================ HELPERS NUMÉRICOS ============================ */

const hasScore = (r) => r && Number.isFinite(r.pj) && Number.isFinite(r.pb);
const margem = (r) => Math.abs((r.pj || 0) - (r.pb || 0));
const scoreOf = (r) => (r.resultado === J ? r.pj : r.pb);
const scoreRange = (s) => {
  if (!Number.isFinite(s)) return null;
  if (s <= 2) return 'MB';
  if (s <= 4) return 'B';
  if (s <= 6) return 'M';
  if (s <= 8) return 'A';
  return 'MA';
};

function streak(seq) {
  if (!seq.length) return 0;
  let n = 0;
  const cor = seq[0].resultado;
  for (const r of seq) {
    if (r.resultado === cor) n++;
    else break;
  }
  return n;
}

/* ============================ ESTRATÉGIAS (FOCO SEQUENCIAL + NUMÉRICO) ============================ */

const STRATS = [
  // ========== SEQUÊNCIAS FORTES ==========
  function seqRepeticao({ seq }) {
    const n = streak(seq);
    if (n < 3 || n > 7) return null;
    const strength = [0, 0, 0, 72, 78, 84, 86, 86][n] || 86;
    return { cor: seq[0].resultado, key: `SEQ_REP_\( {n}`, nome: `Sequência x \){n}`, emoji: '🔁', strength, tipo: 'seq' };
  },
  function seqQuebra({ seq }) {
    const n = streak(seq);
    if (n < 4 || n > 8) return null;
    const strength = [0, 0, 0, 0, 74, 80, 84, 85, 85][n] || 85;
    return { cor: opp(seq[0].resultado), key: `SEQ_QUEBRA_\( {n}`, nome: `Quebra x \){n}`, emoji: '💥', strength, tipo: 'seq' };
  },
  function seqSurf({ seq }) {
    if (seq.length < 5) return null;
    let L = 1;
    while (L < seq.length && seq[L].resultado !== seq[L - 1].resultado) L++;
    if (L < 4) return null;
    const strength = L >= 6 ? 84 : L >= 5 ? 78 : 72;
    return { cor: opp(seq[0].resultado), key: `SEQ_SURF_${L}`, nome: `Surf ${L}`, emoji: '🌊', strength, tipo: 'seq' };
  },
  function seqDoisDois({ seq }) {
    if (seq.length < 4) return null;
    const [a, b, c, d] = seq.map(x => x.resultado);
    if (a === b && c === d && a !== c) {
      return { cor: c, key: 'SEQ_2x2', nome: 'Bloco 2+2', emoji: '♟️', strength: 76, tipo: 'seq' };
    }
    return null;
  },
  function seqTresUm({ seq }) {
    if (seq.length < 4) return null;
    const [a, b, c, d] = seq.map(x => x.resultado);
    if (a !== b && b === c && c === d) {
      return { cor: a, key: 'SEQ_3_1', nome: '3→1 confirmado', emoji: '🧩', strength: 78, tipo: 'seq' };
    }
    return null;
  },

  // ========== NUMÉRICOS (PRIORIDADE ALTA) ==========
  function numEscada({ seq }) {
    if (seq.length < 3 || !seq.slice(0, 3).every(hasScore)) return null;
    const [a, b, c] = seq;
    if (a.resultado !== b.resultado || b.resultado !== c.resultado) return null;
    const sa = scoreOf(a), sb = scoreOf(b), sc = scoreOf(c);
    if ((sb - sa === 1 && sc - sb === 1) || (sa - sb === 1 && sb - sc === 1)) {
      return { cor: a.resultado, key: 'NUM_ESCADA', nome: `Escada \( {sa}→ \){sb}→${sc}`, emoji: '🪜', strength: 88, tipo: 'num' };
    }
    return null;
  },
  function numRepeteScore({ seq }) {
    if (seq.length < 2 || !hasScore(seq[0]) || !hasScore(seq[1])) return null;
    const [a, b] = seq;
    if (a.resultado !== b.resultado) return null;
    if (scoreOf(a) === scoreOf(b)) {
      return { cor: a.resultado, key: 'NUM_REPETE', nome: `Score repetido ${scoreOf(a)}`, emoji: '🔂', strength: 84, tipo: 'num' };
    }
    return null;
  },
  function numGatilho({ seq }) {
    if (!seq.length || !hasScore(seq[0])) return null;
    const r = seq[0];
    const s = scoreOf(r);
    if (s === 0 || s === 1 || s === 9) {
      return { cor: opp(r.resultado), key: `NUM_GATILHO_${s}`, nome: `Gatilho extremo ${s}`, emoji: '🎯', strength: 86, tipo: 'num' };
    }
    if (s === 8) {
      return { cor: opp(r.resultado), key: 'NUM_GATILHO_8', nome: 'Gatilho 8', emoji: '🎯', strength: 78, tipo: 'num' };
    }
    return null;
  },
  function numMargemAlta({ rows }) {
    const r = rows[0];
    if (!r || !isCol(r.resultado) || !hasScore(r) || margem(r) < 5) return null;
    return { cor: r.resultado, key: 'NUM_MARGEM5', nome: `Margem alta \( {r.pj}× \){r.pb}`, emoji: '📊', strength: 82, tipo: 'num' };
  },
  function numDiff4x3({ seq }) {
    if (seq.length < 3) return null;
    const t = seq.slice(0, 3);
    if (!t.every(r => hasScore(r) && margem(r) >= 4 && r.resultado === t[0].resultado)) return null;
    return { cor: t[0].resultado, key: 'NUM_DIFF4x3', nome: 'Diff ≥4 x3', emoji: '🔢', strength: 85, tipo: 'num' };
  },
  function numViradaBaixa({ seq }) {
    if (seq.length < 2) return null;
    const [a, b] = seq;
    if (!hasScore(a) || !hasScore(b) || a.resultado !== b.resultado) return null;
    if (scoreOf(a) > 4 || scoreOf(b) > 4) return null;
    return { cor: opp(a.resultado), key: 'NUM_VIRADA_BAIXA', nome: 'Virada de baixos', emoji: '🔄', strength: 80, tipo: 'num' };
  },
  function numEspelho({ rows }) {
    const r = rows[0];
    if (!r || !hasScore(r) || r.pj !== r.pb) return null;
    return { cor: r.resultado, key: 'NUM_ESPELHO', nome: `Espelho \( {r.pj}× \){r.pb}`, emoji: '🪞', strength: 83, tipo: 'num' };
  },
  function numTendencia({ rows }) {
    const s = rows.slice(0, 8).filter(hasScore);
    if (s.length < 6) return null;
    const mj = s.reduce((a, r) => a + r.pj, 0) / s.length;
    const mb = s.reduce((a, r) => a + r.pb, 0) / s.length;
    const diff = mj - mb;
    if (Math.abs(diff) < 1.5) return null;
    return {
      cor: diff > 0 ? J : B,
      key: 'NUM_TENDENCIA',
      nome: `Tendência \( {mj.toFixed(1)}× \){mb.toFixed(1)}`,
      emoji: '📈',
      strength: Math.abs(diff) >= 2.2 ? 84 : 76,
      tipo: 'num'
    };
  },
  function numRangeSalto({ seq }) {
    if (seq.length < 2 || !hasScore(seq[0]) || !hasScore(seq[1])) return null;
    const [a, b] = seq;
    const ra = scoreRange(scoreOf(a));
    const rb = scoreRange(scoreOf(b));
    if (!ra || !rb) return null;
    if ((ra === 'MB' && rb === 'MA') || (ra === 'MA' && rb === 'MB')) {
      return { cor: opp(a.resultado), key: 'NUM_SALTO_RANGE', nome: `Salto \( {scoreOf(b)}→ \){scoreOf(a)}`, emoji: '📐', strength: 81, tipo: 'num' };
    }
    return null;
  },
  function numPosEmpate({ rows, seq }) {
    if (!rows[0] || rows[0].resultado !== E || !seq.length) return null;
    return { cor: seq[0].resultado, key: 'NUM_POS_EMPATE', nome: 'Pós-empate', emoji: '🎯', strength: 74, tipo: 'num' };
  }
];

function runStrategies(rows) {
  const seq = rows.filter(r => isCol(r.resultado));
  const ctx = { rows, seq };
  const out = [];
  for (const fn of STRATS) {
    try {
      const r = fn(ctx);
      if (Array.isArray(r)) {
        for (const c of r) if (c && isCol(c.cor)) out.push(c);
      } else if (r && isCol(r.cor)) {
        out.push(r);
      }
    } catch (_) {}
  }
  return out;
}

/* ============================ BACKTEST SIMPLES ============================ */

function windowAt(H, i) {
  const rows = [];
  for (let j = i; j >= 0 && rows.length < 20; j--) {
    if (rows.length && rows[rows.length - 1].t - H[j].t > CFG.gapMs) break;
    rows.push(H[j]);
  }
  return rows;
}

function evalEntry(H, i, cor) {
  for (let k = 0; k <= MAX_GALE; k++) {
    const idx = i + 1 + k;
    if (idx >= H.length) return null;
    if (H[idx].t - H[idx - 1].t > CFG.gapMs) return null;
    const r = H[idx].resultado;
    if (r === cor || (r === E && CFG.empateGreen)) return true;
  }
  return false;
}

function backtest() {
  const H = S.hist;
  const per = {};
  const ini = 10;
  const fim = H.length - 1;
  if (fim - ini < 30) return { per, baseRate: 0.52 };

  let bw = 0, bn = 0;
  for (let i = ini; i < fim; i++) {
    const win = windowAt(H, i);
    if (win.length < 6) continue;
    for (const c of runStrategies(win)) {
      const v = evalEntry(H, i, c.cor);
      if (v === null) continue;
      const p = per[c.key] || (per[c.key] = { n: 0, w: 0 });
      p.n++;
      if (v) p.w++;
      bn++;
      if (v) bw++;
    }
  }
  return { per, baseRate: bn ? bw / bn : 0.52 };
}

function getStats(force = false) {
  if (force || !S.stats || S.hist.length - S.statsAtLen >= 30) {
    S.stats = backtest();
    S.statsAtLen = S.hist.length;
  }
  return S.stats;
}

function wilsonLB(w, n, z = 1.64) {
  if (!n || n < 5) return 0.5;
  const p = w / n;
  const z2 = z * z;
  const c = p + z2 / (2 * n);
  const m = z * Math.sqrt((p * (1 - p)) / n + z2 / (4 * n * n));
  return Math.max(0.35, (c - m) / (1 + z2 / n));
}

function suspenso(key) {
  const recent = S.signals.filter(x => x.key === key && FECHADOS.has(x.status)).slice(-5);
  if (recent.length < 4) return false;
  const losses = recent.filter(x => x.status === 'LOSS').length;
  return losses >= 3;
}

/* ============================ CONFIANÇA ============================ */

function confianca(cand, stats) {
  const p = stats.per[cand.key] || { n: 0, w: 0 };
  const bt = p.n >= 6 ? wilsonLB(p.w, p.n) : (stats.baseRate || 0.52);

  const live = S.signals.filter(x => x.key === cand.key && FECHADOS.has(x.status));
  const liveW = live.filter(x => x.status !== 'LOSS').length;
  const liveRate = live.length >= 3 ? liveW / live.length : bt;

  let score = 0;
  // Prioridade numérica e sequencial
  const pesoTipo = cand.tipo === 'num' ? 1.18 : cand.tipo === 'seq' ? 1.12 : 1.0;

  score += bt * 36;
  score += liveRate * 24;
  score += (cand.strength / 100) * 22;
  score += Math.min(1, (p.n || 0) / 25) * 10;
  score *= pesoTipo;

  return Math.max(62, Math.min(93, Math.round(score)));
}

/* ============================ TELEGRAM ============================ */

async function tg(texto) {
  if (!BOT_TOKEN || !CHAT_ID) {
    R.erroTelegram = 'BOT_TOKEN ou CHAT_ID em falta';
    return false;
  }
  for (let t = 1; t <= 3; t++) {
    try {
      const r = await axios.post(`https://api.telegram.org/bot${BOT_TOKEN}/sendMessage`, {
        chat_id: CHAT_ID,
        text: texto,
        parse_mode: 'HTML',
        disable_web_page_preview: true
      }, { timeout: 11000 });
      if (r.data?.ok) {
        R.erroTelegram = null;
        return true;
      }
      R.erroTelegram = JSON.stringify(r.data);
    } catch (e) {
      R.erroTelegram = e.response?.data ? JSON.stringify(e.response.data) : e.message;
      const retry = Number(e.response?.data?.parameters?.retry_after);
      if (retry > 0 && retry <= 20) await sleep(retry * 1000 + 200);
    }
    if (t < 3) await sleep(1100 * t);
  }
  return false;
}

/* ============================ APURAÇÃO (SEM NÃO APURADO) ============================ */

function apurar() {
  const now = Date.now();

  for (const s of S.signals) {
    if (FECHADOS.has(s.status)) continue;

    const after = S.hist.filter(r => r.t > s.t).sort((a, b) => a.t - b.t);

    // Timeout → LOSS obrigatório
    if (after.length === 0) {
      if (now - s.t >= CFG.apuraMaxMs) {
        s.status = 'LOSS';
        s.fechadoEm = now;
        tg(`❌ <b>SINAL #${s.id} → LOSS</b>\nMotivo: timeout de apuração\n⏱ ${hora()}`).catch(() => {});
      }
      continue;
    }

    let hit = false;
    let gale = 0;
    let empate = false;

    for (let k = 0; k <= MAX_GALE; k++) {
      if (k >= after.length) break;
      const r = after[k];
      const prevT = k === 0 ? s.t : after[k - 1].t;
      if (r.t - prevT > CFG.gapMs) break;

      if (r.resultado === s.cor) {
        hit = true;
        gale = k;
        break;
      }
      if (r.resultado === E && CFG.empateGreen) {
        hit = true;
        gale = k;
        empate = true;
        break;
      }
    }

    if (hit) {
      s.status = empate ? 'EMPATE' : (gale === 0 ? 'GREEN' : 'G1');
      s.fechadoEm = now;
      s.gale = gale;
      const emoji = s.status === 'GREEN' ? '✅' : s.status === 'G1' ? '✅✅' : '🟡';
      tg(`\( {emoji} <b>SINAL # \){s.id} → \( {s.status}</b>\n \){corTxt(s.cor)}\n⏱ ${hora()}`).catch(() => {});
    } else if (after.length > MAX_GALE || now - s.t >= CFG.apuraMaxMs) {
      s.status = 'LOSS';
      s.fechadoEm = now;
      tg(`❌ <b>SINAL #\( {s.id} → LOSS</b>\n \){corTxt(s.cor)}\n⏱ ${hora()}`).catch(() => {});
    }
  }
}

/* ============================ GERAÇÃO DE SINAL ============================ */

function podeGerar() {
  if (!CFG.ativos) {
    R.ultimoMotivo = 'sinais desativados';
    return false;
  }
  if (S.signals.some(s => !FECHADOS.has(s.status))) {
    R.ultimoMotivo = 'existe sinal pendente';
    return false;
  }

  const now = Date.now();
  const desde = now - (S.lastSignalAt || 0);
  if (desde < CFG.minIntervalMs) {
    R.ultimoMotivo = `aguardar intervalo (${Math.round((CFG.minIntervalMs - desde) / 1000)}s)`;
    return false;
  }

  const last = S.hist[S.hist.length - 1];
  if (!last) {
    R.ultimoMotivo = 'sem histórico';
    return false;
  }
  if (now - last.t < CFG.atrasoRodadaMs) {
    R.ultimoMotivo = `aguardar ${Math.round((CFG.atrasoRodadaMs - (now - last.t)) / 1000)}s após rodada`;
    return false;
  }
  if (S.hist.length < CFG.minAmostra) {
    R.ultimoMotivo = `amostra baixa (${S.hist.length})`;
    return false;
  }
  return true;
}

function tentarSinal() {
  if (!podeGerar()) return;

  const rows = S.hist.slice(-25).reverse();
  const cands = runStrategies(rows);
  if (!cands.length) {
    R.ultimoMotivo = 'nenhuma estratégia ativada';
    return;
  }

  const stats = getStats();
  let melhor = null;
  let melhorScore = 0;

  // Prioriza numéricos e sequenciais
  const ordenados = cands.sort((a, b) => {
    const pa = a.tipo === 'num' ? 3 : a.tipo === 'seq' ? 2 : 1;
    const pb = b.tipo === 'num' ? 3 : b.tipo === 'seq' ? 2 : 1;
    return pb - pa;
  });

  for (const c of ordenados) {
    if (suspenso(c.key)) continue;
    const conf = confianca(c, stats);
    if (conf < 74) continue;
    if (conf > melhorScore) {
      melhorScore = conf;
      melhor = { ...c, conf };
    }
  }

  if (!melhor) {
    R.ultimoMotivo = 'nenhum candidato com confiança suficiente';
    return;
  }

  const id = S.nextId++;
  const now = Date.now();
  const sinal = {
    id,
    cor: melhor.cor,
    key: melhor.key,
    nome: melhor.nome,
    emoji: melhor.emoji,
    conf: melhor.conf,
    tipo: melhor.tipo,
    status: 'PENDENTE',
    t: now,
    criadoEm: now
  };

  S.signals.push(sinal);
  if (S.signals.length > 200) S.signals = S.signals.slice(-150);
  S.lastSignalAt = now;
  R.ultimoSinal = sinal;
  R.ultimoMotivo = null;

  const msg = [
    `🎯 <b>SINAL #${id}</b>`,
    `${melhor.emoji} ${melhor.nome}`,
    ``,
    `Entrada: ${corTxt(melhor.cor)}`,
    `Confiança: <b>${melhor.conf}%</b> ${barra(melhor.conf)}`,
    `Tipo: ${melhor.tipo === 'num' ? 'Numérico' : 'Sequencial'}`,
    `Gale máximo: 1`,
    CFG.empateGreen ? `Empate = GREEN` : `Empate = LOSS`,
    ``,
    `⏱ ${hora()}`
  ].join('\n');

  tg(msg).catch(() => {});
  log('SINAL', { id, cor: melhor.cor, conf: melhor.conf, key: melhor.key, tipo: melhor.tipo });
}

/* ============================ SYNC ============================ */

async function sync() {
  try {
    const rounds = await fetchRounds();
    let novos = 0;
    for (const r of rounds) {
      if (S.seen.has(r.id)) continue;
      S.seen.add(r.id);
      S.hist.push(r);
      novos++;
      R.ultimaRodadaNova = new Date(r.t).toISOString();
    }
    if (S.hist.length > CFG.maxHist) {
      const excesso = S.hist.length - CFG.maxHist;
      for (let i = 0; i < excesso; i++) S.seen.delete(S.hist[i].id);
      S.hist = S.hist.slice(excesso);
    }
    S.hist.sort((a, b) => a.t - b.t || (a.id < b.id ? -1 : 1));
    R.ultimaSync = new Date().toISOString();
    R.ciclosSemNovos = novos ? 0 : R.ciclosSemNovos + 1;
    if (novos) log('SYNC', { novos, total: S.hist.length });
  } catch (e) {
    R.erroAPI = e.message;
    log('SYNC ERRO', e.message);
  }
}

/* ============================ LOOP ============================ */

async function tick() {
  if (busy || stopping) return;
  busy = true;
  lastTickMs = Date.now();
  try {
    await sync();
    apurar();       // fecha primeiro
    tentarSinal();  // só depois tenta gerar
  } catch (e) {
    log('TICK ERRO', e.message);
  } finally {
    busy = false;
  }
}

/* ============================ KEEP-ALIVE ============================ */

async function keepAlive() {
  R.ultimoKeepAlive = new Date().toISOString();
  const self = process.env.SELF_URL || process.env.RENDER_EXTERNAL_URL || '';
  if (self) {
    try {
      await axios.get(self.replace(/\/$/, '') + '/health', { timeout: 7000 });
    } catch (_) {}
  }
}

function wakeCheck() {
  const agora = Date.now();
  if (agora - lastTickMs > CFG.syncMs * 5) {
    R.wakes++;
    R.ultimoWake = new Date().toISOString();
    log('WAKE', `atraso ${Math.round((agora - lastTickMs) / 1000)}s`);
    tick().catch(() => {});
  }
}

/* ============================ EXPRESS ============================ */

const app = express();
app.disable('x-powered-by');
app.use(cors());
app.use(express.json({ limit: '200kb' }));

app.get('/health', (req, res) => {
  res.json({
    ok: true,
    versao: '12.0',
    deployId: DEPLOY_ID,
    hist: S.hist.length,
    sinais: S.signals.length,
    ultimoSinal: R.ultimoSinal ? { id: R.ultimoSinal.id, status: R.ultimoSinal.status, conf: R.ultimoSinal.conf } : null,
    ultimaSync: R.ultimaSync,
    erroAPI: R.erroAPI,
    uptime: Math.round(process.uptime())
  });
});

app.get('/status', (req, res) => {
  if (ADMIN_TOKEN && req.query.token !== ADMIN_TOKEN) return res.status(401).json({ erro: 'token' });
  res.json({
    hist: S.hist.length,
    nextId: S.nextId,
    lastSignalAt: S.lastSignalAt,
    motivo: R.ultimoMotivo,
    R
  });
});

/* ============================ START ============================ */

async function start() {
  log('BOOT', {
    versao: '12.0',
    deployId: DEPLOY_ID,
    minInterval: CFG.minIntervalMs,
    atrasoRodada: CFG.atrasoRodadaMs,
    apuraMax: CFG.apuraMaxMs
  });

  if (DATABASE_URL) {
    try {
      pool = new Pool({
        connectionString: DATABASE_URL,
        ssl: /localhost|127\.0\.0\.1/i.test(DATABASE_URL) ? false : { rejectUnauthorized: false },
        max: 3
      });
      await pool.query('SELECT 1');
      log('DB', 'ok');
    } catch (e) {
      log('DB ERRO', e.message);
      pool = null;
    }
  }

  await sync();
  getStats(true);

  server = app.listen(PORT, () => log('HTTP', `porta ${PORT}`));

  tickTimer = setInterval(() => tick().catch(() => {}), CFG.syncMs);
  keepTimer = setInterval(() => {
    keepAlive().catch(() => {});
    wakeCheck();
  }, CFG.keepAliveMs);

  setTimeout(() => tick().catch(() => {}), 2000);

  process.on('SIGTERM', shutdown);
  process.on('SIGINT', shutdown);
}

async function shutdown() {
  if (stopping) return;
  stopping = true;
  log('SHUTDOWN', 'ok');
  clearInterval(tickTimer);
  clearInterval(keepTimer);
  if (server) server.close();
  if (pool) await pool.end().catch(() => {});
  process.exit(0);
}

start().catch(e => {
  console.error(e);
  process.exit(1);
});