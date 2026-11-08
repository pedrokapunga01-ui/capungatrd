'use strict';

/**
 * CAPUNGA DADOS — BAC BO · v11.1 (FIXED)
 *
 * Correções principais:
 * 1. Nunca mais "NÃO APURADO". Após timeout força LOSS. Histórico sempre limpo.
 * 2. Frequência flexível 4m30s–5m30s + atraso mínimo após a última rodada (25-40s).
 *    Impede sinal + green no mesmo instante.
 * 3. Ordem rígida: sync → apurar → gerar sinal.
 * 4. Gale 1 (máx. 2 tentativas). Empate vale green (configurável).
 *
 * ENV obrigatórias: BOT_TOKEN, CHAT_ID
 * ENV opcionais:    DATABASE_URL, ADMIN_TOKEN, SELF_URL/RENDER_EXTERNAL_URL
 *                   MIN_AMOSTRA(60) · SYNC_MS(2000) · SINAL_INICIAL(1) · SINAIS_ATIVOS(true)
 *                   EMPATE_VALE_GREEN(true) · KEEPALIVE_MS(60000) · DEPLOY_ID
 *                   APURA_GAP_MS(180000) · APURA_MAX_MS(1200000)
 *                   MIN_INTERVAL_MS(270000) · MAX_INTERVAL_MS(330000) · ATRASO_RODADA_MS(30000)
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

// 🚨 TRAVA DE SEGURANÇA: API FIXA NO CASINO.ORG 🚨
const API_BACBO =
  'https://api-cs.casino.org/svc-evolution-game-events/api/bacbo?page=0&size=50&sort=data.settledAt,desc&duration=120&wheelResults=PlayerWon,BankerWon,Tie';

const MAX_GALE = 1;

const CFG = {
  ativos: bool(process.env.SINAIS_ATIVOS, true),
  empateGreen: bool(process.env.EMPATE_VALE_GREEN, true),
  minAmostra: num('MIN_AMOSTRA', 60, 20, 5000),
  syncMs: num('SYNC_MS', 2000, 1000, 15000),
  sinalInicial: num('SINAL_INICIAL', 1, 1, 100000000),
  maxHist: 5000,
  gapMs: 6 * 60 * 1000,
  frescoMs: 4 * 60 * 1000,
  keepAliveMs: num('KEEPALIVE_MS', 60000, 30000, 300000),
  apuraGapMs: num('APURA_GAP_MS', 180000, 60000, 1800000),
  apuraMaxMs: num('APURA_MAX_MS', 1200000, 300000, 7200000),
  // NOVO: intervalo flexível + atraso após rodada
  minIntervalMs: num('MIN_INTERVAL_MS', 270000, 180000, 600000),   // 4m30s
  maxIntervalMs: num('MAX_INTERVAL_MS', 330000, 240000, 900000),   // 5m30s
  atrasoRodadaMs: num('ATRASO_RODADA_MS', 30000, 15000, 90000)     // 30s após última rodada
};

const J = 'Jogador';
const B = 'Banca';
const E = 'Empate';
const isCol = (x) => x === J || x === B;
const opp = (c) => (c === J ? B : c === B ? J : null);
const FECHADOS = ['GREEN', 'G1', 'EMPATE', 'LOSS'];

/* ============================ DEPLOY ID / RESET ============================ */

function calcDeployId() {
  if (process.env.DEPLOY_ID) return String(process.env.DEPLOY_ID).trim();
  try {
    const buf = fs.readFileSync(__filename);
    return crypto.createHash('sha256').update(buf).digest('hex').slice(0, 16);
  } catch {
    return '';
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
  erroDB: null,
  erroTelegram: null,
  ultimoMotivo: null,
  ultimoSinal: null,
  telegramPendentes: 0,
  ultimoKeepAlive: null,
  ultimoWake: null,
  wakes: 0,
  naoApurados: 0 // mantido só para log, nunca mais usado como status
};

let pool = null;
let server = null;
let syncTimer = null;
let tickTimer = null;
let keepTimer = null;
let busy = false;
let stopping = false;
let lastTickMs = Date.now();

/* ============================ UTILS ============================ */

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
function log(tag, data) {
  const p = typeof data === 'string' ? data : JSON.stringify(data);
  console.log(`[\( {new Date().toISOString()}] [ \){tag}] ${p}`);
}

const fmtHora = new Intl.DateTimeFormat('pt-PT', {
  timeZone: 'Africa/Luanda', hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false
});
const fmtDia = new Intl.DateTimeFormat('en-CA', {
  timeZone: 'Africa/Luanda', year: 'numeric', month: '2-digit', day: '2-digit'
});
const hora = (ms = Date.now()) => fmtHora.format(new Date(ms));
const dia = (ms = Date.now()) => fmtDia.format(new Date(ms));

const esc = (s) => String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
const corTxt = (c) => (c === B ? '🔴 <b>BANCA</b>' : c === J ? '🔵 <b>JOGADOR</b>' : '🟡 <b>EMPATE</b>');
const corCurta = (c) => (c === B ? '🔴 Banca' : c === J ? '🔵 Jogador' : '🟡 Empate');
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
  if (['player', 'jogador', 'playerwin', 'playerwins', 'playerwon', 'playerwinner', 'blue'].includes(s)) return J;
  if (['banker', 'banca', 'bankerwin', 'bankerwins', 'bankerwon', 'bankerwinner', 'red', 'vermelho'].includes(s)) return B;
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
  const c = [
    body.data, body.results, body.items, body.resultados, body.rounds, body.events,
    body.data?.results, body.data?.items, body.data?.resultados, body.data?.rounds,
    body.data?.events, body.data?.data
  ];
  return c.find(Array.isArray) || [];
}

function normalizeRound(item) {
  const d = item?.data || item?.round || item || {};
  const r = d.result && typeof d.result === 'object'
    ? d.result
    : item?.result && typeof item.result === 'object' ? item.result : {};

  const status = String(d.status ?? d.estado ?? item?.status ?? item?.estado ?? '').toLowerCase();
  const t = validMs(d.settledAt ?? d.endTime ?? d.endedAt ?? d.finishedAt ?? d.fim ?? item?.settledAt ?? item?.fim);

  const raw =
    r.outcome ?? r.resultado ?? r.winner ??
    d.outcome ?? d.resultado ?? d.winner ??
    item?.outcome ?? item?.resultado ?? item?.winner ??
    (typeof d.result === 'string' ? d.result : null) ??
    (typeof item?.result === 'string' ? item.result : null);

  const resultado = normalizeOutcome(raw);
  const resolvida =
    ['resolved', 'settled', 'finished', 'complete', 'completed', 'resolvido', 'concluido', 'finalizado']
      .some((w) => status.includes(w)) || Boolean(t);

  const id = String(
    d.id ?? d.roundId ?? d.round_id ?? d.eventId ?? item?.id ?? item?.roundId ?? item?.round_id ?? item?.eventId ?? ''
  ).trim();
  if (!id || !resultado || !resolvida || !t) return null;

  const pj = Number(r.playerDice?.score ?? d.playerDice?.score ?? d.playerScore ?? d.player_score ?? d.pontos_jogador);
  const pb = Number(r.bankerDice?.score ?? d.bankerDice?.score ?? d.bankerScore ?? d.banker_score ?? d.pontos_banca);
  const mult = r.multiplier ?? d.multiplier ?? d.multiplicador ?? null;

  return {
    id, resultado, t,
    pj: Number.isFinite(pj) ? pj : null,
    pb: Number.isFinite(pb) ? pb : null,
    mult: mult == null ? null : String(mult)
  };
}

async function fetchRounds() {
  let last = null;
  for (let i = 1; i <= 3; i++) {
    try {
      const resp = await axios.get(API_BACBO, {
        timeout: 15000,
        headers: { Accept: 'application/json', 'User-Agent': 'CapungaDadosBot/11.1' }
      });
      const map = new Map();
      for (const it of extractItems(resp.data)) {
        const rd = normalizeRound(it);
        if (rd) map.set(rd.id, rd);
      }
      R.erroAPI = null;
      return [...map.values()].sort((a, b) => a.t - b.t || (a.id < b.id ? -1 : 1));
    } catch (e) {
      last = e;
      R.erroAPI = e.message;
      if (i < 3) await sleep(600 * i);
    }
  }
  throw last || new Error('Falha na API');
}

/* ============================ ESTRATÉGIAS (mantidas) ============================ */

function streak(seq) {
  if (!seq.length) return 0;
  let n = 0;
  for (const r of seq) { if (r.resultado === seq[0].resultado) n++; else break; }
  return n;
}

const hasScore = (r) => r && Number.isFinite(r.pj) && Number.isFinite(r.pb);
const margem = (r) => Math.abs(r.pj - r.pb);
const scoreOf = (r) => (r.resultado === J ? r.pj : r.pb);
const scoreRange = (s) => {
  if (!Number.isFinite(s)) return null;
  if (s <= 2) return 'MB';
  if (s <= 4) return 'B';
  if (s <= 6) return 'M';
  if (s <= 8) return 'A';
  return 'MA';
};

const STRATS = [
  function repeticao({ seq }) {
    const n = streak(seq);
    if (n < 2 || n > 8) return null;
    const strength = [0, 0, 58, 68, 75, 80, 82, 82, 82][n] || 82;
    return { cor: seq[0].resultado, key: `REPETICAO_\( {Math.min(n, 6)}`, nome: `Repetição x \){n}`, emoji: '🔁', strength };
  },
  function quebra({ seq }) {
    const n = streak(seq);
    if (n < 3 || n > 9) return null;
    const strength = [0, 0, 0, 65, 72, 78, 80, 80, 80, 80][n] || 80;
    return { cor: opp(seq[0].resultado), key: `QUEBRA_\( {Math.min(n, 7)}`, nome: `Quebra de sequência x \){n}`, emoji: '💥', strength };
  },
  function surf({ seq }) {
    if (seq.length < 4) return null;
    let L = 1;
    while (L < seq.length && seq[L].resultado !== seq[L - 1].resultado) L++;
    if (L < 3) return null;
    const strength = L >= 6 ? 82 : L >= 5 ? 78 : L >= 4 ? 72 : 66;
    return { cor: opp(seq[0].resultado), key: `SURF_${Math.min(L, 6)}`, nome: `Surf (alternância ${L})`, emoji: '🌊', strength };
  },
  function doisDois({ seq }) {
    if (seq.length < 4) return null;
    const [a, b, c, d] = seq.map((x) => x.resultado);
    if (a === b && c === d && a !== c) return { cor: c, key: 'DOIS_DOIS', nome: '2+2 (blocos de dois)', emoji: '♟️', strength: 65 };
    return null;
  },
  function tresUm({ seq }) {
    if (seq.length < 4) return null;
    const [a, b, c, d] = seq.map((x) => x.resultado);
    if (a !== b && b === c && c === d) return { cor: a, key: 'TRES_UM', nome: 'Quebra confirmada (3→1)', emoji: '🧩', strength: 70 };
    return null;
  },
  function forca({ seq }) {
    const s = seq.slice(0, 12);
    if (s.length < 10) return null;
    const pj = s.filter((x) => x.resultado === J).length / s.length;
    const dom = pj >= 0.5 ? J : B;
    const pct = Math.max(pj, 1 - pj);
    if (pct < 0.67) return null;
    const bucket = pct >= 0.75 ? 75 : pct >= 0.7 ? 70 : 67;
    const strength = pct >= 0.75 ? 82 : pct >= 0.7 ? 74 : 68;
    return { cor: dom, key: `FORCA_${bucket}`, nome: `Força da mesa ${Math.round(pct * 100)}%`, emoji: '💪', strength };
  },
  function reversao({ seq }) {
    const s = seq.slice(0, 20);
    if (s.length < 16) return null;
    const pj = s.filter((x) => x.resultado === J).length / s.length;
    const pct = Math.max(pj, 1 - pj);
    if (pct < 0.65) return null;
    const dom = pj >= 0.5 ? J : B;
    return { cor: opp(dom), key: 'REVERSAO_20', nome: `Retorno à média (${Math.round(pct * 100)}% ${dom})`, emoji: '⚖️', strength: 65 };
  },
  function contexto({ seq }) {
    const out = [];
    for (const k of [3, 4, 5]) {
      if (seq.length < k) continue;
      const p = seq.slice(0, k).map((x) => (x.resultado === J ? 'J' : 'B')).join('');
      const vis = p.split('').reverse().join('');
      const last = seq[0].resultado;
      const st = k === 3 ? 62 : k === 4 ? 68 : 72;
      out.push({ cor: last, key: `CTX\( {k}_ \){p}_SEG`, nome: `Sequência ${vis} → segue`, emoji: '🧬', strength: st });
      out.push({ cor: opp(last), key: `CTX\( {k}_ \){p}_CON`, nome: `Sequência ${vis} → contraria`, emoji: '🧬', strength: st });
    }
    return out;
  },
  function numEscada({ seq }) {
    if (seq.length < 3 || !seq.slice(0, 3).every(hasScore)) return null;
    const [a, b, c] = seq.slice(0, 3);
    if (a.resultado !== b.resultado || b.resultado !== c.resultado) return null;
    const sa = scoreOf(a), sb = scoreOf(b), sc = scoreOf(c);
    if ((sb - sa === 1 && sc - sb === 1) || (sa - sb === 1 && sb - sc === 1)) {
      return { cor: a.resultado, key: 'NUM_ESCADA_3', nome: `Escada \( {sa}→ \){sb}→${sc}`, emoji: '🪜', strength: 80 };
    }
    return null;
  },
  function numRange({ seq }) {
    if (seq.length < 2 || !hasScore(seq[0]) || !hasScore(seq[1])) return null;
    const [a, b] = seq;
    const ra = scoreRange(scoreOf(a)), rb = scoreRange(scoreOf(b));
    if (!ra || !rb || ra === rb) return null;
    if ((ra === 'MB' && rb === 'MA') || (ra === 'MA' && rb === 'MB')) {
      return { cor: opp(a.resultado), key: `NUM_RANGE_\( {ra}_ \){rb}`, nome: `Salto de range \( {scoreOf(b)}→ \){scoreOf(a)}`, emoji: '📐', strength: 72 };
    }
    return null;
  },
  function numEspelho({ rows }) {
    const r = rows[0];
    if (!r || !hasScore(r) || r.pj !== r.pb) return null;
    return { cor: r.resultado, key: 'NUM_ESPELHO', nome: `Espelho \( {r.pj}× \){r.pb}`, emoji: '🪞', strength: 78 };
  },
  function numRepeteScore({ seq }) {
    if (seq.length < 2 || !hasScore(seq[0]) || !hasScore(seq[1])) return null;
    const [a, b] = seq;
    if (a.resultado !== b.resultado) return null;
    if (scoreOf(a) === scoreOf(b)) {
      return { cor: a.resultado, key: 'NUM_REPETE_SCORE', nome: `Score repetido \( {scoreOf(b)}→ \){scoreOf(a)}`, emoji: '🔂', strength: 76 };
    }
    return null;
  },
  function numGatilho({ seq }) {
    if (!seq.length || !hasScore(seq[0])) return null;
    const r = seq[0], s = scoreOf(r);
    if (s === 0 || s === 1 || s === 8 || s === 9) {
      const strength = (s === 0 || s === 9) ? 76 : 66;
      return { cor: opp(r.resultado), key: `NUM_GATILHO_${s}`, nome: `Gatilho ${s}`, emoji: '🎯', strength };
    }
    return null;
  },
  function numMargemTrend({ seq }) {
    if (seq.length < 3) return null;
    const t = seq.slice(0, 3);
    if (!t.every(hasScore)) return null;
    const [m2, m1, m0] = [margem(t[2]), margem(t[1]), margem(t[0])];
    if ((m2 < m1 && m1 < m0) || (m0 < m1 && m1 < m2)) {
      return {
        cor: t[0].resultado,
        key: 'NUM_MARGEM_TREND',
        nome: `Margem em \( {m0 > m1 ? 'alta' : 'queda'} ( \){m2}→\( {m1}→ \){m0})`,
        emoji: '📊', strength: 70
      };
    }
    return null;
  },
  function numParidade({ seq }) {
    if (seq.length < 3 || !seq.slice(0, 3).every(hasScore)) return null;
    const t = seq.slice(0, 3).map(scoreOf);
    const p = t.map((s) => s % 2);
    if (p[0] === p[1] && p[1] === p[2]) {
      return { cor: seq[0].resultado, key: `NUM_PARIDADE_${p[0] ? 'I' : 'P'}`, nome: `Paridade ${p[0] ? 'ímpar' : 'par'} x3`, emoji: '🔢', strength: 68 };
    }
    return null;
  },
  function numSeq({ seq }) {
    if (seq.length < 3 || !seq.slice(0, 3).every(hasScore)) return null;
    const t = seq.slice(0, 3).map(scoreOf);
    if (t[0] < t[1] && t[1] < t[2]) {
      return { cor: seq[0].resultado, key: 'NUM_SEQ_CRESC', nome: `Scores crescentes \( {t[2]}→ \){t[1]}→${t[0]}`, emoji: '📈', strength: 72 };
    }
    if (t[0] > t[1] && t[1] > t[2]) {
      return { cor: seq[0].resultado, key: 'NUM_SEQ_DECR', nome: `Scores decrescentes \( {t[2]}→ \){t[1]}→${t[0]}`, emoji: '📉', strength: 72 };
    }
    return null;
  },
  function numAlta({ rows }) {
    const r = rows[0];
    if (!r || !isCol(r.resultado) || !hasScore(r)) return null;
    const v = scoreOf(r);
    if (v < 10 || margem(r) < 1) return null;
    return { cor: r.resultado, key: 'NUM_ALTA_10', nome: `Numérica Alta (${v})`, emoji: '🔢', strength: 74 };
  },
  function numDiff4({ seq }) {
    if (seq.length < 3) return null;
    const t = seq.slice(0, 3);
    if (!t.every((r) => hasScore(r) && margem(r) >= 4 && r.resultado === t[0].resultado)) return null;
    return { cor: t[0].resultado, key: 'NUM_DIFF4_X3', nome: 'Diff≥4 x3 mesma cor', emoji: '🔢', strength: 78 };
  },
  function numViradaBaixa({ seq }) {
    if (seq.length < 2) return null;
    const [a, b] = seq;
    if (!hasScore(a) || !hasScore(b) || a.resultado !== b.resultado) return null;
    if (scoreOf(a) > 5 || scoreOf(b) > 5) return null;
    return { cor: opp(a.resultado), key: 'NUM_VIRADA_BAIXA', nome: 'Virada Baixa (baixos repetidos)', emoji: '🔢', strength: 70 };
  },
  function numMargem({ rows }) {
    const r = rows[0];
    if (!r || !isCol(r.resultado) || !hasScore(r) || margem(r) < 5) return null;
    return { cor: r.resultado, key: 'NUM_MARGEM_5', nome: `Margem \( {r.pj}× \){r.pb}`, emoji: '🔢', strength: 72 };
  },
  function numTendencia({ rows }) {
    const s = rows.slice(0, 6).filter(hasScore);
    if (s.length < 6) return null;
    const mj = s.reduce((a, r) => a + r.pj, 0) / s.length;
    const mb = s.reduce((a, r) => a + r.pb, 0) / s.length;
    const diff = mj - mb;
    if (Math.abs(diff) < 1.3) return null;
    return {
      cor: diff > 0 ? J : B,
      key: `NUM_TEND_${Math.abs(diff) >= 2 ? 2 : 1}`,
      nome: `Média pontos \( {mj.toFixed(1)}× \){mb.toFixed(1)}`,
      emoji: '📊', strength: Math.abs(diff) >= 2 ? 76 : 68
    };
  },
  function numPosEmpate({ rows, seq }) {
    if (!rows[0] || rows[0].resultado !== E || !seq.length) return null;
    return { cor: seq[0].resultado, key: 'NUM_POS_EMPATE', nome: 'Pós-empate (cor anterior)', emoji: '🎯', strength: 62 };
  }
];

function runStrategies(rows) {
  const ctx = { rows, seq: rows.filter((r) => isCol(r.resultado)) };
  const out = [];
  for (const fn of STRATS) {
    try {
      const r = fn(ctx);
      for (const c of Array.isArray(r) ? r : [r]) if (c && isCol(c.cor)) out.push(c);
    } catch (_) {}
  }
  return out;
}

/* ============================ BACKTEST ============================ */

function windowAt(H, i) {
  const rows = [];
  for (let j = i; j >= 0 && rows.length < 24; j--) {
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
  const ini = 8;
  const fim = H.length - 1;
  const corte = ini + Math.floor((fim - ini) * 0.7);
  let bw = 0, bn = 0, tw = 0, tn = 0;
  for (let i = ini; i < fim; i++) {
    const win = windowAt(H, i);
    if (win.length < 6) continue;
    const teste = i >= corte;
    for (const cor of [J, B]) {
      const v = evalEntry(H, i, cor);
      if (v === null) continue;
      bn++; if (v) bw++;
      if (teste) { tn++; if (v) tw++; }
    }
    for (const c of runStrategies(win)) {
      const v = evalEntry(H, i, c.cor);
      if (v === null) continue;
      const p = per[c.key] || (per[c.key] = { n: 0, w: 0, nt: 0, wt: 0 });
      p.n++; if (v) p.w++;
      if (teste) { p.nt++; if (v) p.wt++; }
    }
  }
  return { per, baseW: bw, baseN: bn, baseRate: bn ? bw / bn : null, testeRate: tn ? tw / tn : null };
}

function getStats(force = false) {
  if (force || !S.stats || S.hist.length - S.statsAtLen >= 25) {
    S.stats = backtest();
    S.statsAtLen = S.hist.length;
  }
  return S.stats;
}

function wilsonLB(w, n, z = 1.96) {
  if (!n) return 0;
  const p = w / n;
  const z2 = z * z;
  const c = p + z2 / (2 * n);
  const m = z * Math.sqrt((p * (1 - p)) / n + z2 / (4 * n * n));
  return (c - m) / (1 + z2 / n);
}

function suspensoAoVivo(key) {
  const L = S.signals.filter((x) => x.key === key && FECHADOS.includes(x.status)).slice(-6);
  if (L.length < 4) return false;
  const losses = L.filter((x) => x.status === 'LOSS').length;
  return losses >= 3;
}

/* ============================ CONFIANÇA COMPOSTA ============================ */

function confiancaComposta(cand, stats) {
  const p = stats.per[cand.key] || { n: 0, w: 0, nt: 0, wt: 0 };
  const bt = p.n >= 8 ? wilsonLB(p.w, p.n) : (stats.baseRate || 0.5);
  const live = S.signals.filter((x) => x.key === cand.key && FECHADOS.includes(x.status));
  const lw = live.filter((x) => x.status === 'GREEN' || x.status === 'G1' || x.status === 'EMPATE').length;
  const ln = live.length;
  const liveRate = ln >= 3 ? lw / ln : bt;

  let score = 0;
  score += bt * 38;
  score += liveRate * 22;
  score += (cand.strength / 100) * 18;
  score += Math.min(1, (p.n || 0) / 40) * 12; // atividade
  // consenso simples
  const mesmos = runStrategies(S.hist.slice(-20).reverse()).filter((c) => c.cor === cand.cor);
  score += Math.min(1, mesmos.length / 5) * 10;

  return Math.max(55, Math.min(94, Math.round(score)));
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
      }, { timeout: 12000 });
      if (r.data?.ok) {
        R.erroTelegram = null;
        return true;
      }
      R.erroTelegram = JSON.stringify(r.data);
    } catch (e) {
      R.erroTelegram = e.response?.data ? JSON.stringify(e.response.data) : e.message;
      const retry = Number(e.response?.data?.parameters?.retry_after);
      if (retry > 0 && retry <= 25) await sleep(retry * 1000 + 300);
    }
    if (t < 3) await sleep(1200 * t);
  }
  return false;
}

/* ============================ APURAÇÃO (CORRIGIDA) ============================ */

function apurar() {
  const now = Date.now();
  let mudou = false;

  for (const s of S.signals) {
    if (FECHADOS.includes(s.status)) continue;

    const after = S.hist.filter((r) => r.t > s.t).sort((a, b) => a.t - b.t);

    // Timeout → força LOSS (nunca mais NÃO APURADO)
    if (after.length === 0) {
      if (now - s.t >= CFG.apuraMaxMs) {
        s.status = 'LOSS';
        s.fechadoEm = now;
        s.motivo = 'timeout sem rodadas após o sinal';
        mudou = true;
        tg(`❌ <b>SINAL #\( {s.id} → LOSS</b>\n⏱ Timeout de apuração\n \){hora()}`).catch(() => {});
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
      mudou = true;
      const emoji = s.status === 'GREEN' ? '✅' : s.status === 'G1' ? '✅✅' : '🟡';
      tg(`\( {emoji} <b>SINAL # \){s.id} → \( {s.status}</b>\n \){corTxt(s.cor)}\n⏱ ${hora()}`).catch(() => {});
    } else if (after.length > MAX_GALE) {
      s.status = 'LOSS';
      s.fechadoEm = now;
      mudou = true;
      tg(`❌ <b>SINAL #\( {s.id} → LOSS</b>\n \){corTxt(s.cor)}\n⏱ ${hora()}`).catch(() => {});
    } else if (now - s.t >= CFG.apuraMaxMs) {
      s.status = 'LOSS';
      s.fechadoEm = now;
      s.motivo = 'timeout';
      mudou = true;
      tg(`❌ <b>SINAL #${s.id} → LOSS</b> (timeout)\n⏱ ${hora()}`).catch(() => {});
    }
  }

  if (mudou) R.ultimoSinal = S.signals[S.signals.length - 1] || null;
}

/* ============================ GERAÇÃO DE SINAL (CORRIGIDA) ============================ */

function podeGerar() {
  if (!CFG.ativos) {
    R.ultimoMotivo = 'sinais desativados';
    return false;
  }
  if (S.signals.some((s) => !FECHADOS.includes(s.status))) {
    R.ultimoMotivo = 'já existe sinal pendente';
    return false;
  }
  const now = Date.now();
  const desdeUltimo = now - (S.lastSignalAt || 0);
  if (desdeUltimo < CFG.minIntervalMs) {
    R.ultimoMotivo = `intervalo mínimo (${Math.round((CFG.minIntervalMs - desdeUltimo) / 1000)}s restantes)`;
    return false;
  }
  // Se passou do máximo, força (mas ainda respeita o atraso da rodada)
  const last = S.hist[S.hist.length - 1];
  if (!last) {
    R.ultimoMotivo = 'sem histórico';
    return false;
  }
  if (now - last.t < CFG.atrasoRodadaMs) {
    R.ultimoMotivo = `aguardar ${Math.round((CFG.atrasoRodadaMs - (now - last.t)) / 1000)}s após última rodada`;
    return false;
  }
  if (S.hist.length < CFG.minAmostra) {
    R.ultimoMotivo = `amostra insuficiente (\( {S.hist.length}/ \){CFG.minAmostra})`;
    return false;
  }
  return true;
}

function tentarSinal() {
  if (!podeGerar()) return;

  const rows = S.hist.slice(-30).reverse();
  const cands = runStrategies(rows);
  if (!cands.length) {
    R.ultimoMotivo = 'nenhuma estratégia disparou';
    return;
  }

  const stats = getStats();
  let melhor = null;
  let melhorScore = 0;

  for (const c of cands) {
    if (suspensoAoVivo(c.key)) continue;
    const conf = confiancaComposta(c, stats);
    if (conf < 72) continue;
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
    status: 'PENDENTE',
    t: now,
    criadoEm: now,
    gale: 0
  };

  S.signals.push(sinal);
  if (S.signals.length > 300) S.signals = S.signals.slice(-250);
  S.lastSignalAt = now;
  R.ultimoSinal = sinal;
  R.ultimoMotivo = null;

  const msg = [
    `🎯 <b>SINAL #${id}</b>`,
    `${melhor.emoji} ${melhor.nome}`,
    ``,
    `Entrada: ${corTxt(melhor.cor)}`,
    `Confiança: <b>${melhor.conf}%</b> ${barra(melhor.conf)}`,
    `Gale máximo: 1`,
    CFG.empateGreen ? `Empate = GREEN` : `Empate = LOSS`,
    ``,
    `⏱ ${hora()}`
  ].join('\n');

  tg(msg).catch(() => {});
  log('SINAL', { id, cor: melhor.cor, conf: melhor.conf, key: melhor.key });
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
      const cut = S.hist.length - CFG.maxHist;
      for (let i = 0; i < cut; i++) S.seen.delete(S.hist[i].id);
      S.hist = S.hist.slice(cut);
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

/* ============================ LOOP PRINCIPAL ============================ */

async function tick() {
  if (busy || stopping) return;
  busy = true;
  lastTickMs = Date.now();
  try {
    await sync();
    apurar();          // primeiro fecha o que der
    tentarSinal();     // só depois tenta gerar (respeitando atraso)
  } catch (e) {
    log('TICK ERRO', e.message);
  } finally {
    busy = false;
  }
}

/* ============================ KEEP-ALIVE + WAKE ============================ */

async function keepAlive() {
  R.ultimoKeepAlive = new Date().toISOString();
  const self = process.env.SELF_URL || process.env.RENDER_EXTERNAL_URL || '';
  if (self) {
    try {
      await axios.get(self.replace(/\/$/, '') + '/health', { timeout: 8000 });
    } catch (_) {}
  }
}

function wakeCheck() {
  const now = Date.now();
  if (now - lastTickMs > CFG.syncMs * 4) {
    R.wakes++;
    R.ultimoWake = new Date().toISOString();
    log('WAKE', `detectado atraso de ${Math.round((now - lastTickMs) / 1000)}s`);
    tick().catch(() => {});
  }
}

/* ============================ EXPRESS ============================ */

const app = express();
app.disable('x-powered-by');
app.use(cors());
app.use(express.json({ limit: '256kb' }));

app.get('/health', (req, res) => {
  res.json({
    ok: true,
    versao: '11.1',
    deployId: DEPLOY_ID,
    hist: S.hist.length,
    sinais: S.signals.length,
    ultimoSinal: R.ultimoSinal ? { id: R.ultimoSinal.id, status: R.ultimoSinal.status } : null,
    ultimaSync: R.ultimaSync,
    erroAPI: R.erroAPI,
    uptime: process.uptime()
  });
});

app.get('/status', (req, res) => {
  if (ADMIN_TOKEN && req.query.token !== ADMIN_TOKEN) return res.status(401).json({ erro: 'token' });
  res.json({ S: { histLen: S.hist.length, nextId: S.nextId, lastSignalAt: S.lastSignalAt }, R });
});

app.post('/admin/reset-hist', (req, res) => {
  if (ADMIN_TOKEN && req.headers['x-admin-token'] !== ADMIN_TOKEN) return res.status(401).json({ erro: 'token' });
  S.hist = [];
  S.seen.clear();
  S.stats = null;
  res.json({ ok: true });
});

/* ============================ START ============================ */

async function start() {
  log('BOOT', { versao: '11.1', deployId: DEPLOY_ID, minInterval: CFG.minIntervalMs, atrasoRodada: CFG.atrasoRodadaMs });

  if (DATABASE_URL) {
    try {
      pool = new Pool({
        connectionString: DATABASE_URL,
        ssl: /localhost|127\.0\.0\.1/i.test(DATABASE_URL) ? false : { rejectUnauthorized: false },
        max: 3
      });
      await pool.query('SELECT 1');
      log('DB', 'conectado');
    } catch (e) {
      R.erroDB = e.message;
      log('DB ERRO', e.message);
      pool = null;
    }
  }

  // carga inicial
  await sync();
  getStats(true);

  server = app.listen(PORT, () => log('HTTP', `porta ${PORT}`));

  tickTimer = setInterval(() => tick().catch(() => {}), CFG.syncMs);
  keepTimer = setInterval(() => {
    keepAlive().catch(() => {});
    wakeCheck();
  }, CFG.keepAliveMs);

  // primeiro tick imediato
  setTimeout(() => tick().catch(() => {}), 1500);

  process.on('SIGTERM', shutdown);
  process.on('SIGINT', shutdown);
}

async function shutdown() {
  if (stopping) return;
  stopping = true;
  log('SHUTDOWN', 'iniciado');
  clearInterval(tickTimer);
  clearInterval(keepTimer);
  if (server) server.close();
  if (pool) await pool.end().catch(() => {});
  process.exit(0);
}

start().catch((e) => {
  console.error(e);
  process.exit(1);
});