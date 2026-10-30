'use strict';

/**
 * CAPUNGA DADOS — BAC BO · v11 (LOCKED TO CASINO.ORG)
 *
 *  • FONTE DE DADOS ÚNICA E EXCLUSIVA: API Casino.org (Evolution).
 *  • 24/7: keep-alive (60s) + wake-detection (auto-recover se dormir).
 *  • GALE 1 (máx. 2 tentativas).
 *  • CONFIANÇA COMPOSTA: backtest + live + momento + padrão + atividade + consenso.
 *  • APURAÇÃO POR RECONCILIAÇÃO: o sinal fica PENDENTE até haver rodadas reais depois dele.
 *  • RESET AUTOMÁTICO do histórico quando o ficheiro muda (hash SHA-256).
 *
 * ENV obrigatórias: BOT_TOKEN, CHAT_ID
 * ENV opcionais:    DATABASE_URL, ADMIN_TOKEN, SELF_URL/RENDER_EXTERNAL_URL
 *                   MIN_AMOSTRA(60) · SYNC_MS(2000) · SINAL_INICIAL(1) · SINAIS_ATIVOS(true)
 *                   EMPATE_VALE_GREEN(true) · KEEPALIVE_MS(60000) · DEPLOY_ID
 *                   APURA_GAP_MS(180000) · APURA_MAX_MS(1200000)
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
const API_BACBO = 'https://api-cs.casino.org/svc-evolution-game-events/api/bacbo';

const MAX_GALE = 1;
const SLOT_MS = 5 * 60 * 1000;

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
  apuraMaxMs: num('APURA_MAX_MS', 1200000, 300000, 7200000)
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
  lastSlot: Math.floor(Date.now() / SLOT_MS),
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
  naoApurados: 0
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
  console.log(`[${new Date().toISOString()}] [${tag}] ${p}`);
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
        headers: { Accept: 'application/json', 'User-Agent': 'CapungaDadosBot/11.0' }
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

/* ============================ ESTRATÉGIAS ============================ */

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
  /* ---------- COR ---------- */
  function repeticao({ seq }) {
    const n = streak(seq);
    if (n < 2 || n > 8) return null;
    const strength = [0, 0, 58, 68, 75, 80, 82, 82, 82][n] || 82;
    return { cor: seq[0].resultado, key: `REPETICAO_${Math.min(n, 6)}`, nome: `Repetição x${n}`, emoji: '🔁', strength };
  },
  function quebra({ seq }) {
    const n = streak(seq);
    if (n < 3 || n > 9) return null;
    const strength = [0, 0, 0, 65, 72, 78, 80, 80, 80, 80][n] || 80;
    return { cor: opp(seq[0].resultado), key: `QUEBRA_${Math.min(n, 7)}`, nome: `Quebra de sequência x${n}`, emoji: '💥', strength };
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
      out.push({ cor: last, key: `CTX${k}_${p}_SEG`, nome: `Sequência ${vis} → segue`, emoji: '🧬', strength: st });
      out.push({ cor: opp(last), key: `CTX${k}_${p}_CON`, nome: `Sequência ${vis} → contraria`, emoji: '🧬', strength: st });
    }
    return out;
  },

  /* ---------- NUMÉRICAS ---------- */
  function numEscada({ seq }) {
    if (seq.length < 3 || !seq.slice(0, 3).every(hasScore)) return null;
    const [a, b, c] = seq.slice(0, 3);
    if (a.resultado !== b.resultado || b.resultado !== c.resultado) return null;
    const sa = scoreOf(a), sb = scoreOf(b), sc = scoreOf(c);
    if ((sb - sa === 1 && sc - sb === 1) || (sa - sb === 1 && sb - sc === 1)) {
      return { cor: a.resultado, key: 'NUM_ESCADA_3', nome: `Escada ${sa}→${sb}→${sc}`, emoji: '🪜', strength: 80 };
    }
    return null;
  },
  function numRange({ seq }) {
    if (seq.length < 2 || !hasScore(seq[0]) || !hasScore(seq[1])) return null;
    const [a, b] = seq;
    const ra = scoreRange(scoreOf(a)), rb = scoreRange(scoreOf(b));
    if (!ra || !rb || ra === rb) return null;
    if ((ra === 'MB' && rb === 'MA') || (ra === 'MA' && rb === 'MB')) {
      return { cor: opp(a.resultado), key: `NUM_RANGE_${ra}_${rb}`, nome: `Salto de range ${scoreOf(b)}→${scoreOf(a)}`, emoji: '📐', strength: 72 };
    }
    return null;
  },
  function numEspelho({ rows }) {
    const r = rows[0];
    if (!r || !hasScore(r) || r.pj !== r.pb) return null;
    return { cor: r.resultado, key: 'NUM_ESPELHO', nome: `Espelho ${r.pj}×${r.pb}`, emoji: '🪞', strength: 78 };
  },
  function numRepeteScore({ seq }) {
    if (seq.length < 2 || !hasScore(seq[0]) || !hasScore(seq[1])) return null;
    const [a, b] = seq;
    if (a.resultado !== b.resultado) return null;
    if (scoreOf(a) === scoreOf(b)) {
      return { cor: a.resultado, key: 'NUM_REPETE_SCORE', nome: `Score repetido ${scoreOf(b)}→${scoreOf(a)}`, emoji: '🔂', strength: 76 };
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
        nome: `Margem em ${m0 > m1 ? 'alta' : 'queda'} (${m2}→${m1}→${m0})`,
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
      return { cor: seq[0].resultado, key: 'NUM_SEQ_CRESC', nome: `Scores crescentes ${t[2]}→${t[1]}→${t[0]}`, emoji: '📈', strength: 72 };
    }
    if (t[0] > t[1] && t[1] > t[2]) {
      return { cor: seq[0].resultado, key: 'NUM_SEQ_DECR', nome: `Scores decrescentes ${t[2]}→${t[1]}→${t[0]}`, emoji: '📉', strength: 72 };
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
    return { cor: r.resultado, key: 'NUM_MARGEM_5', nome: `Margem ${r.pj}×${r.pb}`, emoji: '🔢', strength: 72 };
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
      nome: `Média pontos ${mj.toFixed(1)}×${mb.toFixed(1)}`,
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
  const L = S.signals.filter((x) => x.key === key && FECHADOS.includes(x.estado)).slice(-20);
  if (L.length < 12) return false;
  return L.filter((x) => x.estado !== 'LOSS').length / L.length < 0.35;
}

/* ============================ CONFIANÇA COMPOSTA ============================ */

function liveStatsFor(key) {
  const L = S.signals.filter((s) => s.key === key && FECHADOS.includes(s.estado)).slice(-20);
  const total = L.length;
  const greens = L.filter((s) => s.estado !== 'LOSS').length;
  return { total, greens, reds: total - greens };
}

function momentumScore(targetCor) {
  const seq = S.hist.filter((r) => isCol(r.resultado)).slice(-10);
  if (seq.length < 3) return 50;
  const count = seq.filter((r) => r.resultado === targetCor).length;
  let hot = 0;
  for (let i = seq.length - 1; i >= 0; i--) {
    if (seq[i].resultado === targetCor) hot++; else break;
  }
  const freq = count / seq.length;
  const bonus = Math.min(0.30, hot * 0.10);
  return Math.round(Math.min(100, Math.max(0, (freq + bonus) * 100)));
}

function activityScore() {
  const now = Date.now();
  const rec = S.hist.filter((r) => now - r.t < 5 * 60000);
  const rpm = rec.length / 5;
  return Math.round(Math.min(100, Math.max(0, rpm * 40)));
}

function computeCoreConfidence(c) {
  const live = liveStatsFor(c.key);
  const f_bt = Math.max(0, Math.min(100, c.piso ?? 0));
  let f_live = f_bt;
  if (live.total >= 5) f_live = ((live.greens + 4) / (live.total + 6)) * 100;
  const f_mom = momentumScore(c.cor);
  const f_pat = Math.max(0, Math.min(100, c.strength ?? 65));
  const f_act = activityScore();

  const score = f_bt * 0.30 + f_live * 0.22 + f_mom * 0.14 + f_pat * 0.18 + f_act * 0.16;

  return {
    score: Math.max(1, Math.min(99, score)),
    factors: {
      backtest: Math.round(f_bt),
      live: Math.round(f_live),
      momento: Math.round(f_mom),
      padrao: Math.round(f_pat),
      atividade: Math.round(f_act)
    },
    liveTotal: live.total,
    liveGreens: live.greens
  };
}

/* ============================ SELEÇÃO DE SINAL ============================ */

function pickEntry(rows) {
  const st = getStats();
  const base = st.baseN
    ? { taxa: (st.baseW / st.baseN) * 100, piso: wilsonLB(st.baseW, st.baseN) * 100, amostra: st.baseN }
    : null;

  const todos = runStrategies(rows)
    .map((c) => {
      const e = st.per[c.key] || { n: 0, w: 0, nt: 0, wt: 0 };
      const cand = {
        ...c,
        amostra: e.n,
        taxa: e.n ? (e.w / e.n) * 100 : 0,
        piso: wilsonLB(e.w, e.n) * 100,
        pisoTreino: wilsonLB(e.w - e.wt, e.n - e.nt) * 100,
        testeN: e.nt,
        testeTaxa: e.nt ? (e.wt / e.nt) * 100 : 0
      };
      cand.core = computeCoreConfidence(cand);
      return cand;
    })
    .filter((c) => !suspensoAoVivo(c.key));

  const refTeste = (st.testeRate != null ? st.testeRate : st.baseRate || 0) * 100;
  const validado = (c) => c.testeN >= 15 && c.testeTaxa >= refTeste - 0.5;

  const pickBest = (arr) => {
    if (!arr.length) return null;
    const g = { [J]: [], [B]: [] };
    for (const c of arr) g[c.cor].push(c);
    const top = (a) => (a.length ? Math.max(...a.map((c) => c.core.score)) : -1);
    const a = top(g[J]) >= top(g[B]) ? g[J] : g[B];
    a.sort((x, y) => y.core.score - x.core.score || y.amostra - x.amostra);
    const chosen = a[0];
    const consenso = a.length;
    const boost = Math.min(8, (consenso - 1) * 3);
    return {
      ...chosen,
      consenso,
      confidence: {
        score: Math.min(99, Math.round(chosen.core.score + boost)),
        factors: { ...chosen.core.factors, consenso: boost }
      }
    };
  };

  let e = pickBest(todos.filter((c) => c.amostra >= CFG.minAmostra && validado(c)));
  let nivel = 'padrao';
  if (!e) {
    e = pickBest(todos.filter((c) => c.amostra >= 20 && c.testeN >= 8));
    nivel = 'amostra_baixa';
  }
  if (!e) {
    const seq = rows.filter((r) => isCol(r.resultado)).slice(0, 12);
    const j = seq.filter((x) => x.resultado === J).length;
    const cor = j >= seq.length - j ? J : B;
    const coreFallback = {
      score: base ? base.piso : 55,
      factors: { backtest: base ? Math.round(base.piso) : 55, live: 55, momento: momentumScore(cor), padrao: 40, atividade: activityScore(), consenso: 0 }
    };
    e = {
      cor, key: 'MAIORIA_12', nome: 'Maioria dos últimos 12', emoji: '📈', consenso: 1,
      amostra: base ? base.amostra : 0, taxa: base ? base.taxa : 0, piso: base ? base.piso : 0,
      strength: 40, confidence: coreFallback, nivel: 'base'
    };
    nivel = 'base';
  }
  return { ...e, nivel, base: base ? base.taxa : null };
}

/* ============================ BASE DE DADOS ============================ */

let dbChain = Promise.resolve();
function dbq(fn) {
  if (!pool) return;
  dbChain = dbChain.then(fn).catch((e) => { R.erroDB = e.message; log('DB_ERRO', e.message); });
}

async function ensureSchema() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS g2_rodadas (
      id TEXT PRIMARY KEY, resultado TEXT NOT NULL, fim TIMESTAMPTZ NOT NULL,
      pj INTEGER, pb INTEGER, mult TEXT
    )`);
  await pool.query(`CREATE INDEX IF NOT EXISTS idx_g2_rodadas_fim ON g2_rodadas (fim DESC)`);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS g2_sinais (
      id BIGINT PRIMARY KEY, criado_em TIMESTAMPTZ NOT NULL, cor TEXT NOT NULL,
      estrategia TEXT, nome TEXT, assertividade INTEGER, taxa INTEGER, amostra INTEGER,
      estado TEXT NOT NULL, gale INTEGER NOT NULL DEFAULT 0,
      empate_gale INTEGER, empate_mult TEXT,
      resultado_final TEXT, resolvido_em TIMESTAMPTZ, base_id TEXT, base_t TIMESTAMPTZ,
      confidence_factors JSONB
    )`);
  await pool.query(`ALTER TABLE g2_sinais ADD COLUMN IF NOT EXISTS confidence_factors JSONB`);
  await pool.query(`ALTER TABLE g2_sinais ADD COLUMN IF NOT EXISTS base_t TIMESTAMPTZ`);
  await pool.query(`CREATE INDEX IF NOT EXISTS idx_g2_sinais_criado ON g2_sinais (criado_em DESC)`);

  await pool.query(`CREATE TABLE IF NOT EXISTS g2_meta (chave TEXT PRIMARY KEY, valor TEXT)`);
}

async function checkDeployReset() {
  if (!pool) return;
  const r = await pool.query(`SELECT valor FROM g2_meta WHERE chave = 'deploy_id'`);
  const stored = r.rows[0]?.valor || '';
  const atual = DEPLOY_ID || '';
  if (atual && atual !== stored) {
    log('DEPLOY_RESET', { de: stored || '(vazio)', para: atual });
    await pool.query(`DELETE FROM g2_sinais`);
    S.signals = [];
    S.nextId = CFG.sinalInicial;
  }
  await pool.query(
    `INSERT INTO g2_meta (chave, valor) VALUES ('deploy_id', $1)
     ON CONFLICT (chave) DO UPDATE SET valor = EXCLUDED.valor`,
    [atual]
  );
}

async function loadFromDB() {
  const rr = await pool.query(
    `SELECT id, resultado, fim, pj, pb, mult FROM g2_rodadas ORDER BY fim DESC LIMIT $1`, [CFG.maxHist]
  );
  S.hist = rr.rows.map((x) => ({
    id: x.id, resultado: x.resultado, t: new Date(x.fim).getTime(), pj: x.pj, pb: x.pb, mult: x.mult
  })).sort((a, b) => a.t - b.t);
  S.seen = new Set(S.hist.map((x) => x.id));

  const sr = await pool.query(`SELECT * FROM g2_sinais WHERE criado_em >= NOW() - INTERVAL '36 hours' ORDER BY id ASC`);
  S.signals = sr.rows.map((x) => ({
    id: Number(x.id),
    criadoMs: new Date(x.criado_em).getTime(),
    cor: x.cor, key: x.estrategia, nome: x.nome,
    assertividade: x.assertividade, taxa: x.taxa, amostra: x.amostra,
    confidenceFactors: x.confidence_factors || null,
    estado: x.estado, gale: x.gale,
    empateGale: x.empate_gale, empateMult: x.empate_mult,
    resultadoFinal: x.resultado_final,
    resolvidoMs: x.resolvido_em ? new Date(x.resolvido_em).getTime() : null,
    baseId: x.base_id,
    baseT: x.base_t ? new Date(x.base_t).getTime() : 0,
    galeMsgIds: []
  }));

  const mx = await pool.query(`SELECT COALESCE(MAX(id), 0)::bigint AS m FROM g2_sinais`);
  const m = Number(mx.rows[0].m);
  S.nextId = m > 0 ? m + 1 : CFG.sinalInicial;

  const abertos = S.signals.filter((s) => s.estado === 'PENDENTE').length;
  if (abertos) log('PENDENTES_RETOMADOS', { abertos });
}

function dbSaveRound(r) {
  dbq(() =>
    pool.query(
      `INSERT INTO g2_rodadas (id, resultado, fim, pj, pb, mult) VALUES ($1,$2,$3,$4,$5,$6)
       ON CONFLICT (id) DO NOTHING`,
      [r.id, r.resultado, new Date(r.t), r.pj, r.pb, r.mult]
    )
  );
}

function dbSaveSignal(s) {
  dbq(() =>
    pool.query(
      `INSERT INTO g2_sinais (id, criado_em, cor, estrategia, nome, assertividade, taxa, amostra, estado, gale,
                              empate_gale, empate_mult, resultado_final, resolvido_em, base_id, base_t, confidence_factors)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)
       ON CONFLICT (id) DO UPDATE SET
         estado = EXCLUDED.estado, gale = EXCLUDED.gale,
         empate_gale = EXCLUDED.empate_gale, empate_mult = EXCLUDED.empate_mult,
         resultado_final = EXCLUDED.resultado_final, resolvido_em = EXCLUDED.resolvido_em,
         confidence_factors = EXCLUDED.confidence_factors`,
      [
        s.id, new Date(s.criadoMs), s.cor, s.key, s.nome,
        s.assertividade ?? null, s.taxa ?? null, s.amostra ?? null,
        s.estado, s.gale, s.empateGale ?? null, s.empateMult ?? null,
        s.resultadoFinal || null,
        s.resolvidoMs ? new Date(s.resolvidoMs) : null,
        s.baseId || null,
        s.baseT ? new Date(s.baseT) : null,
        s.confidenceFactors ? JSON.stringify(s.confidenceFactors) : null
      ]
    )
  );
}

/* ============================ TELEGRAM ============================ */

const tgQueue = [];
let tgRunning = false;

const stripHtml = (t) =>
  t.replace(/<[^>]+>/g, '').replace(/&lt;/g, '<').replace(/&gt;/g, '>').replace(/&amp;/g, '&');

async function tgCall(method, payload) {
  if (!BOT_TOKEN || !CHAT_ID) {
    R.erroTelegram = 'BOT_TOKEN ou CHAT_ID não configurado';
    return { ok: true, skipped: true };
  }
  try {
    const resp = await axios.post(
      `https://api.telegram.org/bot${BOT_TOKEN}/${method}`,
      { chat_id: CHAT_ID, ...payload },
      { timeout: 15000 }
    );
    if (resp.data?.ok === true) { R.erroTelegram = null; return { ok: true, result: resp.data.result }; }
    R.erroTelegram = JSON.stringify(resp.data);
    return { ok: false };
  } catch (e) {
    const data = e.response?.data;
    R.erroTelegram = data ? JSON.stringify(data) : e.message;
    const status = e.response?.status;
    if (status === 429) return { ok: false, retryAfter: Number(data?.parameters?.retry_after) || 5 };
    if (status === 400 && /parse entities/i.test(String(data?.description))) return { ok: false, parseError: true };
    if (status === 400 && method === 'deleteMessage') return { ok: true, gone: true };
    return { ok: false };
  }
}

async function tgRun() {
  tgRunning = true;
  while (tgQueue.length) {
    const m = tgQueue[0];

    if (m.kind === 'delete') {
      for (const id of [...(m.sig.galeMsgIds || [])]) {
        for (let t = 0; t < 3; t++) {
          const rr = await tgCall('deleteMessage', { message_id: id });
          if (rr.retryAfter) { await sleep(rr.retryAfter * 1000 + 300); continue; }
          break;
        }
        await sleep(250);
      }
      m.sig.galeMsgIds = [];
      tgQueue.shift();
      R.telegramPendentes = tgQueue.length;
      continue;
    }

    const r = await tgCall('sendMessage', m.plain
      ? { text: stripHtml(m.text), disable_web_page_preview: true }
      : { text: m.text, parse_mode: 'HTML', disable_web_page_preview: true });

    if (r.ok) {
      if (m.onId && r.result?.message_id) m.onId(r.result.message_id);
      tgQueue.shift();
      R.telegramPendentes = tgQueue.length;
      await sleep(400);
      continue;
    }
    if (r.retryAfter) { await sleep(r.retryAfter * 1000 + 300); continue; }
    if (r.parseError) { m.plain = true; continue; }
    m.tries = (m.tries || 0) + 1;
    if (m.tries >= 6) { log('TELEGRAM_DESCARTADA', { erro: R.erroTelegram }); tgQueue.shift(); }
    else await sleep(1500 * m.tries);
    R.telegramPendentes = tgQueue.length;
  }
  tgRunning = false;
}

function kick() {
  if (!tgRunning) tgRun().catch((e) => { tgRunning = false; log('TG_FILA_ERRO', e.message); });
}
function tgSend(text, onId) { tgQueue.push({ kind: 'send', text, onId, tries: 0, plain: false }); R.telegramPendentes = tgQueue.length; kick(); }
function tgApagarGale(sig) { tgQueue.push({ kind: 'delete', sig }); kick(); }

/* ============================ MENSAGENS ============================ */

function placar() {
  const hoje = dia();
  const L = S.signals.filter((s) => dia(s.criadoMs) === hoje);
  const c = (st) => L.filter((s) => s.estado === st).length;
  const green = c('GREEN'), g1 = c('G1'), emp = c('EMPATE'), loss = c('LOSS');
  const wins = green + g1 + emp;
  const fechados = wins + loss;
  const taxa = fechados ? (wins / fechados) * 100 : 0;
  let seguidos = 0;
  const fin = L.filter((s) => FECHADOS.includes(s.estado));
  for (let i = fin.length - 1; i >= 0; i--) { if (fin[i].estado === 'LOSS') break; seguidos++; }
  return { green, g1, emp, loss, wins, fechados, taxa, seguidos, naoApurados: c('NAO_APURADO') };
}

function msgPlacar(p) {
  return [
    '┌─ 📊 <b>BALANÇO DE HOJE</b>',
    `├ ✅ ${p.wins} Greens  ·  🟥 ${p.loss} Reds  ·  <b>${p.taxa.toFixed(1)}%</b>`,
    `├ Direto ${p.green}  ·  G1: ${p.g1}  ·  🟡 Empate: ${p.emp}`,
    `└ 🔥 Greens seguidos: ${p.seguidos}`
  ].join('\n');
}

function placarDados(r) { return hasScore(r) ? `  <i>(J ${r.pj} × ${r.pb} B)</i>` : ''; }

function msgEntrada(s) {
  const conf = s.assertividade;
  const f = s.confidenceFactors || {};
  const nivel = conf >= 88 ? '💎 Premium' : conf >= 78 ? '🟢 Forte' : conf >= 68 ? '🟡 Moderada' : '🟠 Baixa';
  const alerta = conf >= 88 ? '\n⭐ <b>SINAL PREMIUM</b> — todos os fatores alinhados.' : '';
  const aviso = s.nivel === 'amostra_baixa' ? '⚠️ Padrão com poucas amostras no histórico'
    : s.nivel === 'base' ? '⚠️ Sem padrão forte agora · usando média geral' : null;
  return [
    '┏━━━ 🎲 <b>CAPUNGA · BAC BO</b> ━━━┓',
    `🎯 <b>SINAL #${s.id}</b>  ·  ${hora(s.criadoMs).slice(0, 5)}`,
    '',
    `👉 Entrar: ${corTxt(s.cor)}`,
    `🔁 Proteção: até <b>Gale 1</b>${CFG.empateGreen ? '  ·  🟡 Empate = Green' : ''}`,
    '',
    `${s.emoji || '🧠'} Padrão: ${esc(s.nome)}`,
    `📈 <b>Confiança: ${conf}%</b>  ${nivel}`,
    `${barra(conf)}`,
    `📊 Backtest ${f.backtest ?? '?'}% · Ao vivo ${f.live ?? '?'}% · Momento ${f.momento ?? '?'}%`,
    `⚡ Padrão ${f.padrao ?? '?'} · Atividade ${f.atividade ?? '?'}%${s.consenso > 1 ? ` · ${s.consenso}× consenso` : ''}`,
    aviso,
    alerta,
    '┗━━━━━━━━━━━━━━━━━━━━┛',
    '<i>Confiança = estimativa composta (backtest + live + momento + padrão + atividade + consenso). Não é garantia. RED após G1 custa ~3x a entrada.</i>'
  ].filter((x) => x !== null).join('\n');
}

function msgGale(s, r) {
  return [
    `⚠️ <b>GALE 1</b>  ·  #${s.id}`,
    `Rodada anterior: ${corCurta(r.resultado)}${placarDados(r)}`,
    `👉 Repita: ${corTxt(s.cor)}`,
    '<i>(esta mensagem some quando o sinal fechar)</i>'
  ].join('\n');
}

function multTxt(m) {
  if (m == null || m === '') return 'não informado pela mesa';
  return `×${String(m).replace(/^x/i, '')}`;
}

function msgGreen(s, r) {
  let titulo, extra = null;
  if (s.estado === 'EMPATE') {
    const g = s.empateGale === 0 ? 'ENTRADA DIRETA' : 'GALE 1';
    titulo = `🟡✅ <b>GREEN POR EMPATE</b> · ${g}`;
    extra = `💰 Multiplicador do empate: <b>${multTxt(r.mult)}</b>\n<i>Só paga o multiplicador quem apostou no EMPATE.</i>`;
  } else if (s.estado === 'GREEN') {
    titulo = '✅ <b>GREEN · ENTRADA DIRETA</b>';
  } else {
    titulo = `⚡✅ <b>GREEN · GALE 1</b>`;
  }
  return [
    `${titulo}  ·  #${s.id}`,
    `🎯 ${corCurta(s.cor)}  →  saiu ${corCurta(r.resultado)}${placarDados(r)}`,
    extra,
    `${s.emoji || '🧠'} ${esc(s.nome)}  ·  ${s.assertividade}%`,
    `⏱ ${hora()}`,
    '',
    msgPlacar(placar())
  ].filter((x) => x !== null).join('\n');
}

function msgRed(s, r) {
  return [
    `🟥 <b>RED</b>  ·  #${s.id}`,
    `🎯 ${corCurta(s.cor)}  →  saiu ${corCurta(r.resultado)}${placarDados(r)}`,
    `${s.emoji || '🧠'} ${esc(s.nome)}  ·  ${s.assertividade}%`,
    '🔁 Gale 1 usado sem acerto',
    `⏱ ${hora()}`,
    '',
    msgPlacar(placar())
  ].join('\n');
}

/* ============================ MOTOR DE SINAIS ============================ */

const pendente = () => S.signals.find((s) => s.estado === 'PENDENTE') || null;

function fechar(s, r, estado) {
  s.estado = estado;
  s.resultadoFinal = r.resultado;
  s.resolvidoMs = Date.now();
  dbSaveSignal(s);
  tgSend(msgGreen(s, r));
  tgApagarGale(s);
  log('GREEN', { id: s.id, estado });
}

function fecharLoss(s, r) {
  s.estado = 'LOSS';
  s.resultadoFinal = r.resultado;
  s.resolvidoMs = Date.now();
  dbSaveSignal(s);
  tgSend(msgRed(s, r));
  tgApagarGale(s);
  log('RED', { id: s.id });
}

function naoApurado(s, motivo) {
  s.estado = 'NAO_APURADO';
  s.resolvidoMs = Date.now();
  R.naoApurados += 1;
  dbSaveSignal(s);
  tgSend(`⚪ <b>SINAL #${s.id} NÃO APURADO</b> (falha de dados da mesa)\n<i>Não conta no placar.</i>\n⏱ ${hora()}`);
  tgApagarGale(s);
  log('NAO_APURADO', { id: s.id, motivo });
}

function reconciliar() {
  const s = pendente();
  if (!s) return;

  const rodadas = S.hist.filter((r) => r.t > (s.baseT || 0) && r.id !== s.baseId);
  let prevT = s.baseT || 0;

  for (let k = 0; k <= MAX_GALE; k++) {
    const r = rodadas[k];
    if (!r) break; 
    if (prevT && r.t - prevT > CFG.apuraGapMs) return naoApurado(s, 'buraco_nos_dados');
    prevT = r.t;

    if (r.resultado === E && CFG.empateGreen) {
      s.empateGale = k;
      s.empateMult = r.mult ?? null;
      return fechar(s, r, 'EMPATE');
    }
    if (r.resultado === s.cor) {
      return fechar(s, r, k === 0 ? 'GREEN' : 'G1');
    }

    if (k < MAX_GALE) {
      if (s.gale < k + 1) {
        s.gale = k + 1;
        dbSaveSignal(s);
        if (!rodadas[k + 1]) {
          tgSend(msgGale(s, r), (id) => { (s.galeMsgIds = s.galeMsgIds || []).push(id); });
        }
        log('GALE', { id: s.id, gale: s.gale });
      }
      continue;
    }

    return fecharLoss(s, r);
  }

  if (Date.now() - s.criadoMs > CFG.apuraMaxMs) naoApurado(s, 'sem_dados_tempo_maximo');
}

function criarSinal(ultima) {
  const rows = windowAt(S.hist, S.hist.length - 1);
  const e = pickEntry(rows);

  const s = {
    id: S.nextId++,
    criadoMs: Date.now(),
    cor: e.cor,
    key: e.key,
    nome: e.nome,
    emoji: e.emoji,
    assertividade: Math.max(1, Math.min(99, Math.round(e.confidence.score))),
    confidenceFactors: e.confidence.factors,
    taxa: Math.round(e.taxa),
    amostra: e.amostra,
    base: e.base,
    consenso: e.consenso,
    nivel: e.nivel,
    estado: 'PENDENTE',
    gale: 0,
    galeMsgIds: [],
    baseId: ultima.id,
    baseT: ultima.t
  };
  S.signals.push(s);
  if (S.signals.length > 1500) S.signals.splice(0, S.signals.length - 1500);
  R.ultimoSinal = { id: s.id, cor: s.cor, nome: s.nome, assertividade: s.assertividade, hora: hora(s.criadoMs) };
  R.ultimoMotivo = null;
  dbSaveSignal(s);
  tgSend(msgEntrada(s));
  log('SINAL', {
    id: s.id, cor: s.cor, key: s.key,
    confianca: s.assertividade, taxa: s.taxa, n: s.amostra, nivel: s.nivel,
    fatores: s.confidenceFactors
  });
}

function tentarSlot() {
  if (stopping) return;
  const slot = Math.floor(Date.now() / SLOT_MS);
  if (slot <= S.lastSlot) return;
  S.lastSlot = slot;

  if (!CFG.ativos) { R.ultimoMotivo = 'sinais_desativados'; return; }
  if (S.hist.length < 10) { R.ultimoMotivo = 'historico_curto'; return; }
  if (pendente()) { R.ultimoMotivo = 'slot_saltado_sinal_anterior_aberto'; return; }

  const ultima = S.hist[S.hist.length - 1];
  if (Date.now() - ultima.t > CFG.frescoMs) { R.ultimoMotivo = 'slot_saltado_mesa_parada'; return; }

  try { criarSinal(ultima); }
  catch (e) { R.ultimoMotivo = `erro_criar_sinal: ${e.message}`; log('ERRO_SINAL', e.message); }
}

/* ============================ SINCRONIZAÇÃO + WAKE ============================ */

async function syncOnce() {
  if (busy || stopping) return { ignorada: true };
  busy = true;
  try {
    const rounds = await fetchRounds();
    const warm = S.hist.length === 0;
    let lastT = S.hist.length ? S.hist[S.hist.length - 1].t : 0;

    const fresh = rounds.filter((r) => !S.seen.has(r.id));
    let desordem = false;

    for (const r of fresh) {
      S.seen.add(r.id);
      if (r.t < lastT) desordem = true;
      S.hist.push(r);
      dbSaveRound(r);
      if (r.t > lastT) lastT = r.t;
    }

    if (desordem) S.hist.sort((a, b) => a.t - b.t);
    if (S.hist.length > CFG.maxHist) S.hist.splice(0, S.hist.length - CFG.maxHist);

    reconciliar();

    if (fresh.length) { R.ultimaRodadaNova = new Date().toISOString(); R.ciclosSemNovos = 0; }
    else R.ciclosSemNovos++;

    if (warm && fresh.length) { R.ultimoMotivo = 'base_historica_inicializada'; log('BASE_INICIAL', { rodadas: fresh.length }); }
    R.ultimaSync = new Date().toISOString();
    return { sucesso: true, novas: fresh.length };
  } catch (e) {
    log('SYNC_ERRO', e.message);
    return { sucesso: false, erro: e.message };
  } finally {
    busy = false;
  }
}

function agendarSync() {
  if (stopping) return;
  syncTimer = setTimeout(async () => {
    await syncOnce();
    agendarSync();
  }, CFG.syncMs);
}

function tick() {
  const now = Date.now();
  const gap = now - lastTickMs;
  if (gap > 3000) {
    R.wakes += 1;
    R.ultimoWake = { at: new Date().toISOString(), gapMs: gap };
    log('WAKE_DETECTED', { gapMs: gap });
    syncOnce().catch(() => {});
  }
  lastTickMs = now;
  try { reconciliar(); } catch (e) { log('RECONCILIAR_ERRO', e.message); }
  tentarSlot();
}

/* ============================ KEEP-ALIVE ============================ */

const SELF_URL = process.env.SELF_URL || process.env.RENDER_EXTERNAL_URL || '';

function startKeepAlive() {
  if (!SELF_URL) {
    log('KEEPALIVE', 'SELF_URL/RENDER_EXTERNAL_URL ausente — usa UptimeRobot em /ping (60s)');
    return;
  }
  const alvo = `${SELF_URL.replace(/\/+$/, '')}/ping`;
  keepTimer = setInterval(() => {
    axios.get(alvo, { timeout: 15000 })
      .then(() => { R.ultimoKeepAlive = new Date().toISOString(); })
      .catch((e) => log('KEEPALIVE_ERRO', e.message));
  }, CFG.keepAliveMs);
  if (typeof keepTimer.unref === 'function') keepTimer.unref();
  log('KEEPALIVE', { alvo, ms: CFG.keepAliveMs });
}

/* ============================ ROTAS ============================ */

const app = express();
app.disable('x-powered-by');
app.use(cors());
app.use(express.json({ limit: '100kb' }));
app.use((_req, _res, next) => {
  const ult = R.ultimaSync ? Date.parse(R.ultimaSync) : 0;
  if (!busy && Date.now() - ult > CFG.syncMs * 3) syncOnce().catch(() => {});
  next();
});

function admin(req, res, next) {
  if (!ADMIN_TOKEN) return res.status(503).json({ sucesso: false, erro: 'ADMIN_TOKEN não configurado' });
  if (req.get('x-admin-token') !== ADMIN_TOKEN) return res.status(401).json({ sucesso: false, erro: 'Token inválido' });
  next();
}

const proximoSlotEmSeg = () =>
  Math.max(0, Math.ceil(((Math.floor(Date.now() / SLOT_MS) + 1) * SLOT_MS - Date.now()) / 1000));

app.get('/', (_req, res) =>
  res.json({
    nome: 'Capunga Dados — Bac Bo v11 (gale 1 · multi-fator · apuração por reconciliação · 24/7)',
    estado: 'online',
    horaAngola: hora(),
    estrategias: STRATS.length,
    deployId: R.deployId || '(hash do ficheiro)',
    rotas: ['/ping', '/health', '/diagnostico', '/sinais', '/estatisticas']
  })
);

app.get('/ping', (_req, res) => res.send('ok'));

app.get('/health', (_req, res) => {
  const p = pendente();
  res.json({
    sucesso: true,
    horaAngola: hora(),
    iniciadoEm: R.iniciadoEm,
    deployId: R.deployId,
    ultimaSync: R.ultimaSync,
    ultimaRodadaNova: R.ultimaRodadaNova,
    ciclosSemNovos: R.ciclosSemNovos,
    motivoUltimoSlotSemSinal: R.ultimoMotivo,
    proximoSlotEmSeg: proximoSlotEmSeg(),
    sinalPendente: p ? { id: p.id, gale: p.gale, idadeSeg: Math.round((Date.now() - p.criadoMs) / 1000) } : null,
    naoApuradosDesdeInicio: R.naoApurados,
    erroAPI: R.erroAPI,
    erroTelegram: R.erroTelegram,
    erroDB: R.erroDB,
    telegramPendentes: R.telegramPendentes,
    ultimoKeepAlive: R.ultimoKeepAlive,
    ultimoWake: R.ultimoWake,
    wakesTotal: R.wakes
  });
});

app.get('/diagnostico', (_req, res) => {
  const st = S.stats || { per: {}, baseRate: null, baseN: 0 };
  res.json({
    sucesso: true,
    horaAngola: hora(),
    deployId: R.deployId,
    telegramConfigurado: Boolean(BOT_TOKEN && CHAT_ID),
    dbConfigurada: Boolean(pool),
    rodadasEmMemoria: S.hist.length,
    ultimasRodadas: S.hist.slice(-12).reverse().map((r) => ({ r: r.resultado, pj: r.pj, pb: r.pb, hora: hora(r.t) })),
    sinalPendente: pendente(),
    ultimoSinal: R.ultimoSinal,
    proximoSlotEmSeg: proximoSlotEmSeg(),
    taxaBaseGale1Percent: st.baseRate ? Number((st.baseRate * 100).toFixed(1)) : null,
    amostrasBacktest: st.baseN,
    configuracao: { ...CFG, maxGale: MAX_GALE, slotMin: 5 },
    placar: placar(),
    wake: { ultimoWake: R.ultimoWake, total: R.wakes }
  });
});

app.get('/sinais', (req, res) => {
  const n = Math.min(Math.max(Number.parseInt(req.query.limit, 10) || 50, 1), 500);
  res.json({ sucesso: true, sinais: S.signals.slice(-n).reverse().map(({ galeMsgIds, ...x }) => x) });
});

app.get('/estatisticas', (_req, res) => {
  const st = getStats();
  const aoVivo = {};
  for (const s of S.signals) {
    if (!FECHADOS.includes(s.estado)) continue;
    const k = s.key || 'N/D';
    const e = aoVivo[k] || (aoVivo[k] = { sinais: 0, greens: 0, reds: 0 });
    e.sinais++;
    if (s.estado === 'LOSS') e.reds++; else e.greens++;
  }
  const bt = Object.entries(st.per).map(([key, v]) => ({
    key,
    amostras: v.n,
    acertoPercent: Number(((v.w / v.n) * 100).toFixed(1)),
    wilsonLB: Number((wilsonLB(v.w, v.n) * 100).toFixed(1)),
    testeAmostras: v.nt,
    testeAcertoPercent: v.nt ? Number(((v.wt / v.nt) * 100).toFixed(1)) : null,
    elegivel: v.n >= CFG.minAmostra
  })).sort((a, b) => b.wilsonLB - a.wilsonLB).slice(0, 60);
  res.json({
    sucesso: true,
    placarDia: placar(),
    taxaBaseGale1Percent: st.baseRate ? Number((st.baseRate * 100).toFixed(1)) : null,
    aviso: 'Confiança combina Wilson LB + live + momento + padrão + atividade + consenso.',
    backtest: bt,
    ao_vivo_por_estrategia: aoVivo
  });
});

app.post('/admin/sincronizar', admin, async (_req, res) => res.json(await syncOnce()));

app.post('/admin/forcar-sinal', admin, (_req, res) => {
  if (pendente()) return res.status(409).json({ sucesso: false, erro: 'Há um sinal aberto' });
  if (!S.hist.length) return res.status(409).json({ sucesso: false, erro: 'Sem histórico' });
  criarSinal(S.hist[S.hist.length - 1]);
  res.json({ sucesso: true, sinal: R.ultimoSinal });
});

app.post('/admin/cancelar-sinal', admin, (_req, res) => {
  const s = pendente();
  if (!s) return res.status(404).json({ sucesso: false, erro: 'Sem sinal aberto' });
  s.estado = 'CANCELADO';
  s.resolvidoMs = Date.now();
  dbSaveSignal(s);
  tgSend(`🚫 <b>SINAL #${s.id} CANCELADO</b> (admin)\n⏱ ${hora()}`);
  tgApagarGale(s);
  res.json({ sucesso: true });
});

app.post('/admin/telegram-teste', admin, async (_req, res) => {
  const r = await tgCall('sendMessage', {
    text: `🎲 <b>CAPUNGA · BAC BO v11</b>\n✅ Ligação Telegram OK\n🕒 ${hora()}`,
    parse_mode: 'HTML'
  });
  res.status(r.ok ? 200 : 502).json({ sucesso: r.ok, erro: R.erroTelegram });
});

app.post('/admin/reset-sinais', admin, async (req, res) => {
  if (req.body?.confirmar !== 'APAGAR') return res.status(400).json({ sucesso: false, erro: 'Envie {"confirmar":"APAGAR"}' });
  try {
    if (pool) { await dbChain; await pool.query('TRUNCATE g2_sinais RESTART IDENTITY'); }
    S.signals = []; S.nextId = CFG.sinalInicial;
    res.json({ sucesso: true, mensagem: 'Sinais apagados. Rodadas mantidas.' });
  } catch (e) { res.status(500).json({ sucesso: false, erro: e.message }); }
});

app.post('/admin/reset-tudo', admin, async (req, res) => {
  if (req.body?.confirmar !== 'APAGAR') return res.status(400).json({ sucesso: false, erro: 'Envie {"confirmar":"APAGAR"}' });
  try {
    S.hist = []; S.seen = new Set(); S.signals = []; S.stats = null; S.statsAtLen = -1;
    S.nextId = CFG.sinalInicial;
    if (pool) {
      await dbChain;
      await pool.query('DROP TABLE IF EXISTS g2_sinais, g2_rodadas, g2_meta');
      await ensureSchema();
    }
    res.json({ sucesso: true, mensagem: 'Base totalmente apagada. Vai reimportar o histórico.' });
  } catch (e) { res.status(500).json({ sucesso: false, erro: e.message }); }
});

app.use((req, res) => res.status(404).json({ sucesso: false, erro: 'Rota não encontrada', caminho: req.path }));

/* ============================ ARRANQUE ============================ */

async function start() {
  if (DATABASE_URL) {
    pool = new Pool({
      connectionString: DATABASE_URL,
      ssl: /localhost|127\.0\.0\.1/i.test(DATABASE_URL) ? false : { rejectUnauthorized: false },
      max: 4, idleTimeoutMillis: 20000, connectionTimeoutMillis: 12000
    });
    pool.on('error', (e) => { R.erroDB = e.message; log('POOL_ERRO', e.message); });
    try {
      await ensureSchema();
      await checkDeployReset();
      await loadFromDB();
      log('DB', { rodadas: S.hist.length, sinaisRecentes: S.signals.length, proximoId: S.nextId, deployId: DEPLOY_ID });
    } catch (e) {
      R.erroDB = e.message;
      log('DB_INDISPONIVEL_SEGUE_EM_MEMORIA', e.message);
      pool = null;
    }
  } else {
    log('DB', 'DATABASE_URL ausente: a funcionar apenas em memória');
  }

  server = app.listen(PORT, '0.0.0.0', () =>
    log('START', {
      porta: PORT, estrategias: STRATS.length, maxGale: MAX_GALE, slotMin: 5,
      proximoSlotEmSeg: proximoSlotEmSeg(), deployId: DEPLOY_ID
    })
  );

  const first = await syncOnce();
  log('PRIMEIRA_SYNC', first);
  if (S.hist.length >= 10) getStats(true);

  agendarSync();
  lastTickMs = Date.now();
  tickTimer = setInterval(tick, 1000);
  startKeepAlive();
}

async function shutdown() {
  if (stopping) return;
  stopping = true;
  if (syncTimer) clearTimeout(syncTimer);
  if (tickTimer) clearInterval(tickTimer);
  if (keepTimer) clearInterval(keepTimer);
  try { await dbChain; } catch (_) {}
  if (server) await new Promise((r) => server.close(r));
  if (pool) await pool.end().catch(() => {});
  process.exit(0);
}

process.on('unhandledRejection', (e) => log('UNHANDLED_REJECTION', String(e)));
process.on('uncaughtException', (e) => log('UNCAUGHT_EXCEPTION', e.message || String(e)));
process.on('SIGTERM', () => shutdown().catch(() => process.exit(1)));
process.on('SIGINT', () => shutdown().catch(() => process.exit(1)));

start().catch((e) => { console.error('[FATAL]', e); process.exit(1); });