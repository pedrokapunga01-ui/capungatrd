'use strict';

/**
 * CAPUNGA DADOS — BAC BO · v11
 *
 *  ✅ Gale 1 (máximo)
 *  ✅ Sem "sinal expirado" — o sinal fica aberto até a mesa resolver
 *  ✅ Confiança multi-fator (>88% = dispara · configurável)
 *  ✅ Estilo de mensagem reformulado
 *  ✅ Keep-alive robusto (não dorme)
 *  ✅ Filtro estrito — só entra quando as probabilidades alinham
 *
 * Variáveis:
 *   BOT_TOKEN · CHAT_ID · WEBHOOK_SECRET · DATABASE_URL · ADMIN_TOKEN
 *   API_BACBO · MIN_AMOSTRA (80) · MIN_CONFIANCA (88)
 *   SYNC_MS (3000) · SINAL_INICIAL (1)
 *   EMPATE_VALE_GREEN (true) · STREAK_ADJUST (true)
 *   SELF_URL / RENDER_EXTERNAL_URL · KEEPALIVE_MS (240000)
 */

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
const WEBHOOK_SECRET = process.env.WEBHOOK_SECRET || '';
const DATABASE_URL = process.env.DATABASE_URL || '';
const API_BACBO =
  process.env.API_BACBO || 'https://api-cs.casino.org/svc-evolution-game-events/api/bacbo';

const MAX_GALE = 1;
const SLOT_MS = 5 * 60 * 1000;

const CFG = {
  ativos: bool(process.env.SINAIS_ATIVOS, true),
  empateGreen: bool(process.env.EMPATE_VALE_GREEN, true),
  minAmostra: num('MIN_AMOSTRA', 80, 20, 5000),
  minConfianca: num('MIN_CONFIANCA', 88, 60, 99),
  streakAdjust: bool(process.env.STREAK_ADJUST, true),
  syncMs: num('SYNC_MS', 3000, 2000, 30000),
  sinalInicial: num('SINAL_INICIAL', 1, 1, 100000000),
  maxHist: 5000,
  gapMs: 6 * 60 * 1000,
  frescoMs: 6 * 60 * 1000
};

const J = 'Jogador';
const B = 'Banca';
const E = 'Empate';
const FECHADOS = ['GREEN', 'G1', 'EMPATE', 'LOSS'];

const isCol = (x) => x === J || x === B;
const opp = (c) => (c === J ? B : c === B ? J : null);

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
  ultimaSync: null,
  ultimaRodadaNova: null,
  ultimaWebhookMs: null,
  rodadasPorWebhook: 0,
  ciclosSemNovos: 0,
  erroAPI: null,
  erroDB: null,
  erroTelegram: null,
  ultimoMotivo: null,
  ultimoSinal: null,
  ultimaConfiancaBloqueada: null,
  telegramPendentes: 0,
  ultimoKeepAlive: null
};

let pool = null;
let server = null;
let syncTimer = null;
let slotTimer = null;
let busy = false;
let stopping = false;

/* ============================ UTILS ============================ */

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const log = (tag, data) =>
  console.log(`[${new Date().toISOString()}] [${tag}] ${typeof data === 'string' ? data : JSON.stringify(data)}`);

const fmtHora = new Intl.DateTimeFormat('pt-PT', {
  timeZone: 'Africa/Luanda', hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false
});
const fmtDia = new Intl.DateTimeFormat('en-CA', {
  timeZone: 'Africa/Luanda', year: 'numeric', month: '2-digit', day: '2-digit'
});
const hora = (ms = Date.now()) => fmtHora.format(new Date(ms));
const dia = (ms = Date.now()) => fmtDia.format(new Date(ms));
const slotLabel = (ms = Date.now()) => {
  const d = new Date(Math.floor(ms / SLOT_MS) * SLOT_MS);
  return `${String(d.getHours()).padStart(2, '0')}:${String(d.getMinutes()).padStart(2, '0')}`;
};

const esc = (s) => String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
const corCurta = (c) => (c === B ? '🔴 Banca' : c === J ? '🔵 Jogador' : '🟡 Empate');

/* ============================ NORMALIZAÇÃO ============================ */

function normalizeOutcome(v) {
  if (v && typeof v === 'object') v = v.outcome ?? v.resultado ?? v.winner ?? v.name;
  const s = String(v ?? '')
    .normalize('NFD').replace(/[̀-ͯ]/g, '')
    .trim().toLowerCase().replace(/[\s_-]+/g, '');
  if (['player', 'jogador', 'playerwin', 'playerwins', 'playerwon', 'playerwinner', 'blue', 'azul'].includes(s)) return J;
  if (['banker', 'banca', 'bankerwin', 'bankerwins', 'bankerwon', 'bankerwinner', 'red', 'vermelho'].includes(s)) return B;
  if (['tie', 'draw', 'empate', 'equal', 'push', 'tied', 'verde', 'green', 'amarelo', 'yellow'].includes(s)) return E;
  return null;
}

const validMs = (v) => {
  if (!v) return null;
  const t = new Date(v).getTime();
  return Number.isNaN(t) ? null : t;
};

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
  const t = validMs(
    d.settledAt ?? d.endTime ?? d.endedAt ?? d.finishedAt ?? d.fim ??
    item?.settledAt ?? item?.endTime ?? item?.fim ?? item?.t ?? item?.timestamp
  );

  const raw =
    r.outcome ?? r.resultado ?? r.winner ??
    d.outcome ?? d.resultado ?? d.winner ??
    item?.outcome ?? item?.resultado ?? item?.winner ?? item?.side ??
    (typeof d.result === 'string' ? d.result : null) ??
    (typeof item?.result === 'string' ? item.result : null);

  const resultado = normalizeOutcome(raw);
  const resolvida =
    ['resolved', 'settled', 'finished', 'complete', 'completed', 'resolvido', 'concluido', 'finalizado']
      .some((w) => status.includes(w)) || Boolean(t);

  const id = String(
    d.id ?? d.roundId ?? d.round_id ?? d.eventId ??
    item?.id ?? item?.roundId ?? item?.round_id ?? item?.eventId ?? ''
  ).trim();

  if (!id || !resultado || !resolvida || !t) return null;

  const pj = Number(r.playerDice?.score ?? d.playerDice?.score ?? d.playerScore ?? d.player_score ?? d.pontos_jogador ?? item?.pj);
  const pb = Number(r.bankerDice?.score ?? d.bankerDice?.score ?? d.bankerScore ?? d.banker_score ?? d.pontos_banca ?? item?.pb);
  const mult = r.multiplier ?? d.multiplier ?? d.multiplicador ?? item?.mult ?? null;

  return {
    id, resultado, t,
    pj: Number.isFinite(pj) ? pj : null,
    pb: Number.isFinite(pb) ? pb : null,
    mult: mult == null ? null : String(mult)
  };
}

/* ============================ INGESTÃO ============================ */

async function ingestRound(r) {
  if (!r || !r.id || S.seen.has(r.id)) return { aceita: false, motivo: 'duplicada' };

  S.seen.add(r.id);
  const ultima = S.hist[S.hist.length - 1];
  const aoVivo = ultima && r.t > ultima.t;

  S.hist.push(r);
  dbSaveRound(r);

  if (S.hist.length > CFG.maxHist) {
    const removed = S.hist.splice(0, S.hist.length - CFG.maxHist);
    for (const x of removed) S.seen.delete(x.id);
  }

  R.ultimaRodadaNova = new Date().toISOString();
  R.ciclosSemNovos = 0;

  if (aoVivo) {
    try { resolver(r); } catch (e) { log('RESOLVER_ERRO', e.message); }
  }
  return { aceita: true, aoVivo, id: r.id, resultado: r.resultado };
}

/* ============================ FONTE A — API ============================ */

async function fetchRoundsFromAPI() {
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
  throw last || new Error('API indisponível');
}

async function syncFromAPI() {
  if (busy || stopping) return;
  busy = true;
  try {
    const rounds = await fetchRoundsFromAPI();
    let novas = 0;
    for (const r of rounds) {
      const res = await ingestRound(r);
      if (res.aceita) novas++;
    }
    R.ultimaSync = new Date().toISOString();
    if (!novas) R.ciclosSemNovos++;
  } catch (e) {
    log('SYNC_ERRO', e.message);
  } finally {
    busy = false;
  }
}

/* ============================ ESTRATÉGIAS ============================ */

function windowAt(H, i) {
  const rows = [];
  for (let j = i; j >= 0 && rows.length < 24; j--) {
    if (rows.length && rows[rows.length - 1].t - H[j].t > CFG.gapMs) break;
    rows.push(H[j]);
  }
  return rows;
}

const hasScore = (r) => r && Number.isFinite(r.pj) && Number.isFinite(r.pb);
const margem = (r) => Math.abs(r.pj - r.pb);
const streak = (seq) => {
  if (!seq.length) return 0;
  let n = 0;
  for (const r of seq) { if (r.resultado === seq[0].resultado) n++; else break; }
  return n;
};

const STRATS = [
  function repeticao({ seq }) {
    const n = streak(seq);
    if (n < 2 || n > 8) return null;
    return { cor: seq[0].resultado, key: `REPETICAO_${Math.min(n, 6)}`, nome: `Repetição x${n}`, emoji: '🔁' };
  },
  function quebra({ seq }) {
    const n = streak(seq);
    if (n < 3 || n > 9) return null;
    return { cor: opp(seq[0].resultado), key: `QUEBRA_${Math.min(n, 7)}`, nome: `Quebra de sequência x${n}`, emoji: '💥' };
  },
  function surf({ seq }) {
    if (seq.length < 4) return null;
    let L = 1;
    while (L < seq.length && seq[L].resultado !== seq[L - 1].resultado) L++;
    if (L < 3) return null;
    return { cor: opp(seq[0].resultado), key: `SURF_${Math.min(L, 6)}`, nome: `Surf (alternância ${L})`, emoji: '🌊' };
  },
  function doisDois({ seq }) {
    if (seq.length < 4) return null;
    const [a, b, c, d] = seq.map((x) => x.resultado);
    if (a === b && c === d && a !== c) return { cor: c, key: 'DOIS_DOIS', nome: '2+2 (blocos de dois)', emoji: '♟️' };
    return null;
  },
  function tresUm({ seq }) {
    if (seq.length < 4) return null;
    const [a, b, c, d] = seq.map((x) => x.resultado);
    if (a !== b && b === c && c === d) return { cor: a, key: 'TRES_UM', nome: 'Quebra confirmada (3→1)', emoji: '🧩' };
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
    return { cor: dom, key: `FORCA_${bucket}`, nome: `Força da mesa ${Math.round(pct * 100)}%`, emoji: '💪' };
  },
  function reversao({ seq }) {
    const s = seq.slice(0, 20);
    if (s.length < 16) return null;
    const pj = s.filter((x) => x.resultado === J).length / s.length;
    const pct = Math.max(pj, 1 - pj);
    if (pct < 0.65) return null;
    return { cor: opp(pj >= 0.5 ? J : B), key: 'REVERSAO_20', nome: `Retorno à média (${Math.round(pct * 100)}%)`, emoji: '⚖️' };
  },
  function numMargem({ rows }) {
    const r = rows[0];
    if (!r || !isCol(r.resultado) || !hasScore(r) || margem(r) < 5) return null;
    return { cor: r.resultado, key: 'NUM_MARGEM_5', nome: `Numérica · margem ${r.pj}×${r.pb}`, emoji: '🔢' };
  },
  function numTendencia({ rows }) {
    const s = rows.slice(0, 6).filter(hasScore);
    if (s.length < 6) return null;
    const mj = s.reduce((a, r) => a + r.pj, 0) / s.length;
    const mb = s.reduce((a, r) => a + r.pb, 0) / s.length;
    const diff = mj - mb;
    if (Math.abs(diff) < 1.3) return null;
    return { cor: diff > 0 ? J : B, key: `NUM_TEND_${Math.abs(diff) >= 2 ? 2 : 1}`, nome: `Numérica · média ${mj.toFixed(1)}×${mb.toFixed(1)}`, emoji: '📊' };
  },
  function doisDoisPar({ seq }) {
    if (seq.length < 3) return null;
    const [a, b, c] = seq.map((x) => x.resultado);
    if (a !== b && b === c) return { cor: a, key: 'DOIS_DOIS_PAR', nome: '2+2 (completa par)', emoji: '♟️' };
    return null;
  },
  function numAlta({ rows }) {
    const r = rows[0];
    if (!r || !isCol(r.resultado) || !hasScore(r)) return null;
    const v = r.resultado === J ? r.pj : r.pb;
    if (v < 10 || margem(r) < 1) return null;
    return { cor: r.resultado, key: 'NUM_ALTA_10', nome: `Numérica Alta (${v})`, emoji: '🔢' };
  },
  function numDiff4({ seq }) {
    if (seq.length < 3) return null;
    const t = seq.slice(0, 3);
    if (!t.every((r) => hasScore(r) && margem(r) >= 4 && r.resultado === t[0].resultado)) return null;
    return { cor: t[0].resultado, key: 'NUM_DIFF4_X3', nome: 'Numérica Diff≥4 x3', emoji: '🔢' };
  },
  function numViradaBaixa({ seq }) {
    if (seq.length < 2) return null;
    const [a, b] = seq;
    if (!hasScore(a) || !hasScore(b) || a.resultado !== b.resultado) return null;
    const sc = (r) => (r.resultado === J ? r.pj : r.pb);
    if (sc(a) > 5 || sc(b) > 5) return null;
    return { cor: opp(a.resultado), key: 'NUM_VIRADA_BAIXA', nome: 'Numérica Virada Baixa', emoji: '🔢' };
  },
  function contexto({ seq }) {
    const out = [];
    for (const k of [3, 4, 5]) {
      if (seq.length < k) continue;
      const p = seq.slice(0, k).map((x) => (x.resultado === J ? 'J' : 'B')).join('');
      const vis = p.split('').reverse().join('');
      const last = seq[0].resultado;
      out.push({ cor: last, key: `CTX${k}_${p}_SEG`, nome: `Sequência ${vis} → segue`, emoji: '🧬' });
      out.push({ cor: opp(last), key: `CTX${k}_${p}_CON`, nome: `Sequência ${vis} → contraria`, emoji: '🧬' });
    }
    return out;
  },
  function numPosEmpate({ rows, seq }) {
    if (!rows[0] || rows[0].resultado !== E || !seq.length) return null;
    return { cor: seq[0].resultado, key: 'NUM_POS_EMPATE', nome: 'Pós-empate (cor anterior)', emoji: '🎯' };
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
  const L = S.signals.filter((x) => x.key === key && FECHADOS.includes(x.estado)).slice(-15);
  if (L.length < 10) return false;
  return L.filter((x) => x.estado !== 'LOSS').length / L.length < 0.6;
}

/* ============================ CONFIANÇA MULTI-FATOR ============================ */

function computeConfidence(strat, stats) {
  // A âncora é o limite inferior de Wilson do backtest com Gale 1
  const anchor = strat.piso;

  const mods = [];
  const add = (name, delta) => { if (delta) mods.push({ name, delta }); };

  // 1. Consenso entre estratégias
  if (strat.consenso >= 4) add('consenso_forte', 1.5);
  else if (strat.consenso >= 2) add('consenso', 0.8);

  // 2. Tamanho da amostra
  if (strat.amostra >= 500) add('amostra_forte', 1.2);
  else if (strat.amostra >= 250) add('amostra_ok', 0.6);
  else if (strat.amostra < 80) add('amostra_baixa', -2);

  // 3. Holdout concordante
  if (strat.testeN >= 30 && strat.testeTaxa >= strat.taxa - 3) add('holdout_ok', 1);
  else if (strat.testeN >= 15 && strat.testeTaxa < strat.taxa - 15) add('holdout_fraca', -3);

  // 4. Desempenho ao vivo desta estratégia (últimos 8 sinais fechados)
  const recentLive = S.signals.filter((s) => s.key === strat.key && FECHADOS.includes(s.estado)).slice(-8);
  if (recentLive.length >= 4) {
    const rate = recentLive.filter((s) => s.estado !== 'LOSS').length / recentLive.length;
    if (rate >= 0.9) add('ao_vivo_forte', 1);
    else if (rate < 0.6) add('ao_vivo_fraca', -5);
  }

  // 5. Vantagem sobre a taxa-base
  const baseRate = stats.baseRate != null ? stats.baseRate * 100 : 0;
  const edge = strat.taxa - baseRate;
  if (edge >= 15) add('edge_forte', 0.8);
  else if (edge < 5) add('edge_fraca', -1);

  const total = anchor + mods.reduce((a, m) => a + m.delta, 0);
  const score = Math.round(Math.max(0, Math.min(99, total)) * 10) / 10;

  return { score, anchor: Math.round(anchor * 10) / 10, mods };
}

function dynamicThreshold() {
  if (!CFG.streakAdjust) return CFG.minConfianca;
  const last5 = S.signals.filter((s) => FECHADOS.includes(s.estado)).slice(-5);
  if (last5.length < 3) return CFG.minConfianca;
  const last3 = last5.slice(-3);
  const losses = last3.filter((s) => s.estado === 'LOSS').length;
  if (losses >= 3) return Math.min(99, CFG.minConfianca + 3);
  if (losses >= 2) return Math.min(99, CFG.minConfianca + 1);
  return CFG.minConfianca;
}

/* ============================ SELEÇÃO ============================ */

function pickEntry(rows, stats) {
  const todos = runStrategies(rows)
    .map((c) => {
      const e = stats.per[c.key] || { n: 0, w: 0, nt: 0, wt: 0 };
      const taxa = e.n ? (e.w / e.n) * 100 : 0;
      const piso = wilsonLB(e.w, e.n) * 100;
      const nTreino = e.n - e.nt;
      const pisoTreino = nTreino > 0 ? wilsonLB(e.w - e.wt, nTreino) * 100 : 0;
      return {
        ...c,
        amostra: e.n,
        taxa,
        piso,
        pisoTreino,
        testeN: e.nt,
        testeTaxa: e.nt ? (e.wt / e.nt) * 100 : 0
      };
    })
    .filter((c) => !suspensoAoVivo(c.key));

  const porCor = { [J]: [], [B]: [] };
  for (const c of todos) porCor[c.cor].push(c);

  const finalistas = [];
  for (const cor of [J, B]) {
    const arr = porCor[cor].filter((c) => c.amostra >= CFG.minAmostra);
    if (!arr.length) continue;
    arr.sort((a, b) => b.pisoTreino - a.pisoTreino || b.amostra - a.amostra);
    const top = arr[0];
    const conf = computeConfidence({ ...top, consenso: arr.length }, stats);
    finalistas.push({
      ...top,
      consenso: arr.length,
      confianca: conf.score,
      confiancaAnchor: conf.anchor,
      confiancaMods: conf.mods
    });
  }

  if (!finalistas.length) return null;
  finalistas.sort((a, b) => b.confianca - a.confianca);
  const best = finalistas[0];
  const minimo = dynamicThreshold();

  if (best.confianca < minimo) {
    return {
      skip: true,
      motivo: 'confianca_abaixo',
      confianca: best.confianca,
      minimo,
      cor: best.cor,
      key: best.key
    };
  }
  return best;
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
      resultado_final TEXT, resolvido_em TIMESTAMPTZ, base_id TEXT
    )`);
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
    id: Number(x.id), criadoMs: new Date(x.criado_em).getTime(), cor: x.cor,
    key: x.estrategia, nome: x.nome, assertividade: x.assertividade, taxa: x.taxa, amostra: x.amostra,
    estado: x.estado, gale: x.gale, empateGale: x.empate_gale, empateMult: x.empate_mult,
    resultadoFinal: x.resultado_final, resolvidoMs: x.resolvido_em ? new Date(x.resolvido_em).getTime() : null,
    baseId: x.base_id, galeMsgIds: []
  }));

  const mx = await pool.query(`SELECT COALESCE(MAX(id), 0)::bigint AS m FROM g2_sinais`);
  const m = Number(mx.rows[0].m);
  S.nextId = m > 0 ? m + 1 : CFG.sinalInicial;

  // Segurança de arranque: sinais pendentes órfãos (>2h) viram CANCELADO.
  // Isto NÃO é expiração — é limpeza de estado após crash/restart.
  for (const s of S.signals) {
    if (s.estado === 'PENDENTE' && (Date.now() - s.criadoMs) > 2 * 60 * 60 * 1000) {
      s.estado = 'CANCELADO';
      s.resultadoFinal = 'stale_no_arranque';
      s.resolvidoMs = Date.now();
      dbSaveSignal(s);
    }
  }
}

function dbSaveRound(r) {
  dbq(() => pool.query(
    `INSERT INTO g2_rodadas (id, resultado, fim, pj, pb, mult) VALUES ($1,$2,$3,$4,$5,$6) ON CONFLICT (id) DO NOTHING`,
    [r.id, r.resultado, new Date(r.t), r.pj, r.pb, r.mult]
  ));
}

function dbSaveSignal(s) {
  dbq(() => pool.query(
    `INSERT INTO g2_sinais (id, criado_em, cor, estrategia, nome, assertividade, taxa, amostra, estado, gale,
                            empate_gale, empate_mult, resultado_final, resolvido_em, base_id)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)
     ON CONFLICT (id) DO UPDATE SET
       estado = EXCLUDED.estado, gale = EXCLUDED.gale,
       empate_gale = EXCLUDED.empate_gale, empate_mult = EXCLUDED.empate_mult,
       resultado_final = EXCLUDED.resultado_final, resolvido_em = EXCLUDED.resolvido_em`,
    [
      s.id, new Date(s.criadoMs), s.cor, s.key, s.nome, s.assertividade ?? null, s.taxa ?? null, s.amostra ?? null,
      s.estado, s.gale, s.empateGale ?? null, s.empateMult ?? null, s.resultadoFinal || null,
      s.resolvidoMs ? new Date(s.resolvidoMs) : null, s.baseId || null
    ]
  ));
}

/* ============================ TELEGRAM ============================ */

const tgQueue = [];
let tgRunning = false;
const stripHtml = (t) => t.replace(/<[^>]+>/g, '').replace(/&lt;/g, '<').replace(/&gt;/g, '>').replace(/&amp;/g, '&');

async function tgCall(method, payload) {
  if (!BOT_TOKEN || !CHAT_ID) { R.erroTelegram = 'BOT_TOKEN ou CHAT_ID não configurado'; return { ok: true, skipped: true }; }
  try {
    const resp = await axios.post(`https://api.telegram.org/bot${BOT_TOKEN}/${method}`, { chat_id: CHAT_ID, ...payload }, { timeout: 15000 });
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
      tgQueue.shift(); R.telegramPendentes = tgQueue.length;
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

const kick = () => { if (!tgRunning) tgRun().catch((e) => { tgRunning = false; log('TG_FILA_ERRO', e.message); }); };
const tgSend = (text, onId) => { tgQueue.push({ kind: 'send', text, onId, tries: 0, plain: false }); R.telegramPendentes = tgQueue.length; kick(); };
const tgApagarGale = (sig) => { tgQueue.push({ kind: 'delete', sig }); kick(); };

/* ============================ MENSAGENS (novo estilo) ============================ */

function placar() {
  const hoje = dia();
  const L = S.signals.filter((s) => dia(s.criadoMs) === hoje);
  const c = (st) => L.filter((s) => s.estado === st).length;
  const green = c('GREEN'), g1 = c('G1'), emp = c('EMPATE'), loss = c('LOSS');
  const wins = green + g1 + emp;
  const fechados = wins + loss;
  const taxa = fechados ? (wins / fechados) * 100 : 0;

  const fin = L.filter((s) => FECHADOS.includes(s.estado));
  let seguidos = 0;
  for (let i = fin.length - 1; i >= 0; i--) { if (fin[i].estado === 'LOSS') break; seguidos++; }

  return { green, g1, emp, loss, wins, fechados, taxa, seguidos };
}

const rodapeHoje = () => {
  const p = placar();
  const taxa = p.fechados ? p.taxa.toFixed(1) : '—';
  return `📈 <b>Hoje: ${p.wins}G · ${p.loss}R · ${taxa}%</b>`;
};

function msgEntrada(s) {
  return [
    `🎯 <b>SINAL #${String(s.id).padStart(3, '0')}</b>`,
    `▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬`,
    '',
    `${s.cor === J ? '🔵' : '🔴'} <b>${s.cor.toUpperCase()}</b>  ·  Gale 1`,
    CFG.empateGreen ? '🟡 Empate = Green' : null,
    '',
    `▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬`,
    `📊 Confiança: <b>${s.confianca.toFixed(1)}%</b>`,
    `${s.emoji || '🧠'} ${esc(s.nome)}`,
    s.consenso > 1 ? `🤝 ${s.consenso} padrões alinhados` : null,
    '',
    `⏰ ${hora(s.criadoMs).slice(0, 5)} · Slot ${slotLabel(s.criadoMs)}`
  ].filter((x) => x !== null).join('\n');
}

function msgGale(s, r) {
  const placarTxt = hasScore(r) ? ` (${r.pj}×${r.pb})` : '';
  return [
    `⚡ <b>GALE 1 · #${String(s.id).padStart(3, '0')}</b>`,
    '',
    `Saiu ${corCurta(r.resultado)}${placarTxt}`,
    `👉 Repetir: ${s.cor === J ? '🔵' : '🔴'} <b>${s.cor.toUpperCase()}</b>`
  ].join('\n');
}

function msgGreen(s, r) {
  const placarTxt = hasScore(r) ? ` (${r.pj}×${r.pb})` : '';
  const cab = [];
  if (s.estado === 'EMPATE') {
    cab.push(`🟡✅ <b>GREEN · EMPATE · #${String(s.id).padStart(3, '0')}</b>`, '');
    cab.push(`Empate na entrada`);
    if (r.mult) cab.push(`🎰 Multiplicador: ×${String(r.mult).replace(/^x/i, '')}`);
  } else if (s.estado === 'GREEN') {
    cab.push(`✅ <b>GREEN · #${String(s.id).padStart(3, '0')}</b>`, '');
    cab.push(`🎯 ${s.cor} → ${r.resultado}${placarTxt}`);
  } else {
    cab.push(`⚡✅ <b>GREEN · GALE 1 · #${String(s.id).padStart(3, '0')}</b>`, '');
    cab.push(`🎯 ${s.cor} → ${r.resultado}${placarTxt}`);
    cab.push('🛡 Gale 1 funcionou');
  }
  cab.push(`⏱ ${hora()}`, '', rodapeHoje());
  return cab.join('\n');
}

function msgRed(s, r) {
  const placarTxt = hasScore(r) ? ` (${r.pj}×${r.pb})` : '';
  return [
    `🔴 <b>RED · #${String(s.id).padStart(3, '0')}</b>`,
    '',
    `🎯 ${s.cor} → ${r.resultado}${placarTxt}`,
    '🛡 Gale 1 sem acerto',
    `⏱ ${hora()}`,
    '',
    rodapeHoje()
  ].join('\n');
}

/* ============================ MOTOR ============================ */

const pendente = () => S.signals.find((s) => s.estado === 'PENDENTE') || null;

function fechar(s, r, estado) {
  s.estado = estado;
  s.resultadoFinal = r.resultado;
  s.resolvidoMs = Date.now();
  dbSaveSignal(s);
  tgSend(msgGreen(s, r));
  tgApagarGale(s);
}

function resolver(r) {
  const s = pendente();
  if (!s) return false;
  if (r.t <= s.baseT || r.id === s.baseId) return false;

  // Empate conta como Green (na entrada ou no Gale)
  if (r.resultado === E && CFG.empateGreen) {
    s.empateGale = s.gale;
    s.empateMult = r.mult ?? null;
    fechar(s, r, 'EMPATE');
    return true;
  }

  // Cor certa (entrada ou Gale)
  if (r.resultado === s.cor) {
    fechar(s, r, s.gale === 0 ? 'GREEN' : `G${s.gale}`);
    return true;
  }

  // Cor errada — se ainda cabe Gale, abre Gale 1
  if (s.gale < MAX_GALE) {
    s.gale += 1;
    dbSaveSignal(s);
    tgSend(msgGale(s, r), (id) => { (s.galeMsgIds = s.galeMsgIds || []).push(id); });
    return false;
  }

  // Cor errada e Gale esgotado → RED
  s.estado = 'LOSS';
  s.resultadoFinal = r.resultado;
  s.resolvidoMs = Date.now();
  dbSaveSignal(s);
  tgSend(msgRed(s, r));
  tgApagarGale(s);
  return true;
}

function criarSinal(ultima, override = false) {
  const rows = windowAt(S.hist, S.hist.length - 1);
  if (!rows.length) { R.ultimoMotivo = 'sem_janela'; return null; }

  const stats = getStats();
  const entry = pickEntry(rows, stats);

  if (!entry || entry.skip) {
    if (entry?.skip) {
      R.ultimaConfiancaBloqueada = {
        confianca: entry.confianca, minimo: entry.minimo,
        cor: entry.cor, key: entry.key, hora: hora()
      };
      R.ultimoMotivo = `confianca_${entry.confianca}<${entry.minimo}`;
    } else {
      R.ultimoMotivo = 'sem_padrao_elegivel';
    }
    if (!override) return null;
    if (!entry || !entry.cor) return null;
  }

  const e = entry;
  const s = {
    id: S.nextId++,
    criadoMs: Date.now(),
    cor: e.cor,
    key: e.key,
    nome: e.nome,
    emoji: e.emoji,
    confianca: e.confianca,
    assertividade: Math.floor(e.confianca),
    taxa: Math.round(e.taxa),
    amostra: e.amostra,
    consenso: e.consenso,
    estado: 'PENDENTE',
    gale: 0,
    galeMsgIds: [],
    baseId: ultima.id,
    baseT: ultima.t
  };

  S.signals.push(s);
  if (S.signals.length > 1500) S.signals.splice(0, S.signals.length - 1500);

  R.ultimoSinal = {
    id: s.id, cor: s.cor, nome: s.nome,
    confianca: s.confianca, hora: hora(s.criadoMs)
  };
  R.ultimoMotivo = null;

  dbSaveSignal(s);
  tgSend(msgEntrada(s));
  log('SINAL', {
    id: s.id, cor: s.cor, key: s.key,
    confianca: s.confianca, taxa: s.taxa, n: s.amostra, consenso: s.consenso
  });
  return s;
}

function tentarSlot() {
  if (stopping) return;

  const slot = Math.floor(Date.now() / SLOT_MS);
  if (slot <= S.lastSlot) return;
  S.lastSlot = slot;

  if (!CFG.ativos) return (R.ultimoMotivo = 'sinais_desativados');
  if (S.hist.length < 15) return (R.ultimoMotivo = 'historico_curto');
  if (pendente()) return (R.ultimoMotivo = 'slot_saltado_sinal_anterior_aberto');

  const ultima = S.hist[S.hist.length - 1];
  if (Date.now() - ultima.t > CFG.frescoMs) return (R.ultimoMotivo = 'slot_saltado_mesa_parada');

  try { criarSinal(ultima); }
  catch (e) { R.ultimoMotivo = `erro_criar_sinal: ${e.message}`; log('ERRO_SINAL', e.message); }
}

/* ============================ KEEP-ALIVE ============================ */

const SELF_URL = process.env.SELF_URL || process.env.RENDER_EXTERNAL_URL || '';
let keepTimer = null;

function startKeepAlive() {
  if (!SELF_URL) {
    log('KEEPALIVE', 'SELF_URL/RENDER_EXTERNAL_URL ausente — configure para evitar sleep no Render');
    return;
  }
  const alvo = `${SELF_URL.replace(/\/+$/, '')}/health`;
  const ms = num('KEEPALIVE_MS', 240000, 60000, 840000);
  keepTimer = setInterval(() => {
    axios.get(alvo, { timeout: 15000 })
      .then(() => { R.ultimoKeepAlive = new Date().toISOString(); })
      .catch((e) => log('KEEPALIVE_ERRO', e.message));
  }, ms);
  if (typeof keepTimer.unref === 'function') keepTimer.unref();
  log('KEEPALIVE', { alvo, ms });
}

/* ============================ ROTAS ============================ */

const app = express();
app.disable('x-powered-by');
app.use(cors());
app.use(express.json({ limit: '200kb' }));

function admin(req, res, next) {
  if (!ADMIN_TOKEN) return res.status(503).json({ sucesso: false, erro: 'ADMIN_TOKEN não configurado' });
  if (req.get('x-admin-token') !== ADMIN_TOKEN) return res.status(401).json({ sucesso: false, erro: 'Token inválido' });
  next();
}

const proximoSlotEmSeg = () =>
  Math.max(0, Math.ceil(((Math.floor(Date.now() / SLOT_MS) + 1) * SLOT_MS - Date.now()) / 1000));

app.post('/webhook/rodada', async (req, res) => {
  if (WEBHOOK_SECRET && req.get('x-secret') !== WEBHOOK_SECRET) {
    return res.status(401).json({ sucesso: false, erro: 'secret inválido' });
  }
  try {
    const rd = normalizeRound(req.body || {});
    if (!rd) return res.status(400).json({ sucesso: false, erro: 'rodada inválida (id/resultado/fim obrigatórios)' });

    const resultado = await ingestRound(rd);
    if (!resultado.aceita) return res.json({ sucesso: true, ...resultado });

    R.ultimaWebhookMs = new Date().toISOString();
    R.rodadasPorWebhook += 1;
    R.ultimaSync = R.ultimaSync || new Date().toISOString();
    res.json({ sucesso: true, ...resultado, historicoTotal: S.hist.length });
  } catch (e) {
    log('WEBHOOK_ERRO', e.message);
    res.status(500).json({ sucesso: false, erro: e.message });
  }
});

app.get('/', (_req, res) =>
  res.json({
    nome: 'Capunga Dados — Bac Bo v11',
    estado: 'online',
    horaAngola: hora(),
    fontes: { api: Boolean(API_BACBO), webhook: true },
    estrategias: STRATS.length,
    gale: MAX_GALE,
    confiancaMinima: CFG.minConfianca,
    rotas: ['/health', '/diagnostico', '/sinais', '/estatisticas', 'POST /webhook/rodada']
  })
);

app.get('/health', (_req, res) =>
  res.json({
    sucesso: true,
    horaAngola: hora(),
    iniciadoEm: R.iniciadoEm,
    ultimaSync: R.ultimaSync,
    ultimaRodadaNova: R.ultimaRodadaNova,
    ultimaWebhookMs: R.ultimaWebhookMs,
    rodadasPorWebhook: R.rodadasPorWebhook,
    ciclosSemNovos: R.ciclosSemNovos,
    motivoUltimoSlotSemSinal: R.ultimoMotivo,
    ultimaConfiancaBloqueada: R.ultimaConfiancaBloqueada,
    proximoSlotEmSeg: proximoSlotEmSeg(),
    erroAPI: R.erroAPI,
    erroTelegram: R.erroTelegram,
    erroDB: R.erroDB,
    telegramPendentes: R.telegramPendentes,
    ultimoKeepAlive: R.ultimoKeepAlive,
    fontesAtivas: { api: Boolean(API_BACBO), webhook: true }
  })
);

app.get('/diagnostico', (_req, res) => {
  const st = S.stats || { baseRate: null, baseN: 0 };
  res.json({
    sucesso: true, horaAngola: hora(),
    telegramConfigurado: Boolean(BOT_TOKEN && CHAT_ID),
    dbConfigurada: Boolean(pool),
    webhookProtegido: Boolean(WEBHOOK_SECRET),
    rodadasEmMemoria: S.hist.length,
    ultimasRodadas: S.hist.slice(-12).reverse().map((r) => ({
      r: r.resultado, pj: r.pj, pb: r.pb, hora: hora(r.t)
    })),
    sinalPendente: pendente(),
    ultimoSinal: R.ultimoSinal,
    ultimaConfiancaBloqueada: R.ultimaConfiancaBloqueada,
    proximoSlotEmSeg: proximoSlotEmSeg(),
    confiancaMinimaAtual: dynamicThreshold(),
    confiancaMinimaBase: CFG.minConfianca,
    taxaBaseGale1Percent: st.baseRate ? Number((st.baseRate * 100).toFixed(1)) : null,
    amostrasBacktest: st.baseN,
    configuracao: { ...CFG, maxGale: MAX_GALE, slotMin: 5 },
    placar: placar()
  });
});

app.get('/sinais', (req, res) => {
  const n = Math.min(Math.max(Number.parseInt(req.query.limit, 10) || 50, 1), 500);
  res.json({
    sucesso: true,
    sinais: S.signals.slice(-n).reverse().map(({ galeMsgIds, ...x }) => x)
  });
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
  const bt = Object.entries(st.per)
    .map(([key, v]) => ({
      key, amostras: v.n,
      acertoPercent: Number(((v.w / v.n) * 100).toFixed(1)),
      pisoWilsonPercent: Number((wilsonLB(v.w, v.n) * 100).toFixed(1)),
      testeAmostras: v.nt,
      testeAcertoPercent: v.nt ? Number(((v.wt / v.nt) * 100).toFixed(1)) : null,
      elegivel: v.n >= CFG.minAmostra
    }))
    .sort((a, b) => b.pisoWilsonPercent - a.pisoWilsonPercent)
    .slice(0, 60);
  res.json({
    sucesso: true, placarDia: placar(),
    taxaBaseGale1Percent: st.baseRate ? Number((st.baseRate * 100).toFixed(1)) : null,
    aviso: 'Confiança interna = expectativa de acerto com Gale 1 (multi-fator). Histórico não é garantia.',
    backtest: bt, ao_vivo_por_estrategia: aoVivo
  });
});

app.post('/admin/sincronizar', admin, async (_req, res) => {
  await syncFromAPI();
  res.json({ sucesso: true, rodadas: S.hist.length });
});

app.post('/admin/forcar-sinal', admin, (req, res) => {
  if (pendente()) return res.status(409).json({ sucesso: false, erro: 'Há um sinal aberto' });
  if (!S.hist.length) return res.status(409).json({ sucesso: false, erro: 'Sem histórico' });
  const force = req.query.force === '1' || req.body?.force === true;
  const s = criarSinal(S.hist[S.hist.length - 1], force);
  if (!s) return res.status(422).json({ sucesso: false, erro: R.ultimoMotivo });
  res.json({ sucesso: true, sinal: R.ultimoSinal });
});

app.post('/admin/telegram-teste', admin, async (_req, res) => {
  const r = await tgCall('sendMessage', {
    text: `🎯 <b>CAPUNGA · BAC BO v11</b>\n✅ Ligação Telegram OK\n🕒 ${hora()}\n🛡 Gale 1 · Confiança mínima ${CFG.minConfianca}%`,
    parse_mode: 'HTML'
  });
  res.status(r.ok ? 200 : 502).json({ sucesso: r.ok, erro: R.erroTelegram });
});

app.post('/admin/reset', admin, async (req, res) => {
  if (req.body?.confirmar !== 'APAGAR') return res.status(400).json({ sucesso: false, erro: 'Envie {"confirmar":"APAGAR"}' });
  try {
    S.hist = []; S.seen = new Set(); S.signals = []; S.stats = null; S.statsAtLen = -1; S.nextId = CFG.sinalInicial;
    if (pool) { await dbChain; await pool.query('DROP TABLE IF EXISTS g2_sinais, g2_rodadas'); await ensureSchema(); }
    res.json({ sucesso: true, mensagem: 'Base apagada.' });
  } catch (e) {
    res.status(500).json({ sucesso: false, erro: e.message });
  }
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
      await loadFromDB();
      log('DB', { rodadas: S.hist.length, sinaisRecentes: S.signals.length, proximoId: S.nextId });
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
      porta: PORT, estrategias: STRATS.length,
      maxGale: MAX_GALE, slotMin: 5,
      minConfianca: CFG.minConfianca,
      proximoSlotEmSeg: proximoSlotEmSeg()
    })
  );

  syncFromAPI().catch(() => {});
  if (S.hist.length >= 15) getStats(true);

  if (CFG.syncMs > 0) {
    syncTimer = setInterval(() => { syncFromAPI().catch(() => {}); }, CFG.syncMs);
  }
  slotTimer = setInterval(tentarSlot, 1000);
  startKeepAlive();
}

async function shutdown() {
  if (stopping) return;
  stopping = true;
  if (syncTimer) clearInterval(syncTimer);
  if (slotTimer) clearInterval(slotTimer);
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