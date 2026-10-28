'use strict';

/* =============================================================
   CAPUNGA v3 — BAC BO BRASILEIRO
   -------------------------------------------------------------
   Fonte de dados:
     SOURCE = 'scraper'  → lê a mesa da Betano via Puppeteer
     SOURCE = 'api'      → polling de HISTORY_URL
     SOURCE = 'webhook'  → só recebe POST /webhook/result
   ============================================================= */
const CONFIG = {
  TELEGRAM_TOKEN:   'COLOQUE_SEU_TOKEN_AQUI',
  TELEGRAM_CHAT_ID: '@seu_canal',
  ADMIN_ID:         '0',

  // === FONTE DE DADOS ===
  SOURCE:           'scraper',        // 'scraper' | 'api' | 'webhook'
  HISTORY_URL:      '',               // usado se SOURCE='api'
  SCRAPER_URL:      'https://www.betano.bet.br/cassino/ao-vivo/bac-bo-brasileiro', // ajuste se necessário
  POLL_MS:          15000,            // polling da API (se SOURCE='api')
  SCRAPE_MS:        10000,            // intervalo entre leituras do scraper

  PORT:             3000,
  WEBHOOK_SECRET:   'capunga',

  MIN_CONFIDENCE:   94,
  MIN_AGREEMENT:    3,
  COOLDOWN_MS:      0,
};

/* ============================================================= */

const express     = require('express');
const TelegramBot = require('node-telegram-bot-api');
const fs          = require('fs');
const path        = require('path');
const puppeteer   = require('puppeteer');

const BRAND      = 'CAPUNGA';
const STATE_FILE = path.join(__dirname, 'capunga-state.json');

if (!CONFIG.TELEGRAM_TOKEN || CONFIG.TELEGRAM_TOKEN === 'COLOQUE_SEU_TOKEN_AQUI') {
  console.error('\n❌ Edite TELEGRAM_TOKEN no topo do server.js\n'); process.exit(1);
}
if (!CONFIG.TELEGRAM_CHAT_ID) {
  console.error('\n❌ Edite TELEGRAM_CHAT_ID no topo do server.js\n'); process.exit(1);
}

const ts   = () => new Date().toISOString();
const info = (...a) => console.log(`[${ts()}] [INFO]`, ...a);
const warn = (...a) => console.warn(`[${ts()}] [WARN]`, ...a);
const err  = (...a) => console.error(`[${ts()}] [ERR ]`, ...a);

/* ---------- Angola ---------- */
function getAngolaDateKey() {
  try {
    return new Intl.DateTimeFormat('en-CA', {
      timeZone: 'Africa/Luanda', year: 'numeric', month: '2-digit', day: '2-digit',
    }).format(new Date());
  } catch (_) { return new Date(Date.now() + 3600000).toISOString().slice(0, 10); }
}

/* ---------- Estado ---------- */
const BASE_WEIGHTS = {
  streak: 1.3, zigzag: 1.1, doublePair: 1.0, triplePattern: 1.0,
  markov: 1.4, ngram: 1.2, regression: 1.0, swing: 0.9,
};
const DEFAULT_STATE = {
  history: [], processedRounds: [],
  wins: 0, losses: 0, totalSignals: 0,
  pendingSignal: null, stopped: false, stoppedReason: null,
  lastSignalTs: 0, dayKey: getAngolaDateKey(),
  analyzerStats: {
    streak:{wins:0,losses:0}, zigzag:{wins:0,losses:0}, doublePair:{wins:0,losses:0},
    triplePattern:{wins:0,losses:0}, markov:{wins:0,losses:0}, ngram:{wins:0,losses:0},
    regression:{wins:0,losses:0}, swing:{wins:0,losses:0},
  },
  regime: { entropy: 1.5, sampleSize: 0 },
};

function loadState() {
  try {
    if (fs.existsSync(STATE_FILE)) {
      const p = JSON.parse(fs.readFileSync(STATE_FILE, 'utf8'));
      return { ...DEFAULT_STATE, ...p,
        analyzerStats: { ...DEFAULT_STATE.analyzerStats, ...(p.analyzerStats||{}) } };
    }
  } catch (e) { err('loadState:', e.message); }
  return JSON.parse(JSON.stringify(DEFAULT_STATE));
}
function saveState() {
  try { fs.writeFileSync(STATE_FILE, JSON.stringify(state, null, 2)); }
  catch (e) { err('saveState:', e.message); }
}
const state = loadState();
const MAX_HISTORY = 100, MAX_PROCESSED = 500;

function isProcessed(r) { return r != null && state.processedRounds.includes(String(r)); }
function markProcessed(r) {
  if (r == null) return;
  state.processedRounds.push(String(r));
  if (state.processedRounds.length > MAX_PROCESSED) state.processedRounds.shift();
}

/* ---------- Telegram ---------- */
const bot = new TelegramBot(CONFIG.TELEGRAM_TOKEN, { polling: true });
async function tgSend(text) {
  try { await bot.sendMessage(CONFIG.TELEGRAM_CHAT_ID, text, { parse_mode: 'Markdown' }); }
  catch (e) { err('tgSend:', e.message); }
}
function tgLabel(s) { return s === 'B' ? 'BANKER' : s === 'P' ? 'PLAYER' : 'EMPATE'; }
function isAdmin(msg) {
  if (!CONFIG.ADMIN_ID || CONFIG.ADMIN_ID === '0') return false;
  return String(msg.chat.id) === String(CONFIG.ADMIN_ID);
}

/* =============================================================
   ENTROPIA / REGIME
   ============================================================= */
function entropy(sides) {
  if (!sides.length) return 0;
  const c = { B:0, P:0, T:0 };
  for (const s of sides) c[s]++;
  let h = 0;
  for (const k of ['B','P','T']) if (c[k]) { const p = c[k]/sides.length; h -= p*Math.log2(p); }
  return h;
}
function updateRegime() {
  const r = state.history.slice(-20).map(x=>x.side);
  state.regime = { entropy: r.length>=10 ? entropy(r) : 1.5, sampleSize: r.length };
}

/* =============================================================
   8 ANALISADORES (idênticos à v3)
   ============================================================= */
function streakAnalyzer(s) {
  if (s.length<3) return null; const last=s[s.length-1]; if(last==='T') return null;
  let len=0; for(let i=s.length-1;i>=0;i--){ if(s[i]===last) len++; else break; }
  if(len<3) return null;
  return { bet:last==='B'?'P':'B', confidence:Math.min(95,60+(len-3)*8) };
}
function zigzagAnalyzer(s) {
  if(s.length<4) return null; let ch=0;
  for(let i=s.length-1;i>0;i--){ if(s[i]==='T'||s[i-1]==='T') return null;
    if(s[i]!==s[i-1]) ch++; else break; }
  if(ch<3) return null; const last=s[s.length-1];
  return { bet:last==='B'?'P':'B', confidence:Math.min(92,60+(ch-3)*8) };
}
function doublePairAnalyzer(s) {
  if(s.length<4) return null; const l=s.slice(-4); if(l.includes('T')) return null;
  if(l[0]===l[1]&&l[2]===l[3]&&l[0]!==l[2]) return { bet:l[0], confidence:72 };
  return null;
}
function triplePatternAnalyzer(s) {
  if(s.length<6) return null; const l=s.slice(-6); if(l.includes('T')) return null;
  if(l.slice(0,3).every(x=>x===l[0])&&l.slice(3).every(x=>x===l[3])&&l[0]!==l[3])
    return { bet:l[0], confidence:76 };
  if(l.slice(0,3).join('')===l.slice(3).join('')) return { bet:l[0], confidence:79 };
  return null;
}
function markovAnalyzer(s) {
  if(s.length<10) return null;
  const tr={B:{B:0,P:0,T:0},P:{B:0,P:0,T:0},T:{B:0,P:0,T:0}};
  for(let i=1;i<s.length;i++) tr[s[i-1]][s[i]]++;
  const last=s[s.length-1], row=tr[last], tot=row.B+row.P+row.T;
  if(tot<5) return null; const pB=row.B/tot, pP=row.P/tot;
  if(pB>0.62&&pB>pP*1.3) return { bet:'B', confidence:Math.min(92,50+pB*50) };
  if(pP>0.62&&pP>pB*1.3) return { bet:'P', confidence:Math.min(92,50+pP*50) };
  return null;
}
function ngramAnalyzer(s) {
  if(s.length<8) return null;
  for(const k of [4,3]) {
    if(s.length<k+3) continue; const tail=s.slice(-k).join('');
    const f={B:0,P:0,T:0};
    for(let i=0;i<s.length-k;i++) if(s.slice(i,i+k).join('')===tail) f[s[i+k]]++;
    const tot=f.B+f.P+f.T; if(tot<3) continue;
    const pB=f.B/tot, pP=f.P/tot;
    if(pB>=0.67) return { bet:'B', confidence:Math.min(90,55+pB*40) };
    if(pP>=0.67) return { bet:'P', confidence:Math.min(90,55+pP*40) };
  }
  return null;
}
function regressionAnalyzer(s) {
  if(s.length<10) return null; const l=s.slice(-10);
  const b=l.filter(x=>x==='B').length, p=l.filter(x=>x==='P').length;
  if(b<=2&&p>=6) return { bet:'B', confidence:68 };
  if(p<=2&&b>=6) return { bet:'P', confidence:68 };
  return null;
}
function swingAnalyzer(s) {
  if(s.length<8) return null; const first=s.slice(-8,-4), second=s.slice(-4);
  const bF=first.filter(x=>x==='B').length, bS=second.filter(x=>x==='B').length;
  if(bF>=3&&bS<=1) return { bet:'B', confidence:63 };
  if(bF<=1&&bS>=3) return { bet:'P', confidence:63 };
  return null;
}

function getWeight(name) {
  const base=BASE_WEIGHTS[name]||1, st=state.analyzerStats[name];
  if(!st) return base; const tot=st.wins+st.losses;
  if(tot<5) return base; return base*(0.5+st.wins/tot);
}
function runAnalyzers() {
  const s=state.history.map(r=>r.side), found=[];
  const map={streak:streakAnalyzer,zigzag:zigzagAnalyzer,doublePair:doublePairAnalyzer,
    triplePattern:triplePatternAnalyzer,markov:markovAnalyzer,ngram:ngramAnalyzer,
    regression:regressionAnalyzer,swing:swingAnalyzer};
  for(const [n,fn] of Object.entries(map)) { const r=fn(s); if(r) found.push({name:n,...r,weight:getWeight(n)}); }
  return found;
}
function computeConsensus(an) {
  if(!an.length) return null;
  let wB=0,wP=0; const aB=[],aP=[];
  for(const a of an) { if(a.bet==='B'){wB+=a.weight;aB.push(a);} else if(a.bet==='P'){wP+=a.weight;aP.push(a);} }
  const tot=wB+wP; if(!tot) return null;
  const cB=wB/tot,cP=wP/tot;
  if(cB>=cP) return { bet:'B', ratio:cB, agreeing:aB, disagreeing:aP };
  return { bet:'P', ratio:cP, agreeing:aP, disagreeing:aB };
}
function computeFinalConfidence(cs) {
  if(cs.agreeing.length<CONFIG.MIN_AGREEMENT) return 0;
  let conf=0;
  conf += cs.ratio*25;
  conf += Math.min(cs.agreeing.length/5,1)*15;
  const avgInd=cs.agreeing.reduce((a,x)=>a+x.confidence,0)/cs.agreeing.length;
  conf += (avgInd/100)*20;
  let ha=0; for(const a of cs.agreeing){ const st=state.analyzerStats[a.name]; const t=st?st.wins+st.losses:0; ha += t>=3 ? st.wins/t : 0.5; }
  ha/=cs.agreeing.length; conf += ha*20;
  const e=state.regime.entropy; const rs=Math.max(0,Math.min(1,(1.6-e)/0.6)); conf += rs*10;
  conf += Math.min(state.history.length/40,1)*5;
  const t=state.wins+state.losses; if(t>=10) conf += Math.max(0,Math.min(5,(state.wins/t-0.5)*20));
  conf -= cs.disagreeing.length*2.5;
  return Math.max(0,Math.min(100,Math.round(conf)));
}

/* =============================================================
   MAP + PROCESSAMENTO
   ============================================================= */
function mapSide(raw) {
  if(raw==null) return null; const s=String(raw).trim().toLowerCase();
  if(['b','banker','banco','1','blue','azul'].includes(s)) return 'B';
  if(['p','player','jogador','2','red','vermelho'].includes(s)) return 'P';
  if(['t','tie','empate','0','green','verde'].includes(s)) return 'T';
  return null;
}

async function processResult({ round, side }) {
  if(!side) return;
  if(getAngolaDateKey()!==state.dayKey) { /* reset diário */ }

  if(state.pendingSignal) {
    const p=state.pendingSignal, won=side===p.bet;
    for(const name of p.analyzers){ const st=state.analyzerStats[name]; if(st){won?st.wins++:st.losses++;} }
    won?state.wins++:state.losses++; state.pendingSignal=null;
    const tot=state.wins+state.losses, acc=tot?Math.round((state.wins/tot)*100):0;
    await tgSend(won
      ? `✅ *${BRAND} — WIN*\n🎯 ${tgLabel(p.bet)}\n📊 Hoje: ${state.wins}W / ${state.losses}L (${acc}%)`
      : `❌ *${BRAND} — LOSS*\n🎯 ${tgLabel(p.bet)} (saiu ${tgLabel(side)})\n📊 Hoje: ${state.wins}W / ${state.losses}L (${acc}%)`);
  }

  state.history.push({ round, side, ts: Date.now() });
  if(state.history.length>MAX_HISTORY) state.history.shift();
  updateRegime();

  if(!state.stopped && !state.pendingSignal) {
    const an=runAnalyzers(), cs=computeConsensus(an);
    if(cs && (CONFIG.COOLDOWN_MS===0 || Date.now()-state.lastSignalTs>=CONFIG.COOLDOWN_MS)) {
      const conf=computeFinalConfidence(cs);
      if(conf>=CONFIG.MIN_CONFIDENCE) {
        state.pendingSignal={ bet:cs.bet, confidence:conf, analyzers:cs.agreeing.map(a=>a.name), ts:Date.now() };
        state.lastSignalTs=Date.now(); state.totalSignals++;
        await tgSend(`🚨 *${BRAND}* 🚨\n🎯 *SINAL BAC BO*\n➡️ Entrada: *${tgLabel(cs.bet)}*\n💯 Confiança: *${conf}%*\n📊 Hoje: ${state.wins}W / ${state.losses}L`);
      }
    }
  }
  saveState();
}

/* =============================================================
   FONTE 1 — SCRAPER BETANO (Puppeteer)
   ============================================================= */
let browser = null;
let scraperTimer = null;

async function startScraper() {
  info('A iniciar scraper Betano (Bac Bo Brasileiro)...');
  try {
    browser = await puppeteer.launch({
      headless: 'new',
      args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage'],
    });
    const page = await browser.newPage();
    await page.setUserAgent('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120 Safari/537.36');
    await page.goto(CONFIG.SCRAPER_URL, { waitUntil: 'networkidle2', timeout: 60000 });
    info('Página da Betano carregada.');

    const seen = new Set();

    scraperTimer = setInterval(async () => {
      try {
        /* ⚠️ SELETORES: ajuste conforme o DOM real da Betano.
           O histórico do Bac Bo costuma estar em elementos com classes
           que contêm 'bacbo', 'history', 'round', 'result'.
           Abaixo, um exemplo genérico que procura por data-round e classes de cor. */
        const rounds = await page.evaluate(() => {
          const out = [];
          const els = document.querySelectorAll('[data-round], .bacbo-round, .round-result, .history-item');
          els.forEach((el, i) => {
            const round = el.getAttribute('data-round') || el.getAttribute('data-id') || `idx-${i}`;
            let side = null;
            const cls = (el.className || '').toLowerCase();
            if (cls.includes('blue') || cls.includes('banker') || cls.includes('azul')) side = 'B';
            else if (cls.includes('red') || cls.includes('player') || cls.includes('vermelho')) side = 'P';
            else if (cls.includes('green') || cls.includes('tie') || cls.includes('empate')) side = 'T';
            else {
              const txt = (el.textContent || '').toLowerCase();
              if (txt.includes('banker') || txt.includes('azul')) side = 'B';
              else if (txt.includes('player') || txt.includes('vermelho')) side = 'P';
            }
            if (side) out.push({ round, side });
          });
          return out;
        });

        for (const r of rounds) {
          if (!seen.has(r.round)) {
            seen.add(r.round);
            markProcessed(r.round);
            await processResult({ round: r.round, side: r.side });
            info(`Scraper: round ${r.round} → ${r.side}`);
          }
        }
      } catch (e) { warn('scraper loop:', e.message); }
    }, CONFIG.SCRAPE_MS);

  } catch (e) {
    err('Falha ao iniciar scraper:', e.message);
    warn('Continuando sem scraper. Use SOURCE="api" ou webhook.');
  }
}

/* =============================================================
   FONTE 2 — POLLING DE API
   ============================================================= */
function normalizeHistory(payload) {
  if(!payload) return [];
  const list = Array.isArray(payload) ? payload : (payload.results || payload.data || payload.history || []);
  return list.map(it => ({
    round: it.round ?? it.round_id ?? it.id ?? it.roundId,
    side: mapSide(it.result ?? it.outcome ?? it.winner ?? it.color),
  })).filter(x=>x.side);
}
async function pollHistory() {
  if(!CONFIG.HISTORY_URL) return;
  try {
    const res = await fetch(CONFIG.HISTORY_URL, { headers: { Accept:'application/json' } });
    if(!res.ok) return warn('pollHistory status', res.status);
    const results = normalizeHistory(await res.json());
    results.sort((a,b)=>String(a.round).localeCompare(String(b.round)));
    for(const r of results) if(!isProcessed(r.round)){ markProcessed(r.round); await processResult(r); }
  } catch(e){ warn('pollHistory:', e.message); }
}

/* =============================================================
   FONTE 3 — WEBHOOK
   ============================================================= */
const app = express();
app.use(express.json({ limit:'64kb' }));
app.get('/health', (_req,res)=>res.json({ brand:BRAND, ok:true, source:CONFIG.SOURCE, stopped:state.stopped, wins:state.wins, losses:state.losses, pending:!!state.pendingSignal }));
app.post('/webhook/result', async (req,res)=>{
  if(CONFIG.WEBHOOK_SECRET && req.headers['x-secret']!==CONFIG.WEBHOOK_SECRET) return res.status(401).json({error:'unauthorized'});
  const {round,result}=req.body||{}, side=mapSide(result);
  if(!side) return res.status(400).json({error:'invalid result'});
  if(isProcessed(round)) return res.json({ok:true,skipped:'already_processed'});
  markProcessed(round); await processResult({round,side});
  res.json({ok:true,side});
});

/* =============================================================
   RESET DIÁRIO
   ============================================================= */
async function resetForNewDay() {
  const prev={w:state.wins,l:state.losses,s:state.totalSignals};
  state.history=[]; state.wins=0; state.losses=0; state.totalSignals=0;
  state.pendingSignal=null; state.lastSignalTs=0; state.dayKey=getAngolaDateKey(); saveState();
  const tot=prev.w+prev.l, acc=tot?Math.round((prev.w/tot)*100):0;
  await tgSend(`🔄 *${BRAND} — NOVO DIA*\n📅 00:00 Angola\n📊 Anterior: ${prev.w}W / ${prev.l}L (${acc}%)`);
}
setInterval(()=>{ if(getAngolaDateKey()!==state.dayKey) resetForNewDay().catch(e=>err('reset:',e.message)); }, 30000);

/* =============================================================
   COMANDOS ADMIN
   ============================================================= */
bot.onText(/\/status/, (msg)=>{
  if(!isAdmin(msg)) return;
  const tot=state.wins+state.losses, acc=tot?Math.round((state.wins/tot)*100):0;
  const aStats=Object.entries(state.analyzerStats).map(([k,v])=>{const t=v.wins+v.losses;return `  ${k}: ${t?Math.round((v.wins/t)*100):0}% (${t})`;}).join('\n');
  bot.sendMessage(msg.chat.id, `📊 *${BRAND} — STATUS*\n📅 ${state.dayKey}\n🎯 Hoje: ${state.wins}W / ${state.losses}L (${acc}%)\n📡 Sinais: ${state.totalSignals}\n🌡 Entropia: ${state.regime.entropy.toFixed(2)}\n⏸ Pausado: ${state.stopped?'Sim':'Não'}\n\n*Analisadores:*\n${aStats}`, {parse_mode:'Markdown'});
});
bot.onText(/\/stop/,  (m)=>{ if(!isAdmin(m))return; state.stopped=true; saveState(); bot.sendMessage(m.chat.id,`🛑 ${BRAND} pausado.`); });
bot.onText(/\/start/, (m)=>{ if(!isAdmin(m))return; state.stopped=false; saveState(); bot.sendMessage(m.chat.id,`▶️ ${BRAND} retomado.`); });
bot.onText(/\/reset/, (m)=>{ if(!isAdmin(m))return; state.history=[];state.wins=0;state.losses=0;state.totalSignals=0;state.pendingSignal=null;state.stopped=false;saveState();bot.sendMessage(m.chat.id,`♻️ ${BRAND} resetado.`); });
bot.on('polling_error', (e)=>err('tg polling:',e.message));

/* =============================================================
   BOOT
   ============================================================= */
let pollTimer=null;
app.listen(CONFIG.PORT, ()=>{
  info(`${BRAND} v3 — Bac Bo Brasileiro`);
  info(`Fonte: ${CONFIG.SOURCE}`);
  info(`Webhook: POST http://SEU_IP:${CONFIG.PORT}/webhook/result`);
});

if(CONFIG.SOURCE==='scraper') startScraper();
else if(CONFIG.SOURCE==='api' && CONFIG.HISTORY_URL){
  info(`Polling API a cada ${CONFIG.POLL_MS}ms`);
  pollTimer=setInterval(pollHistory, CONFIG.POLL_MS);
  pollHistory();
} else {
  warn('Fonte passiva — à espera de webhooks POST /webhook/result');
}

tgSend(`🚀 *${BRAND}* online — Bac Bo Brasileiro (${CONFIG.SOURCE})`).catch(()=>{});

/* ---------- Shutdown ---------- */
async function shutdown(sig){
  info(`Recebido ${sig}`);
  saveState();
  if(pollTimer) clearInterval(pollTimer);
  if(scraperTimer) clearInterval(scraperTimer);
  if(browser) try{ await browser.close(); }catch(_){}
  try{ bot.stopPolling(); }catch(_){}
  process.exit(0);
}
process.on('SIGINT', ()=>shutdown('SIGINT'));
process.on('SIGTERM', ()=>shutdown('SIGTERM'));
process.on('unhandledRejection', (e)=>err('unhandled:',e?.message||e));
process.on('uncaughtException',  (e)=>err('uncaught:',e?.message||e));