'use strict';
require('dotenv').config();
const express = require('express');
const cors = require('cors');
const axios = require('axios');
const { Pool } = require('pg');
const app = express();
app.disable('x-powered-by');
app.use(cors());
app.use(express.json({ limit: '32kb' }));
function numEnv(name, fallback, min, max) {
const n = Number(process.env[name] ?? fallback);
return Number.isFinite(n) ? Math.min(max, Math.max(min, n)) : fallback;
}
function round(n, places = 2) { const p = 10 ** places; return Math.round((Number(n) + Number.EPSILON) * p) / p; }
function errText(e) { return String(e?.message || e || 'Unknown error').slice(0, 400); }
const CFG = Object.freeze({
port: numEnv('PORT', 3000, 1, 65535),
symbol: String(process.env.SYMBOL || 'BTCUSDT').toUpperCase(),
category: 'spot',
bybitUrl: (process.env.BYBIT_BASE_URL || 'https://api.bybit.com').replace(/\/$/, ''),
loopMs: numEnv('LOOP_MS', 60000, 15000, 3600000),
minScore: numEnv('MIN_SCORE', 5, 3, 8),
minRR: numEnv('MIN_RR', 1.4, 1, 5),
riskPct: numEnv('RISK_PCT', 1, 0.1, 1) / 100,
maxDeviationPct: numEnv('MAX_ENTRY_DEVIATION_PCT', 0.35, 0.05, 3) / 100,
tpAtr: numEnv('ATR_TP_MULT', 1.8, 0.5, 10),
slAtr: numEnv('ATR_SL_MULT', 1.2, 0.5, 10),
tpMinPct: numEnv('TP_MIN_PCT', 0.25, 0.05, 5) / 100,
slMinPct: numEnv('SL_MIN_PCT', 0.20, 0.05, 5) / 100,
expiryMs: numEnv('SIGNAL_EXPIRY_MIN', 240, 15, 10080) * 60000,
antiSpamMs: numEnv('ANTI_SPAM_MIN', 60, 1, 1440) * 60000,
candleLimit: numEnv('CANDLE_LIMIT', 200, 100, 1000),
apiKey: process.env.API_KEY || '',
botToken: process.env.BOT_TOKEN || '',
chatId: process.env.CHAT_ID || '',
keepaliveUrl: process.env.KEEPALIVE_URL || '',
databaseUrl: process.env.DATABASE_URL || '',
databaseSsl: String(process.env.DATABASE_SSL || 'true').toLowerCase() !== 'false',
telegramPollMs: numEnv('TELEGRAM_POLL_MS', 4000, 1500, 30000),
allowTelegramCommands: String(process.env.TELEGRAM_COMMANDS || 'true').toLowerCase() !== 'false'
});
const pool = CFG.databaseUrl ? new Pool({
connectionString: CFG.databaseUrl,
ssl: CFG.databaseSsl ? { rejectUnauthorized: false } : false,
max: 8,
idleTimeoutMillis: 30000,
connectionTimeoutMillis: 10000
}) : null;
let server;
let scanTimer;
let keepaliveTimer;
let telegramTimer;
let scanning = false;
let shuttingDown = false;
let lastAnalysis = null;
let lastError = null;
let lastSource = 'none';
let lastSignalAt = 0;
let telegramOffset = 0;
let consecutiveErrors = 0;
async function logEvent(type, message, data = {}) {
if (!pool) return;
try {
await pool.query('INSERT INTO trade_eventos(tipo,mensagem,dados) VALUES($1,$2,$3)', [type, message, JSON.stringify(data)]);
} catch (e) { console.error('[DB event]', errText(e)); }
}
async function initDB() {
if (!pool) throw new Error('DATABASE_URL não configurado. PostgreSQL é obrigatório para histórico e controlo de duplicados.');
await pool.query(`
CREATE TABLE IF NOT EXISTS trade_sinais (
id BIGSERIAL PRIMARY KEY,
symbol TEXT NOT NULL,
direcao TEXT NOT NULL DEFAULT 'BUY',
preco_entrada NUMERIC NOT NULL,
preco_tp NUMERIC NOT NULL,
preco_sl NUMERIC NOT NULL,
score INTEGER NOT NULL,
estado TEXT NOT NULL DEFAULT 'PENDENTE',
tp_pct NUMERIC NOT NULL,
sl_pct NUMERIC NOT NULL,
preco_saida NUMERIC,
pnl_pct NUMERIC,
criado_em TIMESTAMPTZ NOT NULL DEFAULT NOW(),
resolvido_em TIMESTAMPTZ,
expiracao_em TIMESTAMPTZ NOT NULL,
metadados JSONB NOT NULL DEFAULT '{}'::jsonb
);
CREATE INDEX IF NOT EXISTS trade_sinais_estado_idx ON trade_sinais(estado, criado_em DESC);
CREATE INDEX IF NOT EXISTS trade_sinais_criado_idx ON trade_sinais(criado_em DESC);
CREATE TABLE IF NOT EXISTS trade_eventos (
id BIGSERIAL PRIMARY KEY,
tipo TEXT NOT NULL,
mensagem TEXT NOT NULL,
dados JSONB NOT NULL DEFAULT '{}'::jsonb,
criado_em TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS trade_config (
chave TEXT PRIMARY KEY,
valor TEXT NOT NULL,
atualizado_em TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
`);
// Safe migrations for databases created by earlier versions.
await pool.query(`
ALTER TABLE trade_sinais ADD COLUMN IF NOT EXISTS preco_tp NUMERIC;
ALTER TABLE trade_sinais ADD COLUMN IF NOT EXISTS preco_sl NUMERIC;
ALTER TABLE trade_sinais ADD COLUMN IF NOT EXISTS tp_pct NUMERIC;
ALTER TABLE trade_sinais ADD COLUMN IF NOT EXISTS sl_pct NUMERIC;
ALTER TABLE trade_sinais ADD COLUMN IF NOT EXISTS preco_saida NUMERIC;
ALTER TABLE trade_sinais ADD COLUMN IF NOT EXISTS pnl_pct NUMERIC;
ALTER TABLE trade_sinais ADD COLUMN IF NOT EXISTS resolvido_em TIMESTAMPTZ;
ALTER TABLE trade_sinais ADD COLUMN IF NOT EXISTS expiracao_em TIMESTAMPTZ;
ALTER TABLE trade_sinais ADD COLUMN IF NOT EXISTS metadados JSONB NOT NULL DEFAULT '{}'::jsonb;
`);
// Existing legacy rows may not have an expiry; expire them conservatively.
await pool.query(`UPDATE trade_sinais SET expiracao_em = COALESCE(expiracao_em, criado_em + INTERVAL '4 hours') WHERE expiracao_em IS N
const { rows } = await pool.query("SELECT EXTRACT(EPOCH FROM MAX(criado_em))*1000 AS last_ms FROM trade_sinais");
lastSignalAt = Number(rows[0]?.last_ms || 0);
}
async function telegramSend(text) {
if (!CFG.botToken || !CFG.chatId) return { skipped: true, reason: 'Telegram not configured' };
try {
const { data } = await axios.post(`https://api.telegram.org/bot${CFG.botToken}/sendMessage`, {
chat_id: CFG.chatId, text, disable_web_page_preview: true
}, { timeout: 12000 });
if (!data?.ok) throw new Error(data?.description || 'Telegram API rejected message');
return { sent: true };
} catch (e) {
await logEvent('telegram_error', errText(e));
console.error('[Telegram send]', errText(e));
return { sent: false, error: errText(e) };
}
}
async function fetchBybitCandles(interval) {
const { data } = await axios.get(`${CFG.bybitUrl}/v5/market/kline`, {
params: { category: 'spot', symbol: CFG.symbol, interval: String(interval), limit: CFG.candleLimit }, timeout: 12000
});
if (data?.retCode !== 0 || !Array.isArray(data?.result?.list)) throw new Error(`Bybit kline: ${data?.retMsg || 'resposta inválida'}`);
const ms = Number(interval) * 60000;
return data.result.list.map(r => ({ time: Number(r[0]), open: Number(r[1]), high: Number(r[2]), low: Number(r[3]), close: Number(r[4]),
.filter(c => [c.time,c.open,c.high,c.low,c.close,c.volume].every(Number.isFinite) && c.close > 0)
.sort((a,b) => a.time - b.time).filter(c => c.time + ms <= Date.now());
}
async function fetchOkxCandles(interval) {
const instId = CFG.symbol.replace(/USDT$/, '-USDT');
const { data } = await axios.get('https://www.okx.com/api/v5/market/candles', {
params: { instId, bar: `${interval}m`, limit: String(Math.min(CFG.candleLimit, 300)) }, timeout: 12000
});
if (data?.code !== '0' || !Array.isArray(data?.data)) throw new Error('OKX candles response invalid');
const ms = Number(interval) * 60000;
return data.data.map(r => ({ time: Number(r[0]), open: Number(r[1]), high: Number(r[2]), low: Number(r[3]), close: Number(r[4]), volume
.filter(c => [c.time,c.open,c.high,c.low,c.close,c.volume].every(Number.isFinite) && c.close > 0)
.sort((a,b) => a.time - b.time).filter(c => c.time + ms <= Date.now());
}
async function fetchMarketBundle() {
// Fetch all timeframes from the same exchange to avoid mixing market sources.
try {
const [m1,m5,m15] = await Promise.all([fetchBybitCandles(1),fetchBybitCandles(5),fetchBybitCandles(15)]);
if ([m1,m5,m15].some(a => a.length < 60)) throw new Error('Bybit returned insufficient closed candles');
lastSource = 'bybit-spot';
return { m1,m5,m15,source:lastSource };
} catch (bybitError) {
try {
const [m1,m5,m15] = await Promise.all([fetchOkxCandles(1),fetchOkxCandles(5),fetchOkxCandles(15)]);
if ([m1,m5,m15].some(a => a.length < 60)) throw new Error('OKX returned insufficient closed candles');
lastSource = 'okx-spot-fallback';
return { m1,m5,m15,source:lastSource };
} catch (okxError) {
throw new Error(`Market data unavailable. Bybit: ${errText(bybitError)}; OKX: ${errText(okxError)}`);
}
}
}
async function fetchPrice(source = 'bybit-spot') {
if (source === 'okx-spot-fallback') {
const instId = CFG.symbol.replace(/USDT$/, '-USDT');
const { data } = await axios.get('https://www.okx.com/api/v5/market/ticker', { params: { instId }, timeout: 10000 });
const p = Number(data?.data?.[0]?.last);
if (data?.code !== '0' || !Number.isFinite(p) || p <= 0) throw new Error('OKX ticker unavailable');
return p;
}
const { data } = await axios.get(`${CFG.bybitUrl}/v5/market/tickers`, { params: { category:'spot', symbol:CFG.symbol }, timeout:10000 }
const p = Number(data?.result?.list?.[0]?.lastPrice);
if (data?.retCode !== 0 || !Number.isFinite(p) || p <= 0) throw new Error('Bybit ticker unavailable');
return p;
}
function ema(values, period) {
if (values.length < period) return null;
const k = 2 / (period + 1);
let v = values.slice(0, period).reduce((a,b) => a+b, 0) / period;
for (let i=period; i<values.length; i++) v = values[i]*k + v*(1-k);
return v;
}
function rsi(values, period=14) {
if (values.length < period+1) return null;
let gains=0, losses=0;
for (let i=values.length-period; i<values.length; i++) { const d=values[i]-values[i-1]; if (d>0) gains+=d; else losses-=d; }
if (losses===0) return gains===0 ? 50 : 100;
const rs=(gains/period)/(losses/period); return 100-(100/(1+rs));
}
function atr(candles, period=14) {
if (candles.length < period+1) return null;
const tr=[];
for (let i=1; i<candles.length; i++) { const c=candles[i], pc=candles[i-1].close; tr.push(Math.max(c.high-c.low,Math.abs(c.high-pc),Mat
return tr.slice(-period).reduce((a,b)=>a+b,0)/period;
}
function adxApprox(candles, period=14) {
if (candles.length < period+2) return null;
let plus=0,minus=0,tr=0;
for(let i=candles.length-period;i<candles.length;i++) { const c=candles[i],p=candles[i-1]; const up=c.high-p.high,down=p.low-c.low; plu
if(tr<=0) return 0; const pdi=100*plus/tr,mdi=100*minus/tr; return pdi+mdi===0?0:100*Math.abs(pdi-mdi)/(pdi+mdi);
}
function getFeatures(candles) {
const closes=candles.map(c=>c.close), last=candles[candles.length-1];
return { close:last.close, ema9:ema(closes,9), ema21:ema(closes,21), ema50:ema(closes,50), rsi:rsi(closes), atr:atr(candles), adx:adxAp
}
function analyze(m1,m5,m15) {
const a=getFeatures(m1), b=getFeatures(m5), c=getFeatures(m15);
const reasons=[]; let score=0;
if (b.ema9>b.ema21) { score++; reasons.push('EMA9 acima da EMA21 em 5m'); }
if (c.ema9>c.ema21 && c.ema21>c.ema50) { score+=2; reasons.push('Tendência ascendente confirmada em 15m'); }
if (b.rsi>=50 && b.rsi<=68) { score++; reasons.push('RSI de 5m em zona favorável'); }
if (b.close>b.ema9 && b.ema9>b.ema21) { score++; reasons.push('Preço acima das médias curtas'); }
if (b.adx>=18) { score++; reasons.push('Força de tendência suficiente'); }
if (a.close>a.ema9 && a.ema9>a.ema21) { score++; reasons.push('Confirmação de momentum em 1m'); }
if (b.rsi>72) { score-=2; reasons.push('Penalização por RSI sobrecomprado'); }
if (c.ema9<c.ema21) { score-=2; reasons.push('Tendência de 15m não confirma compra'); }
if (!Number.isFinite(b.atr) || b.atr<=0 || !Number.isFinite(b.close)) return { ok:false, score, reasons:['Indicadores insuficientes'],
const entry=b.close;
const slDist=Math.max(b.atr*CFG.slAtr,entry*CFG.slMinPct);
const tpDist=Math.max(b.atr*CFG.tpAtr,entry*CFG.tpMinPct);
const rr=tpDist/slDist;
const ok=score>=CFG.minScore && rr>=CFG.minRR && entry>0;
return { ok, direction:'BUY', score, entry, tp:entry+tpDist, sl:entry-slDist, tpPct:tpDist/entry*100, slPct:slDist/entry*100, rr, reaso
features:{m1:{close:a.close,ema9:a.ema9,ema21:a.ema21},m5:{close:b.close,ema9:b.ema9,ema21:b.ema21,rsi:b.rsi,atr:b.atr,adx:b.adx},m15
}
async function pendingSignal(client = pool) {
const { rows } = await client.query("SELECT * FROM trade_sinais WHERE estado='PENDENTE' ORDER BY id DESC LIMIT 1");
return rows[0] || null;
}
async function resolvePending(price, candles5m) {
const signal = await pendingSignal();
if (!signal) return;
const createdAt = new Date(signal.criado_em).getTime();
const expiresAt = new Date(signal.expiracao_em).getTime();
const sl = Number(signal.preco_sl);
const tp = Number(signal.preco_tp);
const entry = Number(signal.preco_entrada);
if (![createdAt, expiresAt, sl, tp, entry].every(Number.isFinite)) {
await logEvent('signal_data_error', 'Sinal pendente tem dados inválidos', { id: signal.id });
return;
}
// Replays every available closed candle after signal creation, not just the
// latest candle. This reduces missed TP/SL events after temporary downtime.
// If TP and SL are both inside the same candle, conservatively count LOSS.
const eligibleCandles = (candles5m || []).filter(c => {
const candleCloseTime = c.time + 5 * 60000;
return candleCloseTime > createdAt && c.time < expiresAt;
});
for (const candle of eligibleCandles) {
let state = null;
let exit = null;
if (candle.low <= sl) {
state = 'LOSS';
exit = sl;
} else if (candle.high >= tp) {
state = 'GREEN';
exit = tp;
}
if (!state) continue;
const pnl = (exit - entry) / entry * 100;
const result = await pool.query(
`UPDATE trade_sinais
SET estado=$2, preco_saida=$3, pnl_pct=$4, resolvido_em=NOW()
WHERE id=$1 AND estado='PENDENTE' RETURNING id`,
[signal.id, state, exit, round(pnl, 5)]
);
if (!result.rowCount) return;
await logEvent('signal_resolved', `Sinal ${state}`, {
id: signal.id, exit, pnlPct: round(pnl, 5), candleTime: candle.time
});
await telegramSend(
`${state === 'GREEN' ? '■ GREEN' : '■ LOSS'} — CAPUNGA TRADE\n` +
`Sinal #${signal.id} · BTC/USDT Spot\n` +
`Entrada de referência: ${round(entry, 2)}\n` +
`Saída simulada: ${round(exit, 2)}\n` +
`Variação bruta: ${round(pnl, 3)}%\n` +
`Resultado apurado por vela fechada; taxas e slippage excluídos.`
);
return;
}
if (Date.now() >= expiresAt) {
const result = await pool.query(
`UPDATE trade_sinais SET estado='EXPIRADO', preco_saida=$2, resolvido_em=NOW()
WHERE id=$1 AND estado='PENDENTE' RETURNING id`,
[signal.id, price]
);
if (result.rowCount) {
await logEvent('signal_expired', 'Sinal expirado', { id: signal.id, price });
await telegramSend(
`■ CAPUNGA TRADE — SINAL EXPIRADO\n#${signal.id} · BTC/USDT Spot\n` +
`Preço de referência: ${round(price, 2)}\n` +
`Expirado sem TP/SL confirmado nas velas disponíveis.`
);
}
}
}
async function createSignal(a, source) {
const client=await pool.connect(); let signal;
try {
await client.query('BEGIN');
await client.query("SELECT pg_advisory_xact_lock(hashtext('capunga_trade_signal_engine'))");
const pending=await pendingSignal(client);
if(pending) { await client.query('COMMIT'); return { opened:false, reason:'pending_exists', id:pending.id }; }
const recent=await client.query("SELECT criado_em FROM trade_sinais ORDER BY id DESC LIMIT 1");
if(recent.rowCount && Date.now()-new Date(recent.rows[0].criado_em).getTime()<CFG.antiSpamMs) { await client.query('COMMIT'); return
const expires=new Date(Date.now()+CFG.expiryMs);
const result=await client.query(`INSERT INTO trade_sinais(symbol,direcao,preco_entrada,preco_tp,preco_sl,score,estado,tp_pct,sl_pct,e
VALUES($1,'BUY',$2,$3,$4,$5,'PENDENTE',$6,$7,$8,$9::jsonb) RETURNING id,criado_em`,[
CFG.symbol,a.entry,a.tp,a.sl,a.score,round(a.tpPct,5),round(a.slPct,5),expires,JSON.stringify({reasons:a.reasons,rr:a.rr,features:a
]);
signal=result.rows[0]; await client.query('COMMIT'); lastSignalAt=Date.now();
} catch(e) { try{await client.query('ROLLBACK');}catch(_){} throw e; } finally { client.release(); }
await logEvent('signal_opened','Novo sinal BUY',{id:signal.id,entry:a.entry,tp:a.tp,sl:a.sl,score:a.score,source});
const msg=`■ CAPUNGA TRADE — NOVO SINAL\n\n■ ATIVO: ${CFG.symbol.replace('USDT','/USDT')} Spot\n■ DIREÇÃO: BUY / COMPRA\n■ SINAL: #${s
await telegramSend(msg);
return {opened:true,id:signal.id};
}
async function scanOnce() {
if(scanning) return {skipped:true,reason:'scan_in_progress'};
scanning=true;
try {
const bundle=await fetchMarketBundle();
const price=await fetchPrice(bundle.source);
await resolvePending(price,bundle.m5);
const a=analyze(bundle.m1,bundle.m5,bundle.m15);
lastAnalysis={at:new Date().toISOString(),symbol:CFG.symbol,market:'Spot',price,source:bundle.source,score:a.score,eligible:a.ok,sign
lastError=null; consecutiveErrors=0;
if (a.ok) {
const deviation = Math.abs(price - a.entry) / a.entry;
if (deviation > CFG.maxDeviationPct) {
lastAnalysis.eligible = false;
lastAnalysis.signal = null;
lastAnalysis.signalResult = { opened: false, reason: 'entry_price_moved', deviationPct: round(deviation * 100, 4), maxDeviationPc
} else {
lastAnalysis.signalResult = await createSignal(a, bundle.source);
}
}
await logEvent('analysis','Análise de mercado concluída',{price,score:a.score,eligible:a.ok,source:bundle.source});
return lastAnalysis;
} catch(e) {
lastError=errText(e); consecutiveErrors++;
await logEvent('scan_error',lastError,{consecutiveErrors});
console.error('[SCAN]',lastError);
throw e;
} finally { scanning=false; }
}
function auth(req,res,next) {
if(!CFG.apiKey) return res.status(503).json({ok:false,error:'API_KEY não configurada; endpoints de controlo bloqueados.'});
const key=req.get('x-api-key')||'';
if(key.length!==CFG.apiKey.length || key!==CFG.apiKey) return res.status(401).json({ok:false,error:'API key inválida.'});
next();
}
function asyncRoute(fn) { return (req,res,next)=>Promise.resolve(fn(req,res,next)).catch(next); }
app.get('/',(_req,res)=>res.json({name:'CAPUNGA TRADE Signal Server Pro',version:'5.1.0',market:'BTC/USDT Spot',mode:'continuous manual s
app.get('/health',asyncRoute(async(_req,res)=>{
let db='up'; try { await pool.query('SELECT 1'); } catch(e) { db='down'; }
const ok=db==='up'&&!shuttingDown;
res.status(ok?200:503).json({ok,db,uptimeSeconds:Math.floor(process.uptime()),symbol:CFG.symbol,market:'Spot',source:lastSource,lastAna
}));
app.get('/status',asyncRoute(async(_req,res)=>{
const {rows}=await pool.query(`SELECT COUNT(*) FILTER(WHERE estado='GREEN')::int green,COUNT(*) FILTER(WHERE estado='LOSS')::int loss,C
const s=rows[0]; const settled=s.green+s.loss;
const pnl=await pool.query("SELECT COALESCE(SUM(pnl_pct),0)::float AS total_pnl_pct FROM trade_sinais WHERE estado IN ('GREEN','LOSS')"
res.json({ok:true,symbol:CFG.symbol,market:'Spot',ordersExecuted:false,source:lastSource,config:{loopMs:CFG.loopMs,minScore:CFG.minScor
}));
app.get('/sinais',asyncRoute(async(req,res)=>{
const limit=Math.min(100,Math.max(1,Number(req.query.limit)||20));
const {rows}=await pool.query('SELECT id,symbol,direcao,preco_entrada,preco_tp,preco_sl,score,estado,tp_pct,sl_pct,preco_saida,pnl_pct,
res.json({ok:true,count:rows.length,items:rows});
}));
app.get('/eventos',auth,asyncRoute(async(req,res)=>{
const limit=Math.min(100,Math.max(1,Number(req.query.limit)||30));
const {rows}=await pool.query('SELECT id,tipo,mensagem,dados,criado_em FROM trade_eventos ORDER BY id DESC LIMIT $1',[limit]); res.json
}));
app.get('/placar',asyncRoute(async(_req,res)=>{
const {rows}=await pool.query(`SELECT COUNT(*) FILTER(WHERE estado='GREEN')::int green,COUNT(*) FILTER(WHERE estado='LOSS')::int loss,C
const s=rows[0], n=s.green+s.loss;
res.json({ok:true,date:new Date().toISOString().slice(0,10),...s,settled:n,winrate:n?round(s.green/n*100,2):null});
}));
app.post('/scan',auth,asyncRoute(async(_req,res)=>res.json({ok:true,result:await scanOnce()})));
app.post('/validar-entrada',auth,asyncRoute(async(req,res)=>{
const id=Number(req.body?.signalId), capital=Number(req.body?.capital);
if(!Number.isSafeInteger(id)||id<1) return res.status(400).json({ok:false,valid:false,error:'signalId inválido.'});
if(!Number.isFinite(capital)||capital<=0) return res.status(400).json({ok:false,valid:false,error:'capital deve ser positivo e expresso
const {rows}=await pool.query('SELECT * FROM trade_sinais WHERE id=$1',[id]); const s=rows[0];
if(!s) return res.status(404).json({ok:false,valid:false,error:'Sinal não encontrado.'});
if(s.estado!=='PENDENTE'||Date.now()>=new Date(s.expiracao_em).getTime()) return res.status(409).json({ok:true,valid:false,reason:'Sina
const entry=Number(s.preco_entrada),tp=Number(s.preco_tp),sl=Number(s.preco_sl),price=await fetchPrice(lastSource);
const deviation=Math.abs(price-entry)/entry;
if(!(sl<price&&price<tp)||deviation>CFG.maxDeviationPct) return res.status(409).json({ok:true,valid:false,reason:'Preço atual fora dos
const riskBudget=capital*CFG.riskPct, qtyRisk=riskBudget/(price-sl), qtyCapital=capital/price;
const qty=Math.floor(Math.min(qtyRisk,qtyCapital)*1e6)/1e6, exposure=qty*price;
if(qty<=0||exposure<5) return res.status(422).json({ok:true,valid:false,reason:'Capital insuficiente para o mínimo indicativo de 5 USDT
res.json({ok:true,valid:true,mode:'manual',ordersSent:false,signalId:id,symbol:s.symbol,signalEntryReference:entry,assumedManualEntry:p
}));
app.use((err,_req,res,_next)=>{ console.error('[HTTP]',errText(err)); res.status(500).json({ok:false,error:errText(err)}); });
async function telegramCommand(text, chatId) {
if (!CFG.botToken || !CFG.allowTelegramCommands) return;
// Restrict command replies to configured chat ID.
if (String(chatId) !== String(CFG.chatId)) return;
const command=String(text||'').trim().split(/\s+/)[0].split('@')[0].toLowerCase();
const send=async msg=>axios.post(`https://api.telegram.org/bot${CFG.botToken}/sendMessage`,{chat_id:chatId,text:msg,disable_web_page_pr
if(command==='/start'||command==='/ajuda'||command==='/help') return send('■ CAPUNGA TRADE Signal Server Pro\n/status — estado do motor
if(command==='/status') {
const r=await pool.query("SELECT COUNT(*) FILTER(WHERE estado='GREEN')::int green,COUNT(*) FILTER(WHERE estado='LOSS')::int loss,COUN
return send(`■ CAPUNGA TRADE\nMotor: ${scanning?'a analisar':'ativo'}\nMercado: ${CFG.symbol} Spot\nFonte: ${lastSource}\nGREEN: ${r.
}
if(command==='/placar') {
const r=await pool.query("SELECT COUNT(*) FILTER(WHERE estado='GREEN')::int green,COUNT(*) FILTER(WHERE estado='LOSS')::int loss,COUN
const s=r.rows[0], settled=s.green+s.loss;
return send(`■ PLACAR DO DIA\n■ GREEN: ${s.green}\n■ LOSS: ${s.loss}\n■ PENDENTES: ${s.pending}\nPrecisão histórica apurada: ${settl
}
if(command==='/historico') {
const r=await pool.query('SELECT id,estado,preco_entrada,preco_saida,criado_em FROM trade_sinais ORDER BY id DESC LIMIT 10');
const lines=r.rows.map(s=>`#${s.id} ${s.estado} | entrada ${round(Number(s.preco_entrada),2)} | saída ${s.preco_saida?round(Number(s.
return send(`■ ÚLTIMOS SINAIS\n${lines||'Ainda não existem sinais.'}`);
}
if(command==='/scan') { await send('■ A analisar o mercado.'); try { const result=await scanOnce(); await send(`■ RESULTADO DA ANÁLISE\
}
async function pollTelegram() {
if(!CFG.botToken||!CFG.allowTelegramCommands||shuttingDown) return;
try {
const {data}=await axios.get(`https://api.telegram.org/bot${CFG.botToken}/getUpdates`,{params:{offset:telegramOffset,timeout:0,allowe
if(!data?.ok||!Array.isArray(data.result)) return;
for(const update of data.result) {
telegramOffset=Math.max(telegramOffset,Number(update.update_id)+1);
const msg=update.message;
if(msg?.text) await telegramCommand(msg.text,msg.chat?.id);
}
} catch(e) { console.error('[Telegram poll]',errText(e)); }
}
async function shutdown() {
if(shuttingDown) return; shuttingDown=true;
if(scanTimer) clearInterval(scanTimer); if(keepaliveTimer) clearInterval(keepaliveTimer); if(telegramTimer) clearInterval(telegramTimer
if(server) await new Promise(resolve=>server.close(resolve));
if(pool) await pool.end();
process.exit(0);
}
async function main() {
await initDB();
server=app.listen(CFG.port,'0.0.0.0',()=>console.log(`[HTTP] CAPUNGA TRADE Signal Server Pro v5.1 · BTC/USDT Spot · :${CFG.port}`));
try { await scanOnce(); } catch(e) { console.error('[initial scan]',errText(e)); }
scanTimer=setInterval(()=>scanOnce().catch(()=>{}),CFG.loopMs);
if(CFG.keepaliveUrl) keepaliveTimer=setInterval(()=>axios.get(CFG.keepaliveUrl.replace(/\/$/,'')+'/health',{timeout:10000}).catch(()=>{
if(CFG.botToken&&CFG.allowTelegramCommands) { telegramTimer=setInterval(()=>pollTelegram().catch(()=>{}),CFG.telegramPollMs); pollTeleg
process.on('SIGTERM',shutdown); process.on('SIGINT',shutdown);
}
main().catch(e=>{console.error('[FATAL]',errText(e));process.exit(1);});
