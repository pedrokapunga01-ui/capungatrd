'use strict';
require('dotenv').config();
const puppeteer = require('puppeteer');
const axios = require('axios');

const TARGET_URL = process.env.TARGET_URL
  || 'https://www.tracksino.com/footballstudio'; // ajuste se necessário
const WEBHOOK_URL = process.env.WEBHOOK_URL;      // https://capungatrd.onrender.com/webhook/resultado
const WEBHOOK_SECRET = process.env.WEBHOOK_SECRET || '';
const POLL_MS = Number(process.env.POLL_MS || 20000);
const MAX_ROUNDS = Number(process.env.MAX_ROUNDS || 30);

let lastSentRoundId = null;
let browser;

async function getBrowser() {
  if (!browser) {
    browser = await puppeteer.launch({
      headless: 'new',
      args: ['--no-sandbox', '--disable-setuid-sandbox']
    });
  }
  return browser;
}

async function scrapeRounds() {
  const b = await getBrowser();
  const page = await b.newPage();
  try {
    await page.setUserAgent(
      'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 ' +
      '(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'
    );

    await page.goto(TARGET_URL, { waitUntil: 'networkidle2', timeout: 45000 });

    // Ajuste estes seletores conforme o site real.
    // Exemplo genérico: procura por blocos de histórico.
    const rounds = await page.evaluate((max) => {
      const out = [];

      // Tenta extrair de múltiplos formatos possíveis de DOM.
      // Estratégia 1: linhas de tabela
      const rows = document.querySelectorAll(
        'table tbody tr, .history-row, .round-row, [data-round]'
      );

      for (const row of rows) {
        if (out.length >= max) break;

        const text = row.innerText || row.textContent || '';

        // Extrai cartas via padrões comuns: "K x 9", "A-10", "3 vs 7"
        const m = text.match(/([2-9]|10|[AJQK])\s*(?:x|×|vs|-|—)\s*([2-9]|10|[AJQK])/i);
        if (!m) continue;

        const homeCard = m[1].toUpperCase();
        const awayCard = m[2].toUpperCase();

        // Tenta extrair roundId
        let roundId = row.getAttribute('data-round')
          || row.getAttribute('data-id')
          || null;

        out.push({ roundId, homeCard, awayCard });
      }

      // Estratégia 2 (fallback): varre todo o texto e captura pares de cartas
      if (!out.length) {
        const bodyText = document.body.innerText;
        const regex = /\b([2-9]|10|[AJQK])\s*(?:x|×|vs|-|—)\s*([2-9]|10|[AJQK])\b/gi;
        let match;
        let i = 0;
        while ((match = regex.exec(bodyText)) !== null && i < max) {
          out.push({
            roundId: `scrape-${Date.now()}-${i}`,
            homeCard: match[1].toUpperCase(),
            awayCard: match[2].toUpperCase()
          });
          i++;
        }
      }

      return out;
    }, MAX_ROUNDS);

    return rounds;
  } finally {
    await page.close();
  }
}

async function sendRound(round) {
  const headers = { 'Content-Type': 'application/json' };
  if (WEBHOOK_SECRET) headers['x-webhook-secret'] = WEBHOOK_SECRET;

  try {
    const { data } = await axios.post(WEBHOOK_URL, round, {
      headers,
      timeout: 15000
    });
    console.log('[sent]', round, '->', data?.inserted ? 'new' : 'duplicate');
  } catch (e) {
    console.error('[send error]', e?.response?.status, e?.response?.data || e.message);
  }
}

async function loop() {
  try {
    const rounds = await scrapeRounds();
    console.log(`[scrape] ${rounds.length} rounds encontrados`);

    // Envia do mais antigo para o mais novo (o servidor deduplica por roundId)
    for (const r of rounds.reverse()) {
      if (!r.homeCard || !r.awayCard) continue;
      await sendRound(r);
    }
  } catch (e) {
    console.error('[scrape error]', e.message);
  } finally {
    setTimeout(loop, POLL_MS);
  }
}

process.on('SIGTERM', async () => {
  if (browser) await browser.close();
  process.exit(0);
});

loop().catch(e => {
  console.error('[fatal]', e);
  process.exit(1);
});