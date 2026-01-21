const dotenv = require('dotenv');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { chromium } = require('playwright');

dotenv.config();

const requiredEnvVars = [
  'SITE_URL',
  'DISCORD_WEBHOOK_URL',
  'MAX_NEWS_MESSAGES',
  'DISCORD_TAG',
  'LOG_FILE'
];

const missingEnvVars = requiredEnvVars.filter((name) => !process.env[name]);
if (missingEnvVars.length > 0) {
  console.error(`Missing required .env values: ${missingEnvVars.join(', ')}`);
  process.exit(1);
}

const siteUrl = process.env.SITE_URL;
const webhookUrl = process.env.DISCORD_WEBHOOK_URL;
const maxMessages = Number(process.env.MAX_NEWS_MESSAGES || 10);
const tag = process.env.DISCORD_TAG || '';
const logFilePath = process.env.LOG_FILE;

const headless = process.env.HEADLESS !== 'false';
const navigationTimeoutMs = Number(process.env.NAV_TIMEOUT_MS || 60000);
const pageWaitTimeoutMs = Number(process.env.PAGE_WAIT_TIMEOUT_MS || 45000);

const userAgent =
  process.env.USER_AGENT ||
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36';

const ensureLogFileDir = () => {
  const dir = path.dirname(logFilePath);
  if (!dir || dir === '.') return;
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
};

const loadProcessedIds = () => {
  try {
    if (!fs.existsSync(logFilePath)) return [];
    const raw = fs.readFileSync(logFilePath, 'utf8').trim();
    if (!raw) return [];
    const parsed = JSON.parse(raw);
    return Array.isArray(parsed) ? parsed : [];
  } catch {
    console.warn('Unable to parse log file, starting with empty log.');
    return [];
  }
};

const writeProcessedIds = (ids) => {
  ensureLogFileDir();
  fs.writeFileSync(logFilePath, `${JSON.stringify(ids, null, 2)}\n`, 'utf8');
};

const createMessageId = (item) => {
  const raw = `${item.time}|${item.sentiment}|${item.fullTweet}|${item.summary}|${item.affectedSecurities}|${item.sector}`;
  return crypto.createHash('sha256').update(raw).digest('sha256');
};

const sendDiscordMessage = async (content) => {
  const response = await fetch(webhookUrl, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ content })
  });

  if (!response.ok) {
    const errorText = await response.text();
    throw new Error(`Discord webhook failed (${response.status}): ${errorText}`);
  }
};

// ✅ UPDATED formatting with source link
const formatMessage = (item) => {
  const esc = (s) => String(s ?? '').trim();

  const time = esc(item.time) || 'N/A';
  const sentiment = esc(item.sentiment) || 'N/A';
  const summary = esc(item.summary) || 'N/A';

  const fullTweetRaw = esc(item.fullTweet);
  const fullTweet = fullTweetRaw && fullTweetRaw !== '00' ? fullTweetRaw : '';

  const affected = esc(item.affectedSecurities) || '—';
  const sector = esc(item.sector) || '—';

  const sentimentEmoji =
    /bullish/i.test(sentiment) ? '🟢' :
    /bearish/i.test(sentiment) ? '🔴' :
    /neutral/i.test(sentiment) ? '⚪' : '🟦';

  const header = `\`${time}\` (${sentimentEmoji} **${sentiment}**)`;

  const lines = [
    header,
    `${summary}`,
	``,
    `**Tickers:** ${affected}`,
    `**Sector:** ${sector}`,
    '',
    `<${siteUrl}>`
  ];

  if (fullTweet) {
    lines.splice(3, 0, `💬 **Full Tweet:** ${fullTweet}`);
  }

  return [lines.join('\n'),tag].filter(Boolean).join('\n');
};

const createContextOptions = () => ({
  locale: 'en-US',
  timezoneId: 'America/New_York',
  viewport: { width: 1280, height: 720 },
  userAgent,
  extraHTTPHeaders: {
    'Accept-Language': 'en-US,en;q=0.9',
    'Upgrade-Insecure-Requests': '1'
  }
});

const dismissOverlaysStrict = async (page) => {
  await page.keyboard.press('Escape').catch(() => {});

  const closeSelectors = [
    'button[aria-label="Close"]',
    'button[aria-label="close"]',
    '[role="dialog"] button:has-text("×")',
    '[role="dialog"] button[aria-label="Close"]',
    '[role="dialog"] svg[aria-label="Close"]',
    '[role="dialog"] .close',
    '[role="dialog"] button'
  ];

  for (let pass = 0; pass < 5; pass++) {
    let closedSomething = false;

    const dialog = page.locator('[role="dialog"]').first();
    if (await dialog.count().catch(() => 0)) {
      for (const sel of closeSelectors) {
        const loc = page.locator(sel).first();
        if (await loc.isVisible().catch(() => false)) {
          await loc.click({ timeout: 800 }).catch(() => {});
          closedSomething = true;
        }
      }
    }

    if (!closedSomething) break;
    await page.waitForTimeout(300);
  }
};

const installNavigationGuard = async (page, targetUrl) => {
  await page.route('**/*', async (route) => {
    const url = route.request().url();
    if (url.includes('/terms') || url.includes('/privacy')) {
      return route.abort();
    }
    return route.continue();
  });

  page.on('framenavigated', async (frame) => {
    if (frame !== page.mainFrame()) return;
    const url = frame.url();
    if (!url.includes('/trump-dashboard')) {
      try {
        await page.goto(targetUrl, { waitUntil: 'domcontentloaded', timeout: navigationTimeoutMs });
      } catch {}
    }
  });
};

const waitForReactTableRows = async (page) => {
  const rows = page.locator('.rt-tbody .rt-tr-group');
  await rows.first().waitFor({ timeout: pageWaitTimeoutMs });
  return rows;
};

const scrollReactTable = async (page) => {
  const recent = page.locator('text=Recent Tweets').first();
  if (await recent.count().catch(() => 0)) {
    await recent.scrollIntoViewIfNeeded().catch(() => {});
    await page.waitForTimeout(400);
  }

  const tableScroller = page.locator('.rt-table').first();
  if (await tableScroller.count().catch(() => 0)) {
    await tableScroller.click({ timeout: 1000 }).catch(() => {});
    for (let i = 0; i < 10; i++) {
      await page.mouse.wheel(0, 1200).catch(() => {});
      await page.waitForTimeout(250);
      const rowCount = await page.locator('.rt-tbody .rt-tr-group').count().catch(() => 0);
      if (rowCount > 0) break;
    }
  } else {
    for (let i = 0; i < 10; i++) {
      await page.mouse.wheel(0, 1200).catch(() => {});
      await page.waitForTimeout(250);
      const rowCount = await page.locator('.rt-tbody .rt-tr-group').count().catch(() => 0);
      if (rowCount > 0) break;
    }
  }
};

const extractNewsItemsFromReactTable = async (page, limit) => {
  return page.evaluate((maxItems) => {
    const clean = (s) => (s || '').replace(/\s+/g, ' ').trim();
    const rows = Array.from(document.querySelectorAll('.rt-tbody .rt-tr-group')).slice(0, maxItems);

    return rows.map((group) => {
      const row = group.querySelector('.rt-tr');
      const cells = row ? Array.from(row.querySelectorAll('.rt-td')) : [];

      const timeCell = cells[0];
      let time = '';
      if (timeCell) {
        const spans = timeCell.querySelectorAll('span');
        const t = spans[0] ? clean(spans[0].textContent) : '';
        const d = spans[1] ? clean(spans[1].textContent) : '';
        time = clean([t, d].filter(Boolean).join(' '));
      }

      const sentimentCell = cells[1];
      const sentiment = sentimentCell ? clean(sentimentCell.textContent) : '';

      const fullTweetCell = cells[2];
      const fullTweet = fullTweetCell ? clean(fullTweetCell.textContent) : '';

      const summaryCell = cells[3];
      let summary = '';
      if (summaryCell) {
        const titled = summaryCell.querySelector('[title]');
        summary = clean((titled && titled.getAttribute('title')) || summaryCell.textContent);
      }

      const affectedCell = cells[4];
      const affectedSecurities = affectedCell ? clean(affectedCell.textContent).replace(/^—+$/, '—') : '';

      const sectorCell = cells[5];
      const sector = sectorCell ? clean(sectorCell.textContent).replace(/^—+$/, '—') : '';

      return { time, sentiment, fullTweet, summary, affectedSecurities, sector };
    });
  }, limit);
};

const run = async () => {
  const processedIds = new Set(loadProcessedIds());

  const browser = await chromium.launch({
    headless,
    args: ['--disable-blink-features=AutomationControlled']
  });

  const context = await browser.newContext(createContextOptions());

  await context.addInitScript(() => {
    Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
  });

  const page = await context.newPage();

  try {
    await installNavigationGuard(page, siteUrl);

    const response = await page.goto(siteUrl, {
      waitUntil: 'domcontentloaded',
      timeout: navigationTimeoutMs
    });

    const status = response?.status();
    if (status && status >= 400) {
      throw new Error(`Site responded with HTTP ${status}.`);
    }

    await page.waitForLoadState('networkidle').catch(() => {});
    await dismissOverlaysStrict(page);
    await scrollReactTable(page);
    await waitForReactTableRows(page);

    const items = await extractNewsItemsFromReactTable(page, maxMessages);

    const newItems = [];
    for (const item of items) {
      const id = createMessageId(item);
      if (!processedIds.has(id)) newItems.push({ id, item });
    }

    for (const entry of newItems.reverse()) {
      await sendDiscordMessage(formatMessage(entry.item));
      processedIds.add(entry.id);
    }

    writeProcessedIds(Array.from(processedIds));
    console.log(`Processed ${newItems.length} new item(s).`);
  } finally {
    await context.close();
    await browser.close();
  }
};

run().catch((error) => {
  console.error('Bot failed:', error);
  process.exit(1);
});
