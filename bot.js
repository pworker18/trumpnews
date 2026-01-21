const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { chromium } = require('playwright');
const dotenv = require('dotenv');

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
const maxMessages = Number.parseInt(process.env.MAX_NEWS_MESSAGES, 10);
const tag = process.env.DISCORD_TAG;
const logFilePath = process.env.LOG_FILE;
const headless = process.env.HEADLESS !== 'false';
const navigationTimeoutMs = Number.parseInt(process.env.NAVIGATION_TIMEOUT_MS || '60000', 10);
const pageWaitTimeoutMs = Number.parseInt(process.env.PAGE_WAIT_TIMEOUT_MS || '45000', 10);
const userAgent = process.env.USER_AGENT;

if (Number.isNaN(maxMessages) || maxMessages <= 0) {
  console.error('MAX_NEWS_MESSAGES must be a positive integer.');
  process.exit(1);
}

if (Number.isNaN(navigationTimeoutMs) || navigationTimeoutMs <= 0) {
  console.error('NAVIGATION_TIMEOUT_MS must be a positive integer.');
  process.exit(1);
}

if (Number.isNaN(pageWaitTimeoutMs) || pageWaitTimeoutMs <= 0) {
  console.error('PAGE_WAIT_TIMEOUT_MS must be a positive integer.');
  process.exit(1);
}

const ensureLogFileDir = () => {
  const dir = path.dirname(logFilePath);
  if (dir && dir !== '.') {
    fs.mkdirSync(dir, { recursive: true });
  }
};

const loadProcessedIds = () => {
  if (!fs.existsSync(logFilePath)) {
    return [];
  }

  try {
    const data = fs.readFileSync(logFilePath, 'utf8');
    if (!data.trim()) {
      return [];
    }
    const parsed = JSON.parse(data);
    return Array.isArray(parsed) ? parsed : [];
  } catch (error) {
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
  return crypto.createHash('sha256').update(raw).digest('hex');
};

const sendDiscordMessage = async (content) => {
  const response = await fetch(webhookUrl, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json'
    },
    body: JSON.stringify({ content })
  });

  if (!response.ok) {
    const errorText = await response.text();
    throw new Error(`Discord webhook failed (${response.status}): ${errorText}`);
  }
};

const buildDiscordMessage = (item) => {
  const formatField = (label, value) => `**${label}:** ${value || 'N/A'}`;
  return [
    tag,
    formatField('Time', item.time),
    formatField('Sentiment', item.sentiment),
    formatField('Full Tweet', item.fullTweet),
    formatField('Summary', item.summary),
    formatField('Affected Securities', item.affectedSecurities),
    formatField('Sector', item.sector)
  ].join('\n');
};

const createContextOptions = () => {
  const options = {
    locale: 'en-US',
    timezoneId: 'America/New_York',
    viewport: { width: 1280, height: 720 }
  };

  if (userAgent) {
    options.userAgent = userAgent;
  }

  return options;
};

const waitForNewsTable = async (page, attempt) => {
  try {
    await page.waitForSelector('table tbody tr', { timeout: pageWaitTimeoutMs });
    return true;
  } catch (error) {
    const content = await page.content();
    const blocked = content.includes('cf-challenge') || content.includes('Attention Required');

    if (blocked && attempt < 2) {
      console.warn('Cloudflare challenge detected, waiting for clearance...');
      await page.waitForTimeout(10000);
      await page.reload({ waitUntil: 'domcontentloaded', timeout: navigationTimeoutMs });
      return waitForNewsTable(page, attempt + 1);
    }

    return false;
  }
};

const extractNewsItems = async (page, limit) => {
  return page.evaluate((maxItems) => {
    const getText = (element) => {
      if (!element) {
        return '';
      }
      const text = element.innerText || element.textContent || '';
      return text.replace(/\s+/g, ' ').trim();
    };

    const getAttributeText = (element) => {
      if (!element) {
        return '';
      }

      const attributeSources = [
        'data-full-tweet',
        'data-tweet',
        'data-tooltip',
        'data-original-title',
        'title',
        'aria-label'
      ];

      for (const attribute of attributeSources) {
        const value = element.getAttribute(attribute);
        if (value) {
          return value.trim();
        }
      }

      return '';
    };

    const extractCellText = (cell) => {
      if (!cell) {
        return '';
      }
      const text = getText(cell);
      if (text) {
        return text;
      }
      const imgAlt = cell.querySelector('img')?.getAttribute('alt');
      if (imgAlt) {
        return imgAlt.trim();
      }
      const svgTitle = cell.querySelector('svg title')?.textContent;
      if (svgTitle) {
        return svgTitle.trim();
      }
      const ariaLabel = cell.querySelector('[aria-label]')?.getAttribute('aria-label');
      if (ariaLabel) {
        return ariaLabel.trim();
      }
      return '';
    };

    const rows = Array.from(document.querySelectorAll('table tbody tr'));
    return rows.slice(0, maxItems).map((row) => {
      const cells = Array.from(row.querySelectorAll('td'));
      const time = extractCellText(cells[0]);
      const sentiment = extractCellText(cells[1]);
      let fullTweet = extractCellText(cells[2]);
      if (!fullTweet) {
        fullTweet = getAttributeText(cells[2]?.querySelector('*'));
      }
      const summary = extractCellText(cells[3]);
      const affectedSecurities = extractCellText(cells[4]);
      const sector = extractCellText(cells[5]);

      return {
        time,
        sentiment,
        fullTweet,
        summary,
        affectedSecurities,
        sector
      };
    });
  }, limit);
};

const run = async () => {
  const processedIds = new Set(loadProcessedIds());

  const browser = await chromium.launch({ headless });
  const context = await browser.newContext(createContextOptions());
  const page = await context.newPage();

  try {
    const response = await page.goto(siteUrl, { waitUntil: 'domcontentloaded', timeout: navigationTimeoutMs });
    const status = response?.status();
    if (status && status >= 400) {
      throw new Error(`Site responded with HTTP ${status}.`);
    }

    const tableReady = await waitForNewsTable(page, 0);
    if (!tableReady) {
      throw new Error('Unable to locate news table; the site may be blocking automated access.');
    }

    const items = await extractNewsItems(page, maxMessages);
    const newItems = [];

    for (const item of items) {
      const id = createMessageId(item);
      if (!processedIds.has(id)) {
        newItems.push({ id, item });
      }
    }

    for (const entry of newItems.reverse()) {
      const message = buildDiscordMessage(entry.item);
      await sendDiscordMessage(message);
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
