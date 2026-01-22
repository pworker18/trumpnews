const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { chromium } = require('playwright');
const dotenv = require('dotenv');
const { GoogleGenerativeAI } = require('@google/generative-ai');

dotenv.config();

const requiredEnvVars = [
  'SITE_URL',
  'DISCORD_WEBHOOK_URL_1',
  'DISCORD_WEBHOOK_URL_2',
  'DISCORD_WEBHOOK_URL_3',
  'MAX_NEWS_MESSAGES',
  'DISCORD_TAG',
  'LOG_FILE'
];

const missingEnvVars = requiredEnvVars.filter((name) => !process.env[name]);
if (missingEnvVars.length > 0) {
  console.error(`Missing required .env values: ${missingEnvVars.join(', ')}`);
  process.exit(1);
}

// Load all Gemini API keys
const geminiApiKeys = [];
for (let i = 1; i <= 100; i++) {
  const key = process.env[`GEMINI_API_KEY_${i}`];
  if (key && String(key).trim()) {
    geminiApiKeys.push(String(key).trim());
  } else {
    break; // Stop at first missing key
  }
}

if (geminiApiKeys.length === 0) {
  console.error('Missing required .env value: GEMINI_API_KEY_1 (at least one Gemini API key is required)');
  process.exit(1);
}

console.log(`Loaded ${geminiApiKeys.length} Gemini API key(s)`);

const siteUrl = process.env.SITE_URL;

const webhookUrls = [
  process.env.DISCORD_WEBHOOK_URL_1,
  process.env.DISCORD_WEBHOOK_URL_2,
  process.env.DISCORD_WEBHOOK_URL_3
].map((s) => String(s || '').trim()).filter(Boolean);

if (webhookUrls.length !== 3) {
  console.error('You must set DISCORD_WEBHOOK_URL_1, DISCORD_WEBHOOK_URL_2, DISCORD_WEBHOOK_URL_3.');
  process.exit(1);
}

const maxMessages = Number(process.env.MAX_NEWS_MESSAGES || 10);
const tag = process.env.DISCORD_TAG || '';
const logFilePath = process.env.LOG_FILE;

const headless = process.env.HEADLESS !== 'false';
const navigationTimeoutMs = Number(process.env.NAV_TIMEOUT_MS || 60000);
const pageWaitTimeoutMs = Number(process.env.PAGE_WAIT_TIMEOUT_MS || 45000);

const userAgent =
  process.env.USER_AGENT ||
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36';

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// Gemini translation settings
const geminiModelName = String(process.env.GEMINI_MODEL || 'gemini-2.5-flash-lite').trim();
const translateTo = String(process.env.TRANSLATE_TO || 'Hebrew').trim();
const geminiBatchSize = Math.max(1, Number(process.env.GEMINI_BATCH_SIZE || 12));
const geminiMinDelayMs = Math.max(0, Number(process.env.GEMINI_MIN_DELAY_MS || 5000));

// Create Gemini clients for all API keys
const geminiClients = geminiApiKeys.map(key => new GoogleGenerativeAI(key));

// Track current key index for cycling
let currentKeyIndex = 0;

// Track rate-limited keys with timestamp
const rateLimitedKeys = new Map(); // keyIndex -> timestamp when it became limited

const isGeminiRateLimitError = (err) => {
  const msg = String(err?.message || '');
  return msg.includes('429') || msg.toLowerCase().includes('too many requests') || msg.toLowerCase().includes('quota');
};

const isGeminiServiceError = (err) => {
  const msg = String(err?.message || '');
  const status = err?.status;
  return status === 503 || status === 500 || msg.toLowerCase().includes('overloaded') || msg.toLowerCase().includes('service unavailable');
};

// Get next key in cycle, moving to the next one automatically
const getNextKeyInCycle = () => {
  const keyIndex = currentKeyIndex;
  currentKeyIndex = (currentKeyIndex + 1) % geminiApiKeys.length;
  return keyIndex;
};

// Check if a key is currently rate-limited
const isKeyRateLimited = (keyIndex) => {
  const limitedTimestamp = rateLimitedKeys.get(keyIndex);
  if (!limitedTimestamp) return false;
  
  const now = Date.now();
  // If key was rate-limited more than 1 hour ago, consider it available again
  if (now - limitedTimestamp >= 3600000) {
    rateLimitedKeys.delete(keyIndex);
    return false;
  }
  
  return true;
};

// Mark a key as rate-limited
const markKeyAsRateLimited = (keyIndex) => {
  console.warn(`⚠️  API key #${keyIndex + 1} hit rate limit. Marking as rate-limited.`);
  rateLimitedKeys.set(keyIndex, Date.now());
};

// Try to find an available (non-rate-limited) key, starting from current position
const findAvailableKey = () => {
  const startIndex = currentKeyIndex;
  let attempts = 0;
  
  while (attempts < geminiApiKeys.length) {
    const keyIndex = (startIndex + attempts) % geminiApiKeys.length;
    
    if (!isKeyRateLimited(keyIndex)) {
      // Update current index to this key for next cycle
      currentKeyIndex = (keyIndex + 1) % geminiApiKeys.length;
      return keyIndex;
    }
    
    attempts++;
  }
  
  // All keys are rate-limited, return first key and hope for the best
  console.warn('⚠️  All Gemini API keys are rate-limited. Using first key anyway.');
  currentKeyIndex = 1 % geminiApiKeys.length;
  return 0;
};

const translateSummariesWithGemini = async (summaries) => {
  if (!Array.isArray(summaries) || summaries.length === 0) return [];

  const translateChunk = async (chunk, attemptCount = 0) => {
    const indices = chunk.map((_, i) => i);
    const payload = indices.map((i) => ({ i, text: String(chunk[i] ?? '') }));

    const prompt =
      `Translate the following news summary texts to ${translateTo}.\n` +
      `Return ONLY valid JSON in this exact shape: {"translations":["...", ...]}\n` +
      `The translations array MUST be the same length and order as the input array.\n\n` +
      `Input JSON:\n${JSON.stringify({ items: payload }, null, 0)}`;

    // Max attempts = try each key at least twice
    const maxAttempts = geminiApiKeys.length * 3;
    
    if (attemptCount >= maxAttempts) {
      throw new Error(`Translation failed after ${maxAttempts} attempts across all ${geminiApiKeys.length} API key(s)`);
    }

    try {
      // Find next available (non-rate-limited) key
      const keyIndex = findAvailableKey();
      const model = geminiClients[keyIndex].getGenerativeModel({ model: geminiModelName });
      
      console.log(`Using Gemini API key #${keyIndex + 1}/${geminiApiKeys.length} for translation (attempt ${attemptCount + 1}/${maxAttempts})`);
      
      const result = await model.generateContent({
        contents: [{ role: 'user', parts: [{ text: prompt }] }],
        generationConfig: {
          temperature: 0,
          maxOutputTokens: 8192
        }
      });

      const text = result?.response?.text?.() ?? '';
      const jsonMatch = String(text).match(/\{[\s\S]*\}/);
      const parsed = jsonMatch ? JSON.parse(jsonMatch[0]) : JSON.parse(text);

      const translations = Array.isArray(parsed?.translations) ? parsed.translations : [];
      if (translations.length !== chunk.length) throw new Error('Gemini returned unexpected translation count.');
      return translations.map((t) => String(t ?? '').trim());
    } catch (err) {
      // Handle rate limit errors - mark key as limited and try next key
      if (isGeminiRateLimitError(err)) {
        // Find which key was just used (it's the one before current in cycle)
        const failedKeyIndex = (currentKeyIndex - 1 + geminiApiKeys.length) % geminiApiKeys.length;
        
        // Only mark as rate-limited if not already marked
        if (!isKeyRateLimited(failedKeyIndex)) {
          markKeyAsRateLimited(failedKeyIndex);
        }
        
        // Check if all keys are now rate-limited
        const allLimited = geminiApiKeys.every((_, idx) => isKeyRateLimited(idx));
        if (allLimited) {
          console.warn(`⚠️  All ${geminiApiKeys.length} API key(s) are rate-limited. Add more keys or wait for rate limits to reset.`);
        }
        
        console.log(`Switching to next API key...`);
        await sleep(1000); // Small delay before retry
        return translateChunk(chunk, attemptCount + 1);
      }
      
      // Handle service errors (503, 500, overloaded) - don't retry, fail fast
      if (isGeminiServiceError(err)) {
        console.warn(`Gemini service error (${err.status || 'unknown'}): ${err.message}`);
        throw err; // Throw to trigger fallback to untranslated
      }
      
      throw err;
    }
  };

  try {
    const out = [];
    for (let i = 0; i < summaries.length; i += geminiBatchSize) {
      const chunk = summaries.slice(i, i + geminiBatchSize);
      const translated = await translateChunk(chunk);
      out.push(...translated);
      if (i + geminiBatchSize < summaries.length && geminiMinDelayMs > 0) {
        await sleep(geminiMinDelayMs);
      }
    }
    return out;
  } catch (err) {
    console.error('Translation failed:', err.message);
    console.warn('⚠️  Falling back to untranslated text');
    return null; // Return null to signal fallback needed
  }
};

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
  const tickersKey = Array.isArray(item.tickers) ? item.tickers.join(',') : '';
  const raw = `${item.time}|${item.sentiment}|${item.fullTweet}|${item.summary}|${tickersKey}|${item.sector}`;
  return crypto.createHash('sha256').update(raw).digest('hex');
};

const sendDiscordMessage = async (webhookUrl, content) => {
  while (true) {
    const response = await fetch(webhookUrl, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ content })
    });

    if (response.ok) return;

    if (response.status === 429) {
      let retryAfterMs = 1000;
      try {
        const body = await response.json();
        retryAfterMs = Math.ceil((Number(body.retry_after) || 1) * 1000);
      } catch {}
      await sleep(retryAfterMs + 250);
      continue;
    }

    const errorText = await response.text();
    throw new Error(`Discord webhook failed (${response.status}): ${errorText}`);
  }
};

const buildTradingViewLinks = (tickers) => {
  if (!Array.isArray(tickers) || tickers.length === 0) return '—';

  const cleaned = [];
  const seen = new Set();

  for (const t of tickers) {
    const sym = String(t || '').trim().toUpperCase();
    if (!sym) continue;
    if (!/^[A-Z0-9.\-]{1,12}$/.test(sym)) continue;
    if (seen.has(sym)) continue;
    seen.add(sym);
    cleaned.push(sym);
  }

  if (cleaned.length === 0) return '—';

  return cleaned
    .map((sym) => `[$${sym}](<https://www.tradingview.com/chart/?symbol=${encodeURIComponent(sym)}>)`)
    .join(', ');
};

// Split long text into chunks that fit within Discord's limit
const splitIntoChunks = (text, maxLength = 1990) => {
  if (text.length <= maxLength) return [text];
  
  const chunks = [];
  let remaining = text;
  
  while (remaining.length > 0) {
    if (remaining.length <= maxLength) {
      chunks.push(remaining);
      break;
    }
    
    // Try to find a good break point (newline, space, punctuation)
    let splitIndex = maxLength;
    
    // Look for newline
    const lastNewline = remaining.lastIndexOf('\n', maxLength);
    if (lastNewline > maxLength * 0.7) {
      splitIndex = lastNewline + 1;
    } else {
      // Look for space
      const lastSpace = remaining.lastIndexOf(' ', maxLength);
      if (lastSpace > maxLength * 0.7) {
        splitIndex = lastSpace + 1;
      }
    }
    
    chunks.push(remaining.substring(0, splitIndex).trim());
    remaining = remaining.substring(splitIndex).trim();
  }
  
  return chunks;
};

// message formatting (with source link & conditional sector and tickers)
// Returns an array of message parts that need to be sent separately
const formatMessage = (item) => {
  const esc = (s) => String(s ?? '').trim();

  const time = esc(item.time) || 'N/A';
  const sentiment = esc(item.sentiment) || 'N/A';
  const summary = esc(item.summary) || 'N/A';

  const fullTweetRaw = esc(item.fullTweet);
  const fullTweet = fullTweetRaw && fullTweetRaw !== '00' ? fullTweetRaw : '';

  const sector = esc(item.sector);
  const tickersLinks = buildTradingViewLinks(item.tickers);

  const sentimentEmoji =
    /bullish/i.test(sentiment) ? '🟢' :
    /bearish/i.test(sentiment) ? '🔴' :
    /neutral/i.test(sentiment) ? '⚪' : '🟦';

  const lines = [];
  
  // Only add tickers line if it's not empty or "—"
  if (tickersLinks && tickersLinks !== '—') {
    lines.push(tickersLinks);
  }
  
  // Only add sector line if it's not empty or "—"
  if (sector && sector != '—') {
    lines.push(`**Sector:** ${sector}`);
  }
  
  // Add empty line only if we added tickers or sector
  if (lines.length > 0) {
    lines.push('');
  }
  
  // Add the rest of the message
  lines.push(
    `\`${time}\` (${sentimentEmoji} **${sentiment}**)`,
    `${summary}`,
    ``,
    `💬 ${fullTweet}`,
    ``,
    `<${siteUrl}>`
  );

  const baseMessage = lines.join('\n');
  const finalMessage = [baseMessage, tag].filter(Boolean).join('\n');
  
  // Split into chunks if message is too long
  return splitIntoChunks(finalMessage, 1990);
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
};

const closeTipRanksPopup = async (page) => {
  // Try multiple methods to close popups/modals
  
  // Method 1: X button that contains icon-cross with specific positioning
  const xButtonWithIcon = page.locator('button:has(i.icon-cross.positionabsolute.anchortopRight)');
  const count = await xButtonWithIcon.count();
  for (let i = 0; i < count; i++) {
    const btn = xButtonWithIcon.nth(i);
    if (await btn.isVisible().catch(() => false)) {
      await btn.click({ timeout: 1500, force: true }).catch(() => {});
      await page.waitForTimeout(200);
    }
  }
  
  // Method 2: Any button with icon-cross
  const closeBtn = page.locator('button:has(i.icon-cross)');
  const closeCount = await closeBtn.count();
  for (let i = 0; i < closeCount; i++) {
    const btn = closeBtn.nth(i);
    if (await btn.isVisible().catch(() => false)) {
      await btn.click({ timeout: 1500, force: true }).catch(() => {});
      await page.waitForTimeout(200);
    }
  }
  
  // Method 3: Direct icon-cross click (sometimes the icon itself is clickable)
  const closeIcons = page.locator('i.icon-cross');
  const iconCount = await closeIcons.count();
  for (let i = 0; i < iconCount; i++) {
    const icon = closeIcons.nth(i);
    if (await icon.isVisible().catch(() => false)) {
      await icon.click({ timeout: 1500, force: true }).catch(() => {});
      await page.waitForTimeout(200);
    }
  }
  
  // Method 4: X button in top-right of modal (common pattern)
  const xButton = page.locator('button[aria-label*="close"], button[aria-label*="Close"]');
  if (await xButton.isVisible().catch(() => false)) {
    await xButton.click({ timeout: 1500, force: true }).catch(() => {});
    await page.waitForTimeout(150);
  }
  
  // Method 5: Press Escape key multiple times
  await page.keyboard.press('Escape').catch(() => {});
  await page.waitForTimeout(100);
  await page.keyboard.press('Escape').catch(() => {});
};

const installNavigationGuard = async (page, targetUrl) => {
  await page.route('**/*', async (route) => {
    const url = route.request().url();
    if (url.includes('/terms') || url.includes('/privacy')) return route.abort();
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

const scrollToRecentTweets = async (page) => {
  const recent = page.locator('text=Recent Tweets').first();
  if (await recent.count().catch(() => 0)) {
    await recent.scrollIntoViewIfNeeded().catch(() => {});
    await page.waitForTimeout(300);
  }
};

const waitForReactTable = async (page) => {
  const scroller = page.locator('.rt-table').first();
  await scroller.waitFor({ timeout: pageWaitTimeoutMs });

  const firstRow = page.locator('.rt-tbody .rt-tr-group').first();
  await firstRow.waitFor({ timeout: pageWaitTimeoutMs });

  return scroller;
};

// Extract full tweet text by programmatically triggering hover
const extractFullTweetFromTooltip = async (page, rowIndex) => {
  try {
    await closeTipRanksPopup(page);
    await page.waitForTimeout(100);
    
    const rowGroup = page.locator(`.rt-tbody .rt-tr-group:nth-child(${rowIndex + 1})`).first();
    const rowExists = await rowGroup.count();
    if (rowExists === 0) {
      console.log(`Row ${rowIndex}: Row not found in DOM`);
      return '';
    }

    // Scroll row into view
    await page.evaluate((idx) => {
      const row = document.querySelectorAll('.rt-tbody .rt-tr-group')[idx];
      if (row) {
        row.scrollIntoView({ behavior: 'smooth', block: 'center', inline: 'nearest' });
      }
    }, rowIndex);
    await page.waitForTimeout(300);
    
    const row = page.locator('.rt-tbody .rt-tr-group').nth(rowIndex);
    const rowVisible = await row.isVisible().catch(() => false);
    if (!rowVisible) {
      console.log(`Row ${rowIndex}: Row not visible`);
      return '';
    }

    const bubbleIcon = page.locator('i.icon-twoBubbles');
    const button = row.locator('button', { has: bubbleIcon }).first();
    const buttonVisible = await button.isVisible().catch(() => false);
    if (!buttonVisible) {
      console.log(`Row ${rowIndex}: Tooltip button not found`);
      return '';
    }

    // Force hover state programmatically to trigger tooltip in headless mode
    await button.dispatchEvent('pointerover').catch(() => {});
    await button.dispatchEvent('mouseenter').catch(() => {});
    await button.dispatchEvent('mouseover').catch(() => {});

    const tooltipSelectors = [
      '[data-tippy-root] .tippy-box[data-state="visible"] span[title]',
      '[data-tippy-root] .tippy-box span[title]',
      '[data-tippy-root] span[title]',
      '.tippy-content span[title]'
    ];

    const fullTweet = await page
      .waitForFunction((selectors) => {
        for (const selector of selectors) {
          const tooltip = document.querySelector(selector);
          if (tooltip) {
            return tooltip.getAttribute('title') || tooltip.textContent || '';
          }
        }
        return '';
      }, tooltipSelectors, { timeout: 2000 })
      .then((handle) => handle.jsonValue())
      .catch(() => '');

    await button.dispatchEvent('mouseout').catch(() => {});
    await button.dispatchEvent('mouseleave').catch(() => {});

    if (fullTweet) {
      console.log(`Row ${rowIndex}: ✓ Extracted (${fullTweet.length} chars)`);
    } else {
      console.log(`Row ${rowIndex}: ✗ No tooltip found`);
    }

    await page.waitForTimeout(200);
    return fullTweet;
  } catch (err) {
    console.warn(`Row ${rowIndex}: Error:`, err.message);
    return '';
  }
};

const extractAllCurrentlyLoadedRows = async (page) => {
  const rowsData = await page.evaluate(() => {
    const clean = (s) => (s || '').replace(/\s+/g, ' ').trim();
    const groups = Array.from(document.querySelectorAll('.rt-tbody .rt-tr-group'));

    const extractTickersFromCell = (cell) => {
      if (!cell) return [];

      // Preferred: symbols are in <a href="/stocks/xxx"><span>SYM</span></a> and /etf/xxx
      const anchors = Array.from(cell.querySelectorAll('a[href^="/stocks/"], a[href^="/etf/"]'));
      const syms = anchors
        .map((a) => clean(a.textContent))
        .filter(Boolean);

      if (syms.length > 0) return syms;

      // Fallback: try to parse patterns like "NWSA-1.45%NXST-2.11%" -> ["NWSA","NXST"]
      const txt = clean(cell.textContent);
      const matches = txt.match(/[A-Z]{1,6}(?=[\-\s]|$)/g) || [];
      return matches;
    };

    return groups.map((group) => {
      const row = group.querySelector('.rt-tr');
      const cells = row ? Array.from(row.querySelectorAll('.rt-td')) : [];

      let time = '';
      if (cells[0]) {
        const spans = cells[0].querySelectorAll('span');
        const t = spans[0] ? clean(spans[0].textContent) : '';
        const d = spans[1] ? clean(spans[1].textContent) : '';
        time = clean([t, d].filter(Boolean).join(' '));
      }

      const sentiment = cells[1] ? clean(cells[1].textContent) : '';

      let summary = '';
      if (cells[3]) {
        const titled = cells[3].querySelector('[title]');
        summary = clean((titled && titled.getAttribute('title')) || cells[3].textContent);
      }

      const tickers = extractTickersFromCell(cells[4]);

      const sector = cells[5] ? clean(cells[5].textContent).replace(/^—+$/, '—') : '—';

      return { time, sentiment, summary, tickers, sector };
    });
  });

  // Now extract full tweet text for each row using programmatic hover
  console.log(`Extracting full tweets for ${rowsData.length} rows...`);
  
  // Scroll back to the top of the table to ensure first rows are visible
  const firstRow = page.locator('.rt-tbody .rt-tr-group').first();
  await firstRow.scrollIntoViewIfNeeded({ timeout: 3000 }).catch(() => {});
  await page.waitForTimeout(500);
  
  for (let i = 0; i < rowsData.length; i++) {
    await closeTipRanksPopup(page);
    
    // Set a timeout for each row extraction to prevent hanging
    try {
      const fullTweet = await Promise.race([
        extractFullTweetFromTooltip(page, i),
        new Promise((_, reject) => 
          setTimeout(() => reject(new Error('Timeout')), 10000)
        )
      ]);
      rowsData[i].fullTweet = fullTweet;
    } catch (err) {
      console.warn(`Row ${i}: Timed out or failed, skipping full tweet`);
      rowsData[i].fullTweet = '';
    }
    
    // Small delay between rows to avoid overwhelming the page
    if (i < rowsData.length - 1) {
      await page.waitForTimeout(250);
    }
  }
  
  console.log('Full tweet extraction complete');
  return rowsData;
};

const getRowCount = async (page) => {
  return page.locator('.rt-tbody .rt-tr-group').count();
};

const clickShowMoreUntil = async (page, targetCount) => {
  // click show more until enough rows loaded or button gone
  let prevCount = await getRowCount(page);
  for (let i = 0; i < 30; i++) {
    await closeTipRanksPopup(page);

    const btn = page.locator('button[data-id="show_more"]').first();
    const visible = await btn.isVisible().catch(() => false);
    if (!visible) break;

    // ensure it is on screen and clickable
    await btn.scrollIntoViewIfNeeded().catch(() => {});
    await page.waitForTimeout(200);
    await closeTipRanksPopup(page);

    // click and wait for row count to increase
    await btn.click({ timeout: 3000 }).catch(() => {});
    await page.waitForTimeout(300);

    let grew = false;
    for (let w = 0; w < 20; w++) {
      await page.waitForTimeout(250);
      await closeTipRanksPopup(page);
      const now = await getRowCount(page);
      if (now > prevCount) {
        prevCount = now;
        grew = true;
        break;
      }
    }

    const nowCount = await getRowCount(
