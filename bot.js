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

    const maxAttempts = geminiApiKeys.length * 3;
    
    if (attemptCount >= maxAttempts) {
      throw new Error(`Translation failed after ${maxAttempts} attempts across all ${geminiApiKeys.length} API key(s)`);
    }

    try {
      const keyIndex = findAvailableKey();
      const model = geminiClients[keyIndex].getGenerativeModel({ model: geminiModelName });
      
      console.log(`\n📡 Gemini API Call #${attemptCount + 1}`);
      console.log(`   Using API key: #${keyIndex + 1}/${geminiApiKeys.length}`);
      console.log(`   Model: ${geminiModelName}`);
      console.log(`   Translating ${chunk.length} item(s) to ${translateTo}`);
      console.log(`   Input preview: "${chunk[0]?.substring(0, 60)}..."`);
      
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
      
      console.log(`   ✅ Translation successful`);
      console.log(`   Output preview: "${translations[0]?.substring(0, 60)}..."\n`);
      
      return translations.map((t) => String(t ?? '').trim());
    } catch (err) {
      console.log(`   ❌ Translation failed: ${err.message}\n`);
      
      if (isGeminiRateLimitError(err)) {
        const failedKeyIndex = (currentKeyIndex - 1 + geminiApiKeys.length) % geminiApiKeys.length;
        
        if (!isKeyRateLimited(failedKeyIndex)) {
          markKeyAsRateLimited(failedKeyIndex);
        }
        
        const allLimited = geminiApiKeys.every((_, idx) => isKeyRateLimited(idx));
        if (allLimited) {
          console.warn(`⚠️  All ${geminiApiKeys.length} API key(s) are rate-limited. Add more keys or wait for rate limits to reset.`);
        }
        
        console.log(`   🔄 Switching to next API key...`);
        await sleep(1000);
        return translateChunk(chunk, attemptCount + 1);
      }
      
      if (isGeminiServiceError(err)) {
        console.warn(`   ⚠️  Gemini service error (${err.status || 'unknown'}): ${err.message}`);
        throw err;
      }
      
      throw err;
    }
  };

  try {
    console.log(`\n🌐 Starting Gemini translation process`);
    console.log(`   Total items: ${summaries.length}`);
    console.log(`   Batch size: ${geminiBatchSize}`);
    console.log(`   Batches: ${Math.ceil(summaries.length / geminiBatchSize)}`);
    
    const out = [];
    for (let i = 0; i < summaries.length; i += geminiBatchSize) {
      const chunk = summaries.slice(i, i + geminiBatchSize);
      console.log(`\n📦 Processing batch ${Math.floor(i / geminiBatchSize) + 1}/${Math.ceil(summaries.length / geminiBatchSize)}`);
      
      const translated = await translateChunk(chunk);
      out.push(...translated);
      
      if (i + geminiBatchSize < summaries.length && geminiMinDelayMs > 0) {
        console.log(`   ⏱️  Waiting ${geminiMinDelayMs}ms before next batch...`);
        await sleep(geminiMinDelayMs);
      }
    }
    
    console.log(`\n✅ Translation complete: ${out.length}/${summaries.length} items translated\n`);
    return out;
  } catch (err) {
    console.error(`\n❌ Translation failed: ${err.message}`);
    console.warn('⚠️  Falling back to untranslated text\n');
    return null;
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

const buildPayloadIndex = (payloadItems) => {
  const clean = (text) => String(text || '').replace(/\s+/g, ' ').trim();
  const timeIndex = new Map(); // Map: time -> array of items
  const summaryIndex = new Map(); // Map: summary -> item

  for (const item of payloadItems || []) {
    const time = clean(item?.postTime);
    const summary = clean(item?.postSummary);
    if (!time || !summary) continue;
    
    // Index by time (multiple items can have same time)
    if (!timeIndex.has(time)) {
      timeIndex.set(time, []);
    }
    timeIndex.get(time).push(item);
    
    // Index by summary (should be unique)
    summaryIndex.set(summary, item);
  }

  return { timeIndex, summaryIndex, clean };
};

const findPayloadMatch = (row, { timeIndex, summaryIndex, clean }) => {
  const rowTime = clean(row?.time);
  const rowSummary = clean(row?.summary);
  if (!rowTime || !rowSummary) return null;

  // Strategy 1: Exact summary match (most reliable)
  if (summaryIndex.has(rowSummary)) {
    return summaryIndex.get(rowSummary);
  }

  // Strategy 2: Fuzzy summary match
  // Sometimes the UI truncates summaries, so check if row summary is a prefix
  for (const [fullSummary, item] of summaryIndex.entries()) {
    if (fullSummary.startsWith(rowSummary) || rowSummary.startsWith(fullSummary)) {
      const itemTime = clean(item?.postTime);
      // Also verify time matches to avoid false positives
      if (itemTime === rowTime) {
        return item;
      }
    }
  }

  // Strategy 3: Match by time only (when there's only one item at that time)
  const itemsAtTime = timeIndex.get(rowTime);
  if (itemsAtTime && itemsAtTime.length === 1) {
    return itemsAtTime[0];
  }

  // Strategy 4: Match by time with fuzzy summary comparison
  if (itemsAtTime && itemsAtTime.length > 1) {
    // Find the item with most similar summary
    let bestMatch = null;
    let bestScore = 0;
    
    for (const item of itemsAtTime) {
      const itemSummary = clean(item?.postSummary);
      if (!itemSummary) continue;
      
      // Simple similarity: count matching words
      const rowWords = new Set(rowSummary.toLowerCase().split(/\s+/));
      const itemWords = itemSummary.toLowerCase().split(/\s+/);
      const matchingWords = itemWords.filter(w => rowWords.has(w)).length;
      const score = matchingWords / Math.max(rowWords.size, itemWords.length);
      
      if (score > bestScore) {
        bestScore = score;
        bestMatch = item;
      }
    }
    
    // If we have a reasonable match (>50% word overlap), use it
    if (bestMatch && bestScore > 0.5) {
      return bestMatch;
    }
  }

  return null;
};

const extractAllCurrentlyLoadedRows = async (page, payloadItems = []) => {
  const rowsData = await page.evaluate(() => {
    const clean = (s) => (s || '').replace(/\s+/g, ' ').trim();
    const groups = Array.from(document.querySelectorAll('.rt-tbody .rt-tr-group'));

    const extractTickersFromCell = (cell) => {
      if (!cell) return [];

      const anchors = Array.from(cell.querySelectorAll('a[href^="/stocks/"], a[href^="/etf/"]'));
      const syms = anchors
        .map((a) => clean(a.textContent))
        .filter(Boolean);

      if (syms.length > 0) return syms;

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

  console.log(`Extracting full tweets for ${rowsData.length} rows from payload...`);
  
  if (rowsData.length > 0) {
    console.log(`\n📊 Sample row from page:`);
    console.log(`   Time: "${rowsData[0].time}"`);
    console.log(`   Sentiment: "${rowsData[0].sentiment}"`);
    console.log(`   Summary: "${rowsData[0].summary?.substring(0, 80)}..."`);
  }
  
  const indices = buildPayloadIndex(payloadItems);
  
  console.log(`\n📑 Payload index summary:`);
  console.log(`   Time index size: ${indices.timeIndex.size} unique times`);
  console.log(`   Summary index size: ${indices.summaryIndex.size} unique summaries`);
  if (indices.timeIndex.size > 0) {
    const firstTime = Array.from(indices.timeIndex.keys())[0];
    console.log(`   First indexed time: "${firstTime}"`);
    console.log(`   Items at that time: ${indices.timeIndex.get(firstTime).length}`);
  }
  
  let payloadMatchCount = 0;
  let noMatchCount = 0;

  for (let i = 0; i < rowsData.length; i++) {
    const payloadMatch = findPayloadMatch(rowsData[i], indices);
    
    if (payloadMatch?.postContent) {
      rowsData[i].fullTweet = indices.clean(payloadMatch.postContent);
      payloadMatchCount++;
      console.log(`Row ${i}: ✓ Matched (${rowsData[i].fullTweet.length} chars)`);
      console.log(`  Full tweet: "${rowsData[i].fullTweet}"`);
    } else {
      rowsData[i].fullTweet = '';
      noMatchCount++;
      console.log(`Row ${i}: ✗ No payload match found`);
      console.log(`  Time: "${rowsData[i].time}", Summary: "${rowsData[i].summary.substring(0, 50)}..."`);
    }
  }
  
  console.log(`Full tweet extraction complete:`);
  console.log(`  - Matched: ${payloadMatchCount}/${rowsData.length}`);
  console.log(`  - No match: ${noMatchCount}/${rowsData.length}`);
  
  if (noMatchCount > 0) {
    console.warn(`⚠️  ${noMatchCount} row(s) could not be matched to payload data`);
  }
  
  return rowsData;
};

const fetchTrumpDashboardPayload = async (page) => {
  console.log('\n📥 Fetching trump-dashboard payload.json...');
  console.log(`   URL: https://tr-cdn.tipranks.com/research/prod/trump-dashboard/payload.json`);
  
  try {
    const response = await page.request.get(
      'https://tr-cdn.tipranks.com/research/prod/trump-dashboard/payload.json',
      { timeout: navigationTimeoutMs }
    );

    console.log(`   Response status: ${response.status()}`);
    const headers = response.headers();
    console.log(`   Content-Type: ${headers['content-type'] || 'not set'}`);
    console.log(`   Content-Length: ${headers['content-length'] || 'not set'}`);

    if (!response.ok()) {
      console.error(`   ❌ Payload request failed with HTTP ${response.status()}`);
      const bodyText = await response.text().catch(() => 'Could not read response body');
      console.error(`   Response body (first 500 chars): ${bodyText.substring(0, 500)}`);
      return [];
    }

    const bodyText = await response.text();
    console.log(`   ✅ Response received: ${bodyText.length} characters`);
    console.log(`   Response preview (first 400 chars):\n${bodyText.substring(0, 400)}\n`);

    let data;
    try {
      data = JSON.parse(bodyText);
    } catch (parseErr) {
      console.error(`   ❌ Failed to parse JSON:`, parseErr.message);
      console.error(`   Raw body: ${bodyText.substring(0, 200)}`);
      return [];
    }

    console.log(`   Parsed JSON successfully`);
    console.log(`   JSON root keys:`, Object.keys(data));
    
    const items = Array.isArray(data?.trumpDashboardList) ? data.trumpDashboardList : [];
    console.log(`   trumpDashboardList found: ${items.length} items`);
    
    if (items.length === 0) {
      console.warn(`   ⚠️  trumpDashboardList is empty or missing`);
      console.warn(`   Full JSON structure:`, JSON.stringify(data, null, 2).substring(0, 500));
    } else {
      console.log(`\n   Sample of first item:`);
      const first = items[0];
      console.log(`      postDate: "${first?.postDate}"`);
      console.log(`      postTime: "${first?.postTime}"`);
      console.log(`      postSummary: "${first?.postSummary?.substring(0, 80)}..."`);
      console.log(`      postContent: "${first?.postContent?.substring(0, 80)}..."`);
      console.log(`      sectors: "${first?.sectors}"`);
      console.log(`      stocks: ${first?.stocks?.length || 0} stock(s)`);
    }
    
    console.log(`\n   ✅ Successfully loaded ${items.length} payload item(s)\n`);
    return items;
  } catch (err) {
    console.error(`\n   ❌ Exception while fetching payload:`, err.message);
    console.error(`   Error type:`, err.constructor.name);
    console.error(`   Stack trace:`, err.stack);
    return [];
  }
};

const getRowCount = async (page) => {
  return page.locator('.rt-tbody .rt-tr-group').count();
};

const clickShowMoreUntil = async (page, targetCount) => {
  // click show more until enough rows loaded or button gone
  let prevCount = await getRowCount(page);
  for (let i = 0; i < 30; i++) {
    const btn = page.locator('button[data-id="show_more"]').first();
    const visible = await btn.isVisible().catch(() => false);
    if (!visible) break;

    // ensure it is on screen and clickable
    await btn.scrollIntoViewIfNeeded().catch(() => {});
    await page.waitForTimeout(200);

    // click and wait for row count to increase
    await btn.click({ timeout: 3000 }).catch(() => {});
    await page.waitForTimeout(300);

    let grew = false;
    for (let w = 0; w < 20; w++) {
      await page.waitForTimeout(250);
      const now = await getRowCount(page);
      if (now > prevCount) {
        prevCount = now;
        grew = true;
        break;
      }
    }

    const nowCount = await getRowCount(page);
    if (nowCount >= targetCount) break;

    // if it didn't grow, stop to avoid infinite loop
    if (!grew) break;
  }
};

const run = async () => {
  const processedIds = new Set(loadProcessedIds());

  const browser = await chromium.launch({
    headless,
    args: ['--disable-blink-features=AutomationControlled', '--headless=new']
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
    if (status && status >= 400) throw new Error(`Site responded with HTTP ${status}.`);

    await page.waitForLoadState('networkidle').catch(() => {});
    
    // Dismiss overlays
    for (let i = 0; i < 3; i++) {
      await dismissOverlaysStrict(page);
      await page.waitForTimeout(300);
    }

    await scrollToRecentTweets(page);
    await waitForReactTable(page);

    // Load enough rows by clicking Show More
    await clickShowMoreUntil(page, maxMessages);

    console.log('\n🔍 Attempting to fetch payload.json...');
    const payloadItems = await fetchTrumpDashboardPayload(page);
    console.log(`📦 Payload fetch complete: ${payloadItems.length} items returned\n`);
    
    const items = await extractAllCurrentlyLoadedRows(page, payloadItems);
    const sliced = items.slice(0, maxMessages);

    const newItems = [];
    for (const item of sliced) {
      const id = createMessageId(item);
      if (!processedIds.has(id)) newItems.push({ id, item });
    }

    // Send oldest-first, rotate webhooks per message in THIS run
    const ordered = newItems.reverse();

    // Translate summaries and full tweets separately
    if (ordered.length > 0) {
      console.log(`Attempting to translate content for ${ordered.length} items...`);
      
      // Translate summaries (all rows have summaries)
      const summaries = ordered.map((e) => e.item.summary);
      const translatedSummaries = await translateSummariesWithGemini(summaries);
      
      // Translate full tweets (only for rows that have them)
      const fullTweets = ordered.map((e) => e.item.fullTweet).filter(Boolean);
      let translatedFullTweets = null;
      
      if (fullTweets.length > 0) {
        console.log(`Attempting to translate ${fullTweets.length} full tweets...`);
        translatedFullTweets = await translateSummariesWithGemini(fullTweets);
      }
      
      // Apply translations ONLY if translation succeeded
      if (translatedSummaries !== null) {
        console.log('✓ Successfully translated summaries');
        for (let i = 0; i < ordered.length; i++) {
          ordered[i].item.summary = translatedSummaries[i] || ordered[i].item.summary;
        }
      } else {
        console.log('✗ Using original summaries (translation failed)');
      }
      
      if (translatedFullTweets !== null && fullTweets.length > 0) {
        console.log('✓ Successfully translated full tweets');
        let fullTweetIndex = 0;
        for (let i = 0; i < ordered.length; i++) {
          if (ordered[i].item.fullTweet) {
            ordered[i].item.fullTweet = translatedFullTweets[fullTweetIndex] || ordered[i].item.fullTweet;
            fullTweetIndex++;
          }
        }
      } else if (fullTweets.length > 0) {
        console.log('✗ Using original full tweets (translation failed)');
      }
    }

    for (let i = 0; i < ordered.length; i++) {
      const entry = ordered[i];
      const webhookUrl = webhookUrls[i % webhookUrls.length];
      
      // Format message returns an array of chunks if message is too long
      const messageChunks = formatMessage(entry.item);
      
      // Send all chunks for this message
      for (let chunkIndex = 0; chunkIndex < messageChunks.length; chunkIndex++) {
        const chunk = messageChunks[chunkIndex];
        
        // Add part indicator if message was split
        const chunkContent = messageChunks.length > 1 
          ? `**[Part ${chunkIndex + 1}/${messageChunks.length}]**\n${chunk}`
          : chunk;
        
        await sendDiscordMessage(webhookUrl, chunkContent);
        
        // Small delay between chunks of the same message
        if (chunkIndex < messageChunks.length - 1) {
          await sleep(300);
        }
      }
      
      processedIds.add(entry.id);
      
      // Delay between different messages
      if (i < ordered.length - 1) {
        await sleep(450);
      }
    }

    writeProcessedIds(Array.from(processedIds));
    console.log(`✓ Processed ${newItems.length} new item(s).`);
  } finally {
    await context.close();
    await browser.close();
  }
};

run().catch((error) => {
  console.error('Bot failed:', error);
  process.exit(1);
});