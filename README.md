# Trump News Discord Bot

This Node.js bot pulls the latest news items from the TipRanks Trump Dashboard and posts unread items to a Discord channel via webhook.

## Setup

1. Install dependencies:
   ```bash
   npm install
   ```

2. Copy the example environment file and update the values:
   ```bash
   cp .env.example .env
   ```

## Configuration

The bot reads all runtime configuration from `.env`:

- `SITE_URL`: TipRanks dashboard URL.
- `DISCORD_WEBHOOK_URL`: Discord webhook URL.
- `MAX_NEWS_MESSAGES`: Maximum number of news messages to read (e.g., 15).
- `DISCORD_TAG`: Tag to prepend to each Discord message.
- `LOG_FILE`: Path to the log file where processed items are stored.
- `HEADLESS`: Set to `false` to run Playwright with a visible browser (helps with Cloudflare challenges).
- `NAVIGATION_TIMEOUT_MS`: Timeout for loading the page (milliseconds).
- `PAGE_WAIT_TIMEOUT_MS`: Timeout for waiting on the news table (milliseconds).
- `USER_AGENT`: Optional custom user agent string.

## Run

```bash
npm start
```

## Notes

- The bot uses Playwright to load the dashboard.
- If you receive a 403 response or a Cloudflare challenge, try setting `HEADLESS=false` and provide a full Chrome user-agent string.
- Processed items are stored in the JSON log file so they are not re-posted on subsequent runs.
