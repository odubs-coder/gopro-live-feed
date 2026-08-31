# GoPro Live Investment Tracker

This package contains:

- `gopro_investment_journal_live.html` — your journal UI.
- `server.js` — a small Node server that connects to Finnhub's GPRO WebSocket feed.
- `package.json` — dependencies.
- `.env.example` — required environment variables.

## How it works

Finnhub WebSocket -> live Node service -> `/api/gpro` -> journal refreshes every second.

Your Finnhub API key stays on the server and is never placed in the public HTML.

The server also refreshes USD -> GBP using the public Frankfurter exchange-rate API once per hour. The journal uses GBP as its main reporting currency.

## Local test

1. Create a Finnhub account and API key.
2. In this folder run:
   `npm install`
3. Set environment variables:
   - macOS/Linux:
     `export FINNHUB_API_KEY="YOUR_KEY"`
   - Windows PowerShell:
     `$env:FINNHUB_API_KEY="YOUR_KEY"`
4. Start:
   `npm start`
5. Open `gopro_investment_journal_live.html`.

By default the page connects to:
`http://localhost:8787`

## Deploy

Deploy this folder as a Node web service on any host that supports persistent WebSocket connections.

Set:
- `FINNHUB_API_KEY` to your private key.
- `ALLOWED_ORIGIN` to `https://odubs-coder.github.io`

After deployment, open the journal once with:

`gopro_investment_journal_live.html?api=https://YOUR-LIVE-SERVER`

The page stores that server address in localStorage, so later visits can use the normal page URL.

## Important

The page updates once per second, but the displayed price only changes when the market-data provider receives a new GPRO trade. Outside market hours, the last traded price remains on screen.
