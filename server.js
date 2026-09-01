"use strict";

const express = require("express");
const cors = require("cors");
const http = require("http");
const { WebSocket, WebSocketServer } = require("ws");

const PORT = process.env.PORT || 8787;
const FINNHUB_API_KEY = process.env.FINNHUB_API_KEY;
const ALLOWED_ORIGIN = process.env.ALLOWED_ORIGIN || "*";

const SYMBOL = "GPRO";
const STOCKTWITS_URL = "https://stocktwits.com/symbol/GPRO";

if (!FINNHUB_API_KEY) {
  console.error("Missing FINNHUB_API_KEY");
  process.exit(1);
}

const app = express();
app.use(cors({ origin: ALLOWED_ORIGIN === "*" ? true : ALLOWED_ORIGIN }));

const server = http.createServer(app);
const wss = new WebSocketServer({ server, path: "/ws" });

let price = null;
let marketTs = null;
let observedTs = null;
let source = "starting";

let usdToGbp = null;
let fxUpdatedAt = null;

let finnhubState = "connecting";
let stocktwitsState = "idle";
let stocktwitsUpdated = null;
let stocktwitsLastSuccess = null;
let lastStocktwitsAttempt = 0;
let lastError = null;

function send(client, data) {
  if (client.readyState === WebSocket.OPEN) {
    client.send(JSON.stringify(data));
  }
}

function broadcast(data) {
  for (const client of wss.clients) send(client, data);
}

function snapshot(type = "snapshot") {
  return {
    type,
    symbol: SYMBOL,
    price,
    timestamp: marketTs,
    marketTimestamp: marketTs,
    observedTimestamp: observedTs,
    serverTime: Date.now(),
    usdToGbp,
    fxUpdatedAt,
    source,
    finnhubState,
    stocktwitsState,
    stocktwitsUpdated,
    stocktwitsLastSuccess
  };
}

function setPrice(nextPrice, nextTs, nextSource, observed = Date.now()) {
  const p = Number(nextPrice);
  const ts = Number(nextTs) || observed;

  if (!(p > 0)) return false;

  // Do not overwrite a newer validated price with older data.
  if (marketTs && ts < marketTs) return false;

  // Scraper safety check.
  if (price && (p < price * 0.20 || p > price * 5)) {
    console.warn(`Rejected suspicious ${nextSource} price ${p}`);
    return false;
  }

  price = p;
  marketTs = ts;
  observedTs = observed;
  source = nextSource;
  lastError = null;

  broadcast(snapshot("price"));
  return true;
}

function stripHtml(html) {
  return html
    .replace(/<script\b[^>]*>[\s\S]*?<\/script>/gi, " ")
    .replace(/<style\b[^>]*>[\s\S]*?<\/style>/gi, " ")
    .replace(/<noscript\b[^>]*>[\s\S]*?<\/noscript>/gi, " ")
    .replace(/&nbsp;/gi, " ")
    .replace(/&middot;|&#183;/gi, "·")
    .replace(/&amp;/gi, "&")
    .replace(/<[^>]+>/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function parseUpdatedTime(text) {
  const m = text.match(/Updated\s+(\d{1,2}:\d{2})\s*(AM|PM)\s*(EDT|EST)/i);
  if (!m) return null;

  const now = new Date();
  const dateParts = new Intl.DateTimeFormat("en-US", {
    timeZone: "America/New_York",
    year: "numeric",
    month: "2-digit",
    day: "2-digit"
  }).formatToParts(now);

  const parts = {};
  for (const p of dateParts) {
    if (p.type !== "literal") parts[p.type] = p.value;
  }

  const parsed = Date.parse(
    `${parts.month}/${parts.day}/${parts.year} ${m[1]} ${m[2]} ${m[3]}`
  );

  if (!Number.isFinite(parsed)) return null;

  return {
    timestamp: parsed,
    label: `${m[1]} ${m[2].toUpperCase()} ${m[3].toUpperCase()}`
  };
}

/* Browser connection */
wss.on("connection", (client, req) => {
  const origin = req.headers.origin;

  if (ALLOWED_ORIGIN !== "*" && origin && origin !== ALLOWED_ORIGIN) {
    client.close(1008, "Origin not allowed");
    return;
  }

  console.log("Journal connected to live feed");

  client.isAlive = true;
  client.on("pong", () => { client.isAlive = true; });

  send(client, snapshot());
});

/* Keep browser connection open */
setInterval(() => {
  for (const client of wss.clients) {
    if (client.isAlive === false) {
      client.terminate();
      continue;
    }
    client.isAlive = false;
    client.ping();
  }
}, 30000);

/* Visible journal refresh every second */
setInterval(() => {
  broadcast(snapshot("heartbeat"));
}, 1000);

async function refreshFx() {
  try {
    const r = await fetch(
      "https://api.frankfurter.app/latest?from=USD&to=GBP",
      { cache: "no-store" }
    );

    if (!r.ok) throw new Error(`FX HTTP ${r.status}`);

    const d = await r.json();
    const rate = Number(d?.rates?.GBP);

    if (rate > 0) {
      usdToGbp = rate;
      fxUpdatedAt = Date.now();
      broadcast({
        type: "fx",
        usdToGbp,
        fxUpdatedAt,
        serverTime: Date.now()
      });
    }
  } catch (err) {
    console.error("FX:", err.message);
  }
}

/* Finnhub REST backup */
async function refreshFinnhub() {
  try {
    const r = await fetch(
      `https://finnhub.io/api/v1/quote?symbol=${SYMBOL}&token=${encodeURIComponent(FINNHUB_API_KEY)}`,
      { cache: "no-store" }
    );

    if (!r.ok) throw new Error(`Finnhub HTTP ${r.status}`);

    const d = await r.json();
    const p = Number(d.c);

    if (!(p > 0)) throw new Error("No usable Finnhub price");

    const ts = Number(d.t) > 0 ? Number(d.t) * 1000 : Date.now();
    setPrice(p, ts, "Finnhub quote");
  } catch (err) {
    lastError = err.message;
    console.error("Finnhub REST:", err.message);
  }
}

/*
  Free overnight fallback.

  Every manual page refresh calls /api/gpro?refresh=1,
  which performs this check before returning the price.

  It only accepts a value if:
  1. Page clearly says GPRO + GoPro Inc
  2. Page contains an Overnight session label
  3. It can identify the displayed current price and regular close
  4. Price passes sanity checks
*/
async function refreshStocktwits(force = false) {
  const now = Date.now();

  // Avoid hammering the public page if refresh is repeatedly clicked.
  if (now - lastStocktwitsAttempt < 5000) return false;

  lastStocktwitsAttempt = now;
  stocktwitsState = "checking";

  try {
    const r = await fetch(STOCKTWITS_URL, {
      cache: "no-store",
      redirect: "follow",
      headers: {
        "User-Agent": "Mozilla/5.0 (compatible; GoProJournal/1.0)",
        "Accept": "text/html,application/xhtml+xml",
        "Accept-Language": "en-US,en;q=0.9"
      }
    });

    if (!r.ok) throw new Error(`Stocktwits HTTP ${r.status}`);

    const html = await r.text();
    const text = stripHtml(html);

    const identity =
      /\bGPRO\b/i.test(text) &&
      /\bGoPro(?:,\s*Inc\.?| Inc\.?)\b/i.test(text);

    if (!identity) {
      throw new Error("GPRO / GoPro identity validation failed");
    }

    const match = text.match(
      /GPRO\s+GoPro(?:,\s*Inc\.?| Inc\.?)\s+\$([0-9]+(?:\.[0-9]+)?)[\s\S]{0,350}?Today[\s\S]{0,250}?Closed\s+\$([0-9]+(?:\.[0-9]+)?)[\s\S]{0,250}?Overnight/i
    );

    if (!match) {
      stocktwitsState = "no-overnight-price";
      return false;
    }

    const overnightPrice = Number(match[1]);
    const regularClose = Number(match[2]);

    if (!(overnightPrice > 0) || !(regularClose > 0)) {
      throw new Error("Invalid overnight values");
    }

    const ratio = overnightPrice / regularClose;
    if (ratio < 0.20 || ratio > 5) {
      throw new Error("Overnight price failed sanity validation");
    }

    const updated = parseUpdatedTime(text);
    const ts = updated ? updated.timestamp : now;

    const accepted = setPrice(
      overnightPrice,
      ts,
      "Stocktwits overnight",
      now
    );

    if (accepted) {
      stocktwitsState = "overnight-found";
      stocktwitsLastSuccess = now;
      stocktwitsUpdated = updated?.label || null;

      console.log(
        `Stocktwits GPRO overnight: $${overnightPrice}` +
        (stocktwitsUpdated ? ` · ${stocktwitsUpdated}` : "")
      );
    } else {
      stocktwitsState = "overnight-stale";
    }

    return accepted;
  } catch (err) {
    stocktwitsState = "error";
    console.error("Stocktwits:", err.message);
    return false;
  }
}

/* Finnhub WebSocket */
let finnhubSocket;
let reconnectTimer;
let reconnectDelay = 3000;

function connectFinnhub() {
  clearTimeout(reconnectTimer);
  finnhubState = "connecting";

  finnhubSocket = new WebSocket(
    `wss://ws.finnhub.io?token=${encodeURIComponent(FINNHUB_API_KEY)}`
  );

  finnhubSocket.on("open", () => {
    finnhubState = "connected";
    reconnectDelay = 3000;

    finnhubSocket.send(JSON.stringify({
      type: "subscribe",
      symbol: SYMBOL
    }));

    console.log("Finnhub connected; subscribed to GPRO");
    broadcast(snapshot("status"));
  });

  finnhubSocket.on("message", raw => {
    try {
      const msg = JSON.parse(raw.toString());

      if (msg.type !== "trade" || !Array.isArray(msg.data)) return;

      const valid = msg.data
        .filter(t => t.s === SYMBOL && Number(t.p) > 0)
        .sort((a, b) => Number(a.t || 0) - Number(b.t || 0));

      if (!valid.length) return;

      const t = valid[valid.length - 1];
      setPrice(Number(t.p), Number(t.t) || Date.now(), "Finnhub trade");
    } catch (err) {
      console.error("Finnhub WS message:", err.message);
    }
  });

  finnhubSocket.on("close", () => {
    finnhubState = "disconnected";
    reconnectTimer = setTimeout(connectFinnhub, reconnectDelay);
    reconnectDelay = Math.min(reconnectDelay * 2, 30000);
  });

  finnhubSocket.on("error", err => {
    finnhubState = "error";
    lastError = err.message;
    try { finnhubSocket.close(); } catch {}
  });
}

/* API */
app.get("/", (req, res) => {
  res.json({
    ok: true,
    service: "GPRO live journal feed",
    liveSource: "Finnhub",
    overnightRefresh: "Stocktwits public GPRO page",
    websocket: "/ws",
    priceApi: "/api/gpro",
    health: "/health"
  });
});

app.get("/api/gpro", async (req, res) => {
  res.set("Cache-Control", "no-store");

  if (req.query.refresh === "1") {
    await Promise.allSettled([
      refreshStocktwits(true),
      refreshFinnhub()
    ]);
  }

  res.json({
    ...snapshot(),
    error: lastError
  });
});

app.get("/health", (req, res) => {
  res.json({
    ok: true,
    symbol: SYMBOL,
    browserClients: wss.clients.size,
    hasPrice: price !== null,
    latestPrice: price,
    latestMarketTimestamp: marketTs,
    latestObservedTimestamp: observedTs,
    latestSource: source,
    finnhub: finnhubState,
    stocktwits: stocktwitsState,
    stocktwitsUpdated,
    stocktwitsLastSuccess,
    hasFx: usdToGbp !== null,
    usdToGbp,
    serverTime: Date.now(),
    error: lastError
  });
});

server.listen(PORT, "0.0.0.0", async () => {
  console.log(`GPRO live service listening on port ${PORT}`);

  await Promise.allSettled([
    refreshFx(),
    refreshFinnhub(),
    refreshStocktwits(true)
  ]);

  connectFinnhub();

  // Safety checks while page stays open.
  setInterval(refreshFinnhub, 15000);
  setInterval(() => refreshStocktwits(false), 30000);
  setInterval(refreshFx, 60 * 60 * 1000);
});
