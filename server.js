
"use strict";

const express = require("express");
const cors = require("cors");
const http = require("http");
const { WebSocket, WebSocketServer } = require("ws");

const PORT = process.env.PORT || 8787;
const FINNHUB_API_KEY = process.env.FINNHUB_API_KEY;
const ALLOWED_ORIGIN = process.env.ALLOWED_ORIGIN || "*";

if (!FINNHUB_API_KEY) {
  console.error("Missing FINNHUB_API_KEY environment variable.");
  process.exit(1);
}

const app = express();
app.use(cors({ origin: ALLOWED_ORIGIN === "*" ? true : ALLOWED_ORIGIN }));
const server = http.createServer(app);

const wss = new WebSocketServer({
  server,
  path: "/ws"
});

wss.on("connection", (client, req) => {
  const origin = req.headers.origin;

  if (
    ALLOWED_ORIGIN !== "*" &&
    origin !== ALLOWED_ORIGIN
  ) {
    client.close();
    return;
  }

  console.log("Journal connected to live feed");

  // Immediately give the page our latest known data
  client.send(JSON.stringify({
    type: "snapshot",
    symbol: "GPRO",
    price: latestPrice,
    timestamp: latestTs,
    usdToGbp
  }));
});

function broadcast(data) {
  const message = JSON.stringify(data);

  wss.clients.forEach(client => {
    if (client.readyState === WebSocket.OPEN) {
      client.send(message);
    }
  });
}

let latestPrice = null;
let latestTs = null;
let wsState = "connecting";
let usdToGbp = null;
let fxUpdatedAt = null;

async function refreshFx() {
  try {
    const r = await fetch("https://api.frankfurter.app/latest?from=USD&to=GBP");
    if (!r.ok) throw new Error(`FX HTTP ${r.status}`);
    const d = await r.json();
    if (d && d.rates && Number(d.rates.GBP) > 0) {
      usdToGbp = Number(d.rates.GBP);
      fxUpdatedAt = Date.now();
    }
  } catch (err) {
    console.error("FX refresh failed:", err.message);
  }
}
refreshFx();
setInterval(refreshFx, 60 * 60 * 1000);

let socket;
let reconnectTimer;

function connectFinnhub() {
  clearTimeout(reconnectTimer);
  wsState = "connecting";

  socket = new WebSocket(`wss://ws.finnhub.io?token=${encodeURIComponent(FINNHUB_API_KEY)}`);

  socket.on("open", () => {
    wsState = "connected";
    socket.send(JSON.stringify({ type: "subscribe", symbol: "GPRO" }));
    console.log("Finnhub connected; subscribed to GPRO");
  });

  socket.on("message", raw => {
    try {
      const msg = JSON.parse(raw.toString());
      if (msg.type === "trade" && Array.isArray(msg.data)) {
        const trades = msg.data.filter(t => t.s === "GPRO" && Number(t.p) > 0);
       if (trades.length) {
  const t = trades[trades.length - 1];

  latestPrice = Number(t.p);
  latestTs = Number(t.t) || Date.now();

  broadcast({
    type: "trade",
    symbol: "GPRO",
    price: latestPrice,
    timestamp: latestTs,
    usdToGbp
  });
}
        }
      }
    } catch (err) {
      console.error("Bad websocket message:", err.message);
    }
  });

  socket.on("close", () => {
    wsState = "disconnected";
    reconnectTimer = setTimeout(connectFinnhub, 3000);
  });

  socket.on("error", err => {
    wsState = "error";
    console.error("Finnhub websocket error:", err.message);
    try { socket.close(); } catch {}
  });
}

connectFinnhub();

app.get("/api/gpro", (req, res) => {
  res.set("Cache-Control", "no-store");
  res.json({
    symbol: "GPRO",
    price: latestPrice,
    timestamp: latestTs,
    usdToGbp,
    fxUpdatedAt,
    feed: "Finnhub WebSocket",
    marketStatus: wsState === "connected" ? "Connected · waiting for trade" : wsState
  });
});

app.get("/health", (req, res) => {
  res.json({ ok: true, websocket: wsState, hasPrice: latestPrice !== null, hasFx: usdToGbp !== null });
});

server.listen(PORT, "0.0.0.0", () => {
  console.log(`GPRO live service listening on port ${PORT}`);
});
