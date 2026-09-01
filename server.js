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

app.use(
  cors({
    origin: ALLOWED_ORIGIN === "*" ? true : ALLOWED_ORIGIN
  })
);

const server = http.createServer(app);

const wss = new WebSocketServer({
  server,
  path: "/ws"
});

let latestPrice = null;
let latestTs = null;
let wsState = "connecting";

let usdToGbp = null;
let fxUpdatedAt = null;

/* --------------------------------
   Browser WebSocket helpers
-------------------------------- */

function sendJson(client, data) {
  if (client.readyState === WebSocket.OPEN) {
    client.send(JSON.stringify(data));
  }
}

function broadcast(data) {
  for (const client of wss.clients) {
    sendJson(client, data);
  }
}

/* --------------------------------
   Browser connections
-------------------------------- */

wss.on("connection", (client, req) => {
  const origin = req.headers.origin;

  if (
    ALLOWED_ORIGIN !== "*" &&
    origin !== ALLOWED_ORIGIN
  ) {
    console.warn(`Rejected WebSocket origin: ${origin}`);

    client.close(
      1008,
      "Origin not allowed"
    );

    return;
  }

  console.log(
    "Journal connected to live feed"
  );

  client.isAlive = true;

  client.on("pong", () => {
    client.isAlive = true;
  });

  // Send the latest known information
  // immediately when the page connects.
  sendJson(client, {
    type: "snapshot",
    symbol: "GPRO",
    price: latestPrice,
    timestamp: latestTs,
    usdToGbp,
    fxUpdatedAt,
    feedState: wsState
  });
});

/* --------------------------------
   Keep browser WebSockets alive
-------------------------------- */

const heartbeat = setInterval(() => {
  for (const client of wss.clients) {
    if (client.isAlive === false) {
      client.terminate();
      continue;
    }

    client.isAlive = false;
    client.ping();
  }
}, 30000);

wss.on("close", () => {
  clearInterval(heartbeat);
});

/* --------------------------------
   USD → GBP
-------------------------------- */

async function refreshFx() {
  try {
    const response = await fetch(
      "https://api.frankfurter.app/latest?from=USD&to=GBP"
    );

    if (!response.ok) {
      throw new Error(
        `FX HTTP ${response.status}`
      );
    }

    const data = await response.json();

    const nextRate =
      Number(data?.rates?.GBP);

    if (nextRate > 0) {
      usdToGbp = nextRate;

      fxUpdatedAt =
        Date.now();

      broadcast({
        type: "fx",
        usdToGbp,
        fxUpdatedAt
      });
    }

  } catch (err) {

    console.error(
      "FX refresh failed:",
      err.message
    );

  }
}

refreshFx();

setInterval(
  refreshFx,
  60 * 60 * 1000
);

/* --------------------------------
   Finnhub WebSocket
-------------------------------- */

let socket = null;

let reconnectTimer = null;
async function getInitialGproPrice() {
  try {
    const response = await fetch(
      `https://finnhub.io/api/v1/quote?symbol=GPRO&token=${encodeURIComponent(
        FINNHUB_API_KEY
      )}`
    );

    if (!response.ok) {
      throw new Error(`Quote HTTP ${response.status}`);
    }

    const data = await response.json();

    if (Number(data.c) > 0) {
      latestPrice = Number(data.c);

      // Finnhub's quote timestamp is seconds.
      latestTs = Number(data.t)
        ? Number(data.t) * 1000
        : Date.now();

      console.log(
        `Initial GPRO price loaded: $${latestPrice}`
      );

      broadcast({
        type: "trade",
        symbol: "GPRO",
        price: latestPrice,
        timestamp: latestTs,
        usdToGbp
      });
    }
  } catch (err) {
    console.error(
      "Initial GPRO quote failed:",
      err.message
    );
  }
}
function getInitialGproPrice();
connectFinnhub();

  clearTimeout(reconnectTimer);

  wsState =
    "connecting";

  socket =
    new WebSocket(

      `wss://ws.finnhub.io?token=${encodeURIComponent(
        FINNHUB_API_KEY
      )}`

    );

  socket.on(
    "open",
    () => {

      wsState =
        "connected";

      socket.send(
        JSON.stringify({
          type: "subscribe",
          symbol: "GPRO"
        })
      );

      console.log(
        "Finnhub connected; subscribed to GPRO"
      );

    }
  );

  socket.on(
    "message",
    raw => {

      try {

        const msg =
          JSON.parse(
            raw.toString()
          );

        if (
          msg.type === "trade" &&
          Array.isArray(msg.data)
        ) {

          const trades =
            msg.data.filter(
              t =>
                t.s === "GPRO" &&
                Number(t.p) > 0
            );

          if (trades.length) {

            const t =
              trades[
                trades.length - 1
              ];

            latestPrice =
              Number(t.p);

            latestTs =
              Number(t.t) ||
              Date.now();

            // Immediately send the new
            // GPRO trade to every journal
            // currently connected.
            broadcast({
              type: "trade",
              symbol: "GPRO",
              price: latestPrice,
              timestamp: latestTs,
              usdToGbp
            });

          }

        }

      } catch (err) {

        console.error(
          "Bad Finnhub websocket message:",
          err.message
        );

      }

    }
  );

  socket.on(
    "close",
    () => {

      wsState =
        "disconnected";

      console.log(
        "Finnhub disconnected; reconnecting in 3 seconds..."
      );

      reconnectTimer =
        setTimeout(
          connectFinnhub,
          3000
        );

    }
  );

  socket.on(
    "error",
    err => {

      wsState =
        "error";

      console.error(
        "Finnhub websocket error:",
        err.message
      );

      try {
        socket.close();
      } catch {}

    }
  );

}

connectFinnhub();

/* --------------------------------
   HTTP endpoints
-------------------------------- */

app.get(
  "/",
  (req, res) => {

    res.json({
      ok: true,
      service: "GPRO live feed",
      websocketPath: "/ws",
      apiPath: "/api/gpro"
    });

  }
);

app.get(
  "/api/gpro",
  (req, res) => {

    res.set(
      "Cache-Control",
      "no-store"
    );

    res.json({
      symbol: "GPRO",
      price: latestPrice,
      timestamp: latestTs,
      usdToGbp,
      fxUpdatedAt,
      feed:
        "Finnhub WebSocket",
      marketStatus:
        wsState === "connected"
          ? "Connected · waiting for trade"
          : wsState
    });

  }
);

app.get(
  "/health",
  (req, res) => {

    res.json({
      ok: true,

      finnhubWebsocket:
        wsState,

      browserClients:
        wss.clients.size,

      hasPrice:
        latestPrice !== null,

      hasFx:
        usdToGbp !== null
    });

  }
);

/* --------------------------------
   Start server
-------------------------------- */

server.listen(
  PORT,
  "0.0.0.0",
  () => {

    console.log(
      `GPRO live service listening on port ${PORT}`
    );

  }
);
