"use strict";

const express = require("express");
const cors = require("cors");
const http = require("http");
const { WebSocket, WebSocketServer } = require("ws");

const PORT = process.env.PORT || 8787;
const ALLOWED_ORIGIN = process.env.ALLOWED_ORIGIN || "*";

const T212_API_KEY = process.env.TRADING212_API_KEY;
const T212_API_SECRET = process.env.TRADING212_API_SECRET;

const T212_ENV = (
  process.env.TRADING212_ENV || "live"
).toLowerCase();

if (!T212_API_KEY || !T212_API_SECRET) {
  console.error(
    "Missing TRADING212_API_KEY or TRADING212_API_SECRET environment variable."
  );

  process.exit(1);
}

const T212_BASE_URL =
  T212_ENV === "demo"
    ? "https://demo.trading212.com/api/v0"
    : "https://live.trading212.com/api/v0";

/* -----------------------------
   Express
----------------------------- */

const app = express();

app.use(
  cors({
    origin:
      ALLOWED_ORIGIN === "*"
        ? true
        : ALLOWED_ORIGIN,
  })
);

const server =
  http.createServer(app);

/* -----------------------------
   Browser WebSocket
----------------------------- */

const wss =
  new WebSocketServer({
    server,
    path: "/ws",
  });

/* -----------------------------
   Current GPRO state
----------------------------- */

let latestPrice = null;
let latestTs = null;

let latestSource =
  "Trading 212";

let latestPosition = null;

let usdToGbp = null;

let lastSuccessfulPoll = null;

let pollState =
  "starting";

let lastError = null;

let pollInFlight = false;

/* -----------------------------
   Helpers
----------------------------- */

function sendJson(client, data) {
  if (
    client.readyState ===
    WebSocket.OPEN
  ) {
    client.send(
      JSON.stringify(data)
    );
  }
}

function broadcast(data) {
  for (
    const client
    of wss.clients
  ) {
    sendJson(
      client,
      data
    );
  }
}

function currentSnapshot() {
  return {
    type: "snapshot",

    symbol: "GPRO",

    price:
      latestPrice,

    timestamp:
      latestTs,

    usdToGbp,

    source:
      latestSource,

    pollState,

    position:
      latestPosition,
  };
}

function numberOrNull(value) {
  const n =
    Number(value);

  return Number.isFinite(n)
    ? n
    : null;
}

/* -----------------------------
   Browser connection
----------------------------- */

wss.on(
  "connection",
  (client, req) => {

    const origin =
      req.headers.origin;

    if (
      ALLOWED_ORIGIN !== "*" &&
      origin !== ALLOWED_ORIGIN
    ) {
      console.warn(
        `Rejected WebSocket origin: ${origin}`
      );

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

    client.on(
      "pong",
      () => {
        client.isAlive = true;
      }
    );

    // Immediately send
    // latest known GPRO info
    sendJson(
      client,
      currentSnapshot()
    );
  }
);

/* -----------------------------
   Keep WebSocket alive
----------------------------- */

const heartbeat =
  setInterval(
    () => {

      for (
        const client
        of wss.clients
      ) {
        if (
          client.isAlive === false
        ) {
          client.terminate();
          continue;
        }

        client.isAlive = false;

        client.ping();
      }

    },
    30000
  );

wss.on(
  "close",
  () => {
    clearInterval(
      heartbeat
    );
  }
);

/* -----------------------------
   Trading 212 authentication
----------------------------- */

function t212AuthHeader() {

  const credentials =
    Buffer.from(
      `${T212_API_KEY}:${T212_API_SECRET}`,
      "utf8"
    ).toString(
      "base64"
    );

  return (
    `Basic ${credentials}`
  );
}

/* -----------------------------
   Find GoPro position
----------------------------- */

function isGoProPosition(
  position
) {

  const ticker =
    String(
      position?.instrument?.ticker
      || ""
    ).toUpperCase();

  const name =
    String(
      position?.instrument?.name
      || ""
    ).toLowerCase();

  return (
    ticker === "GPRO" ||
    ticker.startsWith(
      "GPRO_"
    ) ||
    ticker.includes(
      "GPRO"
    ) ||
    name.includes(
      "gopro"
    )
  );
}

/* -----------------------------
   Poll Trading 212
----------------------------- */

async function pollTrading212() {

  // Prevent overlapping
  // requests
  if (pollInFlight) {
    return;
  }

  pollInFlight = true;

  try {

    pollState =
      "requesting";

    const response =
      await fetch(
        `${T212_BASE_URL}/equity/positions`,
        {
          method: "GET",

          headers: {
            Authorization:
              t212AuthHeader(),

            Accept:
              "application/json",
          },

          cache:
            "no-store",
        }
      );

    if (!response.ok) {

      const body =
        await response
          .text()
          .catch(
            () => ""
          );

      throw new Error(
        `Trading 212 HTTP ${
          response.status
        }${
          body
            ? `: ${body.slice(
                0,
                200
              )}`
            : ""
        }`
      );
    }

    const positions =
      await response.json();

    if (
      !Array.isArray(
        positions
      )
    ) {
      throw new Error(
        "Trading 212 returned an unexpected positions response."
      );
    }

    const position =
      positions.find(
        isGoProPosition
      );

    if (!position) {

      pollState =
        "connected-no-gpro";

      lastError =
        "No open GoPro/GPRO position found in Trading 212.";

      return;
    }

    /* -------------------------
       Extract position
    ------------------------- */

    const price =
      numberOrNull(
        position.currentPrice
      );

    const quantity =
      numberOrNull(
        position.quantity
      );

    const averagePricePaid =
      numberOrNull(
        position.averagePricePaid
      );

    const wallet =
      position.walletImpact
      || {};

    const walletCurrency =
      String(
        wallet.currency
        || ""
      ).toUpperCase();

    const walletCurrentValue =
      numberOrNull(
        wallet.currentValue
      );

    const walletTotalCost =
      numberOrNull(
        wallet.totalCost
      );

    if (!(price > 0)) {
      throw new Error(
        "Trading 212 returned no usable GPRO currentPrice."
      );
    }

    /* -------------------------
       Update live price
    ------------------------- */

    latestPrice =
      price;

    latestTs =
      Date.now();

    latestSource =
      "Trading 212";

    lastSuccessfulPoll =
      latestTs;

    pollState =
      "connected";

    lastError =
      null;

    /* -------------------------
       Derive USD → GBP

       If Trading 212 gives us
       its GBP position value,
       this makes the conversion
       line up more closely with
       Trading 212 itself.
    ------------------------- */

    if (
      walletCurrency === "GBP" &&
      walletCurrentValue > 0 &&
      quantity > 0 &&
      price > 0
    ) {

      const derivedFx =
        walletCurrentValue /
        (
          quantity *
          price
        );

      if (
        Number.isFinite(
          derivedFx
        ) &&
        derivedFx > 0
      ) {
        usdToGbp =
          derivedFx;
      }
    }

    /* -------------------------
       Save position
    ------------------------- */

    latestPosition = {

      ticker:
        position
          ?.instrument
          ?.ticker
        || "GPRO",

      name:
        position
          ?.instrument
          ?.name
        || "GoPro",

      quantity,

      averagePricePaid,

      currentPrice:
        price,

      walletCurrency:
        walletCurrency
        || null,

      currentValue:
        walletCurrentValue,

      totalCost:
        walletTotalCost,
    };

    /* -------------------------
       PUSH immediately
       to the journal
    ------------------------- */

    broadcast({

      type:
        "trade",

      symbol:
        "GPRO",

      price:
        latestPrice,

      timestamp:
        latestTs,

      usdToGbp,

      source:
        latestSource,

      position:
        latestPosition,
    });

  } catch (err) {

    pollState =
      "error";

    lastError =
      err.message;

    console.error(
      "Trading 212 poll failed:",
      err.message
    );

  } finally {

    pollInFlight =
      false;
  }
}

/* -----------------------------
   Main page
----------------------------- */

app.get(
  "/",
  (req, res) => {

    res.json({

      ok: true,

      service:
        "GPRO live feed",

      source:
        "Trading 212 positions API",

      environment:
        T212_ENV,

      websocketPath:
        "/ws",

      apiPath:
        "/api/gpro",
    });
  }
);

/* -----------------------------
   GPRO API
----------------------------- */

app.get(
  "/api/gpro",
  (req, res) => {

    res.set(
      "Cache-Control",
      "no-store"
    );

    res.json({

      symbol:
        "GPRO",

      price:
        latestPrice,

      timestamp:
        latestTs,

      usdToGbp,

      source:
        latestSource,

      pollState,

      lastSuccessfulPoll,

      position:
        latestPosition,

      error:
        lastError,
    });
  }
);

/* -----------------------------
   Health check
----------------------------- */

app.get(
  "/health",
  (req, res) => {

    res.json({

      ok:
        pollState !==
        "error",

      trading212:
        pollState,

      environment:
        T212_ENV,

      browserClients:
        wss.clients.size,

      hasPrice:
        latestPrice !== null,

      hasFx:
        usdToGbp !== null,

      lastSuccessfulPoll,

      error:
        lastError,
    });
  }
);

/* -----------------------------
   Start server
----------------------------- */

server.listen(
  PORT,
  "0.0.0.0",
  async () => {

    console.log(
      `GPRO live service listening on port ${PORT}`
    );

    console.log(
      `Trading 212 environment: ${T212_ENV}`
    );

    // Get first price
    // immediately.
    await pollTrading212();

    // Trading 212's positions
    // endpoint is limited to
    // one request per second.
    //
    // 1100ms gives a small
    // buffer against HTTP 429.
    setInterval(
      pollTrading212,
      1100
    );
  }
);
