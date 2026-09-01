"use strict";

const express = require("express");
const cors = require("cors");
const http = require("http");
const { WebSocket, WebSocketServer } = require("ws");

const PORT = process.env.PORT || 8787;
const FINNHUB_API_KEY = process.env.FINNHUB_API_KEY;
const ALLOWED_ORIGIN = process.env.ALLOWED_ORIGIN || "*";

const SYMBOL = "GPRO";

// Backup REST check.
// Real-time updates still come through WebSocket.
const REST_REFRESH_MS = 15000;

if (!FINNHUB_API_KEY) {
  console.error(
    "Missing FINNHUB_API_KEY environment variable."
  );

  process.exit(1);
}


/* =====================================
   EXPRESS
===================================== */

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


/* =====================================
   JOURNAL WEBSOCKET
===================================== */

const wss =
  new WebSocketServer({
    server,
    path: "/ws",
  });


/* =====================================
   CURRENT STATE
===================================== */

let latestPrice = null;
let latestTs = null;

let latestSource =
  "starting";

let finnhubState =
  "connecting";

let usdToGbp = null;
let fxUpdatedAt = null;

let lastRestRefresh = null;
let lastError = null;


/* =====================================
   HELPERS
===================================== */

function toMs(timestamp) {

  const value =
    Number(timestamp);

  if (
    !Number.isFinite(value) ||
    value <= 0
  ) {
    return Date.now();
  }

  // Finnhub REST uses seconds.
  // Finnhub WebSocket uses milliseconds.
  return value < 1e12
    ? value * 1000
    : value;
}


function sendJson(
  client,
  data
) {

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


function snapshotPayload(
  type = "snapshot"
) {

  return {

    type,

    symbol:
      SYMBOL,

    price:
      latestPrice,

    timestamp:
      latestTs,

    usdToGbp,

    fxUpdatedAt,

    source:
      latestSource,

    feedState:
      finnhubState,

  };
}


/* =====================================
   PRICE UPDATE
===================================== */

function updatePrice(
  price,
  timestamp,
  source
) {

  const p =
    Number(price);

  const ts =
    toMs(timestamp);


  if (!(p > 0)) {
    return false;
  }


  /*
    Never allow an older REST quote
    to replace a newer WebSocket trade.
  */

  if (
    latestTs &&
    ts < latestTs
  ) {

    return false;

  }


  const changed =
    latestPrice !== p ||
    latestTs !== ts ||
    latestSource !== source;


  latestPrice =
    p;

  latestTs =
    ts;

  latestSource =
    source;


  if (changed) {

    /*
      Keep "trade" here because
      your existing index.html
      already understands
      trade + snapshot messages.
    */

    broadcast(
      snapshotPayload(
        "trade"
      )
    );

  }


  return true;
}


/* =====================================
   JOURNAL CONNECTION
===================================== */

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


    client.isAlive =
      true;


    client.on(
      "pong",
      () => {

        client.isAlive =
          true;

      }
    );


    /*
      Send the latest known
      GPRO price immediately.
    */

    sendJson(
      client,
      snapshotPayload(
        "snapshot"
      )
    );

  }
);


/* =====================================
   JOURNAL HEARTBEAT
===================================== */

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


        client.isAlive =
          false;


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


/* =====================================
   USD → GBP
===================================== */

async function refreshFx() {

  try {

    const response =
      await fetch(

        "https://api.frankfurter.app/latest?from=USD&to=GBP",

        {
          cache:
            "no-store",
        }

      );


    if (!response.ok) {

      throw new Error(
        `FX HTTP ${response.status}`
      );

    }


    const data =
      await response.json();


    const rate =
      Number(
        data?.rates?.GBP
      );


    if (rate > 0) {

      usdToGbp =
        rate;


      fxUpdatedAt =
        Date.now();


      broadcast({

        type:
          "fx",

        usdToGbp,

        fxUpdatedAt,

      });

    }

  }

  catch (err) {

    console.error(
      "FX refresh failed:",
      err.message
    );

  }

}


/* =====================================
   FINNHUB REST QUOTE
===================================== */

async function refreshFinnhubQuote() {

  try {

    const response =
      await fetch(

        `https://finnhub.io/api/v1/quote?symbol=${encodeURIComponent(
          SYMBOL
        )}&token=${encodeURIComponent(
          FINNHUB_API_KEY
        )}`,

        {
          cache:
            "no-store",
        }

      );


    if (!response.ok) {

      throw new Error(
        `Finnhub quote HTTP ${response.status}`
      );

    }


    const data =
      await response.json();


    const price =
      Number(
        data.c
      );


    const timestamp =
      toMs(
        data.t
      );


    if (price > 0) {

      /*
        REST is our backup.

        It restores the price
        after Render restarts and
        catches anything missed by
        the WebSocket.
      */

      updatePrice(

        price,

        timestamp,

        "Finnhub quote"

      );


      lastRestRefresh =
        Date.now();


      lastError =
        null;

    }

    else {

      throw new Error(
        "Finnhub returned no usable GPRO price."
      );

    }

  }

  catch (err) {

    lastError =
      err.message;


    console.error(
      "Finnhub quote refresh failed:",
      err.message
    );

  }

}


/* =====================================
   FINNHUB WEBSOCKET
===================================== */

let finnhubSocket =
  null;

let reconnectTimer =
  null;

let reconnectDelay =
  3000;


/* =====================================
   RECONNECT
===================================== */

function scheduleReconnect() {

  clearTimeout(
    reconnectTimer
  );


  reconnectTimer =
    setTimeout(

      connectFinnhub,

      reconnectDelay

    );


  reconnectDelay =
    Math.min(

      reconnectDelay * 2,

      30000

    );

}


/* =====================================
   CONNECT FINNHUB
===================================== */

function connectFinnhub() {

  clearTimeout(
    reconnectTimer
  );


  finnhubState =
    "connecting";


  finnhubSocket =
    new WebSocket(

      `wss://ws.finnhub.io?token=${encodeURIComponent(
        FINNHUB_API_KEY
      )}`

    );


  /* -----------------------------
     CONNECTED
  ----------------------------- */

  finnhubSocket.on(
    "open",
    () => {

      finnhubState =
        "connected";


      reconnectDelay =
        3000;


      finnhubSocket.send(

        JSON.stringify({

          type:
            "subscribe",

          symbol:
            SYMBOL,

        })

      );


      console.log(
        `Finnhub connected; subscribed to ${SYMBOL}`
      );


      /*
        Update journal's connection
        indicator immediately.
      */

      broadcast(
        snapshotPayload(
          "snapshot"
        )
      );

    }
  );


  /* -----------------------------
     LIVE TRADES
  ----------------------------- */

  finnhubSocket.on(
    "message",
    (raw) => {

      try {

        const msg =
          JSON.parse(
            raw.toString()
          );


        if (
          msg.type !== "trade" ||
          !Array.isArray(
            msg.data
          )
        ) {

          return;

        }


        const trades =
          msg.data

            .filter(

              (trade) =>

                trade.s === SYMBOL &&

                Number(
                  trade.p
                ) > 0

            )

            .sort(

              (a, b) =>

                Number(
                  a.t || 0
                ) -

                Number(
                  b.t || 0
                )

            );


        if (
          !trades.length
        ) {

          return;

        }


        /*
          A Finnhub message can
          contain multiple trades.

          Use the newest one.
        */

        const newest =
          trades[
            trades.length - 1
          ];


        updatePrice(

          newest.p,

          newest.t,

          "Finnhub trade"

        );

      }

      catch (err) {

        console.error(
          "Bad Finnhub WebSocket message:",
          err.message
        );

      }

    }
  );


  /* -----------------------------
     DISCONNECTED
  ----------------------------- */

  finnhubSocket.on(
    "close",
    () => {

      finnhubState =
        "disconnected";


      console.log(
        "Finnhub disconnected; reconnecting..."
      );


      /*
        Keep displaying the most
        recent price while reconnecting.
      */

      broadcast(
        snapshotPayload(
          "snapshot"
        )
      );


      scheduleReconnect();

    }
  );


  /* -----------------------------
     ERROR
  ----------------------------- */

  finnhubSocket.on(
    "error",
    (err) => {

      finnhubState =
        "error";


      lastError =
        err.message;


      console.error(
        "Finnhub WebSocket error:",
        err.message
      );


      try {

        finnhubSocket.close();

      }

      catch {}

    }
  );

}


/* =====================================
   ROOT
===================================== */

app.get(
  "/",
  (req, res) => {

    res.json({

      ok:
        true,

      service:
        "GPRO Finnhub live-price bridge",

      websocketPath:
        "/ws",

      apiPath:
        "/api/gpro",

      healthPath:
        "/health",

    });

  }
);


/* =====================================
   CURRENT GPRO PRICE
===================================== */

app.get(
  "/api/gpro",
  (req, res) => {

    res.set(
      "Cache-Control",
      "no-store"
    );


    res.json({

      ...snapshotPayload(
        "snapshot"
      ),

      lastRestRefresh,

      error:
        lastError,

    });

  }
);


/* =====================================
   HEALTH
===================================== */

app.get(
  "/health",
  (req, res) => {

    const ageMs =
      latestTs

        ? Date.now() -
          latestTs

        : null;


    res.json({

      ok:
        true,

      finnhubWebsocket:
        finnhubState,

      browserClients:
        wss.clients.size,

      hasPrice:
        latestPrice !== null,

      latestPrice,

      latestTimestamp:
        latestTs,

      latestAgeSeconds:

        ageMs === null

          ? null

          : Math.max(

              0,

              Math.round(
                ageMs / 1000
              )

            ),

      latestSource,

      hasFx:
        usdToGbp !== null,

      lastRestRefresh,

      error:
        lastError,

    });

  }
);


/* =====================================
   START
===================================== */

server.listen(
  PORT,
  "0.0.0.0",
  async () => {

    console.log(
      `GPRO live service listening on port ${PORT}`
    );


    /*
      Immediately populate the journal
      instead of starting at $—.
    */

    await Promise.allSettled([

      refreshFx(),

      refreshFinnhubQuote(),

    ]);


    /*
      REAL-TIME PATH

      Finnhub pushes each trade
      directly to Render.
    */

    connectFinnhub();

    setInterval(() => {
  broadcast({
    type: "heartbeat",
    serverTime: Date.now(),
    symbol: "GPRO",
    price: latestPrice,
    marketTimestamp: latestTs,
    source: latestSource
  });
}, 1000);

    /*
      BACKUP PATH

      Every 15 seconds, ask for
      Finnhub's latest quote.

      This isn't the primary live feed;
      it's there for missed updates,
      reconnects and Render restarts.
    */

    setInterval(

      refreshFinnhubQuote,

      REST_REFRESH_MS

    );


    /*
      USD / GBP only needs
      occasional refreshing.
    */

    setInterval(

      refreshFx,

      60 * 60 * 1000

    );

  }
);
