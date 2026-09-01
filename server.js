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

app.use(
  cors({
    origin:
      ALLOWED_ORIGIN === "*"
        ? true
        : ALLOWED_ORIGIN
  })
);

const server = http.createServer(app);

const wss = new WebSocketServer({
  server,
  path: "/ws"
});

/* ================================
   CURRENT STATE
================================ */

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


/* ================================
   HELPERS
================================ */

function send(client, data) {
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
    send(
      client,
      data
    );
  }
}


function snapshot(
  type = "snapshot"
) {
  return {
    type,

    symbol:
      SYMBOL,

    price,

    timestamp:
      marketTs,

    marketTimestamp:
      marketTs,

    observedTimestamp:
      observedTs,

    serverTime:
      Date.now(),

    usdToGbp,

    fxUpdatedAt,

    source,

    finnhubState,

    stocktwitsState,

    stocktwitsUpdated,

    stocktwitsLastSuccess
  };
}


/* ================================
   UPDATE PRICE
================================ */

function setPrice(
  nextPrice,
  nextTs,
  nextSource,
  observed = Date.now()
) {
  const p =
    Number(nextPrice);

  const ts =
    Number(nextTs) ||
    observed;


  if (!(p > 0)) {
    return false;
  }


  /*
    Prevent an older quote
    overwriting a newer price.
  */

  if (
    marketTs &&
    ts < marketTs
  ) {
    return false;
  }


  /*
    Safety check.

    Prevent an unrelated dollar
    value from accidentally being
    treated as the GPRO price.
  */

  if (
    price &&
    (
      p < price * 0.20 ||
      p > price * 5
    )
  ) {

    console.warn(
      `Rejected suspicious ${nextSource} price ${p}`
    );

    return false;
  }


  price =
    p;

  marketTs =
    ts;

  observedTs =
    observed;

  source =
    nextSource;

  lastError =
    null;


  broadcast(
    snapshot(
      "price"
    )
  );


  return true;
}


/* ================================
   HTML CLEANER
================================ */

function stripHtml(html) {

  return html

    .replace(
      /<script\b[^>]*>[\s\S]*?<\/script>/gi,
      " "
    )

    .replace(
      /<style\b[^>]*>[\s\S]*?<\/style>/gi,
      " "
    )

    .replace(
      /<noscript\b[^>]*>[\s\S]*?<\/noscript>/gi,
      " "
    )

    .replace(
      /&nbsp;/gi,
      " "
    )

    .replace(
      /&middot;|&#183;/gi,
      "·"
    )

    .replace(
      /&amp;/gi,
      "&"
    )

    .replace(
      /<[^>]+>/g,
      " "
    )

    .replace(
      /\s+/g,
      " "
    )

    .trim();
}


/* ================================
   STOCKTWITS UPDATED TIME
================================ */

function parseUpdatedTime(
  text
) {

  const match =
    text.match(
      /Updated\s+(\d{1,2}:\d{2})\s*(AM|PM)\s*(EDT|EST)/i
    );


  if (!match) {
    return null;
  }


  const now =
    new Date();


  const dateParts =
    new Intl.DateTimeFormat(
      "en-US",
      {
        timeZone:
          "America/New_York",

        year:
          "numeric",

        month:
          "2-digit",

        day:
          "2-digit"
      }
    ).formatToParts(
      now
    );


  const parts = {};


  for (
    const part
    of dateParts
  ) {

    if (
      part.type !==
      "literal"
    ) {

      parts[
        part.type
      ] =
        part.value;

    }

  }


  const parsed =
    Date.parse(

      `${parts.month}/${parts.day}/${parts.year} ${match[1]} ${match[2]} ${match[3]}`

    );


  if (
    !Number.isFinite(
      parsed
    )
  ) {

    return null;

  }


  return {

    timestamp:
      parsed,

    label:
      `${match[1]} ${match[2].toUpperCase()} ${match[3].toUpperCase()}`

  };
}


/* ================================
   BROWSER WEBSOCKET
================================ */

wss.on(
  "connection",
  (
    client,
    req
  ) => {

    const origin =
      req.headers.origin;


    if (
      ALLOWED_ORIGIN !== "*" &&
      origin &&
      origin !==
        ALLOWED_ORIGIN
    ) {

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
      Send current price
      immediately.
    */

    send(
      client,
      snapshot()
    );

  }
);


/* ================================
   KEEP JOURNAL CONNECTION OPEN
================================ */

setInterval(
  () => {

    for (
      const client
      of wss.clients
    ) {

      if (
        client.isAlive ===
        false
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


/* ================================
   JOURNAL UPDATE EVERY SECOND
================================ */

setInterval(
  () => {

    broadcast(
      snapshot(
        "heartbeat"
      )
    );

  },

  1000

);


/* ================================
   USD → GBP
================================ */

async function refreshFx() {

  try {

    const response =
      await fetch(

        "https://api.frankfurter.app/latest?from=USD&to=GBP",

        {
          cache:
            "no-store"
        }

      );


    if (
      !response.ok
    ) {

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


    if (
      rate > 0
    ) {

      usdToGbp =
        rate;


      fxUpdatedAt =
        Date.now();


      broadcast({

        type:
          "fx",

        usdToGbp,

        fxUpdatedAt,

        serverTime:
          Date.now()

      });

    }

  }

  catch (
    err
  ) {

    console.error(
      "FX:",
      err.message
    );

  }

}


/* ================================
   FINNHUB REST BACKUP
================================ */

async function refreshFinnhub() {

  try {

    const response =
      await fetch(

        `https://finnhub.io/api/v1/quote?symbol=${SYMBOL}&token=${encodeURIComponent(
          FINNHUB_API_KEY
        )}`,

        {
          cache:
            "no-store"
        }

      );


    if (
      !response.ok
    ) {

      throw new Error(
        `Finnhub HTTP ${response.status}`
      );

    }


    const data =
      await response.json();


    const currentPrice =
      Number(
        data.c
      );


    if (
      !(currentPrice > 0)
    ) {

      throw new Error(
        "No usable Finnhub price"
      );

    }


    const timestamp =
      Number(
        data.t
      ) > 0

        ? Number(
            data.t
          ) *
          1000

        : Date.now();


    setPrice(

      currentPrice,

      timestamp,

      "Finnhub quote"

    );

  }

  catch (
    err
  ) {

    lastError =
      err.message;


    console.error(
      "Finnhub REST:",
      err.message
    );

  }

}


/* ================================
   STOCKTWITS SESSION PRICE
================================ */

async function refreshStocktwits(
  force = false
) {

  const now =
    Date.now();


  /*
    Don't hammer the site
    if refresh is clicked
    repeatedly.
  */

  if (
    now -
      lastStocktwitsAttempt
      <
      5000
  ) {

    return false;

  }


  lastStocktwitsAttempt =
    now;


  stocktwitsState =
    "checking";


  try {

    const response =
      await fetch(

        STOCKTWITS_URL,

        {

          cache:
            "no-store",

          redirect:
            "follow",

          headers: {

            "User-Agent":
              "Mozilla/5.0 (compatible; GoProJournal/1.0)",

            "Accept":
              "text/html,application/xhtml+xml",

            "Accept-Language":
              "en-US,en;q=0.9"

          }

        }

      );


    if (
      !response.ok
    ) {

      throw new Error(
        `Stocktwits HTTP ${response.status}`
      );

    }


    const html =
      await response.text();


    const text =
      stripHtml(
        html
      );


    /* ------------------------------
       VERIFY PAGE IS GPRO
    ------------------------------ */

    const identityValid =

      /\bGPRO\b/i.test(
        text
      )

      &&

      /\bGoPro(?:,\s*Inc\.?| Inc\.?)\b/i.test(
        text
      );


    if (
      !identityValid
    ) {

      throw new Error(
        "GPRO / GoPro identity validation failed"
      );

    }


    /* ------------------------------
       READ MAIN GPRO PRICE

       Stocktwits may say:

       Overnight
       Pre-Market
       After-Hours
       Today
       Closed

       We accept any session.
    ------------------------------ */

    const quoteBlock =
      text.match(

        /GPRO\s+GoPro(?:,\s*Inc\.?| Inc\.?)\s+\$([0-9]+(?:\.[0-9]+)?)[\s\S]{0,180}?\b(Overnight|Pre-Market|After-Hours|After Hours|Today|Closed)\b/i

      );


    if (
      !quoteBlock
    ) {

      stocktwitsState =
        "quote-not-found";


      throw new Error(
        "Could not locate Stocktwits GPRO quote/session block"
      );

    }


    const displayedPrice =
      Number(
        quoteBlock[1]
      );


    let session =
      quoteBlock[2]

        .replace(
          /\s+/g,
          " "
        )

        .trim();


    /* ------------------------------
       NORMALISE SESSION NAME
    ------------------------------ */

    if (
      /after[\s-]?hours/i.test(
        session
      )
    ) {

      session =
        "After-Hours";

    }

    else if (
      /pre[\s-]?market/i.test(
        session
      )
    ) {

      session =
        "Pre-Market";

    }

    else if (
      /overnight/i.test(
        session
      )
    ) {

      session =
        "Overnight";

    }

    else {

      session =
        "Regular";

    }


    if (
      !(displayedPrice > 0)
    ) {

      throw new Error(
        "Stocktwits displayed GPRO price was invalid"
      );

    }


    /* ------------------------------
       PREVIOUS CLOSE SAFETY CHECK
    ------------------------------ */

    const closeMatch =

      text.match(
        /Prev Close\s+\$([0-9]+(?:\.[0-9]+)?)/i
      )

      ||

      text.match(
        /Closed\s+\$([0-9]+(?:\.[0-9]+)?)/i
      );


    const referenceClose =
      closeMatch

        ? Number(
            closeMatch[1]
          )

        : null;


    if (
      referenceClose > 0
    ) {

      const ratio =
        displayedPrice /
        referenceClose;


      /*
        Very wide range.

        Only rejects a clearly
        unrelated dollar figure.
      */

      if (
        ratio < 0.15 ||
        ratio > 7
      ) {

        throw new Error(

          `Stocktwits price failed sanity check (${displayedPrice} vs ${referenceClose})`

        );

      }

    }


    /* ------------------------------
       READ UPDATED TIME
    ------------------------------ */

    const updated =
      parseUpdatedTime(
        text
      );


    const timestamp =
      updated

        ? updated.timestamp

        : now;


    /* ------------------------------
       SOURCE LABEL
    ------------------------------ */

    const sourceName =

      session ===
      "Overnight"

        ? "Stocktwits overnight"

        : `Stocktwits ${session}`;


    /* ------------------------------
       SAVE PRICE
    ------------------------------ */

    const accepted =
      setPrice(

        displayedPrice,

        timestamp,

        sourceName,

        now

      );


    stocktwitsUpdated =
      updated?.label ||
      null;


    if (
      accepted
    ) {

      stocktwitsLastSuccess =
        now;


      if (
        session ===
        "Overnight"
      ) {

        stocktwitsState =
          "overnight-found";

      }

      else {

        stocktwitsState =
          session

            .toLowerCase()

            .replace(
              /\s+/g,
              "-"
            );

      }


      console.log(

        `Stocktwits GPRO ${session}: $${displayedPrice}`

        +

        (
          stocktwitsUpdated

            ? ` · Updated ${stocktwitsUpdated}`

            : ""
        )

      );

    }

    else {

      /*
        Parser succeeded but
        Finnhub already had a
        newer timestamp.
      */

      stocktwitsState =

        session ===
        "Overnight"

          ? "overnight-parsed-stale"

          : `${session
              .toLowerCase()
              .replace(
                /\s+/g,
                "-"
              )}-parsed-stale`;

    }


    return accepted;

  }

  catch (
    err
  ) {

    if (
      stocktwitsState ===
      "checking"
    ) {

      stocktwitsState =
        "error";

    }


    console.error(
      "Stocktwits:",
      err.message
    );


    return false;

  }

}


/* ================================
   FINNHUB WEBSOCKET
================================ */

let finnhubSocket;

let reconnectTimer;

let reconnectDelay =
  3000;


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


  /* ------------------------------
     CONNECTED
  ------------------------------ */

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
            SYMBOL

        })

      );


      console.log(
        "Finnhub connected; subscribed to GPRO"
      );


      broadcast(
        snapshot(
          "status"
        )
      );

    }
  );


  /* ------------------------------
     LIVE TRADE
  ------------------------------ */

  finnhubSocket.on(
    "message",
    raw => {

      try {

        const msg =
          JSON.parse(
            raw.toString()
          );


        if (
          msg.type !==
            "trade"
          ||
          !Array.isArray(
            msg.data
          )
        ) {

          return;

        }


        const validTrades =
          msg.data

            .filter(

              trade =>

                trade.s ===
                  SYMBOL

                &&

                Number(
                  trade.p
                ) > 0

            )

            .sort(

              (
                a,
                b
              ) =>

                Number(
                  a.t || 0
                )

                -

                Number(
                  b.t || 0
                )

            );


        if (
          !validTrades.length
        ) {

          return;

        }


        const newestTrade =
          validTrades[
            validTrades.length -
            1
          ];


        setPrice(

          Number(
            newestTrade.p
          ),

          Number(
            newestTrade.t
          ) ||
            Date.now(),

          "Finnhub trade"

        );

      }

      catch (
        err
      ) {

        console.error(
          "Finnhub WS message:",
          err.message
        );

      }

    }
  );


  /* ------------------------------
     DISCONNECTED
  ------------------------------ */

  finnhubSocket.on(
    "close",
    () => {

      finnhubState =
        "disconnected";


      console.log(
        "Finnhub disconnected; reconnecting..."
      );


      reconnectTimer =
        setTimeout(

          connectFinnhub,

          reconnectDelay

        );


      reconnectDelay =
        Math.min(

          reconnectDelay *
            2,

          30000

        );

    }
  );


  /* ------------------------------
     ERROR
  ------------------------------ */

  finnhubSocket.on(
    "error",
    err => {

      finnhubState =
        "error";


      lastError =
        err.message;


      console.error(
        "Finnhub WebSocket:",
        err.message
      );


      try {

        finnhubSocket.close();

      }

      catch {}

    }
  );

}


/* ================================
   ROOT
================================ */

app.get(
  "/",
  (
    req,
    res
  ) => {

    res.json({

      ok:
        true,

      service:
        "GPRO live journal feed",

      liveSource:
        "Finnhub",

      secondarySource:
        "Stocktwits public GPRO page",

      websocket:
        "/ws",

      priceApi:
        "/api/gpro",

      health:
        "/health"

    });

  }
);


/* ================================
   PRICE API
================================ */

app.get(
  "/api/gpro",
  async (
    req,
    res
  ) => {

    res.set(
      "Cache-Control",
      "no-store"
    );


    /*
      Manual journal refresh:

      force a fresh Stocktwits
      check and Finnhub REST
      check before responding.
    */

    if (
      req.query.refresh ===
      "1"
    ) {

      await Promise.allSettled([

        refreshStocktwits(
          true
        ),

        refreshFinnhub()

      ]);

    }


    res.json({

      ...snapshot(),

      error:
        lastError

    });

  }
);


/* ================================
   HEALTH
================================ */

app.get(
  "/health",
  (
    req,
    res
  ) => {

    res.json({

      ok:
        true,

      symbol:
        SYMBOL,

      browserClients:
        wss.clients.size,

      hasPrice:
        price !== null,

      latestPrice:
        price,

      latestMarketTimestamp:
        marketTs,

      latestObservedTimestamp:
        observedTs,

      latestSource:
        source,

      finnhub:
        finnhubState,

      stocktwits:
        stocktwitsState,

      stocktwitsUpdated,

      stocktwitsLastSuccess,

      hasFx:
        usdToGbp !== null,

      usdToGbp,

      serverTime:
        Date.now(),

      error:
        lastError

    });

  }
);


/* ================================
   START SERVER
================================ */

server.listen(
  PORT,
  "0.0.0.0",
  async () => {

    console.log(
      `GPRO live service listening on port ${PORT}`
    );


    /*
      Immediately get:
      - GBP FX
      - Finnhub price
      - Stocktwits session price
    */

    await Promise.allSettled([

      refreshFx(),

      refreshFinnhub(),

      refreshStocktwits(
        true
      )

    ]);


    /*
      Start Finnhub realtime
      WebSocket.
    */

    connectFinnhub();


    /*
      Finnhub REST safety
      refresh every 15 sec.
    */

    setInterval(

      refreshFinnhub,

      15000

    );


    /*
      Stocktwits session lookup
      every 30 sec.

      When Stocktwits switches
      to Overnight, the same
      parser automatically
      detects it.
    */

    setInterval(

      () =>
        refreshStocktwits(
          false
        ),

      30000

    );


    /*
      GBP exchange rate
      every hour.
    */

    setInterval(

      refreshFx,

      60 *
      60 *
      1000

    );

  }
);
