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


/* =========================================================
   SEPARATE PRICE SOURCES

   IMPORTANT:
   Finnhub and Stocktwits each keep their own timestamp.

   This prevents an overnight price from permanently blocking
   the daytime price.
========================================================= */

const feeds = {

  finnhub: {
    price: null,
    marketTs: null,
    observedTs: null,
    label: null,
    session: null
  },

  stocktwits: {
    price: null,
    marketTs: null,
    observedTs: null,
    label: null,
    session: null
  }

};


let active = {

  price: null,

  marketTs: null,

  observedTs: null,

  label: "starting",

  session: "Unknown"

};


let usdToGbp = null;

let fxUpdatedAt = null;

let finnhubState = "connecting";

let stocktwitsState = "idle";

let stocktwitsUpdated = null;

let stocktwitsLastSuccess = null;

let lastStocktwitsAttempt = 0;

let lastError = null;


/* =========================================================
   BASIC HELPERS
========================================================= */

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


/* =========================================================
   TIMESTAMP NORMALISATION
========================================================= */

function normalizeTs(value) {

  const n =
    Number(value);


  if (
    Number.isFinite(n) &&
    n > 0
  ) {

    /*
      Nanoseconds
    */

    if (
      n > 1e17
    ) {

      return Math.floor(
        n / 1e6
      );

    }


    /*
      Milliseconds
    */

    if (
      n > 1e12
    ) {

      return Math.floor(
        n
      );

    }


    /*
      Seconds
    */

    return Math.floor(
      n * 1000
    );

  }


  const parsed =
    Date.parse(
      String(value)
    );


  if (
    Number.isFinite(parsed)
  ) {

    return parsed;

  }


  return Date.now();

}


function ageMs(ts) {

  if (!ts) {
    return Infinity;
  }


  return Math.max(
    0,
    Date.now() - ts
  );

}


function validFeed(feed) {

  return (

    feed &&

    Number(feed.price) > 0 &&

    Number(feed.marketTs) > 0

  );

}


/* =========================================================
   CHOOSE THE FRESHEST PRICE

   This is the key part of the fix.

   Whichever genuine market source has the newest timestamp
   becomes the displayed price.

   Therefore:

   overnight → pre-market → market → after-hours → overnight

   can all switch automatically.
========================================================= */

function chooseActiveFeed() {

  const finnhub =
    feeds.finnhub;


  const stocktwits =
    feeds.stocktwits;


  let chosen =
    null;


  if (
    validFeed(finnhub) &&
    validFeed(stocktwits)
  ) {

    const difference =

      finnhub.marketTs -
      stocktwits.marketTs;


    /*
      If both timestamps are basically identical,
      prefer Finnhub because its WebSocket trades are
      our primary live source.
    */

    if (
      Math.abs(
        difference
      ) <= 5000
    ) {

      chosen =
        finnhub;

    }

    else {

      chosen =

        difference > 0
          ? finnhub
          : stocktwits;

    }

  }

  else if (
    validFeed(finnhub)
  ) {

    chosen =
      finnhub;

  }

  else if (
    validFeed(stocktwits)
  ) {

    chosen =
      stocktwits;

  }


  if (!chosen) {

    return false;

  }


  const changed =

    active.price !==
      chosen.price

    ||

    active.marketTs !==
      chosen.marketTs

    ||

    active.label !==
      chosen.label

    ||

    active.session !==
      chosen.session;


  active = {

    price:
      chosen.price,

    marketTs:
      chosen.marketTs,

    observedTs:
      chosen.observedTs,

    label:
      chosen.label,

    session:
      chosen.session

  };


  if (
    changed
  ) {

    console.log(

      `ACTIVE GPRO -> $${active.price} | ${active.label} | ${active.session} | ${new Date(
        active.marketTs
      ).toISOString()}`

    );


    broadcast(
      snapshot(
        "price"
      )
    );

  }


  return true;

}


/* =========================================================
   STORE A PRICE FOR ONE SOURCE
========================================================= */

function storeFeed(
  name,
  nextPrice,
  nextTs,
  label,
  session,
  observed = Date.now()
) {

  const price =
    Number(
      nextPrice
    );


  const timestamp =
    normalizeTs(
      nextTs
    );


  if (
    !(price > 0) ||
    !(timestamp > 0)
  ) {

    return false;

  }


  const previous =
    feeds[name];


  const reference =

    Number(
      previous?.price
    ) > 0

      ? previous.price

      : Number(
          active.price
        ) > 0

        ? active.price

        : null;


  /*
    Very wide safety filter.

    Only meant to reject an obviously unrelated
    number scraped from the page.
  */

  if (
    reference &&

    (
      price <
        reference * 0.15

      ||

      price >
        reference * 7
    )
  ) {

    console.warn(

      `Rejected suspicious ${label} price ${price}`

    );


    return false;

  }


  /*
    CRITICAL:

    We only compare the timestamp against the
    SAME source.

    Finnhub is not allowed to block Stocktwits,
    and Stocktwits is not allowed to block Finnhub.
  */

  if (
    previous.marketTs &&
    timestamp <
      previous.marketTs
  ) {

    return false;

  }


  feeds[name] = {

    price,

    marketTs:
      timestamp,

    observedTs:
      observed,

    label,

    session

  };


  lastError =
    null;


  chooseActiveFeed();


  return true;

}


/* =========================================================
   API SNAPSHOT
========================================================= */

function snapshot(
  type = "snapshot"
) {

  return {

    type,

    symbol:
      SYMBOL,


    price:
      active.price,


    timestamp:
      active.marketTs,


    marketTimestamp:
      active.marketTs,


    observedTimestamp:
      active.observedTs,


    serverTime:
      Date.now(),


    source:
      active.label,


    session:
      active.session,


    priceAgeSeconds:

      active.marketTs

        ? Math.round(

            ageMs(
              active.marketTs
            ) / 1000

          )

        : null,


    usdToGbp,

    fxUpdatedAt,


    finnhubState,

    stocktwitsState,

    stocktwitsUpdated,

    stocktwitsLastSuccess,


    feeds: {

      finnhub: {

        price:
          feeds.finnhub.price,

        timestamp:
          feeds.finnhub.marketTs,

        source:
          feeds.finnhub.label,

        session:
          feeds.finnhub.session

      },


      stocktwits: {

        price:
          feeds.stocktwits.price,

        timestamp:
          feeds.stocktwits.marketTs,

        source:
          feeds.stocktwits.label,

        session:
          feeds.stocktwits.session

      }

    }

  };

}


/* =========================================================
   CLEAN STOCKTWITS HTML
========================================================= */

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


/* =========================================================
   STOCKTWITS UPDATED TIME

   Example:

   Updated 11:38 AM EDT
========================================================= */

function parseStocktwitsUpdatedTime(
  text
) {

  const match =
    text.match(

      /Updated\s+(\d{1,2}:\d{2})\s*(AM|PM)\s*(EDT|EST)/i

    );


  if (!match) {

    return null;

  }


  const parts =

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
      new Date()
    );


  const date = {};


  for (
    const part
    of parts
  ) {

    if (
      part.type !==
      "literal"
    ) {

      date[
        part.type
      ] =
        part.value;

    }

  }


  /*
    EDT = UTC-4
    EST = UTC-5
  */

  let [
    hour,
    minute
  ] = match[1]

    .split(":")

    .map(Number);


  const ampm =
    match[2]
      .toUpperCase();


  if (
    ampm === "PM" &&
    hour !== 12
  ) {

    hour += 12;

  }


  if (
    ampm === "AM" &&
    hour === 12
  ) {

    hour = 0;

  }


  const offsetHours =

    match[3]
      .toUpperCase() ===
      "EDT"

      ? 4

      : 5;


  const timestamp =
    Date.UTC(

      Number(
        date.year
      ),

      Number(
        date.month
      ) - 1,

      Number(
        date.day
      ),

      hour +
        offsetHours,

      minute,

      0

    );


  return {

    timestamp,

    label:

      `${match[1]} ${match[2].toUpperCase()} ${match[3].toUpperCase()}`

  };

}


/* =========================================================
   STOCKTWITS PRICE REFRESH
========================================================= */

async function refreshStocktwits(
  force = false
) {

  const now =
    Date.now();


  /*
    Minimum five seconds between page requests.
  */

  if (
    now -
      lastStocktwitsAttempt <
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
              "Mozilla/5.0 (compatible; GoProJournal/3.0)",

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


    /* --------------------------
       VERIFY GPRO PAGE
    -------------------------- */

    const identity =

      /\bGPRO\b/i.test(
        text
      )

      &&

      /\bGoPro(?:,\s*Inc\.?| Inc\.?)\b/i.test(
        text
      );


    if (
      !identity
    ) {

      throw new Error(

        "Stocktwits GPRO identity validation failed"

      );

    }


    /* --------------------------
       FIND CURRENT PRICE + SESSION
    -------------------------- */

    const block =
      text.match(

        /GPRO\s+GoPro(?:,\s*Inc\.?| Inc\.?)\s+\$([0-9]+(?:\.[0-9]+)?)[\s\S]{0,240}?\b(Overnight|Pre-Market|After-Hours|After Hours|Today|Closed)\b/i

      );


    if (
      !block
    ) {

      stocktwitsState =
        "quote-not-found";


      throw new Error(

        "Stocktwits GPRO quote block not found"

      );

    }


    const displayedPrice =
      Number(
        block[1]
      );


    const sessionRaw =
      block[2];


    let session =
      "Regular";


    if (
      /overnight/i.test(
        sessionRaw
      )
    ) {

      session =
        "Overnight";

    }

    else if (
      /pre[\s-]?market/i.test(
        sessionRaw
      )
    ) {

      session =
        "Pre-Market";

    }

    else if (
      /after[\s-]?hours/i.test(
        sessionRaw
      )
    ) {

      session =
        "After-Hours";

    }


    if (
      !(displayedPrice > 0)
    ) {

      throw new Error(

        "Invalid Stocktwits GPRO price"

      );

    }


    /* --------------------------
       PREVIOUS CLOSE CHECK
    -------------------------- */

    const closeMatch =

      text.match(

        /Prev Close\s+\$([0-9]+(?:\.[0-9]+)?)/i

      )

      ||

      text.match(

        /Closed\s+\$([0-9]+(?:\.[0-9]+)?)/i

      );


    if (
      closeMatch
    ) {

      const close =
        Number(
          closeMatch[1]
        );


      if (
        close > 0
      ) {

        const ratio =

          displayedPrice /
          close;


        if (
          ratio < 0.15 ||
          ratio > 7
        ) {

          throw new Error(

            `Stocktwits sanity check failed (${displayedPrice} vs ${close})`

          );

        }

      }

    }


    /* --------------------------
       STOCKTWITS TIME
    -------------------------- */

    const updated =

      parseStocktwitsUpdatedTime(
        text
      );


    const marketTimestamp =

      updated?.timestamp ||

      now;


    stocktwitsUpdated =

      updated?.label ||

      null;


    /* --------------------------
       SOURCE NAME
    -------------------------- */

    const label =

      session ===
      "Overnight"

        ? "Stocktwits overnight"

        : session ===
          "Pre-Market"

          ? "Stocktwits pre-market"

          : session ===
            "After-Hours"

            ? "Stocktwits after-hours"

            : "Stocktwits regular";


    /* --------------------------
       STORE IT
    -------------------------- */

    const accepted =
      storeFeed(

        "stocktwits",

        displayedPrice,

        marketTimestamp,

        label,

        session,

        now

      );


    stocktwitsLastSuccess =
      now;


    stocktwitsState =

      session ===
      "Overnight"

        ? "overnight-found"

        : session ===
          "Pre-Market"

          ? "pre-market"

          : session ===
            "After-Hours"

            ? "after-hours"

            : "regular";


    console.log(

      `Stocktwits ${session}: $${displayedPrice}`

      +

      (

        stocktwitsUpdated

          ? ` @ ${stocktwitsUpdated}`

          : ""

      )

    );


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


/* =========================================================
   USD → GBP
========================================================= */

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


/* =========================================================
   FINNHUB REST BACKUP
========================================================= */

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
          ) * 1000

        : Date.now();


    storeFeed(

      "finnhub",

      currentPrice,

      timestamp,

      "Finnhub quote",

      "Market",

      Date.now()

    );


  }

  catch (
    err
  ) {

    /*
      REST failure is not fatal if the
      Finnhub WebSocket is still connected.
    */

    console.error(

      "Finnhub REST:",

      err.message

    );

  }

}


/* =========================================================
   FINNHUB LIVE WEBSOCKET
========================================================= */

let finnhubSocket =
  null;


let reconnectTimer =
  null;


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


        const valid =
          msg.data

            .filter(

              trade =>

                String(
                  trade.s
                ).toUpperCase() ===
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
          !valid.length
        ) {

          return;

        }


        const newest =

          valid[
            valid.length -
            1
          ];


        storeFeed(

          "finnhub",

          Number(
            newest.p
          ),

          Number(
            newest.t
          ) ||
            Date.now(),

          "Finnhub trade",

          "Market",

          Date.now()

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


  finnhubSocket.on(
    "error",
    err => {

      finnhubState =
        "error";


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


/* =========================================================
   JOURNAL WEBSOCKET
========================================================= */

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
      Immediately send current price.
    */

    send(

      client,

      snapshot()

    );

  }
);


/* =========================================================
   KEEP WEBSOCKET ALIVE
========================================================= */

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


/* =========================================================
   1 SECOND JOURNAL HEARTBEAT
========================================================= */

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


/* =========================================================
   ROOT
========================================================= */

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
        "GPRO live journal feed v3",

      logic:
        "Freshest validated market timestamp wins",

      websocket:
        "/ws",

      priceApi:
        "/api/gpro",

      health:
        "/health"

    });

  }
);


/* =========================================================
   GPRO API
========================================================= */

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


    if (
      req.query.refresh ===
      "1"
    ) {

      await Promise.allSettled([

        refreshFinnhub(),

        refreshStocktwits(
          true
        )

      ]);


      chooseActiveFeed();

    }


    res.json({

      ...snapshot(),

      error:
        lastError

    });

  }
);


/* =========================================================
   HEALTH
========================================================= */

app.get(
  "/health",
  (
    req,
    res
  ) => {

    chooseActiveFeed();


    res.json({

      ok:
        true,

      symbol:
        SYMBOL,

      browserClients:
        wss.clients.size,


      hasPrice:
        active.price !==
        null,


      latestPrice:
        active.price,


      latestMarketTimestamp:
        active.marketTs,


      latestSource:
        active.label,


      latestSession:
        active.session,


      latestAgeSeconds:

        active.marketTs

          ? Math.round(

              ageMs(
                active.marketTs
              ) /
                1000

            )

          : null,


      finnhub:
        finnhubState,


      stocktwits:
        stocktwitsState,


      stocktwitsUpdated,


      stocktwitsLastSuccess,


      feeds: {

        finnhub:
          feeds.finnhub,

        stocktwits:
          feeds.stocktwits

      },


      hasFx:
        usdToGbp !==
        null,


      usdToGbp,


      serverTime:
        Date.now(),


      error:
        lastError

    });

  }
);


/* =========================================================
   START
========================================================= */

server.listen(
  PORT,
  "0.0.0.0",
  async () => {

    console.log(

      `GPRO live service v3 listening on port ${PORT}`

    );


    /*
      Load all initial data.
    */

    await Promise.allSettled([

      refreshFx(),

      refreshFinnhub(),

      refreshStocktwits(
        true
      )

    ]);


    chooseActiveFeed();


    /*
      Start live Finnhub trades.
    */

    connectFinnhub();


    /*
      Finnhub REST backup every 15 seconds.
    */

    setInterval(

      refreshFinnhub,

      15000

    );


    /*
      Stocktwits every 15 seconds.

      This allows us to detect session changes:

      Overnight
          ↓
      Pre-market
          ↓
      Regular
          ↓
      After-hours
          ↓
      Overnight
    */

    setInterval(

      () =>
        refreshStocktwits(
          false
        ),

      15000

    );


    /*
      Re-evaluate which source is freshest
      every five seconds.
    */

    setInterval(

      chooseActiveFeed,

      5000

    );


    /*
      Update currency conversion hourly.
    */

    setInterval(

      refreshFx,

      60 *
        60 *
        1000

    );

  }
);
