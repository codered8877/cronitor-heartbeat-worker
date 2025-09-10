// src/index.js
// Full pipeline: DOM poller + CVD WS + /aplus webhook + Zap relay + PG logging
// ESM, Node >=18 (uses global fetch)

import express from "express";
import WebSocket from "ws";
import pkg from "pg";
const { Pool } = pkg;

// ------------------------ ENV ------------------------
const {
  // Instrument
  PRODUCT_ID = "BTC-USD",

  // Poll/EMA tuning
  DOM_POLL_MS = "6000",          // >= 2000ms enforced
  CVD_EMA_LEN = "34",

  // TradingView shared secret (optional)
  TV_API_KEY = "",               // require header 'x-tv-key' when set

  // Zapier relay (optional)
  ZAP_B_URL = "",
  ZAP_API_KEY = "",              // sent as 'x-api-key' if present

  // Server
  PORT = "10000",

  // Postgres (Render dashboard → Databases → Connection info)
  POSTGRES_HOST,
  POSTGRES_PORT,
  POSTGRES_USER,
  POSTGRES_PASSWORD,
  POSTGRES_DB,
} = process.env;

const domPollMs = Math.max(2000, parseInt(DOM_POLL_MS, 10) || 6000);
const cvdEmaLen = Math.max(2, parseInt(CVD_EMA_LEN, 10) || 34);
const alpha = 2 / (cvdEmaLen + 1);

// ------------------------ DB -------------------------
const pool = new Pool({
  host: POSTGRES_HOST,
  port: POSTGRES_PORT ? Number(POSTGRES_PORT) : 5432,
  user: POSTGRES_USER,
  password: POSTGRES_PASSWORD,
  database: POSTGRES_DB,
  max: 3,
  idleTimeoutMillis: 30_000,
});

async function initDB() {
  await pool.query(`
    create table if not exists aplus_events (
      id bigserial primary key,
      ts timestamptz not null,
      product text not null,
      payload jsonb not null
    );
    create index if not exists aplus_events_ts_idx on aplus_events(ts);

    create table if not exists dom_ticks (
      id bigserial primary key,
      ts timestamptz not null,
      product text not null,
      p_ask numeric, q_ask numeric,
      p_bid numeric, q_bid numeric,
      sequence bigint
    );
    create index if not exists dom_ticks_ts_idx on dom_ticks(ts);

    create table if not exists cvd_ticks (
      id bigserial primary key,
      ts timestamptz not null,
      product text not null,
      cvd numeric not null,
      cvd_ema numeric not null
    );
    create index if not exists cvd_ticks_ts_idx on cvd_ticks(ts);
  `);
  console.log("DB ready");
}
initDB().catch(e => console.error("DB init error:", e.message));

// --------------------- EXPRESS -----------------------
const app = express();

// Accept JSON *and* text/plain (TV mobile)
app.use(express.json({ limit: "1mb", type: ["application/json", "text/json"] }));
app.use(express.text({ limit: "1mb", type: ["text/*", "application/x-www-form-urlencoded"] }));

app.get("/healthz", (_req, res) => res.status(200).send("ok"));

/**
 * Helper endpoint to produce a sample APlus payload for Zapier “Test Trigger”
 * GET /zap-test
 */
app.get("/zap-test", (_req, res) => {
  const sample = {
    type: "APlus",
    s: PRODUCT_ID.replace("-", ""),
    t: Date.now(),
    f: "1",
    p: 111234.56,
    d: "LONG",
    e: "APlus",
    sc: 60,
    sr: false,
    R: "LOCATION|MICRO"
  };
  res.json(sample);
});

// ------------- TradingView A+ webhook (SINGLE route) -------------
app.post("/aplus", async (req, res) => {
  try {
    // Shared-secret (optional)
    if (TV_API_KEY && req.headers["x-tv-key"] !== TV_API_KEY) {
      return res.status(401).json({ error: "unauthorized" });
    }

    // TV can send text/plain or JSON
    let raw = req.body;
    let parsed = null;

    if (typeof raw === "string") {
      const s = raw.trim();
      if (s.startsWith("{") && s.endsWith("}")) {
        try { parsed = JSON.parse(s); } catch { /* keep null */ }
      }
    } else if (raw && typeof raw === "object") {
      parsed = raw;
    }

    const payload = parsed || (typeof raw === "string" ? { message: raw } : raw) || {};
    const record = {
      received_at: new Date().toISOString(),
      kind: "aplus",
      product: PRODUCT_ID,
      payload_type: parsed ? "json" : typeof raw,
      payload
    };

    if (payload?.type === "APlus") {
      console.log("📥 APlus JSON:", record);
    } else {
      console.log("📥 APlus (raw/unknown):", record);
    }

    // Log to PG (non-blocking failure)
    pool.query(
      `insert into aplus_events (ts, product, payload) values ($1,$2,$3)`,
      [new Date(), PRODUCT_ID, payload]
    ).catch(e => console.error("PG insert aplus_events error:", e.message));

    // Relay to Zapier if configured
    if (ZAP_B_URL) {
      try {
        const zr = await fetch(ZAP_B_URL, {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            ...(ZAP_API_KEY ? { "x-api-key": ZAP_API_KEY } : {}),
          },
          // Send *exactly* the APlus object so Zapier sees the fields
          body: JSON.stringify(payload),
        });
        if (!zr.ok) {
          const t = await zr.text().catch(() => "");
          console.error("Zap POST failed:", zr.status, t);
        } else {
          console.log("✅ Relayed to Zapier");
        }
      } catch (e) {
        console.error("Zap POST error:", e.message);
      }
    }

    res.json({ ok: true });
  } catch (e) {
    console.error("APlus handler error:", e.message);
    res.status(500).json({ error: e.message });
  }
});

// -------------------- DOM POLLER --------------------
async function fetchDOM() {
  try {
    const url = `https://api.exchange.coinbase.com/products/${PRODUCT_ID}/book?level=2`;
    const r = await fetch(url, { headers: { "User-Agent": "render-app" } });
    if (!r.ok) throw new Error(`DOM HTTP ${r.status}`);
    const book = await r.json();

    const bestAsk = book.asks?.[0] || [];
    const bestBid = book.bids?.[0] || [];

    const dom = {
      type: "dom",
      p_ask: bestAsk[0] ? parseFloat(bestAsk[0]) : null,
      q_ask: bestAsk[1] ? parseFloat(bestAsk[1]) : null,
      p_bid: bestBid[0] ? parseFloat(bestBid[0]) : null,
      q_bid: bestBid[1] ? parseFloat(bestBid[1]) : null,
      sequence: book.sequence ?? null,
      ts: new Date().toISOString(),
    };

    console.log("DOM →", dom);

    // Log to PG
    pool.query(
      `insert into dom_ticks (ts, product, p_ask, q_ask, p_bid, q_bid, sequence)
       values ($1,$2,$3,$4,$5,$6,$7)`,
      [new Date(), PRODUCT_ID, dom.p_ask, dom.q_ask, dom.p_bid, dom.q_bid, dom.sequence]
    ).catch(e => console.error("PG insert dom_ticks error:", e.message));

    return dom;
  } catch (e) {
    console.error("DOM fetch error:", e.message);
    return null;
  }
}
setInterval(fetchDOM, domPollMs);
console.log(`Started DOM poll @ ${domPollMs}ms`);

// ------------------- CVD WEBSOCKET ------------------
let ws = null;
let wsHeartbeat = null;
let cvd = 0;     // session cumulative delta volume
let cvdEma = 0;

function startCVD() {
  const endpoint = "wss://ws-feed.exchange.coinbase.com";

  function connect() {
    ws = new WebSocket(endpoint);

    ws.on("open", () => {
      console.log("CVD WS open");
      ws.send(JSON.stringify({
        type: "subscribe",
        channels: [{ name: "matches", product_ids: [PRODUCT_ID] }],
      }));
      clearInterval(wsHeartbeat);
      wsHeartbeat = setInterval(() => { try { ws.ping(); } catch {} }, 25_000);
    });

    ws.on("message", (buf) => {
      try {
        const msg = JSON.parse(buf.toString());
        if (msg.type === "match" && msg.product_id === PRODUCT_ID) {
          const size = parseFloat(msg.size || "0");
          const side = msg.side; // "buy" | "sell"
          const signed = side === "buy" ? size : side === "sell" ? -size : 0;

          cvd += signed;
          cvdEma = cvdEma === 0 ? cvd : alpha * cvd + (1 - alpha) * cvdEma;
        }
      } catch (e) {
        console.error("CVD msg parse error:", e.message);
      }
    });

    ws.on("close", () => {
      console.log("CVD WS closed; reconnecting in 3s");
      clearInterval(wsHeartbeat);
      setTimeout(connect, 3000);
    });

    ws.on("error", (err) => {
      console.error("CVD WS error:", err.message);
      try { ws.close(); } catch {}
    });
  }

  connect();
}
startCVD();

// Heartbeat log + store to PG every 15s
setInterval(() => {
  const row = {
    type: "cvd",
    cvd: Number.isFinite(cvd) ? +cvd.toFixed(6) : 0,
    cvd_ema: Number.isFinite(cvdEma) ? +cvdEma.toFixed(6) : 0,
    ts: new Date().toISOString(),
  };
  console.log("CVD →", row);

  pool.query(
    `insert into cvd_ticks (ts, product, cvd, cvd_ema) values ($1,$2,$3,$4)`,
    [new Date(), PRODUCT_ID, row.cvd, row.cvd_ema]
  ).catch(e => console.error("PG insert cvd_ticks error:", e.message));
}, 15_000);

console.log(`CVD EMA len ${cvdEmaLen}`);

// -------------------- START SERVER -------------------
app.listen(Number(PORT), () => {
  console.log(`HTTP server listening on ${PORT}`);
});
