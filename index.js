// index.js — A+ webhook → (optional) Zap relay → Postgres logging
// + DOM poller + CVD websocket + /feedback + /healthz
// ESM (type: module), Node 18+ (global fetch available)

import express from "express";
import WebSocket from "ws";
import pg from "pg";

// --------------------------- ENV ---------------------------
const {
  // Core
  PRODUCT_ID = "BTC-USD",

  // Poll/EMA
  DOM_POLL_MS = "6000",       // minimum enforced below
  CVD_EMA_LEN = "34",

  // Security
  TV_API_KEY = "",            // require 'x-tv-key' on /aplus if set
  ZAP_API_KEY = "",           // used when we POST to Zapier (header x-api-key)

  // Optional relays
  ZAP_B_URL = "",             // Zapier step (optional)
  ZAP_DOM_URL = "",           // OPTIONAL external fan-out for DOM
  ZAP_DOM_API_KEY = "",       // header for DOM fan-out

  // Health
  CRONITOR_URL = "",          // optional GET ping every 15s

  // Postgres
  POSTGRES_URL = "",          // full URI (recommended!); else individual vars
  POSTGRES_HOST = "",
  POSTGRES_PORT = "5432",
  POSTGRES_DB   = "",
  POSTGRES_USER = "",
  POSTGRES_PASSWORD = "",

  // Server
  PORT = "3000",
} = process.env;

const domPollMs = Math.max(2000, parseInt(DOM_POLL_MS, 10) || 6000);
const cvdEmaLen = Math.max(2, parseInt(CVD_EMA_LEN, 10) || 34);
const alpha = 2 / (cvdEmaLen + 1);

// -------------------------- DB ----------------------------
const pgCfg = POSTGRES_URL
  ? { connectionString: POSTGRES_URL, ssl: { rejectUnauthorized: false } }
  : {
      host: POSTGRES_HOST,
      port: Number(POSTGRES_PORT) || 5432,
      database: POSTGRES_DB,
      user: POSTGRES_USER,
      password: POSTGRES_PASSWORD,
      ssl: { rejectUnauthorized: false },
    };

const pool = new pg.Pool(pgCfg);

async function initDb() {
  await pool.query(`
    create table if not exists aplus_events (
      id bigserial primary key,
      ts timestamptz not null default now(),
      product text not null,
      dir text,          -- LONG/SHORT
      score int,         -- sc
      reasons text,      -- R (pipe list)
      spider_rejected boolean,
      raw jsonb
    );

    create table if not exists dom_ticks (
      id bigserial primary key,
      ts timestamptz not null default now(),
      product text not null,
      p_ask numeric, q_ask numeric,
      p_bid numeric, q_bid numeric,
      sequence bigint
    );

    create table if not exists cvd_ticks (
      id bigserial primary key,
      ts timestamptz not null default now(),
      product text not null,
      cvd numeric,
      cvd_ema numeric
    );

    create table if not exists trade_feedback (
      id bigserial primary key,
      ts timestamptz not null default now(),
      product text not null,
      event_ts timestamptz,    -- optional, link back to alert time
      outcome text not null,   -- win/loss/breakeven/skip
      rr numeric,              -- realized R multiple
      notes text
    );

    create index if not exists idx_aplus_ts on aplus_events(ts);
    create index if not exists idx_dom_ts   on dom_ticks(ts);
    create index if not exists idx_cvd_ts   on cvd_ticks(ts);
    create index if not exists idx_fb_ts    on trade_feedback(ts);
  `);
  console.log("DB ready ✅");
}

async function logAPlus(rec) {
  const { product, dir, score, reasons, spider_rejected, raw } = rec;
  await pool.query(
    `insert into aplus_events (product, dir, score, reasons, spider_rejected, raw)
     values ($1,$2,$3,$4,$5,$6)`,
    [product, dir, score, reasons, spider_rejected ?? null, raw ?? null]
  );
}

async function logDOM(dom) {
  const { p_ask, q_ask, p_bid, q_bid, sequence } = dom;
  await pool.query(
    `insert into dom_ticks (product, p_ask, q_ask, p_bid, q_bid, sequence)
     values ($1,$2,$3,$4,$5,$6)`,
    [PRODUCT_ID, p_ask, q_ask, p_bid, q_bid, sequence ?? null]
  );
}

async function logCVD(c) {
  const { cvd, cvd_ema } = c;
  await pool.query(
    `insert into cvd_ticks (product, cvd, cvd_ema)
     values ($1,$2,$3)`,
    [PRODUCT_ID, cvd, cvd_ema]
  );
}

// ------------------------ APP --------------------------
const app = express();

// Accept JSON and plain text (TV mobile can send text/plain)
app.use(express.json({ limit: "1mb", type: ["application/json", "text/json"] }));
app.use(express.text({ limit: "1mb", type: ["text/*", "application/x-www-form-urlencoded"] }));

// Health
app.get("/healthz", (_req, res) => res.status(200).send("ok"));

// -------------- SINGLE TradingView /aplus ----------------
//
// Security rule you asked for earlier still holds:
// - Only continue if the 'x-tv-key' header equals TV_API_KEY (when TV_API_KEY is set).
//
app.post("/aplus", async (req, res) => {
  if (TV_API_KEY && req.headers["x-tv-key"] !== TV_API_KEY) {
    return res.status(401).json({ error: "unauthorized" });
  }

  // TV may deliver JSON or raw text
  let raw = req.body;
  let parsed = null;

  if (typeof raw === "string") {
    const s = raw.trim();
    if (s.startsWith("{") && s.endsWith("}")) {
      try { parsed = JSON.parse(s); } catch { parsed = null; }
    }
  } else if (raw && typeof raw === "object") {
    parsed = raw;
  }

  // We expect compact A+ payload: { type:"APlus", s, t, f, p, d, e, sc, sr, R }
  const isAPlus = parsed && parsed.type === "APlus";

  const rec = {
    product: PRODUCT_ID,
    dir: isAPlus ? parsed.d : null,
    score: isAPlus ? Number(parsed.sc) : null,
    reasons: isAPlus ? String(parsed.R || "") : null,
    spider_rejected: isAPlus ? Boolean(parsed.sr) : null,
    raw: parsed ?? raw ?? null,
  };

  try {
    await logAPlus(rec);
  } catch (e) {
    console.error("DB log APlus error:", e.message);
  }

  // Optional: relay to Zap
  if (ZAP_B_URL) {
    try {
      const r = await fetch(ZAP_B_URL, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          ...(ZAP_API_KEY ? { "x-api-key": ZAP_API_KEY } : {}),
          "x-pipe-tag": "aplus",
        },
        body: JSON.stringify({
          tag: "aplus",
          product: PRODUCT_ID,
          payload: parsed ?? raw ?? null,
          ts: new Date().toISOString(),
        }),
      });
      if (!r.ok) {
        console.error("Zap POST failed:", r.status, await r.text().catch(() => ""));
      } else {
        console.log("✅ Relayed to Zap (aplus)");
      }
    } catch (e) {
      console.error("Zap POST error:", e.message);
    }
  }

  console.log(isAPlus ? "📥 APlus JSON stored" : "📥 APlus (raw/unknown) stored");
  return res.json({ ok: true });
});

// ---------- Feedback: record trade outcomes (manual/automation) ----------
app.post("/feedback", async (req, res) => {
  // Optional: protect with same key you use for Zap relays
  if (ZAP_API_KEY && req.headers["x-api-key"] !== ZAP_API_KEY) {
    return res.status(401).json({ error: "unauthorized" });
  }

  const { outcome, rr = null, notes = "", event_ts = null } =
    typeof req.body === "string" ? JSON.parse(req.body) : req.body;

  if (!outcome) return res.status(400).json({ error: "missing outcome" });

  try {
    await pool.query(
      `insert into trade_feedback (product, outcome, rr, notes, event_ts)
       values ($1,$2,$3,$4,$5)`,
      [PRODUCT_ID, String(outcome), rr, String(notes), event_ts]
    );
    res.json({ ok: true });
  } catch (e) {
    console.error("feedback insert error:", e.message);
    res.status(500).json({ error: "db_error" });
  }
});

// ----------------------- DOM POLLER -----------------------
async function fetchDOM() {
  try {
    const url = `https://api.exchange.coinbase.com/products/${PRODUCT_ID}/book?level=2`;
    const r = await fetch(url, { headers: { "User-Agent": "render-app" } });
    if (!r.ok) throw new Error(`DOM HTTP ${r.status}`);
    const book = await r.json();

    const bestAsk = book.asks?.[0] || null;
    const bestBid = book.bids?.[0] || null;

    return {
      type: "dom",
      p_ask: bestAsk ? parseFloat(bestAsk[0]) : null,
      q_ask: bestAsk ? parseFloat(bestAsk[1]) : null,
      p_bid: bestBid ? parseFloat(bestBid[0]) : null,
      q_bid: bestBid ? parseFloat(bestBid[1]) : null,
      sequence: book.sequence ?? null,
      ts: new Date().toISOString(),
    };
  } catch (e) {
    console.error("DOM fetch error:", e.message);
    return null;
  }
}

async function domTick() {
  const dom = await fetchDOM();
  if (!dom) return;

  console.log("DOM →", dom);

  // Log to DB
  try { await logDOM(dom); } catch (e) { console.error("DB log DOM error:", e.message); }

  // Optional fan-out
  if (ZAP_DOM_URL) {
    try {
      const r = await fetch(ZAP_DOM_URL, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          ...(ZAP_DOM_API_KEY ? { "x-api-key": ZAP_DOM_API_KEY } : {}),
        },
        body: JSON.stringify(dom),
      });
      if (!r.ok) console.error("DOM POST failed:", r.status);
    } catch (e) {
      console.error("DOM POST error:", e.message);
    }
  }
}
setInterval(domTick, domPollMs);
console.log(`Started DOM poll @ ${domPollMs}ms`);

// -------------------- CVD WEBSOCKET ----------------------
let ws = null;
let wsHeartbeat = null;
let cvd = 0;
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
      wsHeartbeat = setInterval(() => { try { ws.ping(); } catch {} }, 25000);
    });

    ws.on("message", async (buf) => {
      try {
        const msg = JSON.parse(buf.toString());
        if (msg.type === "match" && msg.product_id === PRODUCT_ID) {
          const size = parseFloat(msg.size || "0");
          const signed = msg.side === "buy" ? size : msg.side === "sell" ? -size : 0;
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

setInterval(async () => {
  const snapshot = {
    type: "cvd",
    cvd: Number.isFinite(cvd) ? +cvd.toFixed(6) : 0,
    cvd_ema: Number.isFinite(cvdEma) ? +cvdEma.toFixed(6) : 0,
    ts: new Date().toISOString(),
  };
  console.log("CVD →", snapshot);
  try { await logCVD(snapshot); } catch (e) { console.error("DB log CVD error:", e.message); }
}, 15000);

console.log(`CVD EMA len ${cvdEmaLen}`);

// -------------------- CRONITOR PING ----------------------
async function pingCronitor() {
  if (!CRONITOR_URL) return;
  try {
    const r = await fetch(CRONITOR_URL, { method: "GET" });
    if (!r.ok) console.error("Cronitor ping failed:", r.status);
  } catch (e) {
    console.error("Cronitor ping error:", e.message);
  }
}
setInterval(pingCronitor, 15000);

// ---------------------- BOOT ----------------------------
await initDb();

app.listen(Number(PORT), () => {
  console.log(`HTTP server listening on ${PORT}`);
});
