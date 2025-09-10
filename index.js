// src/index.js
// Full pipeline: TradingView -> Render (/aplus) -> (Zapier -> ChatGPT)
// + DOM poller, CVD websocket, Postgres logging, nightly retention

import express from "express";
import WebSocket from "ws";
import { Pool } from "pg";

// ---------- ENV ----------
const {
  PRODUCT_ID = "BTC-USD",

  // TradingView shared secret (optional)
  TV_API_KEY = "", // require header 'x-tv-key' if set

  // Zapier relay (optional)
  ZAP_B_URL = "",          // Zap trigger/catch URL
  ZAP_API_KEY = "",        // sent as 'x-api-key' header (Zap should verify)

  // Optional: extra DOM fanout target (legacy)
  ZAP_DOM_URL = "",
  ZAP_DOM_API_KEY = "",

  // Coinbase poll + CVD
  DOM_POLL_MS = "6000",
  CVD_EMA_LEN = "34",

  // Keep-alive (optional)
  CRONITOR_URL = "",

  // Server
  PORT = "10000",

  // Postgres (prefer URL, else individual parts)
  POSTGRES_URL = "",
  POSTGRES_HOST = "",
  POSTGRES_PORT = "5432",
  POSTGRES_DB = "",
  POSTGRES_USER = "",
  POSTGRES_PASSWORD = ""
} = process.env;

const domPollMs = Math.max(2000, parseInt(DOM_POLL_MS, 10) || 6000);
const cvdEmaLen = Math.max(2, parseInt(CVD_EMA_LEN, 10) || 34);
const alpha = 2 / (cvdEmaLen + 1);

// ---------- DB ----------
const pool = new Pool(
  POSTGRES_URL
    ? { connectionString: POSTGRES_URL, ssl: { rejectUnauthorized: false } }
    : {
        host: POSTGRES_HOST,
        port: Number(POSTGRES_PORT || 5432),
        database: POSTGRES_DB,
        user: POSTGRES_USER,
        password: POSTGRES_PASSWORD,
        ssl: { rejectUnauthorized: false }
      }
);

// create tables once
await pool.query(`
  create table if not exists aplus_events (
    id bigserial primary key,
    ts timestamptz not null default now(),
    symbol text,
    dir text,
    score int,
    reasons text,
    raw jsonb
  );

  create table if not exists dom_ticks (
    id bigserial primary key,
    ts timestamptz not null default now(),
    product text,
    p_ask numeric,
    q_ask numeric,
    p_bid numeric,
    q_bid numeric,
    sequence bigint
  );

  create table if not exists cvd_ticks (
    id bigserial primary key,
    ts timestamptz not null default now(),
    product text,
    cvd numeric,
    cvd_ema numeric
  );

  create table if not exists trade_feedback (
    id bigserial primary key,
    ts timestamptz not null default now(),
    event_id bigint,
    decision text,   -- 'taken' | 'skipped'
    outcome text,    -- 'win' | 'loss' | 'be'
    rr numeric,      -- realized R multiple
    notes text
  );
`);

// ---------- helpers ----------
const app = express();
app.use(express.json({ limit: "1mb", type: ["application/json", "text/json"] }));
app.use(express.text({ limit: "1mb", type: ["text/*", "application/x-www-form-urlencoded"] }));

const safeJSON = (maybe) => {
  if (typeof maybe === "string") {
    const s = maybe.trim();
    if (s.startsWith("{") && s.endsWith("}")) {
      try { return JSON.parse(s); } catch { return null; }
    }
    return null;
  }
  return (maybe && typeof maybe === "object") ? maybe : null;
};

async function relayToZap(tag, payload) {
  if (!ZAP_B_URL) return;
  try {
    const r = await fetch(ZAP_B_URL, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        ...(ZAP_API_KEY ? { "x-api-key": ZAP_API_KEY } : {}),
        "x-pipe-tag": tag
      },
      body: JSON.stringify({
        tag,
        product: PRODUCT_ID,
        payload,
        ts: new Date().toISOString()
      })
    });
    if (!r.ok) {
      const t = await r.text().catch(() => "");
      console.error("Zap POST failed:", r.status, t);
    } else {
      console.log(`✅ Relayed to Zap (${tag})`);
    }
  } catch (e) {
    console.error("Zap POST error:", e.message);
  }
}

// ---------- /aplus (single route) ----------
app.post("/aplus", async (req, res) => {
  // require TradingView shared secret if configured
  if (TV_API_KEY && req.headers["x-tv-key"] !== TV_API_KEY) {
    return res.status(401).json({ error: "unauthorized" });
  }

  const raw = req.body ?? null;
  const parsed = safeJSON(raw) || raw; // keep as string if TV sent plain text

  // Compact MP payload example:
  // {type:"APlus", s:"BTCUSD", t:..., f:"1", p:..., d:"LONG", e:"APlus", sc:60, sr:false, R:"LOCATION|MICRO"}

  let symbol = null, dir = null, score = null, reasons = null;
  try {
    if (parsed && typeof parsed === "object" && parsed.type === "APlus") {
      symbol = parsed.s || null;
      dir = parsed.d || null;
      score = Number.isFinite(+parsed.sc) ? +parsed.sc : null;
      reasons = parsed.R || null;

      await pool.query(
        `insert into aplus_events(symbol, dir, score, reasons, raw)
         values ($1,$2,$3,$4,$5)`,
        [symbol, dir, score, reasons, parsed]
      );
      console.log("📥 APlus JSON saved:", { symbol, dir, score, reasons });
    } else {
      await pool.query(
        `insert into aplus_events(symbol, dir, score, reasons, raw)
         values ($1,$2,$3,$4,$5)`,
        [null, null, null, null, parsed ?? null]
      );
      console.log("📥 APlus (raw/unknown) saved.");
    }

    // Optional: forward to Zapier (Zap should require header X-Api-Key == ZAP_API_KEY)
    await relayToZap("aplus", parsed);

    res.json({ ok: true });
  } catch (e) {
    console.error("APlus save/relay error:", e.message);
    res.status(500).json({ error: "server_error" });
  }
});

// ---------- health ----------
app.get("/healthz", (_req, res) => res.status(200).send("ok"));

// ---------- Cronitor ping ----------
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

// ---------- DOM poller ----------
async function fetchDOM() {
  try {
    const url = `https://api.exchange.coinbase.com/products/${PRODUCT_ID}/book?level=2`;
    const r = await fetch(url, { headers: { "User-Agent": "render-app" } });
    if (!r.ok) throw new Error(`DOM HTTP ${r.status}`);
    const book = await r.json();

    const bestAsk = book.asks?.[0] || null;
    const bestBid = book.bids?.[0] || null;

    const p_ask = bestAsk ? parseFloat(bestAsk[0]) : null;
    const q_ask = bestAsk ? parseFloat(bestAsk[1]) : null;
    const p_bid = bestBid ? parseFloat(bestBid[0]) : null;
    const q_bid = bestBid ? parseFloat(bestBid[1]) : null;

    return {
      product: PRODUCT_ID,
      p_ask, q_ask, p_bid, q_bid,
      sequence: book.sequence ?? null,
      ts: new Date().toISOString()
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
  try {
    await pool.query(
      `insert into dom_ticks(product, p_ask, q_ask, p_bid, q_bid, sequence, ts)
       values ($1,$2,$3,$4,$5,$6,$7)`,
      [dom.product, dom.p_ask, dom.q_ask, dom.p_bid, dom.q_bid, dom.sequence, dom.ts]
    );
  } catch (e) {
    console.error("DOM save error:", e.message);
  }

  // optional fanout
  if (ZAP_DOM_URL) {
    try {
      const r = await fetch(ZAP_DOM_URL, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          ...(ZAP_DOM_API_KEY ? { "x-api-key": ZAP_DOM_API_KEY } : {})
        },
        body: JSON.stringify({ type: "dom", ...dom })
      });
      if (!r.ok) console.error("DOM POST failed:", r.status);
    } catch (e) {
      console.error("DOM POST error:", e.message);
    }
  }
}
setInterval(domTick, domPollMs);
console.log(`Started DOM poll @ ${domPollMs}ms`);

// ---------- CVD websocket ----------
let ws = null;
let wsHeartbeat = null;
let cvd = 0;
let cvdEma = 0;

function startCVD() {
  const endpoint = "wss://ws-feed.exchange.coinbase.com";

  const connect = () => {
    ws = new WebSocket(endpoint);

    ws.on("open", () => {
      console.log("CVD WS open");
      ws.send(JSON.stringify({
        type: "subscribe",
        channels: [{ name: "matches", product_ids: [PRODUCT_ID] }]
      }));
      clearInterval(wsHeartbeat);
      wsHeartbeat = setInterval(() => { try { ws.ping(); } catch {} }, 25000);
    });

    ws.on("message", (buf) => {
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
  };

  connect();
}
startCVD();

// flush a heartbeat row every 15s
setInterval(async () => {
  const row = {
    product: PRODUCT_ID,
    cvd: Number.isFinite(cvd) ? +cvd.toFixed(6) : 0,
    cvd_ema: Number.isFinite(cvdEma) ? +cvdEma.toFixed(6) : 0,
    ts: new Date().toISOString()
  };
  console.log("CVD →", row);
  try {
    await pool.query(
      `insert into cvd_ticks(product, cvd, cvd_ema, ts) values ($1,$2,$3,$4)`,
      [row.product, row.cvd, row.cvd_ema, row.ts]
    );
  } catch (e) {
    console.error("CVD save error:", e.message);
  }
}, 15000);

// ---------- nightly retention (02:30 UTC) ----------
function msUntil(hourUTC, minUTC) {
  const now = new Date();
  const next = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate(), hourUTC, minUTC, 0));
  if (next <= now) next.setUTCDate(next.getUTCDate() + 1);
  return next - now;
}
async function runRetention() {
  try {
    await pool.query(`
      -- keep A+ events ~180 days
      delete from aplus_events where ts < now() - interval '180 days';
      -- keep noisy DOM/CVD ~14 days
      delete from dom_ticks where ts < now() - interval '14 days';
      delete from cvd_ticks where ts < now() - interval '14 days';
      -- optional space reclaim
      vacuum analyze aplus_events;
      vacuum analyze dom_ticks;
      vacuum analyze cvd_ticks;
      vacuum analyze trade_feedback;
    `);
    console.log("🧹 Retention done.");
  } catch (e) {
    console.error("Retention error:", e.message);
  }
}
setTimeout(() => {
  runRetention();
  setInterval(runRetention, 24 * 3600 * 1000);
}, msUntil(2, 30)); // start at ~02:30 UTC

// ---------- start server ----------
app.listen(PORT, () => {
  console.log(`HTTP server listening on ${PORT}`);
  console.log(`CVD EMA len ${cvdEmaLen}`);
});
