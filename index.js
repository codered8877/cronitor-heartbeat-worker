// src/index.js
// Full pipeline: Express + /aplus webhook + Zap relay + Postgres logging
// + DOM poller + CVD websocket. ESM, Node 18+ (Render compatible).

import express from "express";
import WebSocket from "ws";
import pg from "pg";

// --------------------------- ENV ---------------------------
const {
  // Core
  PRODUCT_ID = "BTC-USD",

  // Poll/EMA tuning
  DOM_POLL_MS = "6000",
  CVD_EMA_LEN = "34",

  // Shared secrets (optional)
  TV_API_KEY = "",          // expect request header: x-tv-key
  ZAP_API_KEY = "",         // sent to Zap as x-api-key (and used to guard /dom)

  // Zapier (optional)
  ZAP_B_URL = "",           // your Zap "catch hook" URL

  // Optional fan-out for DOM (legacy)
  ZAP_DOM_URL = "",
  ZAP_DOM_API_KEY = "",

  // Postgres
  POSTGRES_URL = "",

  // Keep-alive
  CRONITOR_URL = "",

  // Server
  PORT = "10000",           // Render Web Service default is fine
} = process.env;

const domPollMs = Math.max(2000, parseInt(DOM_POLL_MS, 10) || 6000);
const cvdEmaLen = Math.max(2, parseInt(CVD_EMA_LEN, 10) || 34);
const alpha = 2 / (cvdEmaLen + 1);

// ------------------------ DATABASE -------------------------
const { Pool } = pg;
const pool = POSTGRES_URL
  ? new Pool({
      connectionString: POSTGRES_URL,
      ssl: { rejectUnauthorized: false },
    })
  : null;

async function initDB() {
  if (!pool) return;
  await pool.query(`
    create table if not exists aplus_events (
      id bigserial primary key,
      received_at timestamptz not null default now(),
      product text not null,
      raw jsonb not null
    );
    create table if not exists dom_ticks (
      id bigserial primary key,
      ts timestamptz not null,
      product text not null,
      p_ask numeric,
      q_ask numeric,
      p_bid numeric,
      q_bid numeric,
      sequence bigint
    );
    create table if not exists cvd_ticks (
      id bigserial primary key,
      ts timestamptz not null,
      product text not null,
      cvd numeric,
      cvd_ema numeric
    );
    create table if not exists trade_feedback (
      id bigserial primary key,
      ts timestamptz not null default now(),
      product text not null,
      event_id bigint,
      label text,         -- 'win' | 'loss' | ...
      notes text
    );
  `);
  console.log("DB ready");
}

async function logAPlus(raw) {
  if (!pool) return;
  try {
    await pool.query(
      `insert into aplus_events (product, raw) values ($1, $2)`,
      [PRODUCT_ID, raw]
    );
  } catch (e) {
    console.error("DB aplus insert error:", e.message);
  }
}

async function logDOM(dom) {
  if (!pool) return;
  try {
    await pool.query(
      `insert into dom_ticks (ts, product, p_ask, q_ask, p_bid, q_bid, sequence)
       values ($1,$2,$3,$4,$5,$6,$7)`,
      [dom.ts, PRODUCT_ID, dom.p_ask, dom.q_ask, dom.p_bid, dom.q_bid, dom.sequence ?? null]
    );
  } catch (e) {
    console.error("DB dom insert error:", e.message);
  }
}

async function logCVD(state) {
  if (!pool) return;
  try {
    await pool.query(
      `insert into cvd_ticks (ts, product, cvd, cvd_ema)
       values ($1,$2,$3,$4)`,
      [state.ts, PRODUCT_ID, state.cvd, state.cvd_ema]
    );
  } catch (e) {
    console.error("DB cvd insert error:", e.message);
  }
}

// ----------------------- EXPRESS APP -----------------------
const app = express();

// Accept JSON or text (TradingView mobile often posts text/plain)
app.use(express.json({ limit: "1mb", type: ["application/json", "text/json"] }));
app.use(express.text({ limit: "1mb", type: ["text/*", "application/x-www-form-urlencoded"] }));

app.get("/healthz", (_req, res) => res.status(200).send("ok"));

// (Optional) quick Zap test endpoint (for Zapier “Catch Hook” test)
app.get("/zap-test", async (req, res) => {
  try {
    if (!ZAP_B_URL) return res.status(400).json({ error: "ZAP_B_URL not set" });
    const key = req.query.key || req.headers["x-api-key"];
    if (ZAP_API_KEY && key !== ZAP_API_KEY) {
      return res.status(401).json({ error: "unauthorized" });
    }
    const sample = {
      tag: "aplus",
      product: PRODUCT_ID,
      payload: {
        type: "APlus",
        s: PRODUCT_ID.replace("-", ""),
        t: Date.now(),
        f: "1",
        p: 111111.11,
        d: "LONG",
        e: "APlus",
        sc: 60,
        sr: false,
        R: "LOCATION|MICRO",
      },
      ts: new Date().toISOString(),
    };
    const r = await fetch(ZAP_B_URL, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        ...(ZAP_API_KEY ? { "x-api-key": ZAP_API_KEY } : {}),
        "x-pipe-tag": "aplus-test",
      },
      body: JSON.stringify(sample),
    });
    const text = await r.text();
    return res.json({ ok: r.ok, status: r.status, body: text.slice(0, 300) });
  } catch (e) {
    console.error("zap-test error:", e);
    return res.status(500).json({ error: e.message });
  }
});

// (Legacy helper) accept external DOM fan-in (guarded by Zap key if set)
app.post("/dom", (req, res) => {
  if (ZAP_API_KEY && req.headers["x-api-key"] !== ZAP_API_KEY) {
    return res.status(401).json({ error: "unauthorized" });
  }
  console.log("DOM payload (external):", req.body);
  return res.json({ status: "ok" });
});

// -------------------- /aplus WEBHOOK -----------------------
app.post("/aplus", async (req, res) => {
  // Optional TradingView shared-secret guard
  if (TV_API_KEY && req.headers["x-tv-key"] !== TV_API_KEY) {
    return res.status(401).json({ error: "unauthorized" });
  }

  // TV on mobile may be text/plain. Try to parse if it looks like JSON.
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

  const record = {
    received_at: new Date().toISOString(),
    product: PRODUCT_ID,
    payload_type: parsed ? "json" : typeof raw,
    payload: parsed || raw || null,
  };

  // Log to console + DB
  if (parsed?.type === "APlus") {
    console.log("📥 APlus JSON:", record);
  } else {
    console.log("📥 APlus (raw/unknown):", record);
  }
  await logAPlus(record);

  // Optional relay to Zapier
  if (ZAP_B_URL) {
    postToZap({ tag: "aplus", product: PRODUCT_ID, payload: parsed || raw, ts: record.received_at })
      .catch(e => console.error("Zap relay error:", e.message));
  }

  return res.json({ ok: true });
});

// --------------------- START SERVER ------------------------
app.listen(PORT, () => {
  console.log(`HTTP server listening on ${PORT}`);
});

// ----------------- CRONITOR KEEP-ALIVE ---------------------
function pingCronitor() {
  if (!CRONITOR_URL) return;
  fetch(CRONITOR_URL).catch(e => console.error("Cronitor ping error:", e.message));
}
setInterval(pingCronitor, 15000);

// --------------------- ZAP RELAY ---------------------------
async function postToZap(obj) {
  if (!ZAP_B_URL) return;
  const r = await fetch(ZAP_B_URL, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      ...(ZAP_API_KEY ? { "x-api-key": ZAP_API_KEY } : {}),
      "x-pipe-tag": obj.tag || "event",
    },
    body: JSON.stringify(obj),
  });
  if (!r.ok) {
    const t = await r.text().catch(() => "");
    console.error("Zap POST failed:", r.status, t);
  } else {
    console.log(`✅ Relayed to Zap (${obj.tag})`);
  }
}

// ---------------------- DOM POLLER -------------------------
async function fetchDOM() {
  try {
    const url = `https://api.exchange.coinbase.com/products/${PRODUCT_ID}/book?level=2`;
    const r = await fetch(url, { headers: { "User-Agent": "render-app" } });
    if (!r.ok) throw new Error(`DOM HTTP ${r.status}`);
    const book = await r.json();

    const a = book.asks?.[0] ?? null;
    const b = book.bids?.[0] ?? null;

    const dom = {
      type: "dom",
      p_ask: a ? parseFloat(a[0]) : null,
      q_ask: a ? parseFloat(a[1]) : null,
      p_bid: b ? parseFloat(b[0]) : null,
      q_bid: b ? parseFloat(b[1]) : null,
      sequence: book.sequence ?? null,
      ts: new Date().toISOString(),
    };

    console.log("DOM →", dom);
    await logDOM(dom);

    if (ZAP_DOM_URL) {
      try {
        const rr = await fetch(ZAP_DOM_URL, {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            ...(ZAP_DOM_API_KEY ? { "x-api-key": ZAP_DOM_API_KEY } : {}),
          },
          body: JSON.stringify(dom),
        });
        if (!rr.ok) console.error("DOM POST failed:", rr.status);
      } catch (e) {
        console.error("DOM POST error:", e.message);
      }
    }
  } catch (e) {
    console.error("DOM fetch error:", e.message);
  }
}
setInterval(fetchDOM, domPollMs);
console.log(`Started DOM poll @ ${domPollMs}ms`);

// --------------------- CVD WEBSOCKET -----------------------
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
      ws.send(
        JSON.stringify({
          type: "subscribe",
          channels: [{ name: "matches", product_ids: [PRODUCT_ID] }],
        })
      );
      clearInterval(wsHeartbeat);
      wsHeartbeat = setInterval(() => {
        try { ws.ping(); } catch (_e) {}
      }, 25000);
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
      try { ws.close(); } catch (_e) {}
    });
  }

  connect();
}
startCVD();

setInterval(() => {
  const snapshot = {
    type: "cvd",
    cvd: Number.isFinite(cvd) ? +cvd.toFixed(6) : 0,
    cvd_ema: Number.isFinite(cvdEma) ? +cvdEma.toFixed(6) : 0,
    ts: new Date().toISOString(),
  };
  console.log("CVD →", snapshot);
  logCVD(snapshot);
}, 15000);

console.log(`CVD EMA len ${cvdEmaLen}`);

// -------------------- BOOTSTRAP DB ------------------------
initDB().catch((e) => console.error("DB init error:", e.message));
