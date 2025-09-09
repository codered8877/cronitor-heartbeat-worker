// index.js (ESM, no Zapier)

// ---------- Imports ----------
import express from "express";
import fetch from "node-fetch";
import WebSocket from "ws";

// ---------- ENV ----------
const {
  PRODUCT_ID = "BTC-USD",         // e.g., BTC-USD, ETH-USD
  DOM_POLL_MS = "6000",           // min 2000ms
  CVD_EMA_LEN = "34",
  WEBHOOK_API_KEY = "",           // optional: protect /tv endpoint (x-api-key header)
  CRONITOR_URL = "",              // optional: keep-alive ping
  PORT = "3000",
} = process.env;

const domPollMs = Math.max(2000, parseInt(DOM_POLL_MS, 10) || 6000);
const cvdEmaLen = Math.max(2, parseInt(CVD_EMA_LEN, 10) || 34);

// ---------- State ----------
let lastDom = null;               // last DOM snapshot
let lastPayload = null;           // last merged DOM+CVD payload
let lastTvAlert = null;           // last TradingView webhook body

// ---------- EXPRESS ----------
const app = express();
app.use(express.json());

app.get("/healthz", (_req, res) => res.status(200).send("ok"));

app.get("/status", (_req, res) => {
  const out = lastPayload ?? {
    product: PRODUCT_ID,
    note: "No payload yet; waiting for first DOM poll.",
  };
  res.json(out);
});

// Optional: accept external DOM posts (not required — we self-poll)
app.post("/dom", (req, res) => {
  console.log("DOM payload (external):", req.body);
  lastDom = { ...(req.body || {}), ts_recv: new Date().toISOString() };
  res.json({ status: "ok" });
});

// NEW: TradingView → Render webhook
// Set WEBHOOK_API_KEY in Render and send it as header:  x-api-key: <key>
app.post("/tv", (req, res) => {
  if (WEBHOOK_API_KEY && req.headers["x-api-key"] !== WEBHOOK_API_KEY) {
    return res.status(401).json({ error: "unauthorized" });
  }
  lastTvAlert = { body: req.body, ts: new Date().toISOString() };
  console.log("📨 TV alert:", JSON.stringify(lastTvAlert.body));
  return res.json({ ok: true });
});

app.get("/tv/last", (_req, res) => res.json(lastTvAlert ?? { note: "No TV alerts yet." }));

app.listen(PORT, () => {
  console.log(`HTTP server listening on ${PORT}`);
});

// ---------- CRONITOR PINGER (optional) ----------
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

// ---------- DOM (REST L2 snapshot) ----------
async function fetchDOM() {
  try {
    const url = `https://api.exchange.coinbase.com/products/${PRODUCT_ID}/book?level=2`;
    const r = await fetch(url, { headers: { "User-Agent": "render-app" } });
    if (!r.ok) throw new Error(`DOM HTTP ${r.status}`);
    const book = await r.json();

    const bestAsk = book.asks?.[0] ?? null;
    const bestBid = book.bids?.[0] ?? null;

    const p_ask = bestAsk ? parseFloat(bestAsk[0]) : null;
    const q_ask = bestAsk ? parseFloat(bestAsk[1]) : null;
    const p_bid = bestBid ? parseFloat(bestBid[0]) : null;
    const q_bid = bestBid ? parseFloat(bestBid[1]) : null;

    lastDom = {
      type: "dom",
      p_ask,
      q_ask,
      p_bid,
      q_bid,
      sequence: book.sequence ?? null,
      ts: new Date().toISOString(),
    };
    return lastDom;
  } catch (e) {
    console.error("DOM fetch error:", e.message);
    return null;
  }
}

// ---------- CVD (WebSocket matches channel) ----------
let ws = null;
let wsHeartbeat = null;
let cvd = 0; // session cumulative delta volume
let cvdEma = 0;
const alpha = 2 / (cvdEmaLen + 1);

function startCVD() {
  const endpoint = "wss://ws-feed.exchange.coinbase.com";

  function connect() {
    ws = new WebSocket(endpoint);

    ws.on("open", () => {
      console.log("CVD WS open");
      const sub = { type: "subscribe", channels: [{ name: "matches", product_ids: [PRODUCT_ID] }] };
      ws.send(JSON.stringify(sub));
      clearInterval(wsHeartbeat);
      wsHeartbeat = setInterval(() => {
        try { ws.ping(); } catch (_) {}
      }, 25000);
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
      try { ws.close(); } catch (_) {}
    });
  }

  connect();
}
startCVD();

// ---------- MAIN LOOP: Merge DOM + CVD (no outbound posts) ----------
async function tick() {
  const dom = await fetchDOM();
  if (!dom) return;

  const cvdDir = cvd > cvdEma ? "up" : cvd < cvdEma ? "down" : "flat";

  lastPayload = {
    source: "render",
    product: PRODUCT_ID,
    ts: dom.ts,

    // DOM
    p_ask: dom.p_ask,
    q_ask: dom.q_ask,
    p_bid: dom.p_bid,
    q_bid: dom.q_bid,
    sequence: dom.sequence,

    // CVD
    cvd: Number.isFinite(cvd) ? +cvd.toFixed(6) : 0,
    cvd_ema: Number.isFinite(cvdEma) ? +cvdEma.toFixed(6) : 0,
    cvd_dir: cvdDir, // "up" | "down" | "flat"
  };

  console.log("Payload →", lastPayload);
}

setInterval(tick, domPollMs);
console.log(`Started DOM poll @ ${domPollMs}ms, CVD EMA len ${cvdEmaLen}`);
