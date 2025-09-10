// index.js — Full pipeline: DOM poller + CVD WS + /aplus webhook + (optional) Zap relay
// ESM compatible (Render Node 18+)

import express from "express";
import fetch from "node-fetch";
import WebSocket from "ws";

// ------------------------ ENV ------------------------
const {
  // Core
  PRODUCT_ID = "BTC-USD",

  // Poll/EMA tuning
  DOM_POLL_MS = "6000",       // min 2000ms enforced below
  CVD_EMA_LEN = "34",         // EMA length for CVD smoothing

  // Optional: TradingView webhook shared secret (header)
  TV_API_KEY = "",            // if set, require: 'x-tv-key' header to match

  // Optional: relay to Zapier (kept, but harmless if empty)
  ZAP_B_URL = "",             // Zapier step URL (optional)
  ZAP_API_KEY = "",           // if used, sent as 'x-api-key' (optional)

  // Optional: external DOM post target (legacy compat)
  ZAP_DOM_URL = "",           // optional: where to POST DOM snapshots
  ZAP_DOM_API_KEY = "",       // optional header for DOM posts

  // Keep-alive pinger (optional)
  CRONITOR_URL = "",

  // Server
  PORT = "3000",
} = process.env;

const domPollMs = Math.max(2000, parseInt(DOM_POLL_MS, 10) || 6000);
const cvdEmaLen = Math.max(2, parseInt(CVD_EMA_LEN, 10) || 34);
const alpha = 2 / (cvdEmaLen + 1);

// --------------------- EXPRESS APP -------------------
const app = express();

// Accept JSON or plain text (TradingView mobile often sends text/plain)
app.use(express.json({ limit: "1mb", type: ["application/json", "text/json"] }));
app.use(express.text({ limit: "1mb", type: ["text/*", "application/x-www-form-urlencoded"] }));

app.get("/healthz", (_req, res) => res.status(200).send("ok"));

// (Legacy helper) accept external DOM if ever used
app.post("/dom", (req, res) => {
  if (ZAP_API_KEY && req.headers["x-api-key"] !== ZAP_API_KEY) {
    return res.status(401).json({ error: "unauthorized" });
  }
  console.log("DOM payload (external):", req.body);
  return res.json({ status: "ok" });
});

// ------------- TradingView A+ webhook (/aplus) -------------
app.post("/aplus", (req, res) => {
  // optional shared-secret guard
  if (TV_API_KEY && req.headers["x-tv-key"] !== TV_API_KEY) {
    return res.status(401).json({ error: "unauthorized" });
  }

  // TV can post JSON or just a string (esp. when message is empty on mobile)
  let raw = req.body;
  let parsed = null;

  if (typeof raw === "string") {
    // Sometimes empty or a generic text — try to parse JSON if it looks like it
    const maybeJson = raw.trim().startsWith("{") && raw.trim().endsWith("}");
    if (maybeJson) {
      try { parsed = JSON.parse(raw); } catch { parsed = null; }
    }
  } else if (raw && typeof raw === "object") {
    parsed = raw;
  }

  // Normalize a record for logs
  const record = {
    received_at: new Date().toISOString(),
    kind: "aplus",
    product: PRODUCT_ID,
    payload_type: parsed ? "json" : (typeof raw),
    payload: parsed || raw || null,
  };

  // Soft schema detect (works with our compact Pine payloads)
  // Expected keys if compact: { type:"APlus", s, t, f, p, d, e, sc, sr, R }
  if (parsed && parsed.type === "APlus") {
    console.log("📥 APlus JSON:", record);
  } else {
    console.log("📥 APlus (raw/unknown):", record);
  }

  // (Optional) relay the A+ to a Zap if env present
  postToZapSafe(parsed || raw, "aplus");

  return res.json({ ok: true });
});

// Start server
app.listen(PORT, () => {
  console.log(`HTTP server listening on ${PORT}`);
});

// --------------- CRONITOR KEEP-ALIVE ----------------
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

// --------------- OPTIONAL ZAP RELAY -----------------
async function postToZapSafe(payload, tag = "event") {
  if (!ZAP_B_URL) return; // harmless if unset
  try {
    const r = await fetch(ZAP_B_URL, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        ...(ZAP_API_KEY ? { "x-api-key": ZAP_API_KEY } : {}),
        "x-pipe-tag": tag,
      },
      body: JSON.stringify({ tag, product: PRODUCT_ID, payload, ts: new Date().toISOString() }),
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

// -------------------- DOM POLLER --------------------
async function fetchDOM() {
  try {
    const url = `https://api.exchange.coinbase.com/products/${PRODUCT_ID}/book?level=2`;
    const r = await fetch(url, { headers: { "User-Agent": "render-app" } });
    if (!r.ok) throw new Error(`DOM HTTP ${r.status}`);
    const book = await r.json();

    const bestAsk = book.asks && book.asks[0] ? book.asks[0] : null;
    const bestBid = book.bids && book.bids[0] ? book.bids[0] : null;

    const p_ask = bestAsk ? parseFloat(bestAsk[0]) : null;
    const q_ask = bestAsk ? parseFloat(bestAsk[1]) : null;
    const p_bid = bestBid ? parseFloat(bestBid[0]) : null;
    const q_bid = bestBid ? parseFloat(bestBid[1]) : null;

    return {
      type: "dom",
      p_ask, q_ask, p_bid, q_bid,
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

  // (Optional) fan out DOM to a URL if provided
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
      if (!r.ok) {
        console.error("DOM POST failed:", r.status);
      }
    } catch (e) {
      console.error("DOM POST error:", e.message);
    }
  }
}
setInterval(domTick, domPollMs);
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
      const sub = {
        type: "subscribe",
        channels: [{ name: "matches", product_ids: [PRODUCT_ID] }],
      };
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
          const side = msg.side; // "buy" or "sell"
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

setInterval(() => {
  // lightweight heartbeat log for CVD
  console.log("CVD →", {
    type: "cvd",
    cvd: Number.isFinite(cvd) ? +cvd.toFixed(6) : 0,
    cvd_ema: Number.isFinite(cvdEma) ? +cvdEma.toFixed(6) : 0,
    ts: new Date().toISOString(),
  });
}, 15000);

console.log(`CVD EMA len ${cvdEmaLen}`);
