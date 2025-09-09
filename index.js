// index.js (ESM)

import express from "express";
import fetch from "node-fetch";
import WebSocket from "ws";

// ---------- ENV ----------
const {
  PRODUCT_ID = "BTC-USD",
  DOM_POLL_MS = "6000",
  CVD_EMA_LEN = "34",
  ZAP_B_URL = "",
  ZAP_API_KEY = "",
  CRONITOR_URL = "",
  PORT = "3000",
} = process.env;

const domPollMs = Math.max(2000, parseInt(DOM_POLL_MS, 10) || 6000);
const cvdEmaLen = Math.max(2, parseInt(CVD_EMA_LEN, 10) || 34);

// ---------- EXPRESS (keep-alive http for Render) ----------
const app = express();
app.use(express.json());

app.get("/healthz", (_req, res) => res.status(200).send("ok"));

// Optional: accept DOM posts (not required when we self-poll)
app.post("/dom", (req, res) => {
  if (ZAP_API_KEY && req.headers["x-api-key"] !== ZAP_API_KEY) {
    return res.status(401).json({ error: "unauthorized" });
  }
  console.log("DOM payload (external):", req.body);
  return res.json({ status: "ok" });
});

app.listen(PORT, () => {
  console.log(`HTTP server listening on ${PORT}`);
});

// ---------- CRONITOR PINGER (optional/keep) ----------
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

// ---------- ZAP POST HELPER ----------
async function postToZap(payload) {
  if (!ZAP_B_URL) return; // allow running with no Zap during tests
  try {
    const r = await fetch(ZAP_B_URL, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "x-api-key": ZAP_API_KEY || "",
      },
      body: JSON.stringify(payload),
    });
    if (!r.ok) {
      const t = await r.text().catch(() => "");
      console.error("Zap POST failed:", r.status, t);
    } else {
      console.log("✅ Posted to Zap B");
    }
  } catch (e) {
    console.error("Zap POST error:", e.message);
  }
}

// ---------- DOM (REST L2 snapshot) ----------
async function fetchDOM() {
  try {
    // Coinbase Exchange public REST (level 2)
    const url = `https://api.exchange.coinbase.com/products/${PRODUCT_ID}/book?level=2`;
    const r = await fetch(url, { headers: { "User-Agent": "render-app" } });
    if (!r.ok) throw new Error(`DOM HTTP ${r.status}`);
    const book = await r.json();

    // Top of book
    const bestAsk = book.asks && book.asks[0] ? book.asks[0] : null;
    const bestBid = book.bids && book.bids[0] ? book.bids[0] : null;

    const p_ask = bestAsk ? parseFloat(bestAsk[0]) : null;
    const q_ask = bestAsk ? parseFloat(bestAsk[1]) : null;
    const p_bid = bestBid ? parseFloat(bestBid[0]) : null;
    const q_bid = bestBid ? parseFloat(bestBid[1]) : null;

    return {
      type: "dom",
      p_ask,
      q_ask,
      p_bid,
      q_bid,
      sequence: book.sequence ?? null,
      ts: new Date().toISOString(),
    };
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
      // subscribe to matches for our product
      const sub = {
        type: "subscribe",
        channels: [{ name: "matches", product_ids: [PRODUCT_ID] }],
      };
      ws.send(JSON.stringify(sub));
      // simple heartbeat
      clearInterval(wsHeartbeat);
      wsHeartbeat = setInterval(() => {
        try {
          ws.ping();
        } catch (_) {}
      }, 25000);
    });

    ws.on("message", (buf) => {
      try {
        const msg = JSON.parse(buf.toString());
        // "match" messages carry trade side/size
        if (msg.type === "match" && msg.product_id === PRODUCT_ID) {
          const size = parseFloat(msg.size || "0");
          const side = msg.side; // "buy" or "sell"
          const signed = side === "buy" ? size : side === "sell" ? -size : 0;

          cvd += signed;
          // EMA of CVD for stability (like your indicator)
          cvdEma = cvdEma === 0 ? cvd : (alpha * cvd) + (1 - alpha) * cvdEma;
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

// ---------- MAIN LOOP: Merge DOM + CVD and send ----------
async function tick() {
  const dom = await fetchDOM();
  if (!dom) return;

  const cvdDir = cvd > cvdEma ? "up" : cvd < cvdEma ? "down" : "flat";

  const payload = {
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

  console.log("Payload →", payload);
  await postToZap(payload);
}

setInterval(tick, domPollMs);
console.log(`Started DOM poll @ ${domPollMs}ms, CVD EMA len ${cvdEmaLen}`);
