// src/index.js  (ESM)
import express from 'express';

// ====== ENV ======
const {
  CRONITOR_URL,               // required (your existing heartbeat URL)
  ZAP_API_KEY,                // optional: header auth for /dom
  ZAP_B_URL,                  //
  PRODUCT_ID = 'BTC-USD',     // which market to poll
  POLL_MS = '15000',          // poll interval in ms (string env -> number below)
  PORT = '10000',             // Render sets PORT; default for local
} = process.env;

async function sendToZapier(payload) {
  if (!ZAP_B_URL) return; // If no Zap configured, skip silently

  try {
    await fetch(ZAP_B_URL, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "x-api-key": ZAP_API_KEY || "", // Must match Zapier header
      },
      body: JSON.stringify(payload),
    });
    console.log("✅ Sent DOM payload to Zapier");
  } catch (err) {
    console.error("❌ Zapier POST failed:", err.message);
  }
}

if (!CRONITOR_URL) {
  console.error('Missing CRONITOR_URL env var');
  process.exit(1);
}

const pollMs = Number(POLL_MS) || 15000;

// ====== APP (tiny HTTP server so Render sees a web service) ======
const app = express();
app.use(express.json());

// Health check that Render calls
app.get('/healthz', (_req, res) => res.status(200).send('ok'));

// (Optional) Keep the /dom endpoint for backwards compatibility / testing
app.post('/dom', (req, res) => {
  if (ZAP_API_KEY && req.headers['x-api-key'] !== ZAP_API_KEY) {
    return res.status(401).json({ error: 'unauthorized' });
  }
  console.log('DOM payload (from webhook):', JSON.stringify(req.body));
  return res.status(200).json({ status: 'ok' });
});

app.listen(Number(PORT), () => {
  console.log(`HTTP server listening on ${PORT}`);
});

// ====== Cronitor pinger (unchanged) ======
async function pingOnce() {
  try {
    const r = await fetch(CRONITOR_URL, { method: 'GET' });
    if (!r.ok) console.error('Ping failed', r.status);
    else console.log('Ping ok', new Date().toISOString());
  } catch (e) {
    console.error('Ping error', e.message);
  }
}
setInterval(pingOnce, 15000);

// ====== Coinbase DOM poll (public endpoint; no API key needed) ======
// Docs: https://api.exchange.coinbase.com/products/<product_id>/book?level=1
// Returns top-of-book (best bid/ask) with price and size.
const CB_EX_BASE = 'https://api.exchange.coinbase.com';

async function pollDomOnce() {
  const url = `${CB_EX_BASE}/products/${encodeURIComponent(
    PRODUCT_ID
  )}/book?level=1`;

  try {
    const r = await fetch(url, {
      // public market data; just set JSON & a UA
      headers: {
        'Accept': 'application/json',
        'User-Agent': 'render-btc-dom/1.0',
      },
      method: 'GET',
    });

    if (!r.ok) {
      // 429/5xx etc. — log and bail; next tick will retry
      console.error('poll error:', PRODUCT_ID, r.status);
      return;
    }

    const data = await r.json();

    // Expected shape:
    // { bids: [ [price, size, numOrders], ... ],
    //   asks: [ [price, size, numOrders], ... ] }
    const topBid = Array.isArray(data.bids) && data.bids[0] ? data.bids[0] : null;
    const topAsk = Array.isArray(data.asks) && data.asks[0] ? data.asks[0] : null;

    if (!topBid || !topAsk) {
      console.error('poll error: empty book for', PRODUCT_ID);
      return;
    }

    // Parse to numbers
    const [bidPriceStr, bidQtyStr, bidOrdersStr] = topBid;
    const [askPriceStr, askQtyStr, askOrdersStr] = topAsk;

    const payload = {
      // Keep field names consistent with your Zapier step
      btc_dom_top_price: Number(askPriceStr),        // you were mapping this as "Top Ask Price"
      btc_dom_top_ask_qty: Number(askQtyStr),
      btc_dom_top_bid_price: Number(bidPriceStr),
      btc_dom_top_bid_qty: Number(bidQtyStr),
      btc_dom_top_ask_orders: Number(askOrdersStr ?? 1),
      btc_dom_top_bid_orders: Number(bidOrdersStr ?? 1),

      ts_iso: new Date().toISOString(),
      sequence: Date.now(),          // no seq on this endpoint; use ms timestamp
      auction_mode: false,
      product_id: PRODUCT_ID,
      source: 'coinbase-exchange-public', // for clarity in logs
    };

    // Send clean DOM payload to Zapier
sendToZapier({
  type: "dom_update",
  price: payload.btc_dom_top_price,
  bids: payload.btc_dom_top_bid_qty,
  asks: payload.btc_dom_top_ask_qty,
  ts: payload.ts_iso,
});
    
    // Log a compact line you can tail in Render Logs
    console.log(
      'DOM',
      JSON.stringify({
        p_ask: payload.btc_dom_top_price,
        q_ask: payload.btc_dom_top_ask_qty,
        p_bid: payload.btc_dom_top_bid_price,
        q_bid: payload.btc_dom_top_bid_qty,
        t: payload.ts_iso,
      })
    );

    // If you later want to forward this internally, do it here
    // e.g., POST to another route, push to a DB/queue, etc.

  } catch (e) {
    console.error('poll error:', e.message);
  }
}

// Start polling loop
setInterval(pollDomOnce, pollMs);
// Kick off immediately on boot
pollDomOnce();
