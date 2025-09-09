// index.mjs (ESM)
// Requires Node 18+ (global fetch), Express 4.x
import express from 'express';

// ---------- Env ----------
const CRONITOR_URL = process.env.CRONITOR_URL || null;
const ZAP_B_URL    = process.env.ZAP_B_URL || null;
const ZAP_API_KEY  = process.env.ZAP_API_KEY || null;

const DOM_PRODUCT  = process.env.DOM_PRODUCT || 'BTC-USD';
const DOM_LEVEL    = process.env.DOM_LEVEL || '2';        // 1,2,3 supported by Coinbase
const DOM_POLL_MS  = Number(process.env.DOM_POLL_MS || 15000);

// ---------- Web server (Render needs an HTTP service) ----------
const app = express();
app.use(express.json());

// Health check (keep-alive / uptime pingers)
app.get('/healthz', (_req, res) => res.status(200).send('ok'));

// Optional: existing DOM ingest (kept)
app.post('/dom', (req, res) => {
  if (ZAP_API_KEY && req.headers['x-api-key'] !== ZAP_API_KEY) {
    return res.status(401).json({ error: 'unauthorized' });
  }
  console.log('DOM payload (POST /dom):', req.body);
  return res.status(200).json({ status: 'ok' });
});

const port = process.env.PORT || 3000;
app.listen(port, () => console.log(`HTTP server listening on ${port}`));

// ---------- Cronitor heartbeat (optional, if you configured CRONITOR_URL) ----------
async function pingCronitor() {
  if (!CRONITOR_URL) return;
  try {
    const r = await fetch(CRONITOR_URL, { method: 'GET' });
    if (!r.ok) console.error('Cronitor ping failed', r.status);
    else console.log('Cronitor ping ok', new Date().toISOString());
  } catch (e) {
    console.error('Cronitor ping error:', e.message);
  }
}
// keep your existing cadence; or call inside the same 15s loop if you like
if (CRONITOR_URL) setInterval(pingCronitor, 60_000);

// ---------- Coinbase REST poller ----------
const coinbaseRestBase = 'https://api.exchange.coinbase.com'; // public, free

async function fetchOrderBook(product = DOM_PRODUCT, level = DOM_LEVEL) {
  const url = `${coinbaseRestBase}/products/${encodeURIComponent(product)}/book?level=${encodeURIComponent(level)}`;
  const r = await fetch(url, {
    method: 'GET',
    headers: {
      // Public data; UA header helps avoid some anti-bot edges
      'User-Agent': 'render-dom-poller/1.0',
      'Accept': 'application/json'
    },
  });
  if (!r.ok) throw new Error(`Coinbase ${r.status}`);
  return r.json();
}

// Transform Coinbase book → small, typed payload
function toTopOfBookPayload(bookJson) {
  // Coinbase returns arrays of strings:
  // bids: [[price, size, num-orders], ...]
  // asks: [[price, size, num-orders], ...]
  const bestBid = (bookJson?.bids?.[0]) || null;
  const bestAsk = (bookJson?.asks?.[0]) || null;

  const [bidPriceStr, bidQtyStr, bidOrdersStr] = bestBid || [null, null, null];
  const [askPriceStr, askQtyStr, askOrdersStr] = bestAsk || [null, null, null];

  const bidPrice  = bidPriceStr ? Number(bidPriceStr) : null;
  const askPrice  = askPriceStr ? Number(askPriceStr) : null;
  const bidQty    = bidQtyStr ? Number(bidQtyStr) : null;
  const askQty    = askQtyStr ? Number(askQtyStr) : null;
  const bidOrders = bidOrdersStr ? Number(bidOrdersStr) : null;
  const askOrders = askOrdersStr ? Number(askOrdersStr) : null;

  const mid = (bidPrice && askPrice) ? (bidPrice + askPrice) / 2 : null;
  const spread = (bidPrice && askPrice) ? (askPrice - bidPrice) : null;

  return {
    // naming consistent with your Zap B fields
    symbol: DOM_PRODUCT,
    level: Number(DOM_LEVEL),
    ts_iso: new Date().toISOString(),
    sequence: bookJson?.sequence ?? null,

    btc_dom_top_bid_price: bidPrice,
    btc_dom_top_bid_qty: bidQty,
    btc_dom_top_bid_orders: bidOrders,

    btc_dom_top_ask_price: askPrice,
    btc_dom_top_ask_qty: askQty,
    btc_dom_top_ask_orders: askOrders,

    btc_dom_mid: mid,
    btc_dom_spread: spread,

    source: 'coinbase_rest',
  };
}

// ---------- Forward to Zap B (optional) ----------
async function postToZapB(payload) {
  if (!ZAP_B_URL) {
    console.log('ZAP_B_URL not set; skipping forward. Payload:', payload);
    return;
  }
  const headers = { 'Content-Type': 'application/json' };
  if (ZAP_API_KEY) headers['x-api-key'] = ZAP_API_KEY;

  const r = await fetch(ZAP_B_URL, {
    method: 'POST',
    headers,
    body: JSON.stringify(payload),
  });
  if (!r.ok) {
    const txt = await r.text().catch(() => '');
    throw new Error(`Zap B POST failed ${r.status}: ${txt.slice(0, 200)}`);
  }
}

// ---------- Main 15s loop with gentle backoff ----------
let timer = null;
let backoffMs = DOM_POLL_MS;

async function pollOnce() {
  try {
    const book = await fetchOrderBook();
    const payload = toTopOfBookPayload(book);
    console.log('Top-of-book:', payload);

    await postToZapB(payload);   // remove/keep depending on your pipeline
    backoffMs = DOM_POLL_MS;     // reset backoff on success
  } catch (err) {
    console.error('poll error:', err.message || err);
    // exponential-ish backoff: cap at 60s
    backoffMs = Math.min(backoffMs * 1.5, 60_000);
  } finally {
    // jitter ±10% to avoid thundering herd
    const jitter = Math.round(backoffMs * (0.9 + Math.random() * 0.2));
    timer = setTimeout(pollOnce, jitter);
  }
}

// start
pollOnce();
