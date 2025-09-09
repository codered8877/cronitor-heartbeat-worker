// server.js  (ESM)
import express from 'express';

const CRONITOR_URL = process.env.CRONITOR_URL;
const ZAP_API_KEY  = process.env.ZAP_API_KEY || null;

if (!CRONITOR_URL) {
  console.error('Missing CRONITOR_URL env var');
  process.exit(1);
}

const app = express();

// Parse JSON bodies (needed for Zapier POST)
app.use(express.json());

// Health check (Render uses this)
app.get('/healthz', (_req, res) => res.status(200).send('ok'));

// Zapier will POST DOM data here
app.post('/dom', (req, res) => {
  // Optional simple auth
  if (ZAP_API_KEY && req.headers['x-api-key'] !== ZAP_API_KEY) {
    return res.status(401).json({ error: 'unauthorized' });
  }

  // Fields you mapped in Zapier step 5
  const {
    btc_dom_top_price,
    btc_dom_top_ask_qty,
    btc_dom_top_bid_price,
    btc_dom_top_bid_qty,
    btc_dom_top_ask_orders,
    btc_dom_top_bid_orders,
    ts_iso,
    sequence,
    auction_mode,
  } = req.body || {};

  // Minimal validation
  if (typeof btc_dom_top_price === 'undefined') {
    return res.status(400).json({ error: 'missing btc_dom_top_price' });
  }

  console.log('DOM payload:', req.body); // View in Render → Logs
  return res.status(200).json({ status: 'ok' });
});

// Start HTTP server
const port = process.env.PORT || 3000;
app.listen(port, () => {
  console.log(`HTTP server listening on ${port}`);
});

// Cronitor heartbeat (every 15s)
async function pingOnce() {
  try {
    const r = await fetch(CRONITOR_URL, { method: 'GET' });
    if (!r.ok) console.error('Ping failed', r.status);
    else console.log('Ping ok', new Date().toISOString());
  } catch (e) {
    console.error('Ping error', e.message);
  }
}
setInterval(pingOnce, 15000);p
