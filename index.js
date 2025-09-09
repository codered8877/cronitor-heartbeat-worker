// src/index.js  (ESM)
import express from 'express';

const { CRONITOR_URL, ZAP_API_KEY, PORT } = process.env;
if (!CRONITOR_URL) {
  console.error('Missing CRONITOR_URL env var');
  process.exit(1);
}

const app = express();
app.use(express.json());

// --- health check for Render ---
app.get('/healthz', (_req, res) => res.status(200).send('ok'));

// --- simple in-memory buffer of recent DOM events ---
const RECENT_MAX = 100;
const recentDomEvents = [];

// --- Zapier will POST here ---
app.post('/dom', (req, res) => {
  // optional header auth
  if (ZAP_API_KEY && req.headers['x-api-key'] !== ZAP_API_KEY) {
    return res.status(401).json({ error: 'unauthorized' });
  }

  const event = {
    type: 'dom_event',
    ts: new Date().toISOString(),
    ip: req.headers['x-forwarded-for'] || req.socket?.remoteAddress || null,
    ua: req.headers['user-agent'] || null,
    payload: req.body || {},
  };

  // one-line structured log for Render Logs
  console.log(JSON.stringify(event));

  // keep a rolling buffer
  recentDomEvents.push(event);
  if (recentDomEvents.length > RECENT_MAX) recentDomEvents.shift();

  return res.status(200).json({ status: 'ok' });
});

// --- quick peek endpoint (GET) ---
app.get('/dom/last', (_req, res) => {
  res.status(200).json({
    count: recentDomEvents.length,
    latest: recentDomEvents.slice(-10),
  });
});

// --- start server ---
const port = Number(PORT) || 3000;
app.listen(port, () => console.log(`HTTP server listening on ${port}`));

// --- Cronitor pinger (15s) ---
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
