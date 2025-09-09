// src/index.js  (ESM)
import express from 'express';

const { CRONITOR_URL, ZAP_API_KEY, PORT = 3000 } = process.env;
if (!CRONITOR_URL) {
  console.error('Missing CRONITOR_URL env var');
  process.exit(1);
}

const app = express();
app.use(express.json());

// Health check (Render)
app.get('/healthz', (_req, res) => res.status(200).send('ok'));

// POST target for Zapier
app.post('/dom', (req, res) => {
  if (ZAP_API_KEY && req.headers['x-api-key'] !== ZAP_API_KEY) {
    return res.status(401).json({ error: 'unauthorized' });
  }
  console.log('DOM payload:', JSON.stringify(req.body));
  return res.status(200).json({ status: 'ok' });
});

// Start server
app.listen(PORT, () => console.log(`HTTP server listening on ${PORT}`));

// Cronitor heartbeat every 15s
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
