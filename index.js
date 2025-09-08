import express from 'express';

const url = process.env.CRONITOR_URL;
if (!url) {
  console.error('Missing CRONITOR_URL env var');
  process.exit(1);
}

// --- tiny web server so Render sees an open port ---
const app = express();
app.get('/healthz', (_req, res) => res.status(200).send('ok'));
const port = process.env.PORT || 3000;
app.listen(port, () => {
  console.log(`HTTP server listening on :${port}`);
});

// --- 15s Cronitor pinger ---
async function pingOnce() {
  try {
    const res = await fetch(url, { method: 'POST' });
    if (!res.ok) {
      console.error('Ping failed', res.status, await res.text());
    } else {
      console.log('Ping ok', new Date().toISOString());
    }
  } catch (e) {
    console.error('Ping error', e.message);
  }
}
pingOnce();                 // fire immediately on boot
setInterval(pingOnce, 15000); // then every 15s
