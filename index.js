import fetch from 'node-fetch';

const url = process.env.CRONITOR_URL;
if (!url) {
  console.error('Missing CRONITOR_URL environment variable.');
  process.exit(1);
}

async function ping() {
  try {
    const res = await fetch(url, { method: 'GET', timeout: 8000 });
    if (!res.ok) {
      console.error('Ping failed with status', res.status);
    } else {
      console.log('Ping ok', new Date().toISOString());
    }
  } catch (e) {
    console.error('Ping error', e.message);
  }
}

// First ping immediately
await ping();

// Then ping every 15 seconds
setInterval(ping, 15000);
