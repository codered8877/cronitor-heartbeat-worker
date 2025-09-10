// index.js — clean server + workers + safe Postgres init (Render-ready)
// Node 20+ (uses global fetch). "type": "module" in package.json.
// index.js  — full app
// ESM enable in package.json:  "type": "module"
import express from 'express';
import bodyParser from 'body-parser';
import fetch from 'node-fetch';
import crypto from 'crypto';
import { Pool } from 'pg';
import WebSocket from 'ws';

/* =========================================================
   1) Config & helpers
   ========================================================= */
const PORT = Number(process.env.PORT || 10000);

// Coinbase product (ex: BTC-USD)
const DOM_PRODUCT = process.env.DOM_PRODUCT || process.env.PRODUCT_ID || 'BTC-USD';

// Poll/stream cadence
const DOM_POLL_MS = Number(process.env.DOM_POLL_MS || 6000);
const CVD_EMA_LEN = Number(process.env.CVD_EMA_LEN || 34);

// Zapier relay
const ZAP_URL = process.env.ZAP_B_URL || '';
const ZAP_API_KEY = process.env.ZAP_API_KEY || '';

// TradingView webhook shared secret
const TV_SHARED = process.env.TV_API_KEY || '';

// Cronitor heartbeat (optional)
const CRONITOR_URL = process.env.CRONITOR_URL || '';

// Single-source Postgres bits (Render)
const DB = {
  host: process.env.POSTGRES_HOST,
  port: Number(process.env.POSTGRES_PORT || 5432),
  database: process.env.POSTGRES_DB,
  user: process.env.POSTGRES_USER,
  password: process.env.POSTGRES_PASSWORD,
  ssl: { rejectUnauthorized: false } // Render requires SSL
};

function enc(s) { return encodeURIComponent(s ?? ''); }
const CONN_STR =
  `postgresql://${enc(DB.user)}:${enc(DB.password)}@${DB.host}:${DB.port}/${enc(DB.database)}?sslmode=require`;

// Safe boot log (no password)
console.log('==> BOOT CONFIG', {
  PORT,
  DOM_PRODUCT,
  DOM_POLL_MS,
  CVD_EMA_LEN,
  ZAP_URL: ZAP_URL ? 'set' : 'unset',
  TV_SHARED: TV_SHARED ? 'set' : 'unset',
  CRONITOR_URL: CRONITOR_URL ? 'set' : 'unset',
  DB: { host: DB.host, port: DB.port, database: DB.database, user: DB.user, ssl: true }
});

// Hard guard
['host','port','database','user','password'].forEach(k=>{
  if(!DB[k]) console.error(`!! MISSING DB.${k.toUpperCase()}`);
});

/* =========================================================
   2) Postgres: pool & schema bootstrap
   ========================================================= */
const pool = new Pool({ connectionString: CONN_STR, ssl: DB.ssl });

async function bootstrapDb() {
  await pool.query(`
    create table if not exists dom_ticks (
      id bigserial primary key,
      p_ask numeric,
      q_ask numeric,
      p_bid numeric,
      q_bid numeric,
      sequence bigint,
      ts timestamptz default now()
    );
  `);
  await pool.query(`
    create table if not exists cvd_ticks (
      id bigserial primary key,
      cvd numeric,
      cvd_ema numeric,
      ts timestamptz default now()
    );
  `);

  const who = await pool.query('select current_user, current_database()');
  console.log('==> DB connected as:', who.rows[0]);
}

/* =========================================================
   3) Express app
   ========================================================= */
const app = express();
app.use(bodyParser.json({ limit: '1mb' }));

// health
app.get('/', (_req, res) => res.send('OK'));

// TradingView -> /aplus (secured by TV_SHARED)
app.post('/aplus', async (req, res) => {
  try {
    const auth = (req.headers['x-tv-key'] || req.headers['x-shared-secret'] || '').toString();
    if (!TV_SHARED || auth !== TV_SHARED) {
      return res.status(401).json({ ok:false, error:'unauthorized' });
    }

    // Forward payload to Zapier if present
    if (ZAP_URL) {
      await fetch(ZAP_URL, {
        method: 'POST',
        headers: {
          'content-type': 'application/json',
          ...(ZAP_API_KEY ? { 'x-api-key': ZAP_API_KEY } : {})
        },
        body: JSON.stringify({ source:'aplus', payload:req.body, product:DOM_PRODUCT })
      });
    }

    return res.json({ ok:true });
  } catch (err) {
    console.error('[/aplus] error', err);
    return res.status(500).json({ ok:false, error: err.message });
  }
});

// manual test to Zapier
app.post('/zap-test', async (_req, res) => {
  try {
    if (!ZAP_URL) return res.status(400).json({ ok:false, error:'ZAP_B_URL not set' });
    const sent = { type:'test', ts: new Date().toISOString() };
    const r = await fetch(ZAP_URL, {
      method: 'POST',
      headers: {
        'content-type':'application/json',
        ...(ZAP_API_KEY ? { 'x-api-key': ZAP_API_KEY } : {})
      },
      body: JSON.stringify(sent)
    });
    res.json({ ok:true, status:r.status });
  } catch (e) {
    console.error('[/zap-test]', e);
    res.status(500).json({ ok:false, error:e.message });
  }
});

/* =========================================================
   4) Coinbase Advanced Trade — DOM & Trades (CVD)
   ========================================================= */
// Coinbase Advanced Trade WebSocket endpoint
const CB_WS = 'wss://advanced-trade-ws.coinbase.com';

let ws;
let wsTimer;
let lastSeq = 0;

// Exponential moving average helper
function emaFactory(span) {
  let prev;
  const alpha = 2 / (span + 1);
  return (val) => {
    if (prev === undefined) prev = val;
    else prev = alpha * val + (1 - alpha) * prev;
    return prev;
  };
}
const cvdEma = emaFactory(CVD_EMA_LEN);

let cvd = 0; // cumulative volume delta

function wsConnect() {
  if (ws) try { ws.close(); } catch {}
  console.log('==> Opening Coinbase WS…', CB_WS);
  ws = new WebSocket(CB_WS);

  ws.on('open', () => {
    console.log('==> WS open. Subscribing…');
    const sub = {
      type: 'subscribe',
      product_ids: [DOM_PRODUCT],
      channels: [
        // level2_data gives best bid/ask updates; ticker for trades
        { name: 'l2_data', product_ids: [DOM_PRODUCT] },
        { name: 'ticker', product_ids: [DOM_PRODUCT] }
      ]
    };
    ws.send(JSON.stringify(sub));
    // keepalive ping
    wsTimer = setInterval(() => {
      try { ws.ping?.(); } catch {}
    }, 20000);
  });

  ws.on('message', async (data) => {
    try {
      const msg = JSON.parse(data.toString());

      // Level 2 best bid/ask snapshot/update
      if (msg.type === 'l2_data' && msg.product_id === DOM_PRODUCT && msg.events?.length) {
        // For simplicity, compute top-of-book from events' book if present
        const e = msg.events[0];
        if (e.type === 'snapshot' || e.type === 'update') {
          // best prices
          const bids = e.bids?.map(b => [Number(b.price_level), Number(b.new_quantity || b.quantity)]) || [];
          const asks = e.asks?.map(a => [Number(a.price_level), Number(a.new_quantity || a.quantity)]) || [];

          const p_bid = bids.length ? bids.reduce((m,b)=> b[0]>m?b[0]:m, 0) : null;
          const q_bid = bids.find(b => b[0] === p_bid)?.[1] ?? null;

          const p_ask = asks.length ? asks.reduce((m,a)=> m===0? a[0] : (a[0]<m?a[0]:m), 0) : null;
          const q_ask = asks.find(a => a[0] === p_ask)?.[1] ?? null;

          const sequence = Number(msg.sequence || ++lastSeq);
          const ts = new Date().toISOString();

          console.log('DOM → {',
            '\n  p_ask:', p_ask, ',',
            '\n  q_ask:', q_ask, ',',
            '\n  p_bid:', p_bid, ',',
            '\n  q_bid:', q_bid, ',',
            '\n  sequence:', sequence, ',',
            `\n  ts: '${ts}'\n}`
          );

          // Insert DOM tick
          try {
            await pool.query(
              `insert into dom_ticks (p_ask,q_ask,p_bid,q_bid,sequence,ts) values ($1,$2,$3,$4,$5,$6)`,
              [p_ask, q_ask, p_bid, q_bid, sequence, ts]
            );
          } catch (dbErr) {
            console.error('PG insert dom_ticks error:', dbErr.message);
          }
        }
      }

      // Ticker trades — rough CVD estimator:
      // if last trade price >= mid -> buy volume; else sell volume
      if (msg.type === 'ticker' && msg.product_id === DOM_PRODUCT) {
        const price = Number(msg.price);
        const size  = Number(msg.last_size || msg.size || 0);
        if (!price || !size) return;

        // We don't have mid here; approximate buy/sell by side flag if present, else rate of change
        const side = (msg.side || '').toLowerCase(); // 'buy' or 'sell'
        const delta = side === 'buy' ? size : side === 'sell' ? -size : 0;
        cvd += delta;
        const cvd_ema = cvdEma(cvd);
        const ts = new Date().toISOString();

        console.log('CVD → {',
          "\n  type: 'cvd',",
          '\n  cvd:', cvd.toFixed(6), ',',
          '\n  cvd_ema:', Number(cvd_ema.toFixed?.(6) ?? cvd_ema), ',',
          `\n  ts: '${ts}'\n}`
        );

        try {
          await pool.query(
            `insert into cvd_ticks (cvd, cvd_ema, ts) values ($1,$2,$3)`,
            [cvd, cvd_ema, ts]
          );
        } catch (dbErr) {
          console.error('PG insert cvd_ticks error:', dbErr.message);
        }
      }
    } catch (err) {
      // parsing or db
      console.error('WS msg error:', err.message);
    }
  });

  ws.on('close', (code, reason) => {
    console.warn('==> WS closed', code, reason?.toString?.());
    clearInterval(wsTimer);
    // Backoff reconnect
    setTimeout(wsConnect, 3000);
  });

  ws.on('error', (err) => {
    console.error('==> WS error', err.message);
  });
}

/* =========================================================
   5) Dom “poll tick” (optional, if you also used REST ping)
   ========================================================= */
async function cronitorPing(label='') {
  if (!CRONITOR_URL) return;
  try {
    await fetch(CRONITOR_URL, { method:'GET' });
    if (label) console.log('Cronitor ping:', label);
  } catch (e) {
    console.warn('Cronitor ping failed:', e.message);
  }
}

function startDomLoop() {
  console.log(`Started DOM poll @ ${DOM_POLL_MS}ms`);
  setInterval(() => cronitorPing('dom'), DOM_POLL_MS);
}

/* =========================================================
   6) Startup
   ========================================================= */
async function start() {
  try {
    await bootstrapDb();
  } catch (e) {
    console.error('DB STARTUP FAIL:', e.message);
  }

  app.listen(PORT, () => console.log('HTTP server listening on', PORT));

  // WebSocket stream on
  wsConnect();

  // optional cronitor loop
  startDomLoop();

  console.log('==> Your service is live 🎉');
}
start();

/* =========================================================
   7) Graceful shutdown
   ========================================================= */
async function shutdown(sig) {
  try {
    console.log(`\nReceived ${sig}. Closing…`);
    if (ws) try { ws.close(); } catch {}
    await pool.end();
    process.exit(0);
  } catch (e) {
    console.error('Shutdown error:', e);
    process.exit(1);
  }
}
['SIGINT','SIGTERM'].forEach(s => process.on(s, () => shutdown(s)));
import pg from "pg";
const { Pool } = pg;

// ---------- ENV (matches your screenshots) ----------
const PORT            = Number(process.env.PORT || 10000);

// DB: use FIELDS ONLY (ignore POSTGRES_URL on purpose)
const DB = {
  host: process.env.POSTGRES_HOST,                 // e.g. dpg-d300nsvdiees738rlnog-a
  port: Number(process.env.POSTGRES_PORT || 5432), // 5432
  database: process.env.POSTGRES_DB,               // btc_scalp_logs
  user: process.env.POSTGRES_USER,                 // btc_logger
  password: process.env.POSTGRES_PASSWORD,         // ********
  ssl: { rejectUnauthorized: false }               // Render/Neon/PG-hosted
};

// A+ webhook bits
const TV_API_KEY  = process.env.TV_API_KEY;        // shared secret from TradingView (or “YOUR_TV_SHARED_SECRET”)
const ZAP_HOOK    = process.env.ZAP_B_URL;         // https://hooks.zapier.com/hooks/catch/...
const ZAP_API_KEY = process.env.ZAP_API_KEY || ""; // optional header for Zapier filter

// Workers
const DOM_PRODUCT = process.env.DOM_PRODUCT || process.env.PRODUCT_ID || "BTC-USD";
const DOM_LEVEL   = Number(process.env.DOM_LEVEL || 2);     // 1|2
const DOM_POLL_MS = Number(process.env.DOM_POLL_MS || 6000);
const CVD_EMA_LEN = Number(process.env.CVD_EMA_LEN || 34);

// Optional heartbeat
const CRONITOR_URL = process.env.CRONITOR_URL || "";

// ---------- PG POOL (single source) ----------
function makePool() {
  // Loud but safe boot log (no secrets)
  console.log("PG cfg =>", {
    host: DB.host, port: DB.port, db: DB.database,
    user_present: !!DB.user, pass_present: !!DB.password,
    url_present: !!process.env.POSTGRES_URL   // we intentionally do not use this
  });

  for (const [k, v] of Object.entries(DB)) {
    if (v === undefined || v === null || v === "" || (k === "port" && Number.isNaN(v))) {
      throw new Error(`Missing PG env: ${k}`);
    }
  }
  return new Pool(DB);
}
const pool = makePool();

// Prove DB at boot
(async () => {
  try {
    const r = await pool.query("select 1 as ok");
    console.log("PG ready ✓", r.rows[0]);
  } catch (e) {
    console.error("PG boot test failed ✗", e.message);
  }
})();

// ---------- SCHEMA (idempotent) ----------
async function ensureSchema() {
  await pool.query(`
    create table if not exists aplus_events (
      id bigserial primary key,
      ts timestamptz not null default now(),
      score numeric not null,
      reason text not null,
      payload jsonb
    );
    create table if not exists dom_ticks (
      id bigserial primary key,
      ts timestamptz not null,
      sequence bigint,
      p_ask numeric, q_ask numeric,
      p_bid numeric, q_bid numeric
    );
    create table if not exists cvd_ticks (
      id bigserial primary key,
      ts timestamptz not null,
      cvd numeric,
      cvd_ema numeric
    );
    create index if not exists idx_aplus_ts on aplus_events(ts);
    create index if not exists idx_dom_ts on dom_ticks(ts);
    create index if not exists idx_cvd_ts on cvd_ticks(ts);
  `);
  console.log("Schema ready ✓");
}
ensureSchema().catch(err => console.error("Schema error", err));

// ---------- EXPRESS ----------
const app = express();
app.disable("x-powered-by");
app.use(express.json({ limit: "256kb" }));

app.get("/", (_req, res) => res.send("OK"));

/**
 * POST /aplus
 * Headers: x-tv-key: <TV_API_KEY>
 * Body (JSON): { score: number, reason: string, ...anything }  // anything is stored as payload+forwarded
 */
app.post("/aplus", async (req, res) => {
  try {
    // Simple shared-secret gate
    const key = req.header("x-tv-key") || req.query.key;
    if (!TV_API_KEY || key !== TV_API_KEY) {
      return res.status(401).json({ ok: false, error: "unauthorized" });
    }

    const { score, reason, ...rest } = (req.body || {});
    if (score === undefined || reason === undefined) {
      return res.status(400).json({ ok: false, error: "score and reason required" });
    }

    // 1) store
    await pool.query(
      `insert into aplus_events (score, reason, payload) values ($1,$2,$3)`,
      [Number(score), String(reason), rest || {}]
    );

    // 2) relay to Zapier (as JSON so your Zap can filter by fields)
    if (ZAP_HOOK) {
      try {
        const z = await fetch(ZAP_HOOK, {
          method: "POST",
          headers: {
            "content-type": "application/json",
            ...(ZAP_API_KEY ? { "x-zap-key": ZAP_API_KEY } : {})
          },
          body: JSON.stringify({
            type: "aplus",
            score: Number(score),
            reason: String(reason),
            ts: new Date().toISOString(),
            product: DOM_PRODUCT,
            ...rest
          })
        });
        console.log("Zapier relay →", z.status);
      } catch (zerr) {
        console.error("Zapier relay failed:", zerr.message);
      }
    }

    return res.json({ ok: true });
  } catch (e) {
    console.error("POST /aplus error:", e);
    return res.status(500).json({ ok: false, error: "server_error" });
  }
});

// ---------- WORKER: DOM poll (public Coinbase order book) ----------
async function fetchDom() {
  const url = `https://api.exchange.coinbase.com/products/${DOM_PRODUCT}/book?level=${DOM_LEVEL}`;
  const r = await fetch(url, { headers: { "user-agent": "aplus-worker" } });
  if (!r.ok) throw new Error(`DOM fetch HTTP ${r.status}`);
  const j = await r.json();

  // Top of book
  const topAsk = j.asks?.[0] || [];
  const topBid = j.bids?.[0] || [];
  const p_ask = Number(topAsk[0] || 0);
  const q_ask = Number(topAsk[1] || 0);
  const p_bid = Number(topBid[0] || 0);
  const q_bid = Number(topBid[1] || 0);
  const sequence = Number(j.sequence || 0);
  const ts = new Date().toISOString();

  const rec = { type: "dom", p_ask, q_ask, p_bid, q_bid, sequence, ts };
  console.log("DOM →", rec);

  // Insert
  await pool.query(
    `insert into dom_ticks (ts, sequence, p_ask, q_ask, p_bid, q_bid)
     values (to_timestamp($1), $2, $3, $4, $5, $6)`,
    [Date.parse(ts) / 1000, sequence, p_ask, q_ask, p_bid, q_bid]
  );
}

// Poller
setTimeout(() => {
  console.log(`Started DOM poll @ ${DOM_POLL_MS}ms`);
  const loop = async () => {
    try { await fetchDom(); }
    catch (e) { console.error("DOM fetch error:", e.message); }
    finally { setTimeout(loop, DOM_POLL_MS); }
  };
  loop();
}, 1000);

// ---------- WORKER: CVD (toy version using last DOM delta + EMA) ----------
let cvd = 0;
let ema = 0;
const alpha = 2 / (CVD_EMA_LEN + 1);

async function stepCvd() {
  try {
    // Use last DOM snapshot as a crude proxy for delta pressure
    const { rows } = await pool.query(
      `select p_ask, q_ask, p_bid, q_bid
       from dom_ticks order by id desc limit 1`
    );
    if (rows.length) {
      const { q_ask, q_bid } = rows[0];
      const delta = Number(q_bid || 0) - Number(q_ask || 0);
      cvd += delta;
      ema = ema === 0 ? cvd : (alpha * cvd + (1 - alpha) * ema);

      const ts = new Date().toISOString();
      const rec = { type: "cvd", cvd: Number(cvd.toFixed(6)), cvd_ema: Number(ema.toFixed(6)), ts };
      console.log("CVD →", rec);

      await pool.query(
        `insert into cvd_ticks (ts, cvd, cvd_ema)
         values (to_timestamp($1), $2, $3)`,
        [Date.parse(ts) / 1000, rec.cvd, rec.cvd_ema]
      );
    }
  } catch (e) {
    console.error("CVD step error:", e.message);
  }
}
setInterval(stepCvd, Math.max(3000, Math.floor(DOM_POLL_MS / 2)));

// ---------- Cronitor heartbeat (optional) ----------
if (CRONITOR_URL) {
  setInterval(() => {
    fetch(CRONITOR_URL, { method: "GET" }).catch(() => {});
  }, 60_000);
}

// ---------- START ----------
app.listen(PORT, () => {
  console.log(`HTTP server listening on ${PORT}`);
});
