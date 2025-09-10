// index.js — clean server + workers + safe Postgres init (Render-ready)
// Node 20+ (uses global fetch). "type": "module" in package.json.

import express from "express";
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
