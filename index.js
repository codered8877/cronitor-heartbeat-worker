// index.js — Full pipeline (Part 1/2)
// Node 18+ (has global fetch). package.json must include { "type": "module" }.

import express from "express";
import { Pool } from "pg";

// ---------------------------- ENV ----------------------------
const ENV = {
  // Core
  PRODUCT_ID:  process.env.PRODUCT_ID || "BTC-USD",

  // TradingView -> /aplus shared secret (optional, header "x-tv-key")
  TV_API_KEY:  process.env.TV_API_KEY || "",

  // (Optional) Relay A+ to Zapier webhook
  ZAP_B_URL:   process.env.ZAP_B_URL || "",
  ZAP_API_KEY: process.env.ZAP_API_KEY || "", // sent as "x-api-key"

  // (Optional) Fan-out DOM elsewhere (legacy)
  ZAP_DOM_URL:     process.env.ZAP_DOM_URL || "",
  ZAP_DOM_API_KEY: process.env.ZAP_DOM_API_KEY || "",

  // Poll/EMA tuning
  DOM_POLL_MS: Math.max(2000, parseInt(process.env.DOM_POLL_MS || "6000", 10)),
  CVD_EMA_LEN: Math.max(2, parseInt(process.env.CVD_EMA_LEN || "34", 10)),

  // Retention (days)
  PRUNE_DAYS: Math.max(1, parseInt(process.env.PRUNE_DAYS || "14", 10)),

  // Keep-alive (optional)
  CRONITOR_URL: process.env.CRONITOR_URL || "",

  // Postgres — prefer discrete fields; fallback to URL
  PGHOST:      process.env.POSTGRES_HOST || process.env.PGHOST || "",
  PGPORT:      parseInt(process.env.POSTGRES_PORT || process.env.PGPORT || "5432", 10),
  PGDATABASE:  process.env.POSTGRES_DB || process.env.PGDATABASE || "",
  PGUSER:      process.env.POSTGRES_USER || process.env.PGUSER || "",
  PGPASSWORD:  process.env.POSTGRES_PASSWORD || process.env.PGPASSWORD || "",
  DATABASE_URL:process.env.POSTGRES_URL || process.env.DATABASE_URL || "",

  // HTTP
  PORT: parseInt(process.env.PORT || "3000", 10),
};

// --------------- sanity print (no secrets) ---------------
(function bootBanner() {
  const usingFields = !!(ENV.PGHOST && ENV.PGUSER && ENV.PGDATABASE);
  const usingURL = !!ENV.DATABASE_URL;
  console.log("🔧 Boot cfg:", {
    product: ENV.PRODUCT_ID,
    dom_poll_ms: ENV.DOM_POLL_MS,
    cvd_ema_len: ENV.CVD_EMA_LEN,
    prune_days: ENV.PRUNE_DAYS,
    tv_guard: !!ENV.TV_API_KEY,
    zap_relay: !!ENV.ZAP_B_URL,
    dom_fanout: !!ENV.ZAP_DOM_URL,
    cronitor: !!ENV.CRONITOR_URL,
    pg_mode: usingFields ? "fields" : usingURL ? "url" : "none",
    port: ENV.PORT,
  });
})();

// ------------------------- PG CONFIG -------------------------
function buildPgConfig() {
  const haveFields = ENV.PGHOST && ENV.PGUSER && ENV.PGDATABASE && ENV.PGPORT && ENV.PGPASSWORD;
  if (haveFields) {
    return {
      host: ENV.PGHOST,
      port: ENV.PGPORT,
      user: ENV.PGUSER,
      password: ENV.PGPASSWORD,
      database: ENV.PGDATABASE,
      ssl: { rejectUnauthorized: false }, // Render PG requires SSL
      max: 10,
      idleTimeoutMillis: 30_000,
      connectionTimeoutMillis: 10_000,
    };
  }
  if (ENV.DATABASE_URL) {
    return {
      connectionString: ENV.DATABASE_URL,
      ssl: { rejectUnauthorized: false },
      max: 10,
      idleTimeoutMillis: 30_000,
      connectionTimeoutMillis: 10_000,
    };
  }
  throw new Error("❌ No Postgres config. Provide POSTGRES_HOST/PORT/DB/USER/PASSWORD or POSTGRES_URL.");
}

const pg = new Pool(buildPgConfig());

// ------------- DB helpers: schema + persistence -------------
async function dbInit() {
  // boot sanity (who am I)
  const c = await pg.connect();
  try {
    const who = await c.query("select current_user, current_database()");
    console.log("✅ Postgres connected as:", who.rows[0]);
  } finally { c.release(); }

  // tables
  await pg.query(`
    create table if not exists events (
      id        bigserial primary key,
      ts        timestamptz not null default now(),
      kind      text not null,  -- 'aplus' | 'dom' | 'cvd' | 'audit' | 'prune'
      product   text,
      payload   jsonb,
      note      text
    );
  `);

  await pg.query(`
    create table if not exists aplus_signals (
      id          bigserial primary key,
      ts          timestamptz not null default now(),
      product     text not null,
      symbol      text,
      tf          text,
      dir         text,      -- LONG | SHORT
      price       double precision,
      score       int,
      spider_rej  boolean,
      reasons     text
    );
  `);

  await pg.query(`
    create table if not exists dom_snapshots (
      id          bigserial primary key,
      ts          timestamptz not null default now(),
      product     text not null,
      p_bid       double precision,
      q_bid       double precision,
      p_ask       double precision,
      q_ask       double precision,
      sequence    bigint
    );
  `);

  await pg.query(`
    create table if not exists cvd_ticks (
      id          bigserial primary key,
      ts          timestamptz not null default now(),
      product     text not null,
      cvd         double precision,
      cvd_ema     double precision
    );
  `);

  // indexes (idempotent)
  await pg.query(`create index if not exists idx_events_ts      on events(ts desc);`);
  await pg.query(`create index if not exists idx_events_kind    on events(kind);`);
  await pg.query(`create index if not exists idx_events_product on events(product);`);
  await pg.query(`create index if not exists idx_aplus_ts       on aplus_signals(ts desc);`);
  await pg.query(`create index if not exists idx_dom_ts         on dom_snapshots(ts desc);`);
  await pg.query(`create index if not exists idx_cvd_ts         on cvd_ticks(ts desc);`);

  console.log("📦 DB schema ready.");
}

async function persistEvent(kind, payload, note = null) {
  await pg.query(
    `insert into events(kind, product, payload, note) values ($1,$2,$3,$4)`,
    [kind, ENV.PRODUCT_ID, payload ?? null, note]
  );
}

async function persistAPlus(compact) {
  // Compact Pine JSON: { type:"APlus", s, t, f, p, d, e, sc, sr, R }
  const { s, f, d, p, sc, sr, R } = compact || {};
  await pg.query(
    `insert into aplus_signals(product, symbol, tf, dir, price, score, spider_rej, reasons)
     values ($1,$2,$3,$4,$5,$6,$7,$8)`,
    [
      ENV.PRODUCT_ID,
      s ?? null,
      f ?? null,
      d ?? null,
      p != null ? Number(p) : null,
      Number.isFinite(Number(sc)) ? Number(sc) : null,
      !!sr,
      typeof R === "string" ? R : null,
    ]
  );
}

async function persistDOM(dom) {
  await pg.query(
    `insert into dom_snapshots(product, p_bid, q_bid, p_ask, q_ask, sequence)
     values ($1,$2,$3,$4,$5,$6)`,
    [
      ENV.PRODUCT_ID,
      dom.p_bid ?? null, dom.q_bid ?? null,
      dom.p_ask ?? null, dom.q_ask ?? null,
      dom.sequence ?? null,
    ]
  );
}

async function persistCVD(cvdRow) {
  await pg.query(
    `insert into cvd_ticks(product, cvd, cvd_ema) values ($1,$2,$3)`,
    [ENV.PRODUCT_ID, cvdRow.cvd ?? 0, cvdRow.cvd_ema ?? 0]
  );
}

// --------------------- EXPRESS APP ---------------------
const app = express();

// Accept JSON or plain text (TV mobile sometimes posts text/plain)
app.use(express.json({ limit: "1mb", type: ["application/json", "text/json"] }));
app.use(express.text({ limit: "1mb", type: ["text/*", "application/x-www-form-urlencoded"] }));

// ==============================
// RETENTION (token-protected)
// ==============================
const RETENTION_TOKEN = process.env.RETENTION_TOKEN || "";
// call with:  GET /retention?token=YOUR_TOKEN
// or send header: X-Auth-Token: YOUR_TOKEN

app.get("/retention", async (req, res) => {
  const token = req.get("X-Auth-Token") || req.query.token || "";
  if (!RETENTION_TOKEN || token !== RETENTION_TOKEN) {
    return res.status(401).json({ ok: false, error: "unauthorized" });
  }

  // hardcoded table names (safe to template)
  const plan = [
    { table: "aplus_signals",  interval: "180 days" }, // keep compact A+ signals 6mo
    { table: "aplus_events",   interval: "30 days"  }, // raw payloads 30d
    { table: "dom_snapshots",  interval: "14 days"  }, // DOM snapshots 14d
    { table: "cvd_ticks",      interval: "14 days"  }, // CVD ticks 14d
    // optional: keep trade feedback forever. uncomment to prune:
    // { table: "trade_feedback", interval: "180 days" },
  ];

  const vacuums = [
    "aplus_signals",
    "aplus_events",
    "dom_snapshots",
    "cvd_ticks",
    "trade_feedback",
  ];

  const result = { ok: true, deleted: {}, skipped: [], vacuumed: [] };
  const client = await pgPool.connect();

  // helper: only delete if table exists
  async function safeDelete(t, interval) {
    const exists = await client.query("SELECT to_regclass($1) reg", [t]);
    if (!exists.rows[0].reg) { result.skipped.push(t); return; }
    const q = `DELETE FROM ${t} WHERE ts < NOW() - INTERVAL '${interval}'`;
    const r = await client.query(q);
    result.deleted[t] = r.rowCount;
  }

  // helper: only VACUUM if exists
  async function safeVacuum(t) {
    const exists = await client.query("SELECT to_regclass($1) reg", [t]);
    if (!exists.rows[0].reg) return;
    await client.query(`VACUUM ANALYZE ${t}`);
    result.vacuumed.push(t);
  }

  try {
    await client.query("BEGIN");
    // deletes
    for (const { table, interval } of plan) await safeDelete(table, interval);
    // vacuum (optional but nice)
    for (const t of vacuums) await safeVacuum(t);
    await client.query("COMMIT");
    console.log("[RETENTION] done:", result);
    return res.json(result);
  } catch (err) {
    await client.query("ROLLBACK");
    console.error("[RETENTION] error:", err);
    return res.status(500).json({ ok: false, error: err.message });
  } finally {
    client.release();
  }
});

// ====================================
// BACKUP (token-protected, gzipped JSON)
// ====================================
import zlib from "zlib"; // (top of file you likely already import; if not, keep this here)

const BACKUP_TOKEN = process.env.BACKUP_TOKEN || "";

app.get("/backup", async (req, res) => {
  try {
    // Auth (either query token or header)
    const token = req.query.token || req.get("X-Auth-Token");
    if (!BACKUP_TOKEN || token !== BACKUP_TOKEN) {
      return res.status(401).json({ ok: false, error: "unauthorized" });
    }

    // How many days to include (defaults to 30)
    const days = Math.max(1, Math.min(365, parseInt(req.query.days || "30", 10)));

    // Time window
    const since = `NOW() - INTERVAL '${days} days'`;

    // Pull data per table (adjust list to match your schema)
    // NOTE: trade_feedback kept forever; include all or make it windowed like others
    const tables = [
      { name: "aplus_signals",   windowed: true,  ts: "ts" },
      { name: "aplus_events",    windowed: true,  ts: "ts" },
      { name: "dom_snapshots",   windowed: true,  ts: "ts" },
      { name: "cvd_ticks",       windowed: true,  ts: "ts" },
      { name: "trade_feedback",  windowed: false, ts: "ts" }, // full dump
    ];

    // Fetch counts first (cheap sanity)
    const counts = {};
    for (const t of tables) {
      const where = t.windowed ? `WHERE ${t.ts} >= ${since}` : "";
      const { rows } = await pool.query(`SELECT COUNT(*)::int AS c FROM ${t.name} ${where}`);
      counts[t.name] = rows[0].c;
    }

    // Pull rows
    const payload = { meta: {
        generated_at: new Date().toISOString(),
        days,
        counts
      },
      data: {}
    };

    for (const t of tables) {
      const where = t.windowed ? `WHERE ${t.ts} >= ${since}` : "";
      // Keep order stable by timestamp if present
      const order = t.ts ? `ORDER BY ${t.ts} ASC` : "";
      const { rows } = await pool.query(`SELECT * FROM ${t.name} ${where} ${order}`);
      payload.data[t.name] = rows;
    }

    // Gzip + send
    const raw = Buffer.from(JSON.stringify(payload), "utf8");
    const gz = zlib.gzipSync(raw);

    res.setHeader("Content-Type", "application/gzip");
    res.setHeader("Content-Encoding", "gzip");
    res.setHeader("Content-Disposition",
      `attachment; filename="backup_${new Date().toISOString().replace(/[:.]/g, "-")}.json.gz"`);
    res.status(200).send(gz);
  } catch (err) {
    console.error("[BACKUP] error:", err);
    res.status(500).json({ ok: false, error: err.message });
  }
});

// ====================================
// RETENTION ENDPOINT (secure)
// ====================================
app.post("/internal/retention", async (req, res) => {
  try {
    const tokenHeader = req.headers["x-retention-token"];
    const tokenQuery  = req.query.token;
    const token = tokenHeader || tokenQuery || "";

    if (process.env.RETENTION_TOKEN && token !== process.env.RETENTION_TOKEN) {
      return res.status(401).json({ error: "unauthorized" });
    }

    // Prune windows (adjust if you like)
    const DOM_DAYS  = parseInt(process.env.RETENTION_DOM_DAYS  || "30", 10);   // DOM for 30d
    const CVD_DAYS  = parseInt(process.env.RETENTION_CVD_DAYS  || "30", 10);   // CVD for 30d
    const APL_DAYS  = parseInt(process.env.RETENTION_APLUS_DAYS|| "180", 10);  // A+ for 180d

    // Run deletes and capture counts
    const domDel = await pool.query(
      "DELETE FROM dom_ticks WHERE ts < NOW() - INTERVAL $1 DAY",
      [DOM_DAYS]
    );
    const cvdDel = await pool.query(
      "DELETE FROM cvd_ticks WHERE ts < NOW() - INTERVAL $1 DAY",
      [CVD_DAYS]
    );
    const aplDel = await pool.query(
      "DELETE FROM aplus_events WHERE ts < NOW() - INTERVAL $1 DAY",
      [APL_DAYS]
    );

    // Light analyze to keep plans fresh
    await pool.query("ANALYZE dom_ticks");
    await pool.query("ANALYZE cvd_ticks");
    await pool.query("ANALYZE aplus_events");

    const out = {
      pruned: {
        dom_ticks: domDel.rowCount || 0,
        cvd_ticks: cvdDel.rowCount || 0,
        aplus_events: aplDel.rowCount || 0,
      },
      at: new Date().toISOString(),
    };
    console.log("[RETENTION]", out);
    res.json({ ok: true, ...out });
  } catch (err) {
    console.error("[RETENTION] error:", err.message);
    res.status(500).json({ ok: false, error: err.message });
  }
});

// ====================================
// KEEP RENDER SERVICE ALWAYS AWAKE
// ====================================
const PORT = Number(process.env.PORT || 10000);

// Prefer explicit env (best)
const EXTERNAL = process.env.KEEPALIVE_URL ||
  (process.env.RENDER_EXTERNAL_URL ? `https://${process.env.RENDER_EXTERNAL_URL}/health` : null);

// Local fallback (doesn't help Render's idle policy, but avoids noisy logs if external is flaky)
const LOCAL = `http://127.0.0.1:${PORT}/health`;

let kaFails = 0;
const jitter = () => 90000 + Math.floor(Math.random() * 30000); // 90–120s

async function ping(url) {
  const ac = new AbortController();
  const t = setTimeout(() => ac.abort(), 5000);
  try {
    const res = await fetch(url, { method: "GET", cache: "no-store", signal: ac.signal });
    clearTimeout(t);
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    return true;
  } catch (e) {
    clearTimeout(t);
    return false;
  }
}

if (EXTERNAL) {
  console.log(`[KEEPALIVE] Using external ${EXTERNAL}, fallback ${LOCAL}`);
  setInterval(async () => {
    const okExt = await ping(EXTERNAL);
    if (okExt) {
      if (kaFails) console.log("[KEEPALIVE] Recovered");
      kaFails = 0;
      return;
    }
    // try local once before counting it as a fail (just to reduce red noise)
    const okLocal = await ping(LOCAL);
    if (!okLocal) {
      kaFails++;
      if (kaFails >= 2) console.warn(`[KEEPALIVE] ${kaFails} consecutive failures`);
    }
  }, jitter()); // every ~1.5–2 minutes with jitter
} else {
  console.log("[KEEPALIVE] Skipped (no EXTERNAL URL available)");
}

// Health
app.get("/", (_req, res) => res.status(200).send("APlus pipeline up"));
app.get("/healthz", (_req, res) => res.status(200).json({ ok: true }));

// ------------ Single TradingView webhook: /aplus ------------
app.post("/aplus", async (req, res) => {
  try {
    // Shared secret guard (optional)
    if (ENV.TV_API_KEY) {
      const got = req.headers["x-tv-key"];
      if (!got || got !== ENV.TV_API_KEY) {
        await persistEvent("audit", { route: "/aplus", reason: "unauthorized" }, "tv-guard-fail");
        return res.status(401).json({ error: "unauthorized" });
      }
    }

    // TV sends JSON or raw string (esp. if Alert “Message” is empty on mobile)
    let raw = req.body;
    let parsed = null;

    if (typeof raw === "string") {
      const s = raw.trim();
      if (s.startsWith("{") && s.endsWith("}")) {
        try { parsed = JSON.parse(s); } catch { parsed = null; }
      }
    } else if (raw && typeof raw === "object") {
      parsed = raw;
    }

    // Store raw event for audit
    await persistEvent("aplus", parsed ?? raw ?? null, "tv-webhook");

    // If payload looks like compact APlus, also store to aplus_signals
    if (parsed && parsed.type === "APlus") {
      await persistAPlus(parsed);
    }

    // Optionally relay to Zapier
    if (ENV.ZAP_B_URL) {
      try {
        const zr = await fetch(ENV.ZAP_B_URL, {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            ...(ENV.ZAP_API_KEY ? { "x-api-key": ENV.ZAP_API_KEY } : {}),
            "x-pipe-tag": "aplus",
          },
          body: JSON.stringify({
            tag: "aplus",
            product: ENV.PRODUCT_ID,
            payload: parsed ?? raw,
            ts: new Date().toISOString(),
          }),
        });
        if (!zr.ok) {
          const txt = await zr.text().catch(() => "");
          console.warn("⚠️ Zap relay non-200:", zr.status, txt);
          await persistEvent("audit", { status: zr.status, txt }, "zap-non200");
        }
      } catch (e) {
        console.warn("⚠️ Zap relay error:", e.message);
        await persistEvent("audit", { err: e.message }, "zap-error");
      }
    }

    return res.json({ ok: true });
  } catch (e) {
    console.error("❌ /aplus error:", e.message);
    await persistEvent("audit", { err: e.message }, "aplus-handler-error");
    return res.status(500).json({ error: "server_error" });
  }
});

// ------------------------ RETENTION ------------------------
async function pruneOld() {
  const days = ENV.PRUNE_DAYS;
  try {
    const q1 = await pg.query(`delete from events where ts < now() - interval '${days} days'`);
    const q2 = await pg.query(`delete from aplus_signals where ts < now() - interval '${days} days'`);
    const q3 = await pg.query(`delete from dom_snapshots where ts < now() - interval '${days} days'`);
    const q4 = await pg.query(`delete from cvd_ticks where ts < now() - interval '${days} days'`);
    console.log(`🧹 Prune(${days}d): events=${q1.rowCount}, aplus=${q2.rowCount}, dom=${q3.rowCount}, cvd=${q4.rowCount}`);
    await persistEvent("prune", { days, counts: {
      events: q1.rowCount, aplus: q2.rowCount, dom: q3.rowCount, cvd: q4.rowCount
    }}, "retention");
  } catch (e) {
    console.warn("⚠️ prune error:", e.message);
    await persistEvent("audit", { err: e.message }, "prune-error");
  }
}
// Nightly-ish (every 24h)
setInterval(pruneOld, 24 * 60 * 60 * 1000);

// --------------------- CRONITOR PING ---------------------
async function pingCronitor() {
  if (!ENV.CRONITOR_URL) return;
  try {
    const r = await fetch(ENV.CRONITOR_URL);
    if (!r.ok) console.warn("cronitor non-200:", r.status);
  } catch (e) {
    console.warn("cronitor error:", e.message);
  }
}
setInterval(pingCronitor, 15_000);

// ------------------- START + SHUTDOWN -------------------
let server;
(async function start() {
  try {
    await dbInit();
    server = app.listen(ENV.PORT, () => {
      console.log(`🚀 HTTP listening on :${ENV.PORT}`);
    });
  } catch (e) {
    console.error("❌ boot failure:", e.message);
    process.exit(1);
  }
})();

async function shutdown(sig) {
  try {
    console.log(`\n🔻 ${sig} received. Closing…`);
    if (server) {
      await new Promise((r) => server.close(r));
      console.log("HTTP closed.");
    }
    await pg.end();
    console.log("PG pool ended.");
  } catch (e) {
    console.warn("shutdown error:", e.message);
  } finally {
    process.exit(0);
  }
}
process.on("SIGTERM", () => shutdown("SIGTERM"));
process.on("SIGINT",  () => shutdown("SIGINT"));

// index.js — Full pipeline (Part 2/2)
import WebSocket from "ws";

// ---------- DOM POLLER (Coinbase level=2) ----------
const DOM_POLL_MS = Math.max(2000, parseInt(process.env.DOM_POLL_MS || "6000", 10));
const PRODUCT_ID  = process.env.PRODUCT_ID || "BTC-USD";
const ZAP_DOM_URL = process.env.ZAP_DOM_URL || "";
const ZAP_DOM_API_KEY = process.env.ZAP_DOM_API_KEY || "";

async function fetchDOM() {
  try {
    const url = `https://api.exchange.coinbase.com/products/${encodeURIComponent(PRODUCT_ID)}/book?level=2`;
    const r = await fetch(url, { headers: { "User-Agent": "aplus-dom-poller" } });
    if (!r.ok) throw new Error(`DOM HTTP ${r.status}`);
    const book = await r.json();

    const bestAsk = Array.isArray(book.asks) && book.asks[0] || null;
    const bestBid = Array.isArray(book.bids) && book.bids[0] || null;

    const p_ask = bestAsk ? parseFloat(bestAsk[0]) : null;
    const q_ask = bestAsk ? parseFloat(bestAsk[1]) : null;
    const p_bid = bestBid ? parseFloat(bestBid[0]) : null;
    const q_bid = bestBid ? parseFloat(bestBid[1]) : null;

    return {
      type: "dom",
      ts: new Date().toISOString(),
      p_bid, q_bid, p_ask, q_ask,
      sequence: Number(book.sequence ?? 0),
    };
  } catch (e) {
    console.warn("DOM fetch error:", e.message);
    return null;
  }
}

async function domTick() {
  const row = await fetchDOM();
  if (!row) return;

  // persist to DB (events + dom_snapshots)
  try {
    // reuse top-level helpers via globalThis (simple bridge)
    if (!globalThis._persistEvent || !globalThis._persistDOM) {
      // Lazy wire-ins: copy from module scope if not already set
      globalThis._persistEvent = async (k, p, n) => {
        // dynamic import is heavy; instead we stashed real fns in globals (below)
      };
      globalThis._persistDOM = async (_row) => {};
    }
  } catch {}

  // We stored actual functions on globalThis right after their declarations:
  // (placed below definitions in Part 1/2)
  try {
    await globalThis._persistEvent("dom", row, "poll");
    await globalThis._persistDOM(row);
  } catch (e) {
    console.warn("DOM persist error:", e.message);
  }

  // Optional fan-out to an external DOM endpoint (legacy)
  if (ZAP_DOM_URL) {
    try {
      const zr = await fetch(ZAP_DOM_URL, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          ...(ZAP_DOM_API_KEY ? { "x-api-key": ZAP_DOM_API_KEY } : {}),
        },
        body: JSON.stringify(row),
      });
      if (!zr.ok) console.warn("DOM fan-out non-200:", zr.status);
    } catch (e) {
      console.warn("DOM fan-out error:", e.message);
    }
  }
}
setInterval(domTick, DOM_POLL_MS);
console.log(`⏱️  DOM poll @ ${DOM_POLL_MS}ms → ${PRODUCT_ID}`);

// ---------- CVD WS (Coinbase matches) + EMA ----------
const CVD_EMA_LEN = Math.max(2, parseInt(process.env.CVD_EMA_LEN || "34", 10));
const alpha = 2 / (CVD_EMA_LEN + 1);

let ws = null;
let wsHeartbeat = null;
let cvd = 0;
let cvdEma = 0;

function startCVD() {
  const endpoint = "wss://ws-feed.exchange.coinbase.com";

  function connect() {
    ws = new WebSocket(endpoint);

    ws.on("open", () => {
      console.log("🔌 CVD WS open");
      const sub = { type: "subscribe", channels: [{ name: "matches", product_ids: [PRODUCT_ID] }] };
      ws.send(JSON.stringify(sub));
      clearInterval(wsHeartbeat);
      wsHeartbeat = setInterval(() => { try { ws.ping(); } catch {} }, 25_000);
    });

    ws.on("message", async (buf) => {
      try {
        const msg = JSON.parse(buf.toString());
        if (msg.type === "match" && msg.product_id === PRODUCT_ID) {
          const size = parseFloat(msg.size || "0");
          const side = msg.side; // "buy" | "sell"
          const signed = side === "buy" ? size : side === "sell" ? -size : 0;

          cvd += signed;
          cvdEma = cvdEma === 0 ? cvd : alpha * cvd + (1 - alpha) * cvdEma;
        }
      } catch (e) {
        console.warn("CVD parse error:", e.message);
      }
    });

    ws.on("close", () => {
      console.log("CVD WS closed → reconnect in 3s");
      clearInterval(wsHeartbeat);
      setTimeout(connect, 3000);
    });

    ws.on("error", (err) => {
      console.warn("CVD WS error:", err.message);
      try { ws.close(); } catch {}
    });
  }

  connect();
}
startCVD();

// Heartbeat persist every 15s
setInterval(async () => {
  const row = {
    type: "cvd",
    ts: new Date().toISOString(),
    cvd: Number.isFinite(cvd) ? +cvd.toFixed(6) : 0,
    cvd_ema: Number.isFinite(cvdEma) ? +cvdEma.toFixed(6) : 0,
  };

  try {
    await globalThis._persistEvent("cvd", row, "ws-heartbeat");
    await globalThis._persistCVD({ cvd: row.cvd, cvd_ema: row.cvd_ema });
  } catch (e) {
    console.warn("CVD persist error:", e.message);
  }
}, 15_000);

console.log(`📈 CVD EMA len = ${CVD_EMA_LEN}`);

// ---------- Wire DB helpers to globalThis (bridge) ----------
// This lets Part 2 call the Part 1 functions without re-importing.
globalThis._persistEvent = async (kind, payload, note) => {
  // eslint-disable-next-line no-undef
  return persistEvent(kind, payload, note); // defined in Part 1 scope
};
globalThis._persistDOM = async (row) => {
  // eslint-disable-next-line no-undef
  return persistDOM(row); // defined in Part 1 scope
};
globalThis._persistCVD = async (row) => {
  // eslint-disable-next-line no-undef
  return persistCVD(row); // defined in Part 1 scope
};
