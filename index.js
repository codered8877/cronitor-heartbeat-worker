// index.js — APlus pipeline (Part 1/3)
// Node 18+ (global fetch). package.json must include { "type": "module" }.

import express from "express";
import { Pool } from "pg";
import zlib from "zlib";

/* ------------------------------- ENV ------------------------------- */
const ENV = {
  // Core product (Coinbase product id)
  PRODUCT_ID: process.env.PRODUCT_ID || "BTC-USD",

  // TradingView webhook guard (optional). If set, we require header x-tv-key.
  TV_API_KEY: process.env.TV_API_KEY || "",

  // Optional relay of compact A+ signals to Zapier
  ZAP_B_URL:   process.env.ZAP_B_URL || "",
  ZAP_API_KEY: process.env.ZAP_API_KEY || "",

  // Optional fan-out of DOM snapshots to an external endpoint
  ZAP_DOM_URL:     process.env.ZAP_DOM_URL || "",
  ZAP_DOM_API_KEY: process.env.ZAP_DOM_API_KEY || "",

  // Poll/EMA tuning
  DOM_POLL_MS: Math.max(2000, parseInt(process.env.DOM_POLL_MS || "6000", 10)),
  CVD_EMA_LEN: Math.max(2, parseInt(process.env.CVD_EMA_LEN || "34", 10)),

  // Retention window used by nightly prune
  PRUNE_DAYS: Math.max(1, parseInt(process.env.PRUNE_DAYS || "14", 10)),

  // Optional external heartbeat/ping service (e.g. Cronitor)
  CRONITOR_URL: process.env.CRONITOR_URL || "",

  // Postgres (prefer discrete fields; fallback to URL)
  PGHOST:      process.env.POSTGRES_HOST || process.env.PGHOST || "",
  PGPORT:      parseInt(process.env.POSTGRES_PORT || process.env.PGPORT || "5432", 10),
  PGDATABASE:  process.env.POSTGRES_DB || process.env.PGDATABASE || "",
  PGUSER:      process.env.POSTGRES_USER || process.env.PGUSER || "",
  PGPASSWORD:  process.env.POSTGRES_PASSWORD || process.env.PGPASSWORD || "",
  DATABASE_URL:process.env.POSTGRES_URL || process.env.DATABASE_URL || "",

  // HTTP
  PORT: parseInt(process.env.PORT || "3000", 10),
};

// non-secret boot banner (helps your Render logs)
(function bootBanner() {
  const usingFields = !!(ENV.PGHOST && ENV.PGUSER && ENV.PGDATABASE);
  const usingURL    = !!ENV.DATABASE_URL;
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

/* ---------------------------- PG CONFIG ---------------------------- */
function buildPgConfig() {
  const haveFields =
    ENV.PGHOST && ENV.PGUSER && ENV.PGDATABASE && ENV.PGPORT && ENV.PGPASSWORD;
  if (haveFields) {
    return {
      host: ENV.PGHOST,
      port: ENV.PGPORT,
      user: ENV.PGUSER,
      password: ENV.PGPASSWORD,
      database: ENV.PGDATABASE,
      ssl: { rejectUnauthorized: false },   // Render PG requires SSL
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

const ENABLE_TEST_ROUTES = process.env.ENABLE_TEST_ROUTES === "1";

/* -------------------- Schema, indexes, helpers -------------------- */
async function dbInit() {
  const c = await pg.connect();
  try {
    const who = await c.query("select current_user, current_database()");
    console.log("✅ Postgres connected as:", who.rows[0]);
  } finally { c.release(); }

  // Tables
  await pg.query(`
    create table if not exists events (
      id        bigserial primary key,
      ts        timestamptz not null default now(),
      kind      text not null,       -- 'aplus' | 'dom' | 'cvd' | 'audit' | 'prune'
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
      dir         text,              -- LONG | SHORT
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

// --- Trade feedback: ensure table, columns, and indexes
await pg.query(`
  create table if not exists trade_feedback (
    id bigserial primary key,
    ts timestamptz not null default now()
  );
`);

await pg.query(`
  alter table if exists trade_feedback
    add column if not exists signal_id bigint,
    add column if not exists outcome   text,
    add column if not exists rr        double precision,
    add column if not exists notes     text
`);

await pg.query(`create index if not exists idx_feedback_ts     on trade_feedback(ts desc)`);
await pg.query(`create index if not exists idx_feedback_signal on trade_feedback(signal_id)`);
  
  // Indexes
  await pg.query(`create index if not exists idx_events_ts      on events(ts desc)`);
  await pg.query(`create index if not exists idx_events_kind    on events(kind)`);
  await pg.query(`create index if not exists idx_events_product on events(product)`);
  await pg.query(`create index if not exists idx_aplus_ts       on aplus_signals(ts desc)`);
  await pg.query(`create index if not exists idx_dom_ts         on dom_snapshots(ts desc)`);
  await pg.query(`create index if not exists idx_cvd_ts         on cvd_ticks(ts desc)`);

  console.log("📦 DB schema ready.");
}

async function persistEvent(kind, payload, note = null) {
  await pg.query(
    `insert into events(kind, product, payload, note) values ($1,$2,$3,$4)`,
    [kind, ENV.PRODUCT_ID, payload ?? null, note]
  );
}
async function persistAPlus(compact) {
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
    [ENV.PRODUCT_ID, dom.p_bid ?? null, dom.q_bid ?? null, dom.p_ask ?? null, dom.q_ask ?? null, dom.sequence ?? null]
  );
}
async function persistCVD(row) {
  await pg.query(
    `insert into cvd_ticks(product, cvd, cvd_ema) values ($1,$2,$3)`,
    [ENV.PRODUCT_ID, row.cvd ?? 0, row.cvd_ema ?? 0]
  );
}

/* -------------------------- Express app -------------------------- */
const app = express();
// TV mobile sometimes posts text/plain
app.use(express.json({ limit: "1mb", type: ["application/json", "text/json"] }));
app.use(express.text({ limit: "1mb", type: ["text/*", "application/x-www-form-urlencoded"] }));

/* ----------------------------- KPIs (/perf) ----------------------------- */
app.get("/perf", async (req, res) => {
  try {
    const days      = Math.max(1, Math.min(365, parseInt(req.query.days || "90", 10)));
    const minScore  = Number.isFinite(+req.query.min_score) ? Math.trunc(+req.query.min_score) : null;
    const dirFilter = (req.query.dir || "").toUpperCase(); // LONG | SHORT | ""

    // Base WHERE
    const where = [`tf.ts >= now() - interval '${days} days'`];
    if (dirFilter === "LONG" || dirFilter === "SHORT") {
      where.push(`coalesce(asig.dir, '') = '${dirFilter}'`);
    }

    // Optional score join/filter
    const joinScore =
      minScore != null
        ? `left join aplus_signals asig on asig.id = tf.signal_id and asig.score >= ${minScore}`
        : `left join aplus_signals asig on asig.id = tf.signal_id`;

    // Core KPIs
    const coreSql = `
      with t as (
        select
          tf.ts,
          tf.rr,
          tf.outcome,
          asig.dir,
          asig.symbol,
          asig.score
        from trade_feedback tf
        ${joinScore}
        where ${where.join(" and ")}
      )
      select
        count(*)::int                                              as trades,
        avg(case when rr > 0 then 1 else 0 end)::float            as hit_rate,
        avg(rr)::float                                            as expectancy_rr,
        sum(case when rr > 0 then rr else 0 end)
          / nullif(abs(sum(case when rr <= 0 then rr else 0 end)), 0)::float
                                                                  as profit_factor,
        min(ts)                                                   as period_start,
        max(ts)                                                   as period_end
      from t
    `;
    const core = (await pg.query(coreSql)).rows[0];

    // Breakdown by direction (if any signals were joined)
    const byDirSql = `
      with t as (
        select coalesce(asig.dir, 'NA') as dir, tf.rr
        from trade_feedback tf
        ${joinScore}
        where ${where.join(" and ")}
      )
      select dir,
             count(*)::int as n,
             avg(case when rr>0 then 1 else 0 end)::float as hit_rate,
             avg(rr)::float as exp_rr
      from t
      group by dir
      order by dir
    `;
    const by_dir = (await pg.query(byDirSql)).rows;

    // Score buckets (0–100 into 5 buckets) – harmless if score is null
    const byScoreSql = `
      with t as (
        select coalesce(asig.score, 0) as score, tf.rr
        from trade_feedback tf
        ${joinScore}
        where ${where.join(" and ")}
      )
      select
        width_bucket(score, 0, 100, 5) as bucket,
        count(*)::int                   as n,
        round(avg(score)::numeric, 1)   as avg_score,
        avg(case when rr>0 then 1 else 0 end)::float as hit_rate,
        avg(rr)::float as exp_rr
      from t
      group by bucket
      order by bucket
    `;
    const by_score_bucket = (await pg.query(byScoreSql)).rows;

    res.json({
      ok: true,
      window_days: days,
      filter: { min_score: minScore, dir: dirFilter || "ALL" },
      core,
      by_dir,
      by_score_bucket,
      generated_at: new Date().toISOString()
    });
  } catch (e) {
    console.error("[/perf] error:", e.message);
    res.status(500).json({ ok: false, error: e.message });
  }
});
console.log("🧮 KPIs route enabled: GET /perf");

/* ------------------------- RETENTION (GET) ------------------------ */
// token via header `X-Auth-Token` or query `?token=...`
const RETENTION_TOKEN = process.env.RETENTION_TOKEN || "";

app.get("/retention", async (req, res) => {
  const token = req.get("X-Auth-Token") || req.query.token || "";
  if (!RETENTION_TOKEN || token !== RETENTION_TOKEN) {
    return res.status(401).json({ ok: false, error: "unauthorized" });
  }

  // use your canonical table names
  const plan = [
    { table: "aplus_signals", interval: "180 days" },
    { table: "events",        interval: "30 days"  },
    { table: "dom_snapshots", interval: "14 days"  },
    { table: "cvd_ticks",     interval: "14 days"  },
    { table: "trade_feedback",interval: "365 days" },
  ];
  const vacuums = ["aplus_signals", "events", "dom_snapshots", "cvd_ticks", "trade_feedback"];

  const result = { ok: true, deleted: {}, skipped: [], vacuumed: [] };
  const client = await pg.connect();

  async function safeDelete(t, interval) {
    const exists = await client.query("select to_regclass($1) reg", [t]);
    if (!exists.rows[0].reg) { result.skipped.push(t); return; }
    const r = await client.query(`delete from ${t} where ts < now() - interval '${interval}'`);
    result.deleted[t] = r.rowCount;
  }
  async function safeVacuum(t) {
    const exists = await client.query("select to_regclass($1) reg", [t]);
    if (!exists.rows[0].reg) return;
    await client.query(`vacuum analyze ${t}`);
    result.vacuumed.push(t);
  }

  try {
    await client.query("begin");
    for (const { table, interval } of plan) await safeDelete(table, interval);
    for (const t of vacuums) await safeVacuum(t);
    await client.query("commit");
    console.log("[RETENTION] done:", result);
    res.json(result);
  } catch (err) {
    await client.query("rollback");
    console.error("[RETENTION] error:", err);
    res.status(500).json({ ok: false, error: err.message });
  } finally {
    client.release();
  }
});

/* --------------------------- BACKUP (GET) ------------------------- */
// gzipped JSON. Usage: /backup?token=TOKEN&days=30
const BACKUP_TOKEN = process.env.BACKUP_TOKEN || "";

app.get("/backup", async (req, res) => {
  try {
    const token = req.query.token || req.get("X-Auth-Token");
    if (!BACKUP_TOKEN || token !== BACKUP_TOKEN) {
      return res.status(401).json({ ok: false, error: "unauthorized" });
    }

    const days  = Math.max(1, Math.min(365, parseInt(req.query.days || "30", 10)));
    const since = `now() - interval '${days} days'`;

    const tables = [
      { name: "aplus_signals", windowed: true,  ts: "ts" },
      { name: "events",        windowed: true,  ts: "ts" },
      { name: "dom_snapshots", windowed: true,  ts: "ts" },
      { name: "cvd_ticks",     windowed: true,  ts: "ts" },
      { name: "trade_feedback",windowed: false, ts: "ts" },
    ];

    const counts = {};
    for (const t of tables) {
      const where = t.windowed ? `where ${t.ts} >= ${since}` : "";
      const { rows } = await pg.query(`select count(*)::int as c from ${t.name} ${where}`);
      counts[t.name] = rows[0].c;
    }

    const payload = { meta: { generated_at: new Date().toISOString(), days, counts }, data: {} };
    for (const t of tables) {
      const where = t.windowed ? `where ${t.ts} >= ${since}` : "";
      const order = t.ts ? `order by ${t.ts} asc` : "";
      const { rows } = await pg.query(`select * from ${t.name} ${where} ${order}`);
      payload.data[t.name] = rows;
    }

    const raw = Buffer.from(JSON.stringify(payload), "utf8");
    const gz  = zlib.gzipSync(raw);

    res.setHeader("Content-Type", "application/gzip");
    res.setHeader("Content-Encoding", "gzip");
    res.setHeader(
      "Content-Disposition",
      `attachment; filename="backup_${new Date().toISOString().replace(/[:.]/g, "-")}.json.gz"`
    );
    res.status(200).send(gz);
  } catch (err) {
    console.error("[BACKUP] error:", err);
    res.status(500).json({ ok: false, error: err.message });
  }
});

/* -------------------- BACKUP STATUS (DB-based) -------------------- */
// For cron monitoring: /backup/check?token=BACKUP_TOKEN
app.get("/backup/check", async (req, res) => {
  try {
    const token = req.get("X-Auth-Token") || req.query.token;
    if (!BACKUP_TOKEN || token !== BACKUP_TOKEN) {
      return res.status(401).json({ ok: false, error: "unauthorized" });
    }
    const tables = ["aplus_signals", "events", "dom_snapshots", "cvd_ticks", "trade_feedback"];
    const summary = {};
    for (const t of tables) {
      const { rows: c } = await pg.query(`select count(*)::int as n from ${t}`);
      const { rows: m } = await pg.query(`select coalesce(max(ts),'1970-01-01'::timestamptz) as max_ts from ${t}`);
      summary[t] = { rows: c[0].n, latest: m[0].max_ts };
    }
    res.json({ ok: true, generated_at: new Date().toISOString(), summary });
  } catch (err) {
    console.error("[BACKUP CHECK] error:", err);
    res.status(500).json({ ok: false, error: err.message });
  }
});

/* ---------------- Health + Keepalive ---------------- */
app.get("/",        (_req, res) => res.status(200).send("APlus pipeline up"));
app.get("/healthz", (_req, res) => res.status(200).json({ ok: true }));
app.get("/health",  (_req, res) => res.status(200).json({ ok: true })); // keepalive target

const KEEPALIVE_URL =
  process.env.KEEPALIVE_URL ||
  (process.env.RENDER_EXTERNAL_URL
    ? `https://${process.env.RENDER_EXTERNAL_URL}/health`
    : `http://127.0.0.1:${ENV.PORT}/health`);

console.log(`[KEEPALIVE] Using ${KEEPALIVE_URL}`);
setInterval(() => {
  fetch(KEEPALIVE_URL, { cache: "no-store" }).catch(() => {});
}, 240000); // every 4 minutes

/* ---------------- TradingView webhook: /aplus ---------------- */
app.post("/aplus", async (req, res) => {
  try {
    // Optional shared secret
    if (ENV.TV_API_KEY) {
      const got = req.headers["x-tv-key"];
      if (!got || got !== ENV.TV_API_KEY) {
        await persistEvent("audit", { route: "/aplus", reason: "unauthorized" }, "tv-guard-fail");
        return res.status(401).json({ error: "unauthorized" });
      }
    }

    // TV sends JSON or raw string
    let raw = req.body, parsed = null;
    if (typeof raw === "string") {
      const s = raw.trim();
      if (s.startsWith("{") && s.endsWith("}")) { try { parsed = JSON.parse(s); } catch {} }
    } else if (raw && typeof raw === "object") parsed = raw;

    await persistEvent("aplus", parsed ?? raw ?? null, "tv-webhook");

    // If compact A+ shape, persist to aplus_signals
    if (parsed && parsed.type === "APlus") await persistAPlus(parsed);

    // Optional Zapier relay
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

/* -------------------------  A+ sample (dev only)  ------------------------- */
// Visit in browser: /aplus/sample?key=YOUR_TV_SHARED_SECRET
if (ENABLE_TEST_ROUTES) {
  app.get("/aplus/sample", async (req, res) => {
    try {
      // optional shared secret
      if (ENV.TV_API_KEY) {
        const got = req.query.key || req.headers["x-tv-key"];
        if (!got || got !== ENV.TV_API_KEY) {
          return res.status(401).json({ error: "unauthorized" });
        }
      }

      const sample = {
        type: "APlus",
        s: "BTC-USD",
        t: Date.now(),
        f: "15",
        p: 50000,
        d: "LONG",
        e: "APlus",
        sc: 77,
        sr: false,
        R: "SCORE|MODE",
      };

      await persistEvent("aplus", sample);
      await persistAPlus(sample);

      return res.json({ ok: true, injected: sample });
    } catch (e) {
      console.error("❌ /aplus/sample error:", e);
      return res.status(500).json({ error: e.message });
    }
  });
}

/* ---------------- Nightly prune (server-side) ---------------- */
async function pruneOld() {
  const days = ENV.PRUNE_DAYS;
  try {
    const q1 = await pg.query(`delete from events        where ts < now() - interval '${days} days'`);
    const q2 = await pg.query(`delete from aplus_signals where ts < now() - interval '${days} days'`);
    const q3 = await pg.query(`delete from dom_snapshots where ts < now() - interval '${days} days'`);
    const q4 = await pg.query(`delete from cvd_ticks     where ts < now() - interval '${days} days'`);
    console.log(`🧹 Prune(${days}d): events=${q1.rowCount}, aplus=${q2.rowCount}, dom=${q3.rowCount}, cvd=${q4.rowCount}`);
    await persistEvent("prune", { days, counts: {
      events: q1.rowCount, aplus: q2.rowCount, dom: q3.rowCount, cvd: q4.rowCount
    }}, "retention");
  } catch (e) {
    console.warn("⚠️ prune error:", e.message);
    await persistEvent("audit", { err: e.message }, "prune-error");
  }
}
setInterval(pruneOld, 24 * 60 * 60 * 1000);

/* ---------------- Cronitor ping (optional) ---------------- */
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

/* --------------- Internal retention (POST variant) --------------- */
// Endpoint you had before (kept for compatibility):
// POST /internal/retention?token=RETENTION_TOKEN
app.post("/internal/retention", async (req, res) => {
  try {
    const tokenHeader = req.headers["x-retention-token"];
    const tokenQuery  = req.query.token;
    const token = (tokenHeader || tokenQuery || "").toString();

    if (process.env.RETENTION_TOKEN && token !== process.env.RETENTION_TOKEN) {
      return res.status(401).json({ error: "unauthorized" });
    }

    const DOM_DAYS = parseInt(process.env.RETENTION_DOM_DAYS  || "30", 10);
    const CVD_DAYS = parseInt(process.env.RETENTION_CVD_DAYS  || "30", 10);
    const APL_DAYS = parseInt(process.env.RETENTION_APLUS_DAYS|| "180", 10);

    const domDel = await pg.query(`delete from dom_snapshots where ts < now() - interval '${DOM_DAYS} days'`);
    const cvdDel = await pg.query(`delete from cvd_ticks     where ts < now() - interval '${CVD_DAYS} days'`);
    const aplDel = await pg.query(`delete from events        where ts < now() - interval '${APL_DAYS} days'`);

    await pg.query("analyze dom_snapshots");
    await pg.query("analyze cvd_ticks");
    await pg.query("analyze events");

    const out = {
      pruned: {
        dom_snapshots: domDel.rowCount || 0,
        cvd_ticks: cvdDel.rowCount || 0,
        events: aplDel.rowCount || 0,
      },
      at: new Date().toISOString(),
    };
    console.log("[RETENTION/POST]", out);
    res.json({ ok: true, ...out });
  } catch (err) {
    console.error("[RETENTION/POST] error:", err.message);
    res.status(500).json({ ok: false, error: err.message });
  }
});

/* ---------------- Start + graceful shutdown ---------------- */
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

// index.js — APlus pipeline (Part 3/3)
import WebSocket from "ws";

/* ---------------------- DOM POLLER (REST) ---------------------- */
const DOM_POLL_MS = ENV.DOM_POLL_MS;
const PRODUCT_ID  = ENV.PRODUCT_ID;
const ZAP_DOM_URL = ENV.ZAP_DOM_URL;
const ZAP_DOM_API_KEY = ENV.ZAP_DOM_API_KEY;

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

  try {
    await globalThis._persistEvent("dom", row, "poll");
    await globalThis._persistDOM(row);
  } catch (e) {
    console.warn("DOM persist error:", e.message);
  }

  // Optional fan-out
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

/* ------------------- CVD via WS + EMA heartbeat ------------------- */
const CVD_EMA_LEN = ENV.CVD_EMA_LEN;
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
          const size  = parseFloat(msg.size || "0");
          const side  = msg.side; // "buy" | "sell"
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

// Persist CVD heartbeat every 15s
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

/* ---------------- Bridge helpers to Part 1 ---------------- */
globalThis._persistEvent = async (kind, payload, note) => persistEvent(kind, payload, note);
globalThis._persistDOM   = async (row) => persistDOM(row);
globalThis._persistCVD   = async (row) => persistCVD(row);

/* =====================================================================
   ADD-ONS: legacy /dom ingest, admin/test endpoints, simple metrics
   These are optional but helpful for debugging and feature parity.
   ===================================================================== */

/* ---------------- Legacy DOM ingest (external poster) ---------------
   Accepts POSTed DOM snapshots (for old pipelines or third-party sources).
   Guarded by Zap API key if provided.
   Body can be JSON:
     { p_bid, q_bid, p_ask, q_ask, sequence, ts? }
--------------------------------------------------------------------- */
app.post("/dom", async (req, res) => {
  try {
    // Optional guard via ZAP_API_KEY
    if (ENV.ZAP_API_KEY) {
      const got = req.headers["x-api-key"];
      if (!got || got !== ENV.ZAP_API_KEY) {
        await persistEvent("audit", { route: "/dom", reason: "unauthorized" }, "dom-guard-fail");
        return res.status(401).json({ error: "unauthorized" });
      }
    }

    let raw = req.body, parsed = null;
    if (typeof raw === "string") {
      const s = raw.trim();
      if (s.startsWith("{") && s.endsWith("}")) { try { parsed = JSON.parse(s); } catch {} }
    } else if (raw && typeof raw === "object") parsed = raw;

    const dom = parsed || {};
    const row = {
      type: "dom",
      ts: dom.ts || new Date().toISOString(),
      p_bid: Number.isFinite(+dom.p_bid) ? +dom.p_bid : null,
      q_bid: Number.isFinite(+dom.q_bid) ? +dom.q_bid : null,
      p_ask: Number.isFinite(+dom.p_ask) ? +dom.p_ask : null,
      q_ask: Number.isFinite(+dom.q_ask) ? +dom.q_ask : null,
      sequence: Number.isFinite(+dom.sequence) ? +dom.sequence : null,
    };

    await persistEvent("dom", row, "external");
    await persistDOM(row);

    return res.json({ ok: true, stored: row });
  } catch (e) {
    console.error("❌ /dom error:", e.message);
    await persistEvent("audit", { err: e.message }, "dom-handler-error");
    return res.status(500).json({ error: "server_error" });
  }
});


/* ---------------------- A+ test injector ----------------------------
   Create a synthetic compact A+ payload so you can verify end-to-end:
   POST /aplus/test
   Headers: x-tv-key must match TV_API_KEY if it’s set.
   Optional body JSON to override fields (s, f, d, p, sc, sr, R).
--------------------------------------------------------------------- */
app.post("/aplus/test", async (req, res) => {
  console.log("✅ APlus received at", new Date().toISOString());
  console.log("Payload:", JSON.stringify(req.body, null, 2));
  try {
    if (ENV.TV_API_KEY) {
      const got = req.headers["x-tv-key"];
      if (!got || got !== ENV.TV_API_KEY) {
        return res.status(401).json({ error: "unauthorized" });
      }
    }
    const now = Date.now();
    const body = (typeof req.body === "object" && req.body) || {};

    const sample = {
      type: "APlus",
      s: body.s ?? ENV.PRODUCT_ID,
      t: body.t ?? now,
      f: body.f ?? "15",
      p: body.p ?? 50000,
      d: body.d ?? "LONG",
      e: body.e ?? "APlus",
      sc: body.sc ?? 77,
      sr: body.sr ?? false,
      R: body.R ?? "SCORE|MODE",
    };

    await persistEvent("aplus", sample, "aplus-test");
    await persistAPlus(sample);

    // Optional relay
    if (ENV.ZAP_B_URL) {
      try {
        const zr = await fetch(ENV.ZAP_B_URL, {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            ...(ENV.ZAP_API_KEY ? { "x-api-key": ENV.ZAP_API_KEY } : {}),
            "x-pipe-tag": "aplus-test",
          },
          body: JSON.stringify({ tag: "aplus-test", product: ENV.PRODUCT_ID, payload: sample, ts: new Date().toISOString() }),
        });
        if (!zr.ok) {
          const txt = await zr.text().catch(() => "");
          console.warn("⚠️ Zap relay non-200 (test):", zr.status, txt);
          await persistEvent("audit", { status: zr.status, txt }, "zap-non200-test");
        }
      } catch (e) {
        console.warn("⚠️ Zap relay error (test):", e.message);
        await persistEvent("audit", { err: e.message }, "zap-error-test");
      }
    }

    return res.json({ ok: true, injected: sample });
  } catch (e) {
    console.error("❌ /aplus/test error:", e.message);
    await persistEvent("audit", { err: e.message }, "aplus-test-error");
    return res.status(500).json({ error: "server_error" });
  }
});

/* ---------------------- Recent data peeks ---------------------------
   Quick paginated “tail” views for sanity checks in a browser.
   - GET /events/recent?limit=100
   - GET /dom/latest?limit=50
   - GET /cvd/latest?limit=100
   - GET /aplus/latest?limit=50
--------------------------------------------------------------------- */
function clampInt(v, def, min, max) {
  const n = parseInt(v ?? def, 10);
  if (!Number.isFinite(n)) return def;
  return Math.max(min, Math.min(max, n));
}

app.get("/events/recent", async (req, res) => {
  try {
    const limit = clampInt(req.query.limit, 100, 1, 1000);
    const { rows } = await pg.query(`select id, ts, kind, product, note, payload from events order by ts desc limit $1`, [limit]);
    res.json({ ok: true, rows });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

app.get("/dom/latest", async (req, res) => {
  try {
    const limit = clampInt(req.query.limit, 50, 1, 1000);
    const { rows } = await pg.query(`select * from dom_snapshots order by ts desc limit $1`, [limit]);
    res.json({ ok: true, rows });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

app.get("/cvd/latest", async (req, res) => {
  try {
    const limit = clampInt(req.query.limit, 100, 1, 5000);
    const { rows } = await pg.query(`select * from cvd_ticks order by ts desc limit $1`, [limit]);
    res.json({ ok: true, rows });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

app.get("/aplus/latest", async (req, res) => {
  try {
    const limit = clampInt(req.query.limit, 50, 1, 1000);
    const { rows } = await pg.query(`select * from aplus_signals order by ts desc limit $1`, [limit]);
    res.json({ ok: true, rows });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

/* -------------------------- Zap test hook ---------------------------
   POST /zap/test  (sends a tiny ping to ZAP_B_URL if set)
   Headers: x-api-key required if you set ZAP_API_KEY in env.
--------------------------------------------------------------------- */
app.post("/zap/test", async (req, res) => {
  try {
    if (!ENV.ZAP_B_URL) return res.status(400).json({ ok: false, error: "ZAP_B_URL not set" });

    if (ENV.ZAP_API_KEY) {
      const got = req.headers["x-api-key"];
      if (!got || got !== ENV.ZAP_API_KEY) {
        return res.status(401).json({ ok: false, error: "unauthorized" });
      }
    }

    const payload = { hello: "zap", ts: new Date().toISOString(), product: ENV.PRODUCT_ID };
    const r = await fetch(ENV.ZAP_B_URL, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        ...(ENV.ZAP_API_KEY ? { "x-api-key": ENV.ZAP_API_KEY } : {}),
        "x-pipe-tag": "zap-test",
      },
      body: JSON.stringify({ tag: "zap-test", payload }),
    });

    const ok = r.ok;
    const txt = await r.text().catch(() => "");
    if (!ok) await persistEvent("audit", { status: r.status, txt }, "zap-test-non200");
    res.json({ ok, status: r.status, body: txt.slice(0, 400) });
  } catch (e) {
    await persistEvent("audit", { err: e.message }, "zap-test-error");
    res.status(500).json({ ok: false, error: e.message });
  }
});


/* ----------------------------- Metrics ------------------------------
   GET /metrics/simple
   Lightweight counts + freshest timestamps (no Prometheus).
--------------------------------------------------------------------- */
app.get("/metrics/simple", async (_req, res) => {
  try {
    const q = async (sql) => (await pg.query(sql)).rows[0];
    const m = {
      events:        await q(`select count(*)::int c, coalesce(max(ts),'1970-01-01'::timestamptz) mx from events`),
      aplus_signals: await q(`select count(*)::int c, coalesce(max(ts),'1970-01-01'::timestamptz) mx from aplus_signals`),
      dom_snapshots: await q(`select count(*)::int c, coalesce(max(ts),'1970-01-01'::timestamptz) mx from dom_snapshots`),
      cvd_ticks:     await q(`select count(*)::int c, coalesce(max(ts),'1970-01-01'::timestamptz) mx from cvd_ticks`),
    };
    res.json({ ok: true, generated_at: new Date().toISOString(), metrics: m });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});


/* -------------------------- Env inspector ---------------------------
   GET /env (sanitized) – for debugging only. Hides secrets.
--------------------------------------------------------------------- */
app.get("/env", (_req, res) => {
  try {
    const hide = (v) => (v ? "***" : "");
    res.json({
      ok: true,
      env: {
        PRODUCT_ID: ENV.PRODUCT_ID,
        TV_API_KEY: hide(ENV.TV_API_KEY),
        ZAP_B_URL: !!ENV.ZAP_B_URL,
        ZAP_API_KEY: hide(ENV.ZAP_API_KEY),
        ZAP_DOM_URL: !!ENV.ZAP_DOM_URL,
        ZAP_DOM_API_KEY: hide(ENV.ZAP_DOM_API_KEY),
        DOM_POLL_MS: ENV.DOM_POLL_MS,
        CVD_EMA_LEN: ENV.CVD_EMA_LEN,
        PRUNE_DAYS: ENV.PRUNE_DAYS,
        CRONITOR_URL: !!ENV.CRONITOR_URL,
        PG_MODE: (ENV.PGHOST && ENV.PGUSER && ENV.PGDATABASE) ? "fields" : (ENV.DATABASE_URL ? "url" : "none"),
        PORT: ENV.PORT,
      }
    });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});
