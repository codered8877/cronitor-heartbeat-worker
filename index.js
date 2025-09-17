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
  DOM_POLL_MS: Math.max(2000, parseInt(process.env.DOM_POLL_MS || "9000", 10)), // 8–10s is fine
  CVD_EMA_LEN: Math.max(2, parseInt(process.env.CVD_EMA_LEN || "34", 10)),
  OFI_EMA_LEN: Math.max(2, parseInt(process.env.OFI_EMA_LEN || "34", 10)),     // NEW

  // Freshness thresholds
  MAX_DOM_AGE_MS: parseInt(process.env.MAX_DOM_AGE_MS || "30000", 10), // 30s
  MAX_CVD_AGE_MS: parseInt(process.env.MAX_CVD_AGE_MS || "30000", 10), // 30s

  // Cooldown (per-direction)
  COOLDOWN_SEC: parseInt(process.env.COOLDOWN_SEC || "300", 10), // 5 minutes

  // Impact-aware sizing thresholds (override via env if you like)
  IMP_SPREAD_TIGHT_BPS: parseFloat(process.env.IMP_SPREAD_TIGHT_BPS || "1.5"),  // <=1.5 bps
  IMP_SPREAD_WIDE_BPS:  parseFloat(process.env.IMP_SPREAD_WIDE_BPS  || "6"),    // >=6 bps
  IMP_VOL_CALM_BPS:     parseFloat(process.env.IMP_VOL_CALM_BPS     || "5"),    // <=5 bps (60s)
  IMP_VOL_TURB_BPS:     parseFloat(process.env.IMP_VOL_TURB_BPS     || "25"),   // >=25 bps (60s)

  // Payload freshness (APlus payload timestamp)
  MAX_AGE_MS: parseInt(process.env.MAX_AGE_MS || "180000", 10), // 3 minutes

  // Optional score threshold (kept for completeness; safe default)
  MIN_SCORE:  parseInt(process.env.MIN_SCORE  || "0", 10),
  
  // Regime gating (single brain lives in strategy; server only enforces *if* enabled)
  // Defaults are SAFE: shadow-log only (no blocking).
  REQUIRE_REGIME_OK: (process.env.REQUIRE_REGIME_OK ?? "false").toLowerCase() === "true",
  REQUIRE_REGIME_FOR_SIDEWAYS_ONLY: (process.env.REQUIRE_REGIME_FOR_SIDEWAYS_ONLY ?? "false").toLowerCase() === "true",
  MIN_REGIME_CONF: Number.isFinite(parseFloat(process.env.MIN_REGIME_CONF))
    ? parseFloat(process.env.MIN_REGIME_CONF)
    : null,
  
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
    max_dom_age_ms: ENV.MAX_DOM_AGE_MS,
    max_cvd_age_ms: ENV.MAX_CVD_AGE_MS,
    cooldown_sec: ENV.COOLDOWN_SEC,
    prune_days: ENV.PRUNE_DAYS,
    regime_gate: ENV.REQUIRE_REGIME_OK,
    regime_sideways_only: ENV.REQUIRE_REGIME_FOR_SIDEWAYS_ONLY,
    min_regime_conf: ENV.MIN_REGIME_CONF,
    tv_guard: !!ENV.TV_API_KEY,
    zap_relay: !!ENV.ZAP_B_URL,
    dom_fanout: !!ENV.ZAP_DOM_URL,
    cronitor: !!ENV.CRONITOR_URL,
    pg_mode: usingFields ? "fields" : usingURL ? "url" : "none",
    port: ENV.PORT,
    ofi_ema_len: ENV.OFI_EMA_LEN,
    impact_cfg: {
      tight: { spread_bps: ENV.IMP_SPREAD_TIGHT_BPS, vol_bps: ENV.IMP_VOL_CALM_BPS },
      wide:  { spread_bps: ENV.IMP_SPREAD_WIDE_BPS,  vol_bps: ENV.IMP_VOL_TURB_BPS }
    }
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
      kind      text not null,
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
      dir         text,
      price       double precision,
      score       int,
      spider_rej  boolean,
      reasons     text
    );
  `);

  // Extend aplus_signals with regime fields (idempotent)
  await pg.query(`
    alter table if exists aplus_signals
      add column if not exists regime text,
      add column if not exists regime_conf double precision
  `);
  await pg.query(`create index if not exists idx_aplus_regime on aplus_signals(regime)`);

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

  /* -------------------- OFI (order-flow imbalance) -------------------- */
  await pg.query(`
    create table if not exists ofi_ticks (
      id          bigserial primary key,
      ts          timestamptz not null default now(),
      product     text not null,
      ofi         double precision,
      ofi_ema     double precision
    );
  `);
  
  // --- Trade feedback
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
  await pg.query(`create index if not exists idx_ofi_ts         on ofi_ticks(ts desc)`);
  
  console.log("📦 DB schema ready.");
}
async function persistEvent(kind, payload, note = null) {
  await pg.query(
    `insert into events(kind, product, payload, note) values ($1,$2,$3,$4)`,
    [kind, ENV.PRODUCT_ID, payload ?? null, note]
  );
}
async function persistAPlus(compact) {
  const { s, f, d, p, sc, sr, R, regime: rg, regime_conf: rc } = compact || {};
  const dirUC = d ? String(d).toUpperCase() : null;
  const symUC = s ? String(s).toUpperCase() : null;

  await pg.query(
    `insert into aplus_signals(product, symbol, tf, dir, price, score, spider_rej, reasons, regime, regime_conf)
     values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
    [
      ENV.PRODUCT_ID,
      symUC,
      f ?? null,
      dirUC,
      p != null ? Number(p) : null,
      Number.isFinite(Number(sc)) ? Number(sc) : null,
      !!sr,
      typeof R === "string" ? R : null,
      rg != null ? String(rg) : null,
      Number.isFinite(Number(rc)) ? Number(rc) : null,
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

// --- OFI (order-flow imbalance) persist ---
async function persistOFI(row) {
  await pg.query(
    `insert into ofi_ticks(product, ofi, ofi_ema) values ($1,$2,$3)`,
    [ENV.PRODUCT_ID, row.ofi ?? 0, row.ofi_ema ?? 0]
  );
}

// ---------- A+ validation & cooldown helpers ----------
function isFiniteNum(n) { return Number.isFinite(+n); }

function validateAPlus(p) {
  if (!p || p.type !== "APlus") return { ok:false, reason:"not APlus" };
  if (!p.s) return { ok:false, reason:"missing symbol" };
  if (!p.d || !["LONG","SHORT"].includes(String(p.d).toUpperCase())) return { ok:false, reason:"bad dir" };
  if (!isFiniteNum(p.p)) return { ok:false, reason:"bad price" };
  if (ENV.MIN_SCORE && !isFiniteNum(p.sc)) return { ok:false, reason:"missing score" };
  if (ENV.MIN_SCORE && +p.sc < ENV.MIN_SCORE) return { ok:false, reason:"score<threshold" };

  // freshness check using payload timestamp t (ms epoch or ISO)
  if (p.t) {
    const tms = typeof p.t === "number" ? p.t : Date.parse(p.t);
    if (Number.isFinite(tms) && (Date.now() - tms > ENV.MAX_AGE_MS)) {
      return { ok:false, reason:"stale payload" };
    }
  }

  // optional strict symbol match to PRODUCT_ID
  if (ENV.PRODUCT_ID && String(p.s).toUpperCase() !== String(ENV.PRODUCT_ID).toUpperCase()) {
    return { ok:false, reason:"symbol!=PRODUCT_ID" };
  }
  return { ok:true };
}

// DB-backed duplicate/cooldown (same dir & ~same price within COOLDOWN_SEC)
async function isDupeOrCooling(p) {
  const cooldown = Math.max(0, ENV.COOLDOWN_SEC);
  if (!cooldown) return false;
  const epsilon = Math.max(0, (+p.p || 0) * 0.0001); // ~1bp tolerance
  const { rows } = await pg.query(
    `select 1
       from aplus_signals
      where product = $1
        and dir = $2
        and ts >= now() - interval '${cooldown} seconds'
        and price between $3 and $4
      limit 1`,
    [ENV.PRODUCT_ID, String(p.d).toUpperCase(), +p.p - epsilon, +p.p + epsilon]
  );
  return rows.length > 0;
}

// ---------- Regime helpers (server enforces ONLY if enabled) ----------
function _parseRegimeFlags(r) {
  const s = (r || "").toString().toLowerCase();

  // Treat common synonyms and analyst labels:
  //  - Bull: bull/bullish, up, uptrend, trend up, accumulating, expansion/expanding
  //  - Bear: bear/bearish, down, downtrend, trend down, distribution, contraction
  //  - Side: sideways, flat, neutral, range/ranging, chop/choppy, consolidation, compression
  const bull = /\b(bull|bullish|up|uptrend|trend\s*up|trending\s*up|accum|accumulation|expand|expansion|expanding)\b/.test(s);
  const bear = /\b(bear|bearish|down|downtrend|trend\s*down|trending\s*down|distrib|distribution|contract|contraction)\b/.test(s);
  const side = /\b(side|sideways|flat|neutral|range|ranging|chop|choppy|consol|consolidation|compress|compression)\b/.test(s);

  return { bull, bear, side };
}
function dirAllowedByRegime(dir, regime, conf, opts) {
  const { requireOk, sidewaysOnly, minConf } = opts || {};
  if (!requireOk) return true;                 // enforcement disabled → always allow
  if (regime == null) return true;            // no regime provided → allow
  if (Number.isFinite(minConf) && Number(conf) < minConf) return true; // low conf → allow

  const d = String(dir || "").toUpperCase();
  const r = _parseRegimeFlags(regime);

  if (sidewaysOnly) {
    // Only block sideways/neutral regimes; let bull/bear pass
    return !r.side;
  }
  if (d === "LONG")  return r.bull;
  if (d === "SHORT") return r.bear;
  return true;
}

/* ---------- Symbol normalizer → coerce to ENV.PRODUCT_ID ---------- */
function normalizeToProductId(symRaw, productId = ENV.PRODUCT_ID) {
  if (!symRaw) return symRaw;
  const want = String(productId).toUpperCase();         // e.g. BTC-USD
  const s0   = String(symRaw).trim().toUpperCase();     // raw

  // strip exchange prefix: BINANCE:BTCUSDT → BTCUSDT
  const s1 = s0.includes(":") ? s0.split(":").pop() : s0;

  // remove non-alnum/dash; keep dash to catch already-correct forms
  const s2 = s1.replace(/[^A-Z0-9\-]/g, "");

  // direct hit?
  if (s2 === want) return want;

  // common aliases → normalized two-leg with dash
  // USDT → USD (many webhooks send BTCUSDT for BTC-USD)
  let core = s2.replace("USDT", "USD");

  // if it’s a compact pair like BTCUSD, insert dash before the quote leg
  const QUOTES = ["USD","USDC","EUR","GBP","JPY","AUD","CAD","CHF","NZD","BRL","MXN","TRY","ZAR","HKD","SGD"];
  for (const q of QUOTES) {
    if (core.endsWith(q) && !core.includes("-")) {
      const base = core.slice(0, -q.length);
      core = `${base}-${q}`;
      break;
    }
  }

  // XBT → BTC (some feeds use XBTUSD)
  core = core.replace(/^XBT-/, "BTC-");

  // last chance: if still not equal but same base/quote, coerce to want
  const asBaseQuote = (s) => {
    const [b,q] = s.split("-");
    return { b, q };
  };
  if (core.includes("-")) {
    const a = asBaseQuote(core);
    const w = asBaseQuote(want);
    if (a.b === w.b && a.q === w.q) return want;
  }

  // default to product id
  return want;
}

/* ---------- DOM/CVD gate helper (place above /aplus) ---------- */
async function gateDomCvd(parsed) {
  const dir = String(parsed.d).toUpperCase();

  const [domQ, cvdQ] = await Promise.all([
  pg.query(`select * from dom_snapshots where product = $1 order by ts desc limit 1`, [ENV.PRODUCT_ID]),
  pg.query(`select * from cvd_ticks     where product = $1 order by ts desc limit 1`, [ENV.PRODUCT_ID]),
]);
  const dom = domQ.rows[0] || null;
  const cvd = cvdQ.rows[0] || null;

  const nowMs = Date.now();

  const domFreshOk =
    !!dom && dom.ts && (nowMs - new Date(dom.ts).getTime() <= ENV.MAX_DOM_AGE_MS);

  const domSpreadOk =
    !!dom && Number.isFinite(dom.p_bid) && Number.isFinite(dom.p_ask) && dom.p_ask > dom.p_bid;

  const cvdFreshOk =
    !!cvd && cvd.ts && (nowMs - new Date(cvd.ts).getTime() <= ENV.MAX_CVD_AGE_MS);

  const cvdDirOk =
    !!cvd && (dir === "LONG" ? cvd.cvd_ema > 0 : dir === "SHORT" ? cvd.cvd_ema < 0 : true);

  const ok = domFreshOk && domSpreadOk && cvdFreshOk && cvdDirOk;

  return {
    ok,
    details: {
      dir,
      domFreshOk,
      domSpreadOk,
      cvdFreshOk,
      cvdDirOk,
      dom_age_ms: dom?.ts ? nowMs - new Date(dom.ts).getTime() : null,
      cvd_age_ms: cvd?.ts ? nowMs - new Date(cvd.ts).getTime() : null,
      cvd_ema: cvd?.cvd_ema ?? null,
      domRow: dom,
      cvdRow: cvd,
    }
  };
}

/* -------------------------- Express app -------------------------- */
const app = express();
// TV mobile sometimes posts text/plain
app.use(express.json({ limit: "1mb", type: ["application/json", "text/json"] }));
app.use(express.text({ limit: "1mb", type: ["text/plain"] }));         // plain text only
app.use(express.urlencoded({ limit: "1mb", extended: false }));        // proper form parser for TV form posts

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

/* ----------------------------- KPIs by Regime ----------------------------- */
app.get("/perf/by_regime", async (req, res) => {
  try {
    const days = Math.max(1, Math.min(365, parseInt(req.query.days || "90", 10)));
    const minScore = Number.isFinite(+req.query.min_score) ? Math.trunc(+req.query.min_score) : null;
    const dirFilter = (req.query.dir || "").toUpperCase(); // LONG|SHORT|""

    // Interval literal is safe here (bounded int)
    const sinceInterval = `now() - interval '${days} days'`;

    // JOIN with optional score filter; parameter index for score is $1 if present
    const joinScore = minScore != null
      ? `left join aplus_signals asig on asig.id = tf.signal_id and asig.score >= $1`
      : `left join aplus_signals asig on asig.id = tf.signal_id`;

    // Build params per query in the same order the SQL expects:
    // score is $1 (if present), then dir (if present)
    const baseParams = [];
    if (minScore != null) baseParams.push(minScore);
    if (dirFilter === "LONG" || dirFilter === "SHORT") baseParams.push(dirFilter);

    // If score is present it's $1, so dir becomes $2; otherwise dir is $1
    const dirParamIndex = (dirFilter === "LONG" || dirFilter === "SHORT")
      ? (minScore != null ? 2 : 1)
      : null;

    // Core
    const coreSql = `
      with t as (
        select tf.ts, tf.rr, tf.outcome, asig.dir, asig.symbol, asig.score
        from trade_feedback tf
        ${joinScore}
        where tf.ts >= ${sinceInterval}
        ${dirParamIndex ? `and coalesce(asig.dir, '') = $${dirParamIndex}` : ""}
      )
      select
        count(*)::int as trades,
        avg(case when rr > 0 then 1 else 0 end)::float as hit_rate,
        avg(rr)::float as expectancy_rr,
        sum(case when rr > 0 then rr else 0 end)
          / nullif(abs(sum(case when rr <= 0 then rr else 0 end)), 0)::float as profit_factor,
        min(ts) as period_start,
        max(ts) as period_end
      from t
    `;
    const core = (await pg.query(coreSql, baseParams)).rows[0];

    // By direction
    const byDirSql = `
      with t as (
        select coalesce(asig.dir,'NA') as dir, tf.rr
        from trade_feedback tf
        ${joinScore}
        where tf.ts >= ${sinceInterval}
        ${dirParamIndex ? `and coalesce(asig.dir, '') = $${dirParamIndex}` : ""}
      )
      select dir,
             count(*)::int as n,
             avg(case when rr>0 then 1 else 0 end)::float as hit_rate,
             avg(rr)::float as exp_rr
      from t
      group by dir
      order by dir
    `;
    const by_dir = (await pg.query(byDirSql, baseParams)).rows;

    // Score buckets
    const byScoreSql = `
      with t as (
        select coalesce(asig.score, 0) as score, tf.rr
        from trade_feedback tf
        ${joinScore}
        where tf.ts >= ${sinceInterval}
        ${dirParamIndex ? `and coalesce(asig.dir, '') = $${dirParamIndex}` : ""}
      )
      select
        width_bucket(score, 0, 100, 5) as bucket,
        count(*)::int as n,
        round(avg(score)::numeric, 1) as avg_score,
        avg(case when rr>0 then 1 else 0 end)::float as hit_rate,
        avg(rr)::float as exp_rr
      from t
      group by bucket
      order by bucket
    `;
    const by_score_bucket = (await pg.query(byScoreSql, baseParams)).rows;

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
    console.error("[/perf/by_regime] error:", e.message);
    res.status(500).json({ ok: false, error: e.message });
  }
});
console.log("🧮 Regime perf route enabled: GET /perf/by_regime");

/* ------------------------- RETENTION (GET) ------------------------ */
// token via header `X-Auth-Token` or query `?token=...`
const RETENTION_TOKEN = process.env.RETENTION_TOKEN || "";

app.get("/retention", async (req, res) => {
  const token = req.get("X-Auth-Token") || req.query.token;
  if (!RETENTION_TOKEN || token !== RETENTION_TOKEN) {
    return res.status(401).json({ ok: false, error: "unauthorized" });
  }

  const tables = [
  { name: "aplus_signals", windowed: true,  ts: "ts" },
  { name: "events",        windowed: true,  ts: "ts" },
  { name: "dom_snapshots", windowed: true,  ts: "ts" },
  { name: "cvd_ticks",     windowed: true,  ts: "ts" },
  { name: "ofi_ticks",     windowed: true,  ts: "ts" },   // ← add this line
  { name: "trade_feedback",windowed: false, ts: "ts" },
];
  const vacuums = ["aplus_signals", "events", "dom_snapshots", "cvd_ticks", "ofi_ticks", "trade_feedback"];

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

    const plan = tables
      .filter(t => t.windowed)
      .map(t => ({ table: t.name, interval: `${ENV.PRUNE_DAYS} days` }));

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
      { name: "ofi_ticks",     windowed: true,  ts: "ts" },
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
    const tables = ["aplus_signals", "events", "dom_snapshots", "cvd_ticks", "ofi_ticks", "trade_feedback"];
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

    // Immediately after: preview raw (trim if too long)
    const rawPreview = typeof raw === "string" ? raw.slice(0, 500) : raw;
    console.log("[/aplus] raw body:", rawPreview);

    // Parse raw → parsed
    if (typeof raw === "string") {
      const s = raw.trim();
      if (s.startsWith("{") && s.endsWith("}")) { try { parsed = JSON.parse(s); } catch {} }
    } else if (raw && typeof raw === "object") {
      parsed = raw;
    }

    // AFTER parsing: log parsed object
    const parsedPreview = JSON.stringify(parsed);
    console.log("[/aplus] parsed:", parsedPreview);

    // Always log receipt to DB
    await persistEvent("aplus", parsed ?? raw ?? null, "tv-webhook");

    // Only the compact payload goes through the full gate
    if (!(parsed && parsed.type === "APlus")) {
    return res.json({ ok: true });
    }

    // --- normalize symbol to your server’s PRODUCT_ID
    const rawSym = parsed.s; // keep original
    parsed.s = normalizeToProductId(parsed.s, ENV.PRODUCT_ID);

    // Optional: hard-verify base/quote match to avoid misroutes
    const toBQ = (s) => {
      if (!s || !s.includes("-")) return { b: null, q: null };
      const [b,q] = s.toUpperCase().split("-");
      return { b, q };
    };
    const wantBQ = toBQ(ENV.PRODUCT_ID);
    const rawBQ  = toBQ((rawSym || "").toString().replace("USDT","USD").replace(/^XBT-/, "BTC-"));

    if (wantBQ.b && rawBQ.b && (wantBQ.b !== rawBQ.b || wantBQ.q !== rawBQ.q)) {
      await persistEvent("audit", { route: "/aplus", raw: rawSym, want: ENV.PRODUCT_ID }, "symbol-mismatch-skip");
      return res.status(202).json({ ok: true, skipped: "symbol_mismatch" });
    }

    // ---- 1) Payload validation (fast fail)
    const v = validateAPlus(parsed);
    if (!v.ok) {
      await persistEvent("audit", { route: "/aplus", reason: v.reason }, "aplus-validate-fail");
      return res.status(202).json({ ok: true, skipped: v.reason });
    }

    // ---- 2) DB-backed cooldown / de-dup
    if (await isDupeOrCooling(parsed)) {
      await persistEvent("audit", { route: "/aplus", reason: "cooldown" }, "aplus-cooldown-skip");
      return res.status(202).json({ ok: true, skipped: "cooldown" });
    }

    // ---- 3) DOM & CVD checks (freshness + logic alignment via helper)
const gate = await gateDomCvd(parsed);

console.log("[/aplus] gate check:", gate.details);

if (!gate.ok) {
  await persistEvent("audit", { route: "/aplus", gate: gate.details }, "dom-cvd-skip");
  return res.status(202).json({ ok: true, skipped: "DOM/CVD" });
}

await persistEvent(
  "audit",
  { route: "/aplus", gate: "CLEARED", dir: gate.details.dir, cvd_ema: gate.details.cvd_ema },
  "dom-cvd-cleared"
);

/* ---------- 3a) Enrich with OFI + impact sizing ---------- */
let ofi_ema  = null;
let spread_bps = null;
let impact_bucket = "tight";   // default
let size_mult = 1.0;

try {
  // 3a-1) Pull most recent OFI tick
  const ofiQ = await pg.query(
    `select ofi, ofi_ema from ofi_ticks where product = $1 order by ts desc limit 1`,
    [ENV.PRODUCT_ID]
  );
  ofi_ema = ofiQ.rows[0]?.ofi_ema ?? null;

  // 3a-2) Compute current spread (bps) from the same DOM row we used in the gate
  const dom = gate?.details?.domRow || null;
  if (dom && Number.isFinite(dom.p_bid) && Number.isFinite(dom.p_ask) && dom.p_ask > dom.p_bid) {
    const mid = 0.5 * (Number(dom.p_bid) + Number(dom.p_ask));
    const spr = Number(dom.p_ask) - Number(dom.p_bid);
    spread_bps = mid > 0 ? (spr / mid) * 10_000 : null;
  }

  // 3a-3) Bucketize impact and choose a conservative size multiplier
  if (Number.isFinite(spread_bps)) {
    const tightCut = Number(ENV.IMP_SPREAD_TIGHT_BPS); // e.g. 1–2
    const wideCut  = Number(ENV.IMP_SPREAD_WIDE_BPS);  // e.g. 4–6
    impact_bucket =
      spread_bps <= tightCut ? "tight" :
      spread_bps >= wideCut  ? "wide"  : "normal";
  }

  // Optionally tilt by volatility regime if you also pass VOL bps in ENV
  const volCalm = Number(ENV.IMP_VOL_CALM_BPS || 0);
  const volTurb = Number(ENV.IMP_VOL_TURB_BPS || 0);
  // If you later add realized vol (e.g., 1m ATR% in events), fold it here.

  // Simple sizing rules (tweak to taste):
  // - Wide spreads or negative OFI → downsize
  // - Tight spreads and positive OFI → allow slight upsize
  const ofiUp   = ofi_ema != null && ofi_ema > 0;
  const ofiDown = ofi_ema != null && ofi_ema < 0;

  if (impact_bucket === "wide") {
    size_mult = ofiUp ? 0.75 : 0.5;
  } else if (impact_bucket === "tight") {
    size_mult = ofiUp ? 1.15 : 1.0;
  } else {
    size_mult = ofiDown ? 0.85 : 1.0;
  }

  // Attach enrichment to payload (will go to Zap + audit; DB schema for aplus_signals stays unchanged)
  parsed.meta = {
    ...(parsed.meta || {}),
    ofi_ema,
    impact: {
      spread_bps,
      bucket: impact_bucket,
      size_mult
    }
  };

  await persistEvent(
    "audit",
    { route: "/aplus", ofi_ema, spread_bps, impact_bucket, size_mult },
    "aplus-enrich"
  );
} catch (e) {
  console.warn("[/aplus] enrich error:", e.message);
}

// ---- 3b) Regime shadow-gate (LOG what we *would* do; do NOT block)
const rg = parsed.regime ?? null;
const rc = parsed.regime_conf ?? null;
const wouldAllow = dirAllowedByRegime(parsed.d, rg, rc, {
  requireOk: true, // evaluate as-if enforcement ON
  sidewaysOnly: ENV.REQUIRE_REGIME_FOR_SIDEWAYS_ONLY,
  minConf: ENV.MIN_REGIME_CONF,
});
await persistEvent(
  "audit",
  {
    route: "/aplus",
    shadow: "regime",
    regime: rg,
    regime_conf: rc,
    dir: parsed.d,
    would_block: !wouldAllow
  },
  "regime-shadow"
);

// ---- 3c) Optional real enforcement (enabled via env)
if (ENV.REQUIRE_REGIME_OK) {
  const allow = dirAllowedByRegime(parsed.d, rg, rc, {
    requireOk: true,
    sidewaysOnly: ENV.REQUIRE_REGIME_FOR_SIDEWAYS_ONLY,
    minConf: ENV.MIN_REGIME_CONF,
  });
  if (!allow) {
    await persistEvent(
      "audit",
      { route: "/aplus", regime: rg, regime_conf: rc, dir: parsed.d },
      "regime-block"
    );
    return res.status(202).json({ ok: true, skipped: "regime_gate" });
  }
}
    
// ---- 4) Persist A+ signal, then optional Zapier relay
await persistAPlus(parsed);

/* ---------- Optional Zapier forward ---------- */
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
        payload: parsed,                       // the compact A+ you just validated/persisted
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

    return res.json({ ok: true, gated: "passed" });
  } catch (e) {
    console.error("❌ /aplus error:", e.message);
    await persistEvent("audit", { err: e.message }, "aplus-handler-error");
    return res.status(500).json({ error: "server_error" });
  }
});

/* ---------------- Nightly prune (server-side) ---------------- */
async function pruneOld() {
  const days = ENV.PRUNE_DAYS;
  try {
    const q1 = await pg.query(`delete from events        where ts < now() - interval '${days} days'`);
    const q2 = await pg.query(`delete from aplus_signals where ts < now() - interval '${days} days'`);
    const q3 = await pg.query(`delete from dom_snapshots where ts < now() - interval '${days} days'`);
    const q4 = await pg.query(`delete from cvd_ticks     where ts < now() - interval '${days} days'`);
    const q5 = await pg.query(`delete from ofi_ticks     where ts < now() - interval '${days} days'`);

    console.log(
      `🧹 Prune(${days}d): events=${q1.rowCount}, aplus=${q2.rowCount}, dom=${q3.rowCount}, cvd=${q4.rowCount}, ofi=${q5.rowCount}`
    );

    await persistEvent("prune", {
      days,
      counts: {
        events: q1.rowCount,
        aplus: q2.rowCount,
        dom: q3.rowCount,
        cvd: q4.rowCount,
        ofi: q5.rowCount
      }
    }, "retention");
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
    const OFI_DAYS = parseInt(process.env.RETENTION_OFI_DAYS  || "30", 10);
    
    const domDel = await pg.query(`delete from dom_snapshots where ts < now() - interval '${DOM_DAYS} days'`);
    const cvdDel = await pg.query(`delete from cvd_ticks     where ts < now() - interval '${CVD_DAYS} days'`);
    const ofiDel = await pg.query(`delete from ofi_ticks     where ts < now() - interval '${OFI_DAYS} days'`);
    const aplDel = await pg.query(`delete from events        where ts < now() - interval '${APL_DAYS} days'`);

    await pg.query("analyze dom_snapshots");
    await pg.query("analyze cvd_ticks");
    await pg.query("analyze ofi_ticks");
    await pg.query("analyze events");

    const out = {
      pruned: {
        dom_snapshots: domDel.rowCount || 0,
        cvd_ticks:     cvdDel.rowCount || 0,
        ofi_ticks:     ofiDel.rowCount || 0,
        events:        aplDel.rowCount || 0,
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
  if (domTickRunning) return;   // skip if a previous tick is still running
  domTickRunning = true;
  try {
    const row = await fetchDOM();
    if (!row) return;

    try {
      await globalThis._persistEvent("dom", row, "poll");
      await globalThis._persistDOM(row);
    } catch (e) {
      console.warn("DOM persist error:", e.message);
    }

    // --- Compute OFI from successive DOM snapshots (simple microstructure proxy)
    if (lastDom) {
      const pBidNow = Number(row.p_bid ?? 0),  pBidPrev = Number(lastDom.p_bid ?? 0);
      const pAskNow = Number(row.p_ask ?? 0),  pAskPrev = Number(lastDom.p_ask ?? 0);
      const qBidNow = Number(row.q_bid ?? 0),  qBidPrev = Number(lastDom.q_bid ?? 0);
      const qAskNow = Number(row.q_ask ?? 0),  qAskPrev = Number(lastDom.q_ask ?? 0);

      const dBid = qBidNow - qBidPrev;
      const dAsk = qAskNow - qAskPrev;
      const bidUp = pBidNow > pBidPrev;
      const bidDn = pBidNow < pBidPrev;
      const askDn = pAskNow < pAskPrev;
      const askUp = pAskNow > pAskPrev;

      let ofiInst = 0;
      if (bidUp) ofiInst += Math.abs(dBid);
      else if (bidDn) ofiInst -= Math.abs(dBid);

      if (askDn) ofiInst += Math.abs(dAsk);
      else if (askUp) ofiInst -= Math.abs(dAsk);

      ofiEma = (ofiEma === null) ? ofiInst : alphaOFI * ofiInst + (1 - alphaOFI) * ofiEma;

      const ofiRow = {
        ofi: ofiInst,
        ofi_ema: ofiEma == null ? null : +ofiEma.toFixed(6)
      };
      await globalThis._persistEvent("ofi", { ts: row.ts, ...ofiRow }, "calc");
      await globalThis._persistOFI(ofiRow);
    }

    // carry current DOM → next iteration baseline
    lastDom = row;

    // Optional fan-out (poller context: never throw; add hard timeout)
    if (ZAP_DOM_URL) {
      const ac = new AbortController();
      const timer = setTimeout(() => ac.abort(), 3000); // 3s hard cap
      try {
        const zr = await fetch(ZAP_DOM_URL, {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            ...(ZAP_DOM_API_KEY ? { "x-api-key": ZAP_DOM_API_KEY } : {}),
          },
          body: JSON.stringify(row),
          signal: ac.signal,
        });
        if (!zr.ok) console.warn("DOM fan-out non-200:", zr.status);
      } catch (e) {
        if (e.name === "AbortError") {
          console.warn("DOM fan-out timeout (3s) — skipped");
        } else {
          console.warn("DOM fan-out error:", e.message);
        }
      } finally {
        clearTimeout(timer);
      }
    }
  } finally {
    domTickRunning = false;     // always release the lock
  }
}
/* -------------------------- OFI state -------------------------- */
const OFI_EMA_LEN = ENV.OFI_EMA_LEN || 34;
const alphaOFI = 2 / (OFI_EMA_LEN + 1);

let lastDom = null;          // previous DOM snapshot (for deltas)
// Use `null` as warmup sentinel so a first 0-value is handled correctly
let ofiEma  = null;          // running EMA of OFI (null until seeded)

/* ---------------- Bridge helpers to Part 1 ---------------- */
globalThis._persistEvent = async (kind, payload, note) => persistEvent(kind, payload, note);
globalThis._persistDOM   = async (row) => persistDOM(row);
globalThis._persistCVD   = async (row) => persistCVD(row);
globalThis._persistOFI   = async (row) => persistOFI(row);

setInterval(domTick, DOM_POLL_MS);
console.log(`⏱️  DOM poll @ ${DOM_POLL_MS}ms → ${PRODUCT_ID}`);

/*------------------- CVD via WS + EMA heartbeat ------------------- */
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

app.get("/ofi/latest", async (req, res) => {
  try {
    const limit = clampInt(req.query.limit, 100, 1, 5000);
    const { rows } = await pg.query(`select * from ofi_ticks order by ts desc limit $1`, [limit]);
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
      ofi_ticks:     await q(`select count(*)::int c, coalesce(max(ts),'1970-01-01'::timestamptz) mx from ofi_ticks`),
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

                // Tuning
        DOM_POLL_MS: ENV.DOM_POLL_MS,
        CVD_EMA_LEN: ENV.CVD_EMA_LEN,
        OFI_EMA_LEN: ENV.OFI_EMA_LEN,

        // Impact awareness thresholds
        IMP_SPREAD_TIGHT_BPS: ENV.IMP_SPREAD_TIGHT_BPS,
        IMP_SPREAD_WIDE_BPS:  ENV.IMP_SPREAD_WIDE_BPS,
        IMP_VOL_CALM_BPS:     ENV.IMP_VOL_CALM_BPS,
        IMP_VOL_TURB_BPS:     ENV.IMP_VOL_TURB_BPS,

        // Kitchen-sink thresholds
        MIN_SCORE: ENV.MIN_SCORE,
        MAX_AGE_MS: ENV.MAX_AGE_MS,
        MAX_DOM_AGE_MS: ENV.MAX_DOM_AGE_MS,
        MAX_CVD_AGE_MS: ENV.MAX_CVD_AGE_MS,
        COOLDOWN_SEC: ENV.COOLDOWN_SEC,

                // Regime
        REQUIRE_REGIME_OK: ENV.REQUIRE_REGIME_OK,
        REQUIRE_REGIME_FOR_SIDEWAYS_ONLY: ENV.REQUIRE_REGIME_FOR_SIDEWAYS_ONLY,
        MIN_REGIME_CONF: ENV.MIN_REGIME_CONF,
        
        // Ops
        PRUNE_DAYS: ENV.PRUNE_DAYS,
        CRONITOR_URL: !!ENV.CRONITOR_URL,
        PG_MODE: (ENV.PGHOST && ENV.PGUSER && ENV.PGDATABASE)
          ? "fields"
          : (ENV.DATABASE_URL ? "url" : "none"),
        PORT: ENV.PORT,
      }
    });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});
