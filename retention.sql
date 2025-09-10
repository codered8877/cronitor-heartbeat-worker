-- Keep compact A+ signals for 180 days
DELETE FROM aplus_signals
WHERE ts < NOW() - INTERVAL '180 days';

-- Keep raw A+ payloads for 30 days
DELETE FROM aplus_events
WHERE ts < NOW() - INTERVAL '30 days';

-- DOM snapshots are noisy: keep 14 days
DELETE FROM dom_ticks
WHERE ts < NOW() - INTERVAL '14 days';

-- CVD ticks are also noisy: keep 14 days
DELETE FROM cvd_ticks
WHERE ts < NOW() - INTERVAL '14 days';

-- Keep trade feedback forever (optional)
-- DELETE FROM trade_feedback WHERE ts < NOW() - INTERVAL '365 days';

-- Optional: reclaim storage + keep planner stats fresh
VACUUM ANALYZE aplus_signals;
VACUUM ANALYZE aplus_events;
VACUUM ANALYZE dom_ticks;
VACUUM ANALYZE cvd_ticks;
VACUUM ANALYZE trade_feedback;
