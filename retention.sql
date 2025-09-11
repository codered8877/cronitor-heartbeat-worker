-- =====================================
-- Retention Policy & Cleanup
-- =====================================

-- Keep APlus signals for 180 days
DELETE FROM aplus_signals
WHERE ts < NOW() - INTERVAL '180 days';

-- Keep raw TradingView events for 30 days
DELETE FROM events
WHERE ts < NOW() - INTERVAL '30 days';

-- Keep DOM snapshots for 14 days
DELETE FROM dom_snapshots
WHERE ts < NOW() - INTERVAL '14 days';

-- Keep CVD ticks for 14 days
DELETE FROM cvd_ticks
WHERE ts < NOW() - INTERVAL '14 days';

-- Keep trade feedback forever (optional):
-- DELETE FROM trade_feedback
-- WHERE ts < NOW() - INTERVAL '180 days';

-- Vacuum tables to reclaim disk space
VACUUM ANALYZE aplus_signals;
VACUUM ANALYZE events;
VACUUM ANALYZE dom_snapshots;
VACUUM ANALYZE cvd_ticks;
