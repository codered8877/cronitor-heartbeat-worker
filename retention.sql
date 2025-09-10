-- Keep A+ events 180 days (keep longer for analysis)
delete from aplus_events where ts < now() - interval '180 days';

-- Keep feedback forever (skip pruning)
-- delete from trade_feedback where ts < now() - interval '365 days';

-- DOM/CVD are noisy: keep 14 days
delete from dom_ticks where ts < now() - interval '14 days';
delete from cvd_ticks where ts < now() - interval '14 days';

-- Optional: reclaim space
vacuum analyze aplus_events;
vacuum analyze dom_ticks;
vacuum analyze cvd_ticks;
vacuum analyze trade_feedback;
