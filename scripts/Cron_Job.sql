-- ============================================================
-- Cron_Job.sql
-- Multi-Tenant Event Analytics System — Maintenance Schedule
-- ============================================================
-- Ensure extension is active
CREATE EXTENSION IF NOT EXISTS pg_cron;

-- 1. Monthly lifecycle (create partitions + archive/drop old ones)
-- Run on 1st of month at 03:00 UTC
-- This replaces the separate 'create-partitions' and 'drop-old-partitions' jobs
SELECT
    cron.schedule ('monthly-lifecycle', '0 3 1 * *', $$
        SELECT
            run_lifecycle (12, TRUE, FALSE) $$);

-- 2. Daily Materialized View Refresh
-- Run every day at 01:00 UTC
SELECT
    cron.schedule ('daily-view-refresh', '0 1 * * *', $$ CALL refresh_daily_views () $$);

-- 3. Weekly Materialized View Refresh (Cohorts)
-- Run every Monday at 02:00 UTC
SELECT
    cron.schedule ('weekly-view-refresh', '0 2 * * 1', $$ CALL refresh_weekly_views () $$);

-- 4. Hourly Deduplication Log Purge
-- Run every hour at minute 0
SELECT
    cron.schedule ('hourly-dedup-purge', '0 * * * *', $$ CALL purge_dedup_log (48) $$);

-- 5. Daily Schema Backup Verification (dry run)
-- Run every day at 04:00 UTC to verify database integrity
SELECT
    cron.schedule ('daily-integrity-check', '0 4 * * *', $$ CALL verify_backup_integrity () $$);

