-- 07_materialized_views.sql: MV Definitions & Refresh Logic
-- MV: Daily Active Users
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_daily_active_users AS
SELECT
    tenant_id,
    DATE(event_time AT TIME ZONE 'UTC') AS event_date,
    COUNT(DISTINCT user_id) AS dau
FROM
    events
GROUP BY
    1,
    2 WITH DATA;

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_dau_pk ON mv_daily_active_users (tenant_id, event_date);

-- MV: Revenue Summary
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_revenue_summary AS
SELECT
    tenant_id,
    DATE(event_time AT TIME ZONE 'UTC') AS event_date,
    COUNT(*) AS purchase_count,
    ROUND(SUM((properties ->> 'amount')::numeric), 2) AS total_revenue,
    COUNT(DISTINCT user_id) AS unique_buyers
FROM
    events
WHERE
    event_name = 'purchase'
    AND properties ->> 'amount' IS NOT NULL
GROUP BY
    1,
    2 WITH DATA;

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_revenue_pk ON mv_revenue_summary (tenant_id, event_date);

-- MV: Event Distribution
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_event_type_summary AS
SELECT
    tenant_id,
    DATE(event_time AT TIME ZONE 'UTC') AS event_date,
    event_name,
    COUNT(*) AS event_count
FROM
    events
GROUP BY
    1,
    2,
    3 WITH DATA;

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_event_type_pk ON mv_event_type_summary (tenant_id, event_date, event_name);

-- MV: Weekly Cohort Retention (Day 1/7)
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_weekly_cohort_retention AS
WITH cohort_base AS (
    SELECT
        tenant_id,
        user_id,
        DATE_TRUNC('week', first_seen_at)::date AS cohort_week,
        first_seen_at::date AS day0
    FROM
        users
    WHERE
        first_seen_at IS NOT NULL
),
day1 AS (
    SELECT DISTINCT
        e.tenant_id,
        e.user_id
    FROM
        events e
        JOIN cohort_base c ON c.tenant_id = e.tenant_id
            AND c.user_id = e.user_id
    WHERE
        DATE(e.event_time) = c.day0 + 1
),
day7 AS (
    SELECT DISTINCT
        e.tenant_id,
        e.user_id
    FROM
        events e
        JOIN cohort_base c ON c.tenant_id = e.tenant_id
            AND c.user_id = e.user_id
    WHERE
        DATE(e.event_time) = c.day0 + 7
)
SELECT
    c.tenant_id,
    c.cohort_week,
    COUNT(DISTINCT c.user_id) AS cohort_size,
    COUNT(DISTINCT d1.user_id) AS retained_day1,
    COUNT(DISTINCT d7.user_id) AS retained_day7,
    ROUND(100.0 * COUNT(DISTINCT d1.user_id) / NULLIF (COUNT(DISTINCT c.user_id), 0), 2) AS day1_ret_pct,
    ROUND(100.0 * COUNT(DISTINCT d7.user_id) / NULLIF (COUNT(DISTINCT c.user_id), 0), 2) AS day7_ret_pct
FROM
    cohort_base c
    LEFT JOIN day1 d1 ON d1.tenant_id = c.tenant_id
        AND d1.user_id = c.user_id
    LEFT JOIN day7 d7 ON d7.tenant_id = c.tenant_id
        AND d7.user_id = c.user_id
GROUP BY
    1,
    2 WITH DATA;

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_cohort_pk ON mv_weekly_cohort_retention (tenant_id, cohort_week);

-- Procedures for concurrent refresh
CREATE OR REPLACE PROCEDURE refresh_daily_views ()
LANGUAGE plpgsql
AS $$
BEGIN
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_daily_active_users;
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_revenue_summary;
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_event_type_summary;
END;
$$;

CREATE OR REPLACE PROCEDURE refresh_weekly_views ()
LANGUAGE plpgsql
AS $$
BEGIN
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_weekly_cohort_retention;
END;
$$;

CREATE OR REPLACE PROCEDURE refresh_all_views_full ()
LANGUAGE plpgsql
AS $$
BEGIN
    REFRESH MATERIALIZED VIEW mv_daily_active_users;
    REFRESH MATERIALIZED VIEW mv_revenue_summary;
    REFRESH MATERIALIZED VIEW mv_event_type_summary;
    REFRESH MATERIALIZED VIEW mv_weekly_cohort_retention;
END;
$$;

