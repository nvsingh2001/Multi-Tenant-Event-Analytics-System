-- Daily Active Users (DAU) from MV
SELECT
    t.name AS tenant_name,
    m.event_date,
    m.dau
FROM
    mv_daily_active_users m
    JOIN tenants t ON t.tenant_id = m.tenant_id
ORDER BY
    m.event_date DESC;

-- Raw DAU query for real-time data
SELECT
    t.name AS tenant_name,
    DATE(e.event_time AT TIME ZONE 'UTC') AS event_date,
    COUNT(DISTINCT e.user_id) AS dau
FROM
    events e
    JOIN tenants t ON t.tenant_id = e.tenant_id
GROUP BY
    t.name,
    event_date
ORDER BY
    event_date DESC;

-- 7-day rolling average DAU
SELECT
    tenant_id,
    event_date,
    dau,
    ROUND(AVG(dau) OVER (PARTITION BY tenant_id ORDER BY event_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 1) AS dau_7day_avg
FROM
    mv_daily_active_users
ORDER BY
    tenant_id,
    event_date DESC;

-- Funnel Analysis: signup -> add_to_cart -> purchase
WITH step1 AS (
    SELECT DISTINCT
        tenant_id,
        user_id
    FROM
        events
    WHERE
        event_name = 'signup'
),
step2 AS (
    SELECT DISTINCT
        e.tenant_id,
        e.user_id
    FROM
        events e
        JOIN step1 s ON s.tenant_id = e.tenant_id
            AND s.user_id = e.user_id
    WHERE
        e.event_name = 'add_to_cart'
),
step3 AS (
    SELECT DISTINCT
        e.tenant_id,
        e.user_id
    FROM
        events e
        JOIN step2 s ON s.tenant_id = e.tenant_id
            AND s.user_id = e.user_id
    WHERE
        e.event_name = 'purchase'
),
funnel AS (
    SELECT
        t.name AS tenant_name,
        COUNT(DISTINCT s1.user_id) AS stage1,
    COUNT(DISTINCT s2.user_id) AS stage2,
    COUNT(DISTINCT s3.user_id) AS stage3
FROM
    tenants t
    LEFT JOIN step1 s1 ON s1.tenant_id = t.tenant_id
        LEFT JOIN step2 s2 ON s2.tenant_id = t.tenant_id
        LEFT JOIN step3 s3 ON s3.tenant_id = t.tenant_id
    GROUP BY
        t.name
)
SELECT
    tenant_name,
    stage1,
    stage2,
    stage3,
    ROUND(100.0 * stage2 / NULLIF (stage1, 0), 1) AS conv1_2,
    ROUND(100.0 * stage3 / NULLIF (stage2, 0), 1) AS conv2_3
FROM
    funnel
ORDER BY
    stage1 DESC;

-- Retention Analysis (Day 1 and Day 7)
WITH cohort_base AS (
    SELECT
        tenant_id,
        user_id,
        DATE_TRUNC('week', first_seen_at)::date AS cohort_week
    FROM
        users
    WHERE
        first_seen_at IS NOT NULL
),
day1 AS (
    SELECT DISTINCT
        u.tenant_id,
        u.user_id,
        c.cohort_week
    FROM
        users u
        JOIN cohort_base c ON c.tenant_id = u.tenant_id
            AND c.user_id = u.user_id
    WHERE
        EXISTS (
            SELECT
                1
            FROM
                events e
            WHERE
                e.tenant_id = u.tenant_id
                AND e.user_id = u.user_id
                AND DATE(e.event_time) = (u.first_seen_at::date + INTERVAL '1 day'))
),
day7 AS (
    SELECT DISTINCT
        u.tenant_id,
        u.user_id,
        c.cohort_week
    FROM
        users u
        JOIN cohort_base c ON c.tenant_id = u.tenant_id
            AND c.user_id = u.user_id
    WHERE
        EXISTS (
            SELECT
                1
            FROM
                events e
            WHERE
                e.tenant_id = u.tenant_id
                AND e.user_id = u.user_id
                AND DATE(e.event_time) = (u.first_seen_at::date + INTERVAL '7 days')))
SELECT
    t.name AS tenant_name,
    c.cohort_week,
    COUNT(DISTINCT c.user_id) AS cohort_size,
    COUNT(DISTINCT d1.user_id) AS ret1,
    COUNT(DISTINCT d7.user_id) AS ret7
FROM
    cohort_base c
    JOIN tenants t ON t.tenant_id = c.tenant_id
    LEFT JOIN day1 d1 ON d1.tenant_id = c.tenant_id
        AND d1.user_id = c.user_id
    LEFT JOIN day7 d7 ON d7.tenant_id = c.tenant_id
        AND d7.user_id = c.user_id
GROUP BY
    t.name,
    c.cohort_week
ORDER BY
    c.cohort_week DESC;

