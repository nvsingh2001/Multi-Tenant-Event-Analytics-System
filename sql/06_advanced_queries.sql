-- 06_advanced_queries.sql: Complex Insights & Benchmarking
-- Top 5 users per tenant
SELECT
    *
FROM (
    SELECT
        t.name AS tenant_name,
        e.user_id,
        COUNT(*) AS count,
        RANK() OVER (PARTITION BY e.tenant_id ORDER BY COUNT(*) DESC) AS rnk
    FROM
        events e
        JOIN tenants t ON t.tenant_id = e.tenant_id
    GROUP BY
        1,
        e.tenant_id,
        3) r
WHERE
    rnk <= 5;

-- Event distribution % per tenant
SELECT
    t.name AS tenant_name,
    e.event_name,
    COUNT(*) AS count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY e.tenant_id), 2) AS pct
FROM
    events e
    JOIN tenants t ON t.tenant_id = e.tenant_id
GROUP BY
    1,
    e.tenant_id,
    2;

-- Revenue per tenant
SELECT
    t.name AS tenant_name,
    COUNT(*) AS sales,
    SUM((e.properties ->> 'amount')::numeric) AS revenue
FROM
    events e
    JOIN tenants t ON t.tenant_id = e.tenant_id
WHERE
    e.event_name = 'purchase'
GROUP BY
    1;

-- Lapsed users (no events in 30 days)
SELECT
    t.name,
    u.user_id,
    u.last_seen_at
FROM
    users u
    JOIN tenants t ON t.tenant_id = u.tenant_id
    LEFT JOIN events e ON e.tenant_id = u.tenant_id
        AND e.user_id = u.user_id
        AND e.event_time >= NOW() - INTERVAL '30 days'
WHERE
    e.event_id IS NULL;

-- Cross-tenant users
SELECT
    user_id,
    COUNT(DISTINCT tenant_id) AS t_count
FROM
    users
GROUP BY
    1
HAVING
    COUNT(DISTINCT tenant_id) > 1;

-- First event per user
SELECT
    *
FROM (
    SELECT
        e.*,
        ROW_NUMBER() OVER (PARTITION BY tenant_id, user_id ORDER BY event_time ASC) AS rn
    FROM
        events e) n
WHERE
    rn = 1;

-- Session gaps (>30 mins)
SELECT
    tenant_id,
    user_id,
    event_time,
    CASE WHEN event_time - LAG(event_time) OVER (PARTITION BY tenant_id, user_id ORDER BY event_time ASC) > INTERVAL '30 minutes' THEN
        'new_session'
    ELSE
        'same_session'
    END AS session_flag
FROM
    events;

-- High-activity users (> tenant avg)
WITH u_spend AS (
    SELECT
        tenant_id,
        user_id,
        SUM((properties ->> 'amount')::numeric) AS spend
    FROM
        events
    WHERE
        event_name = 'purchase'
    GROUP BY
        1,
        2
),
t_avg AS (
    SELECT
        tenant_id,
        AVG(spend) AS avg_s
    FROM
        u_spend
    GROUP BY
        1
)
SELECT
    u.*
FROM
    u_spend u
    JOIN t_avg t ON t.tenant_id = u.tenant_id
WHERE
    u.spend > t.avg_s;

-- Partition pruning check
EXPLAIN ANALYZE
SELECT
    COUNT(*)
FROM
    events
WHERE
    tenant_id = 'aaaaaaaa-0000-0000-0000-000000000001'
    AND event_time >= '2026-04-01'
    AND event_time < '2026-05-01';

