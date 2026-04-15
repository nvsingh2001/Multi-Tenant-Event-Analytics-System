-- Q1: Top 5 users per tenant
SELECT
    tenant_name,
    user_id,
    event_count
FROM (
    SELECT
        t.name AS tenant_name,
        e.user_id,
        COUNT(*) AS event_count,
        RANK() OVER (PARTITION BY e.tenant_id ORDER BY COUNT(*) DESC) AS rnk
    FROM
        events e
        JOIN tenants t ON t.tenant_id = e.tenant_id
    GROUP BY
        t.name,
        e.tenant_id,
        e.user_id) r
WHERE
    rnk <= 5;

-- Q2: Event distribution per tenant
SELECT
    t.name AS tenant_name,
    e.event_name,
    COUNT(*) AS count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY e.tenant_id), 2) AS pct
FROM
    events e
    JOIN tenants t ON t.tenant_id = e.tenant_id
GROUP BY
    t.name,
    e.tenant_id,
    e.event_name;

-- Q3: Revenue per tenant
SELECT
    t.name AS tenant_name,
    COUNT(*) AS sales,
    SUM((e.properties ->> 'amount')::NUMERIC) AS revenue
FROM
    events e
    JOIN tenants t ON t.tenant_id = e.tenant_id
WHERE
    e.event_name = 'purchase'
GROUP BY
    t.name;

-- Q4: Lapsed users (no events in 30 days)
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

-- Q5: Cross-tenant users
SELECT
    user_id,
    COUNT(DISTINCT tenant_id) AS t_count
FROM
    users
GROUP BY
    user_id
HAVING
    COUNT(DISTINCT tenant_id) > 1;

-- Q6: First event per user
SELECT
    tenant_id,
    user_id,
    event_name,
    event_time
FROM (
    SELECT
        e.*,
        ROW_NUMBER() OVER (PARTITION BY tenant_id, user_id ORDER BY event_time ASC) AS rn
    FROM
        events e) n
WHERE
    rn = 1;

-- Q7: Session gaps (>30 mins)
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

-- Q8: Running total of events
SELECT
    tenant_id,
    user_id,
    event_time,
    COUNT(*) OVER (PARTITION BY tenant_id, user_id ORDER BY event_time ASC) AS running_total
FROM
    events;

-- Q9: Users with above-average activity
SELECT
    tenant_id,
    user_id,
    COUNT(*)
FROM
    events
GROUP BY
    tenant_id,
    user_id
HAVING
    COUNT(*) > (
        SELECT
            AVG(cnt)
        FROM (
            SELECT
                COUNT(*) AS cnt
            FROM
                events
            GROUP BY
                tenant_id,
                user_id) s);

-- Q10: Latest event per user
SELECT
    e.*
FROM
    events e
WHERE
    e.event_time = (
        SELECT
            MAX(e2.event_time)
        FROM
            events e2
        WHERE
            e2.tenant_id = e.tenant_id
            AND e2.user_id = e.user_id);

-- Q11: High-activity tenants
SELECT
    tenant_id,
    COUNT(*)
FROM
    events
GROUP BY
    tenant_id
HAVING
    COUNT(*) > (
        SELECT
            AVG(cnt)
        FROM (
            SELECT
                COUNT(*) AS cnt
            FROM
                events
            GROUP BY
                tenant_id) s);

-- Q12: Funnel using CTEs
WITH s AS (
    SELECT DISTINCT
        tenant_id,
        user_id
    FROM
        events
    WHERE
        event_name = 'signup'
),
c AS (
    SELECT DISTINCT
        tenant_id,
        user_id
    FROM
        events
    WHERE
        event_name = 'add_to_cart'
),
p AS (
    SELECT DISTINCT
        tenant_id,
        user_id
    FROM
        events
    WHERE
        event_name = 'purchase'
)
SELECT
    t.name,
    COUNT(DISTINCT s.user_id) AS signups,
    COUNT(DISTINCT c.user_id) AS carts,
    COUNT(DISTINCT p.user_id) AS buys
FROM
    tenants t
    LEFT JOIN s ON s.tenant_id = t.tenant_id
    LEFT JOIN c ON c.tenant_id = t.tenant_id
    LEFT JOIN p ON p.tenant_id = t.tenant_id
GROUP BY
    t.name;

-- Q13: Retention using CTEs
WITH f AS (
    SELECT
        tenant_id,
        user_id,
        first_seen_at::DATE AS d0
    FROM
        users
),
d1 AS (
    SELECT DISTINCT
        e.tenant_id,
        e.user_id
    FROM
        events e
        JOIN f ON f.tenant_id = e.tenant_id
            AND f.user_id = e.user_id
    WHERE
        e.event_time::DATE = f.d0 + 1
)
SELECT
    t.name,
    COUNT(DISTINCT f.user_id) AS users,
    COUNT(DISTINCT d1.user_id) AS ret1
FROM
    f
    JOIN tenants t ON t.tenant_id = f.tenant_id
    LEFT JOIN d1 ON d1.tenant_id = f.tenant_id
        AND d1.user_id = f.user_id
GROUP BY
    t.name;

-- Q14: Monthly top tenants
WITH m AS (
    SELECT
        tenant_id,
        DATE_TRUNC('month', event_time) AS mon,
        SUM((properties ->> 'amount')::NUMERIC) AS rev
    FROM
        events
    WHERE
        event_name = 'purchase'
    GROUP BY
        1,
        2
)
SELECT
    mon,
    tenant_id,
    rev
FROM (
    SELECT
        *,
        RANK() OVER (PARTITION BY mon ORDER BY rev DESC) AS r
    FROM
        m) r
WHERE
    r <= 3;

-- Q15: Rank users within tenants
SELECT
    t.name,
    user_id,
    COUNT(*),
    DENSE_RANK() OVER (PARTITION BY tenant_id ORDER BY COUNT(*) DESC)
FROM
    events e
    JOIN tenants t ON t.tenant_id = e.tenant_id
GROUP BY
    1,
    2,
    tenant_id;

-- Q16: High-value users (> tenant avg spend)
WITH u_spend AS (
    SELECT
        tenant_id,
        user_id,
        SUM((properties ->> 'amount')::NUMERIC) AS spend
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
    u.tenant_id,
    u.user_id,
    u.spend
FROM
    u_spend u
    JOIN t_avg t ON t.tenant_id = u.tenant_id
WHERE
    u.spend > t.avg_s;

-- Q17: Range predicate optimization
SELECT
    tenant_id,
    user_id,
    event_name,
    event_time::DATE
FROM
    events
WHERE
    tenant_id = 'aaaaaaaa-0000-0000-0000-000000000001'
    AND event_time >= '2026-01-01'
    AND event_time < '2027-01-01'
ORDER BY
    event_time DESC
LIMIT 100;

-- Q18: Partition pruning demonstration
EXPLAIN ANALYZE
SELECT
    COUNT(*)
FROM
    events
WHERE
    tenant_id = 'aaaaaaaa-0000-0000-0000-000000000001'
    AND event_time >= '2026-04-01'
    AND event_time < '2026-05-01';

