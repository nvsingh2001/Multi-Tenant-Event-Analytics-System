-- ============================================================
-- 05_analytical_queries.sql
-- Multi-Tenant Event Analytics System
-- Part 1: Core Analytics — DAU, Funnel, Retention
-- ============================================================


-- ============================================================
-- A. DAILY ACTIVE USERS (DAU)
--
-- Unique users per day per tenant, ordered most recent first.
-- Uses the mv_daily_active_users materialized view when fresh;
-- falls back to the raw query below for real-time accuracy.
-- ============================================================

-- A1. From materialized view (fast — use for dashboards)
SELECT
    t.name                          AS tenant_name,
    m.event_date,
    m.dau
FROM mv_daily_active_users m
JOIN tenants t ON t.tenant_id = m.tenant_id
ORDER BY m.event_date DESC, m.dau DESC;


-- A2. Raw DAU query (use when materialized view is stale)
SELECT
    t.name                                      AS tenant_name,
    DATE(e.event_time AT TIME ZONE 'UTC')       AS event_date,
    COUNT(DISTINCT e.user_id)                   AS dau
FROM events e
JOIN tenants t ON t.tenant_id = e.tenant_id
GROUP BY t.name, DATE(e.event_time AT TIME ZONE 'UTC')
ORDER BY event_date DESC, dau DESC;


-- A3. DAU with 7-day rolling average per tenant
-- Useful for smoothing out weekend dips in dashboards.
SELECT
    tenant_id,
    event_date,
    dau,
    ROUND(
        AVG(dau) OVER (
            PARTITION BY tenant_id
            ORDER BY event_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ), 1
    )                                           AS dau_7day_avg
FROM mv_daily_active_users
ORDER BY tenant_id, event_date DESC;


-- ============================================================
-- B. FUNNEL ANALYSIS
--
-- Tracks users through: signup → add_to_cart → purchase
-- Counts users who completed each stage (not necessarily
-- in a single session — lifetime funnel).
-- Also computes step-to-step conversion rates.
-- ============================================================

WITH
-- Step 1: users who signed up
step1 AS (
    SELECT DISTINCT tenant_id, user_id
    FROM events
    WHERE event_name = 'signup'
),

-- Step 2: among step1 users, those who also added to cart
-- (add_to_cart must come AFTER signup)
step2 AS (
    SELECT DISTINCT e.tenant_id, e.user_id
    FROM events e
    INNER JOIN step1 s ON s.tenant_id = e.tenant_id
                       AND s.user_id   = e.user_id
    WHERE e.event_name = 'add_to_cart'
      AND e.event_time > (
          SELECT MIN(event_time)
          FROM events
          WHERE tenant_id  = e.tenant_id
            AND user_id    = e.user_id
            AND event_name = 'signup'
      )
),

-- Step 3: among step2 users, those who also purchased
step3 AS (
    SELECT DISTINCT e.tenant_id, e.user_id
    FROM events e
    INNER JOIN step2 s ON s.tenant_id = e.tenant_id
                       AND s.user_id   = e.user_id
    WHERE e.event_name = 'purchase'
      AND e.event_time > (
          SELECT MIN(event_time)
          FROM events
          WHERE tenant_id  = e.tenant_id
            AND user_id    = e.user_id
            AND event_name = 'add_to_cart'
      )
),

-- Aggregate per tenant
funnel AS (
    SELECT
        t.tenant_id,
        t.name                              AS tenant_name,
        COUNT(DISTINCT s1.user_id)          AS stage_signup,
        COUNT(DISTINCT s2.user_id)          AS stage_add_to_cart,
        COUNT(DISTINCT s3.user_id)          AS stage_purchase
    FROM tenants t
    LEFT JOIN step1 s1 ON s1.tenant_id = t.tenant_id
    LEFT JOIN step2 s2 ON s2.tenant_id = t.tenant_id
    LEFT JOIN step3 s3 ON s3.tenant_id = t.tenant_id
    GROUP BY t.tenant_id, t.name
)

SELECT
    tenant_name,
    stage_signup                                            AS "1_signup",
    stage_add_to_cart                                       AS "2_add_to_cart",
    stage_purchase                                          AS "3_purchase",
    -- Step conversion rates
    ROUND(100.0 * stage_add_to_cart / NULLIF(stage_signup, 0), 1)       AS "signup→cart_%",
    ROUND(100.0 * stage_purchase    / NULLIF(stage_add_to_cart, 0), 1)  AS "cart→purchase_%",
    -- Overall conversion
    ROUND(100.0 * stage_purchase    / NULLIF(stage_signup, 0), 1)       AS "overall_%"
FROM funnel
ORDER BY stage_signup DESC;


-- ============================================================
-- C. RETENTION ANALYSIS
--
-- Day-1 and Day-7 retention by tenant and signup cohort week.
-- A user is "retained on Day N" if they have any event on the
-- Nth day after their first event (first_seen_at).
--
-- Cohort = the calendar week of the user's first event.
-- ============================================================

WITH
-- Anchor: each user's cohort week
cohort_base AS (
    SELECT
        tenant_id,
        user_id,
        DATE_TRUNC('week', first_seen_at)::DATE     AS cohort_week
    FROM users
    WHERE first_seen_at IS NOT NULL
),

-- Day-1 retention: user had any event 1 day after first_seen_at
day1 AS (
    SELECT DISTINCT
        u.tenant_id,
        u.user_id,
        c.cohort_week
    FROM users u
    JOIN cohort_base c
        ON c.tenant_id = u.tenant_id
        AND c.user_id  = u.user_id
    WHERE EXISTS (
        SELECT 1 FROM events e
        WHERE e.tenant_id  = u.tenant_id
          AND e.user_id    = u.user_id
          AND DATE(e.event_time) = (u.first_seen_at::DATE + INTERVAL '1 day')
    )
),

-- Day-7 retention
day7 AS (
    SELECT DISTINCT
        u.tenant_id,
        u.user_id,
        c.cohort_week
    FROM users u
    JOIN cohort_base c
        ON c.tenant_id = u.tenant_id
        AND c.user_id  = u.user_id
    WHERE EXISTS (
        SELECT 1 FROM events e
        WHERE e.tenant_id  = u.tenant_id
          AND e.user_id    = u.user_id
          AND DATE(e.event_time) = (u.first_seen_at::DATE + INTERVAL '7 days')
    )
)

SELECT
    t.name                                  AS tenant_name,
    c.cohort_week,
    COUNT(DISTINCT c.user_id)               AS cohort_size,

    COUNT(DISTINCT d1.user_id)              AS retained_day1,
    ROUND(
        100.0 * COUNT(DISTINCT d1.user_id)
              / NULLIF(COUNT(DISTINCT c.user_id), 0), 1
    )                                       AS day1_retention_pct,

    COUNT(DISTINCT d7.user_id)              AS retained_day7,
    ROUND(
        100.0 * COUNT(DISTINCT d7.user_id)
              / NULLIF(COUNT(DISTINCT c.user_id), 0), 1
    )                                       AS day7_retention_pct

FROM cohort_base c
JOIN tenants t          ON t.tenant_id = c.tenant_id
LEFT JOIN day1 d1       ON d1.tenant_id = c.tenant_id
                       AND d1.user_id   = c.user_id
LEFT JOIN day7 d7       ON d7.tenant_id = c.tenant_id
                       AND d7.user_id   = c.user_id
GROUP BY t.name, c.cohort_week
HAVING COUNT(DISTINCT c.user_id) >= 5      -- exclude tiny cohorts
ORDER BY t.name, c.cohort_week DESC;
