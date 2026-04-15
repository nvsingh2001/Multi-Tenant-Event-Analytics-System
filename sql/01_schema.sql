-- ============================================================
-- Multi-Tenant Event Analytics System — Schema Design
-- PostgreSQL 15+
-- ============================================================

-- ============================================================
-- EXTENSIONS
-- ============================================================
CREATE EXTENSION IF NOT EXISTS "pgcrypto";   -- gen_random_uuid()
CREATE EXTENSION IF NOT EXISTS "pg_trgm";    -- GIN trigram indexes for text search


-- ============================================================
-- 1. TENANTS
-- ============================================================
-- Represents each organization (customer) using the platform.
-- Small, rarely-written table — fully normalized makes sense here.
-- ============================================================
CREATE TABLE tenants (
    tenant_id   UUID            PRIMARY KEY DEFAULT gen_random_uuid(),
    name        TEXT            NOT NULL,
    slug        TEXT            NOT NULL UNIQUE,        -- URL-safe identifier (e.g. "acme-corp")
    plan_type   TEXT            NOT NULL DEFAULT 'free' CHECK (plan_type IN ('free', 'pro', 'enterprise')),
    is_active   BOOLEAN         NOT NULL DEFAULT TRUE,
    created_at  TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE tenants IS
  'One row per organization. tenant_id is the isolation key propagated to all child tables.';


-- ============================================================
-- 2. USERS
-- ============================================================
-- Represents an end-user within a specific tenant's application.
--
-- DESIGN DECISIONS:
-- • Composite PK (tenant_id, user_id): user_id is supplied by the
--   tenant (e.g. their own UUID or username). The same user_id
--   string could appear across tenants, so tenant_id is required
--   to make it globally unique.
-- • metadata JSONB: profile attributes vary wildly by tenant.
--   Using JSONB avoids a wide, mostly-null EAV table while still
--   keeping structured columns for the fields we always query.
-- • first_seen_at / last_seen_at: maintained by a trigger on the
--   events table; avoids repeated aggregation queries for basic
--   user activity checks.
-- ============================================================
CREATE TABLE users (
    tenant_id       UUID        NOT NULL REFERENCES tenants (tenant_id) ON DELETE CASCADE,
    user_id         TEXT        NOT NULL,
    email           TEXT,
    display_name    TEXT,
    metadata        JSONB       NOT NULL DEFAULT '{}',
    first_seen_at   TIMESTAMPTZ,
    last_seen_at    TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (tenant_id, user_id)
);

COMMENT ON TABLE users IS
  'Per-tenant users. user_id is tenant-supplied — uniqueness is only guaranteed within a tenant.';

COMMENT ON COLUMN users.metadata IS
  'Arbitrary tenant-supplied user attributes (e.g. plan, country, cohort). Queried via GIN index.';


-- ============================================================
-- 3. EVENTS (partitioned)
-- ============================================================
-- Core high-volume table. Every user interaction produces a row.
--
-- DESIGN DECISIONS:
-- • Partitioned by RANGE on event_time (monthly):
--   Monthly partitions balance manageability (fewer partitions
--   than daily) with pruning granularity. A query over "last 30
--   days" touches at most 2 partitions.
-- • Composite PK must include the partition key (event_time).
--   PostgreSQL requires this for partitioned tables.
-- • properties JSONB:
--   Event properties are heterogeneous by design (the spec says
--   so). A structured approach would require a wide nullable
--   table or an EAV model — both are worse for write throughput
--   and query ergonomics. JSONB with a GIN index gives us
--   predicate pushdown on arbitrary keys.
-- • session_id TEXT (nullable):
--   Useful for funnel and session analysis without a separate
--   session table. Can be populated by the ingestion layer.
-- • ingested_at vs event_time:
--   event_time is client-reported (can be backdated).
--   ingested_at is server-set and never lies — useful for
--   lag monitoring and backfill detection.
-- ============================================================
CREATE TABLE events (
    event_id    UUID        NOT NULL DEFAULT gen_random_uuid(),
    tenant_id   UUID        NOT NULL,    -- NOT FK: FKs to non-partitioned tables are expensive at scale
    user_id     TEXT        NOT NULL,
    event_name  TEXT        NOT NULL,
    event_time  TIMESTAMPTZ NOT NULL,    -- partition key
    properties  JSONB       NOT NULL DEFAULT '{}',
    session_id  TEXT,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (event_id, event_time)   -- partition key must be part of PK
) PARTITION BY RANGE (event_time);

COMMENT ON TABLE events IS
  'High-volume event store. Partitioned monthly by event_time. Never delete rows — drop whole partitions.';

COMMENT ON COLUMN events.tenant_id IS
  'Denormalized (no FK) for insert performance. Application layer enforces referential integrity.';

COMMENT ON COLUMN events.properties IS
  'Dynamic event payload. Indexed via GIN for arbitrary key/value lookups.';


-- ============================================================
-- 4. EVENT_DEDUP  (idempotency guard)
-- ============================================================
-- Lightweight table used to detect and reject duplicate events
-- during ingestion. The ingestion function checks here first
-- using INSERT ... ON CONFLICT DO NOTHING.
--
-- DESIGN DECISIONS:
-- • Separate from events: keeps the hot write path clean.
--   No need to scan the full (partitioned) events table for
--   duplicates on every insert.
-- • TTL via received_at: rows older than the ingestion window
--   (typically 24–48 h) can be batch-deleted by a maintenance
--   job. Duplicates arriving days later are extremely rare and
--   can be handled by business logic.
-- • tenant_id stored here to enable per-tenant dedup cleanup.
-- ============================================================
CREATE TABLE event_dedup (
    event_id    UUID        PRIMARY KEY,
    tenant_id   UUID        NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE event_dedup IS
  'Idempotency log. Stores event_id for 48h to reject re-deliveries. Purged by maintenance job.';


-- ============================================================
-- 5. MATERIALIZED VIEW STUBS
--    (defined here; populated after first data load)
-- ============================================================

-- Daily active users — refreshed daily
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_daily_active_users AS
SELECT
    tenant_id,
    DATE(event_time AT TIME ZONE 'UTC') AS event_date,
    COUNT(DISTINCT user_id)             AS dau
FROM events
GROUP BY tenant_id, DATE(event_time AT TIME ZONE 'UTC')
WITH NO DATA;

-- Revenue summary — refreshed daily
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_revenue_summary AS
SELECT
    tenant_id,
    DATE(event_time AT TIME ZONE 'UTC')            AS event_date,
    SUM((properties->>'amount')::NUMERIC)          AS total_revenue,
    COUNT(*)                                       AS purchase_count
FROM events
WHERE event_name = 'purchase'
  AND properties->>'amount' IS NOT NULL
GROUP BY tenant_id, DATE(event_time AT TIME ZONE 'UTC')
WITH NO DATA;


-- ============================================================
-- 6. PARTITIONS (initial set — monthly for 2026)
-- ============================================================
-- See partitioning.sql for the full auto-creation procedure.
-- ============================================================
CREATE TABLE events_2026_01 PARTITION OF events
    FOR VALUES FROM ('2026-01-01') TO ('2026-02-01');

CREATE TABLE events_2026_02 PARTITION OF events
    FOR VALUES FROM ('2026-02-01') TO ('2026-03-01');

CREATE TABLE events_2026_03 PARTITION OF events
    FOR VALUES FROM ('2026-03-01') TO ('2026-04-01');

CREATE TABLE events_2026_04 PARTITION OF events
    FOR VALUES FROM ('2026-04-01') TO ('2026-05-01');

CREATE TABLE events_2026_05 PARTITION OF events
    FOR VALUES FROM ('2026-05-01') TO ('2026-06-01');

CREATE TABLE events_2026_06 PARTITION OF events
    FOR VALUES FROM ('2026-06-01') TO ('2026-07-01');

CREATE TABLE events_2026_07 PARTITION OF events
    FOR VALUES FROM ('2026-07-01') TO ('2026-08-01');

CREATE TABLE events_2026_08 PARTITION OF events
    FOR VALUES FROM ('2026-08-01') TO ('2026-09-01');

CREATE TABLE events_2026_09 PARTITION OF events
    FOR VALUES FROM ('2026-09-01') TO ('2026-10-01');

CREATE TABLE events_2026_10 PARTITION OF events
    FOR VALUES FROM ('2026-10-01') TO ('2026-11-01');

CREATE TABLE events_2026_11 PARTITION OF events
    FOR VALUES FROM ('2026-11-01') TO ('2026-12-01');

CREATE TABLE events_2026_12 PARTITION OF events
    FOR VALUES FROM ('2026-12-01') TO ('2027-01-01');

-- Default partition: catches out-of-range inserts instead of erroring
CREATE TABLE events_default PARTITION OF events DEFAULT;


-- ============================================================
-- 7. INDEXES
-- ============================================================

-- --- tenants ---
CREATE INDEX idx_tenants_slug     ON tenants (slug);
CREATE INDEX idx_tenants_active   ON tenants (is_active) WHERE is_active = TRUE;

-- --- users ---
-- tenant_id alone (for "list all users of a tenant")
CREATE INDEX idx_users_tenant_id         ON users (tenant_id);
-- GIN on metadata for arbitrary property queries
CREATE INDEX idx_users_metadata_gin      ON users USING GIN (metadata);
-- last_seen for user activity lookups
CREATE INDEX idx_users_last_seen         ON users (tenant_id, last_seen_at DESC);

-- --- events (applied to parent; inherited by each partition) ---

-- PRIMARY analytical index: all tenant-scoped time-range queries use this
CREATE INDEX idx_events_tenant_time
    ON events (tenant_id, event_time DESC);

-- Event type filter within a tenant (funnel, distribution queries)
CREATE INDEX idx_events_tenant_name_time
    ON events (tenant_id, event_name, event_time DESC);

-- User-centric queries (session analysis, per-user history)
CREATE INDEX idx_events_tenant_user_time
    ON events (tenant_id, user_id, event_time DESC);

-- GIN index on properties for JSONB predicate pushdown
-- e.g. WHERE properties @> '{"device": "mobile"}'
CREATE INDEX idx_events_properties_gin
    ON events USING GIN (properties);

-- Session-based queries
CREATE INDEX idx_events_session
    ON events (tenant_id, session_id, event_time)
    WHERE session_id IS NOT NULL;

-- Covering index for DAU queries (avoids heap fetch entirely)
CREATE INDEX idx_events_dau
    ON events (tenant_id, DATE(event_time AT TIME ZONE 'UTC'), user_id);

-- --- materialized views ---
CREATE UNIQUE INDEX idx_mv_dau_pk
    ON mv_daily_active_users (tenant_id, event_date);

CREATE UNIQUE INDEX idx_mv_revenue_pk
    ON mv_revenue_summary (tenant_id, event_date);

-- --- event_dedup ---
-- PK covers the lookup; add tenant+time for cleanup jobs
CREATE INDEX idx_event_dedup_cleanup
    ON event_dedup (tenant_id, received_at);


-- ============================================================
-- 8. ROW-LEVEL SECURITY (tenant isolation)
-- ============================================================
-- Enable RLS on all tenant-scoped tables.
-- Applications connect as role 'app_user' and set
-- app.current_tenant_id at session start.
-- ============================================================
ALTER TABLE tenants  ENABLE ROW LEVEL SECURITY;
ALTER TABLE users    ENABLE ROW LEVEL SECURITY;
ALTER TABLE events   ENABLE ROW LEVEL SECURITY;

CREATE ROLE app_user NOLOGIN;

-- Tenants: app sees only its own row
CREATE POLICY tenant_isolation ON tenants
    FOR ALL TO app_user
    USING (tenant_id = current_setting('app.current_tenant_id')::UUID);

-- Users: app sees only users belonging to its tenant
CREATE POLICY tenant_isolation ON users
    FOR ALL TO app_user
    USING (tenant_id = current_setting('app.current_tenant_id')::UUID);

-- Events: app sees only events belonging to its tenant
CREATE POLICY tenant_isolation ON events
    FOR ALL TO app_user
    USING (tenant_id = current_setting('app.current_tenant_id')::UUID);

-- Superuser/analytics role bypasses RLS for cross-tenant reporting
CREATE ROLE analytics_admin NOLOGIN BYPASSRLS;


-- ============================================================
-- END OF SCHEMA
-- ============================================================
