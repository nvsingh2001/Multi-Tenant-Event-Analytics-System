-- 01_schema.sql: Core Tables and Views
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

CREATE EXTENSION IF NOT EXISTS "pg_trgm";

-- Tenants: Organization data
CREATE TABLE tenants (
    tenant_id uuid PRIMARY KEY DEFAULT gen_random_uuid (),
    name text NOT NULL,
    slug text NOT NULL UNIQUE,
    plan_type text NOT NULL DEFAULT 'free' CHECK (plan_type IN ('free', 'pro', 'enterprise')),
    is_active boolean NOT NULL DEFAULT TRUE,
    created_at timestamptz NOT NULL DEFAULT NOW(),
    updated_at timestamptz NOT NULL DEFAULT NOW()
);

-- Users: End-users belonging to a tenant
CREATE TABLE users (
    tenant_id uuid NOT NULL REFERENCES tenants (tenant_id) ON DELETE CASCADE,
    user_id text NOT NULL,
    email text,
    display_name text,
    metadata jsonb NOT NULL DEFAULT '{}',
    first_seen_at timestamptz,
    last_seen_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, user_id)
);

-- Events: Partitioned high-volume store
CREATE TABLE events (
    event_id uuid NOT NULL DEFAULT gen_random_uuid (),
    tenant_id uuid NOT NULL,
    user_id text NOT NULL,
    event_name text NOT NULL,
    event_time timestamptz NOT NULL,
    properties jsonb NOT NULL DEFAULT '{}',
    session_id text,
    ingested_at timestamptz NOT NULL DEFAULT NOW(),
    PRIMARY KEY (event_id, event_time)
)
PARTITION BY RANGE (event_time);

-- Idempotency log for events
CREATE TABLE event_dedup (
    event_id uuid PRIMARY KEY,
    tenant_id uuid NOT NULL,
    received_at timestamptz NOT NULL DEFAULT NOW()
);

-- DAU Materialized View
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_daily_active_users AS
SELECT
    tenant_id,
    DATE(event_time AT TIME ZONE 'UTC') AS event_date,
    COUNT(DISTINCT user_id) AS dau
FROM
    events
GROUP BY
    1,
    2 WITH NO DATA;

-- Revenue Summary Materialized View
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_revenue_summary AS
SELECT
    tenant_id,
    DATE(event_time AT TIME ZONE 'UTC') AS event_date,
    SUM((properties ->> 'amount')::numeric) AS total_revenue,
    COUNT(*) AS purchase_count
FROM
    events
WHERE
    event_name = 'purchase'
    AND properties ->> 'amount' IS NOT NULL
GROUP BY
    1,
    2 WITH NO DATA;

-- 2026 Monthly Partitions
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

CREATE TABLE events_default PARTITION OF events DEFAULT;

-- Indexes
CREATE INDEX idx_tenants_slug ON tenants (slug);

CREATE INDEX idx_users_tenant_id ON users (tenant_id);

CREATE INDEX idx_users_metadata_gin ON users USING GIN (metadata);

CREATE INDEX idx_events_tenant_time ON events (tenant_id, event_time DESC);

CREATE INDEX idx_events_properties_gin ON events USING GIN (properties);

CREATE UNIQUE INDEX idx_mv_dau_pk ON mv_daily_active_users (tenant_id, event_date);

CREATE UNIQUE INDEX idx_mv_revenue_pk ON mv_revenue_summary (tenant_id, event_date);

-- Security (RLS)
ALTER TABLE tenants ENABLE ROW LEVEL SECURITY;

ALTER TABLE users ENABLE ROW LEVEL SECURITY;

ALTER TABLE events ENABLE ROW LEVEL SECURITY;

CREATE ROLE app_user NOLOGIN;

CREATE POLICY tenant_isolation ON tenants
    FOR ALL TO app_user
        USING (tenant_id = current_setting('app.current_tenant_id')::uuid);

CREATE POLICY tenant_isolation ON users
    FOR ALL TO app_user
        USING (tenant_id = current_setting('app.current_tenant_id')::uuid);

CREATE POLICY tenant_isolation ON events
    FOR ALL TO app_user
        USING (tenant_id = current_setting('app.current_tenant_id')::uuid);

CREATE ROLE analytics_admin NOLOGIN BYPASSRLS;

