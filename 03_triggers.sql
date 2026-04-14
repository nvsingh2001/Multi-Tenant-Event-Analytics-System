-- ============================================================
-- 03_triggers.sql
-- Multi-Tenant Event Analytics System
-- Trigger definitions
-- ============================================================
-- Trigger 1: tenants_updated_at   — auto-stamp updated_at on tenants
-- Trigger 2: sync_user_activity   — maintain users.first_seen_at
--                                   and users.last_seen_at from events
-- ============================================================


-- ============================================================
-- TRIGGER 1: tenants_updated_at
--
-- Purpose:
--   Automatically sets tenants.updated_at = NOW() whenever any
--   column in a tenant row is modified.
--
-- Performance impact:
--   Negligible. tenants is a tiny, rarely-written table.
--   One NOW() call per UPDATE — essentially free.
-- ============================================================
CREATE OR REPLACE FUNCTION fn_set_updated_at()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    NEW.updated_at := NOW();
    RETURN NEW;
END;
$$;

COMMENT ON FUNCTION fn_set_updated_at() IS
  'Sets updated_at = NOW() on any UPDATE. Attach to any table that has an updated_at column.';

-- Drop and recreate so this script is re-runnable
DROP TRIGGER IF EXISTS trg_tenants_updated_at ON tenants;

CREATE TRIGGER trg_tenants_updated_at
    BEFORE UPDATE ON tenants
    FOR EACH ROW
    EXECUTE FUNCTION fn_set_updated_at();

COMMENT ON TRIGGER trg_tenants_updated_at ON tenants IS
  'Auto-updates tenants.updated_at on every row modification.';


-- ============================================================
-- TRIGGER 2: sync_user_activity
--
-- Purpose:
--   After an event is inserted, upsert the corresponding user row
--   to keep first_seen_at and last_seen_at accurate — without the
--   application needing to issue a separate UPDATE.
--
-- Logic:
--   • If the user row does not exist: INSERT with first_seen_at
--     and last_seen_at both set to the event's event_time.
--   • If the user row exists:
--       - Update last_seen_at if event_time > current last_seen_at
--       - Update first_seen_at if event_time < current first_seen_at
--         (handles back-dated events arriving out of order)
--
-- Performance impact:
--   Moderate. One upsert per inserted event row.
--   This is an AFTER trigger, so it does not block the event insert.
--   The upsert targets the primary key (tenant_id, user_id) — O(1)
--   index lookup. Acceptable for the write throughput of this system.
--
--   Trade-off: if you need absolute maximum throughput, disable this
--   trigger and instead run a scheduled job that re-derives
--   first_seen_at / last_seen_at from events in bulk. For most
--   workloads the per-row upsert is fine.
-- ============================================================
CREATE OR REPLACE FUNCTION fn_sync_user_activity()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO users (tenant_id, user_id, first_seen_at, last_seen_at)
    VALUES (NEW.tenant_id, NEW.user_id, NEW.event_time, NEW.event_time)
    ON CONFLICT (tenant_id, user_id) DO UPDATE
        SET
            last_seen_at  = GREATEST(users.last_seen_at,  EXCLUDED.last_seen_at),
            first_seen_at = LEAST(users.first_seen_at, EXCLUDED.first_seen_at);

    RETURN NULL;  -- AFTER trigger; return value is ignored for row triggers
END;
$$;

COMMENT ON FUNCTION fn_sync_user_activity() IS
  'Upserts users.first_seen_at and last_seen_at from the newly inserted event. Handles back-dated events correctly via LEAST/GREATEST.';

DROP TRIGGER IF EXISTS trg_sync_user_activity ON events;

CREATE TRIGGER trg_sync_user_activity
    AFTER INSERT ON events
    FOR EACH ROW
    EXECUTE FUNCTION fn_sync_user_activity();

COMMENT ON TRIGGER trg_sync_user_activity ON events IS
  'After each event insert, keeps users.first_seen_at and last_seen_at in sync. One upsert per event row.';


-- ============================================================
-- TRIGGER 3: validate_event_payload  (data quality guard)
--
-- Purpose:
--   Reject events that violate basic data quality rules BEFORE
--   they enter the events table. Catches bad data at the DB
--   boundary — a last line of defence after application-level
--   validation.
--
-- Validation rules:
--   1. event_name must not be blank or whitespace-only
--   2. event_time must not be in the future (> NOW() + 1 hour
--      tolerance for clock skew)
--   3. event_time must not be older than 90 days (reject
--      obviously stale/test data)
--   4. tenant_id must exist in the tenants table and be active
--
-- Performance impact:
--   Rule 1–3: trivial (no I/O — pure column checks).
--   Rule 4:   one indexed lookup on tenants (small table, likely
--             cached in shared_buffers). Cost is ~0.1–0.5 ms.
--             If this becomes a bottleneck at extreme throughput,
--             move the tenant existence check to the application
--             layer and keep only rules 1–3 here.
-- ============================================================
CREATE OR REPLACE FUNCTION fn_validate_event_payload()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    -- Rule 1: event_name must not be blank
    IF TRIM(NEW.event_name) = '' THEN
        RAISE EXCEPTION 'event_name must not be blank (tenant_id=%, user_id=%)',
            NEW.tenant_id, NEW.user_id;
    END IF;

    -- Rule 2: event_time must not be in the future (1-hour tolerance)
    IF NEW.event_time > NOW() + INTERVAL '1 hour' THEN
        RAISE EXCEPTION 'event_time % is in the future (tenant_id=%, user_id=%)',
            NEW.event_time, NEW.tenant_id, NEW.user_id;
    END IF;

    -- Rule 3: event_time must not be older than 90 days
    IF NEW.event_time < NOW() - INTERVAL '90 days' THEN
        RAISE EXCEPTION 'event_time % is older than 90 days — possible stale data (tenant_id=%, user_id=%)',
            NEW.event_time, NEW.tenant_id, NEW.user_id;
    END IF;

    -- Rule 4: tenant must exist and be active
    IF NOT EXISTS (
        SELECT 1 FROM tenants
        WHERE tenant_id = NEW.tenant_id
          AND is_active  = TRUE
    ) THEN
        RAISE EXCEPTION 'tenant_id % does not exist or is inactive', NEW.tenant_id;
    END IF;

    RETURN NEW;
END;
$$;

COMMENT ON FUNCTION fn_validate_event_payload() IS
  'BEFORE INSERT guard: rejects blank event_name, future timestamps, events >90 days old, and inactive tenants.';

DROP TRIGGER IF EXISTS trg_validate_event_payload ON events;

CREATE TRIGGER trg_validate_event_payload
    BEFORE INSERT ON events
    FOR EACH ROW
    EXECUTE FUNCTION fn_validate_event_payload();

COMMENT ON TRIGGER trg_validate_event_payload ON events IS
  'Validates event payload before insert. Raises EXCEPTION on bad data to abort the transaction cleanly.';


-- ============================================================
-- VERIFICATION
-- Run these after loading to confirm triggers are registered.
-- ============================================================
SELECT
    trigger_name,
    event_manipulation,
    event_object_table,
    action_timing,
    action_orientation
FROM information_schema.triggers
WHERE trigger_schema = current_schema()
  AND event_object_table IN ('tenants', 'events')
ORDER BY event_object_table, action_timing, trigger_name;
