-- 03_triggers.sql: Automatic Data Maintenance Triggers
-- Updates updated_at on change.
CREATE OR REPLACE FUNCTION fn_set_updated_at ()
    RETURNS TRIGGER
    LANGUAGE plpgsql
    AS $$
BEGIN
    NEW.updated_at := NOW();
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_tenants_updated_at ON tenants;

CREATE TRIGGER trg_tenants_updated_at
    BEFORE UPDATE ON tenants
    FOR EACH ROW
    EXECUTE FUNCTION fn_set_updated_at ();

-- Syncs user activity based on incoming events.
CREATE OR REPLACE FUNCTION fn_sync_user_activity ()
    RETURNS TRIGGER
    LANGUAGE plpgsql
    AS $$
BEGIN
    INSERT INTO users (tenant_id, user_id, first_seen_at, last_seen_at)
        VALUES (NEW.tenant_id, NEW.user_id, NEW.event_time, NEW.event_time)
    ON CONFLICT (tenant_id, user_id)
        DO UPDATE SET
            last_seen_at = GREATEST (users.last_seen_at, EXCLUDED.last_seen_at),
            first_seen_at = LEAST (users.first_seen_at, EXCLUDED.first_seen_at);
    RETURN NULL;
END;
$$;

DROP TRIGGER IF EXISTS trg_sync_user_activity ON events;

CREATE TRIGGER trg_sync_user_activity
    AFTER INSERT ON events
    FOR EACH ROW
    EXECUTE FUNCTION fn_sync_user_activity ();

-- Validates event payload before insert.
CREATE OR REPLACE FUNCTION fn_validate_event_payload ()
    RETURNS TRIGGER
    LANGUAGE plpgsql
    AS $$
BEGIN
    IF TRIM(NEW.event_name) = '' THEN
        RAISE EXCEPTION 'event_name blank';
    END IF;
    IF NEW.event_time > NOW() + INTERVAL '1 hour' THEN
        RAISE EXCEPTION 'future event';
    END IF;
    IF NEW.event_time < NOW() - INTERVAL '90 days' THEN
        RAISE EXCEPTION 'event too old';
    END IF;
    IF NOT EXISTS (
        SELECT
            1
        FROM
            tenants
        WHERE
            tenant_id = NEW.tenant_id
            AND is_active = TRUE) THEN
    RAISE EXCEPTION 'invalid tenant';
END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_validate_event_payload ON events;

CREATE TRIGGER trg_validate_event_payload
    BEFORE INSERT ON events
    FOR EACH ROW
    EXECUTE FUNCTION fn_validate_event_payload ();

