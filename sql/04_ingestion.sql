-- Ingests a single event with deduplication
CREATE OR REPLACE PROCEDURE ingest_event (p_event_id uuid, p_tenant_id uuid, p_user_id text, p_event_name text, p_event_time timestamptz, p_properties jsonb DEFAULT '{}', p_session_id text DEFAULT NULL)
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO event_dedup (event_id, tenant_id)
        VALUES (p_event_id, p_tenant_id)
    ON CONFLICT (event_id)
        DO NOTHING;
    IF NOT FOUND THEN
        RETURN;
    END IF;
    INSERT INTO events (event_id, tenant_id, user_id, event_name, event_time, properties, session_id)
        VALUES (p_event_id, p_tenant_id, p_user_id, p_event_name, p_event_time, p_properties, p_session_id);
EXCEPTION
    WHEN OTHERS THEN
        RAISE EXCEPTION 'ingest_event failed: %', SQLERRM;
END;
$$;

-- Ingests a batch of events
CREATE OR REPLACE PROCEDURE ingest_event_batch (p_events jsonb[], OUT p_inserted int, OUT p_skipped int)
LANGUAGE plpgsql
AS $$
DECLARE
    v_new_ids uuid[];
BEGIN
    p_inserted := 0;
    p_skipped := 0;
    IF p_events IS NULL OR array_length(p_events, 1) IS NULL THEN
        RETURN;
    END IF;
    WITH inserted AS (
INSERT INTO event_dedup (event_id, tenant_id)
        SELECT
            (e ->> 'event_id')::uuid,
            (e ->> 'tenant_id')::uuid
        FROM
            UNNEST(p_events) AS e
        ON CONFLICT (event_id)
            DO NOTHING
        RETURNING
            event_id
)
    SELECT
        ARRAY_AGG(event_id) INTO v_new_ids
FROM
    inserted;
    p_skipped := array_length(p_events, 1) - COALESCE(array_length(v_new_ids, 1), 0);
    IF v_new_ids IS NULL OR array_length(v_new_ids, 1) = 0 THEN
        RETURN;
    END IF;
    INSERT INTO events (event_id, tenant_id, user_id, event_name, event_time, properties, session_id)
    SELECT
        (e ->> 'event_id')::uuid,
        (e ->> 'tenant_id')::uuid,
        e ->> 'user_id',
        e ->> 'event_name',
        (e ->> 'event_time')::timestamptz,
        COALESCE(e -> 'properties', '{}'),
        e ->> 'session_id'
    FROM
        UNNEST(p_events) AS e
WHERE (e ->> 'event_id')::uuid = ANY (v_new_ids);
    GET DIAGNOSTICS p_inserted = ROW_COUNT;
EXCEPTION
    WHEN OTHERS THEN
        RAISE EXCEPTION 'ingest_event_batch failed: %', SQLERRM;
END;
$$;

-- Purges old deduplication logs
CREATE OR REPLACE PROCEDURE purge_dedup_log (p_retain_hours int DEFAULT 48)
LANGUAGE plpgsql
AS $$
DECLARE
    v_deleted int;
BEGIN
    DELETE FROM event_dedup
    WHERE received_at < NOW() - (p_retain_hours || ' hours')::interval;
    GET DIAGNOSTICS v_deleted = ROW_COUNT;
END;
$$;

