-- ============================================================
-- 04_ingestion.sql
-- Multi-Tenant Event Analytics System
-- Stored procedures for event ingestion
-- ============================================================
-- Procedure 1: ingest_event       — single-event, idempotent write
-- Procedure 2: ingest_event_batch — bulk ingestion (array input)
-- Procedure 3: purge_dedup_log    — maintenance: expire old dedup rows
-- ============================================================


-- ============================================================
-- PROCEDURE 1: ingest_event
--
-- The primary write path. Ingests a single event with full
-- idempotency, deduplication, and error handling.
--
-- Parameters:
--   p_event_id    UUID        -- client-supplied; used for dedup
--   p_tenant_id   UUID
--   p_user_id     TEXT
--   p_event_name  TEXT
--   p_event_time  TIMESTAMPTZ
--   p_properties  JSONB       -- dynamic payload
--   p_session_id  TEXT        -- optional
--
-- Idempotency:
--   Uses event_dedup table. If the same event_id is received
--   twice, the second call is a silent no-op (no error).
--
-- Transaction behaviour:
--   Runs inside a single transaction. Any failure rolls back
--   both the dedup log entry and the event insert atomically.
--
-- Error handling:
--   Catches and re-raises with context so the caller can log
--   the exact event_id and tenant that caused the failure.
-- ============================================================
CREATE OR REPLACE PROCEDURE ingest_event(
    p_event_id   UUID,
    p_tenant_id  UUID,
    p_user_id    TEXT,
    p_event_name TEXT,
    p_event_time TIMESTAMPTZ,
    p_properties JSONB        DEFAULT '{}',
    p_session_id TEXT         DEFAULT NULL
)
LANGUAGE plpgsql
AS $$
BEGIN
    -- Step 1: Attempt to claim this event_id in the dedup log.
    -- INSERT ... ON CONFLICT DO NOTHING: if the row already exists
    -- (duplicate delivery), nothing is inserted and we can detect
    -- this via GET DIAGNOSTICS.
    INSERT INTO event_dedup (event_id, tenant_id)
    VALUES (p_event_id, p_tenant_id)
    ON CONFLICT (event_id) DO NOTHING;

    -- If no row was inserted, this is a duplicate — exit silently.
    IF NOT FOUND THEN
        RAISE NOTICE 'Duplicate event ignored: event_id=%, tenant_id=%',
            p_event_id, p_tenant_id;
        RETURN;
    END IF;

    -- Step 2: Insert the event.
    -- The BEFORE INSERT trigger (fn_validate_event_payload) runs here.
    -- The AFTER INSERT trigger (fn_sync_user_activity) runs after.
    INSERT INTO events (
        event_id,
        tenant_id,
        user_id,
        event_name,
        event_time,
        properties,
        session_id
    ) VALUES (
        p_event_id,
        p_tenant_id,
        p_user_id,
        p_event_name,
        p_event_time,
        p_properties,
        p_session_id
    );

EXCEPTION
    WHEN OTHERS THEN
        -- Roll back is implicit (transaction-level procedure).
        -- Re-raise with contextual information for the caller.
        RAISE EXCEPTION 'ingest_event failed for event_id=%, tenant_id=%, error: %',
            p_event_id, p_tenant_id, SQLERRM;
END;
$$;

COMMENT ON PROCEDURE ingest_event IS
  'Idempotent single-event ingestion. Deduplicates via event_dedup, validates via trigger, upserts user activity via trigger. Silent no-op on duplicate event_id.';


-- ============================================================
-- PROCEDURE 2: ingest_event_batch
--
-- Bulk ingestion for high-throughput scenarios.
-- Accepts an array of JSONB records, deduplicates the batch
-- against event_dedup, then bulk-inserts remaining events
-- in a single INSERT statement.
--
-- Parameters:
--   p_events  JSONB[]  -- array of event objects, each with keys:
--                         event_id, tenant_id, user_id, event_name,
--                         event_time, properties (opt), session_id (opt)
--
-- Returns:
--   p_inserted  INT  -- number of new events written
--   p_skipped   INT  -- number of duplicates skipped
--
-- Performance notes:
--   - Dedup check is a single set-based INSERT ... ON CONFLICT,
--     not N individual calls — O(batch_size) not O(N²).
--   - The event INSERT is a single multi-row statement.
--   - Recommended batch size: 500–2000 events per call.
--     Larger batches hold locks longer; smaller batches increase
--     round-trip overhead.
-- ============================================================
CREATE OR REPLACE PROCEDURE ingest_event_batch(
    p_events   JSONB[],
    OUT p_inserted INT,
    OUT p_skipped  INT
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_new_ids UUID[];
BEGIN
    p_inserted := 0;
    p_skipped  := 0;

    -- Guard: empty batch
    IF p_events IS NULL OR array_length(p_events, 1) IS NULL THEN
        RETURN;
    END IF;

    -- Step 1: Claim all event_ids in the dedup log in one shot.
    -- Rows that conflict (duplicates) are silently skipped.
    -- We capture which IDs were actually inserted using RETURNING.
    WITH inserted AS (
        INSERT INTO event_dedup (event_id, tenant_id)
        SELECT
            (e->>'event_id')::UUID,
            (e->>'tenant_id')::UUID
        FROM UNNEST(p_events) AS e
        ON CONFLICT (event_id) DO NOTHING
        RETURNING event_id
    )
    SELECT ARRAY_AGG(event_id)
    INTO v_new_ids
    FROM inserted;

    -- Calculate skipped count
    p_skipped := array_length(p_events, 1) - COALESCE(array_length(v_new_ids, 1), 0);

    -- Step 2: If nothing new, exit early.
    IF v_new_ids IS NULL OR array_length(v_new_ids, 1) = 0 THEN
        RETURN;
    END IF;

    -- Step 3: Bulk insert only the non-duplicate events.
    -- NOTE: Triggers (validation + user activity sync) fire
    -- FOR EACH ROW, so they still run per-event even in bulk.
    INSERT INTO events (
        event_id,
        tenant_id,
        user_id,
        event_name,
        event_time,
        properties,
        session_id
    )
    SELECT
        (e->>'event_id')::UUID,
        (e->>'tenant_id')::UUID,
        e->>'user_id',
        e->>'event_name',
        (e->>'event_time')::TIMESTAMPTZ,
        COALESCE(e->'properties', '{}'),
        e->>'session_id'
    FROM UNNEST(p_events) AS e
    WHERE (e->>'event_id')::UUID = ANY(v_new_ids);

    GET DIAGNOSTICS p_inserted = ROW_COUNT;

EXCEPTION
    WHEN OTHERS THEN
        RAISE EXCEPTION 'ingest_event_batch failed: %. Batch size: %, first event_id: %',
            SQLERRM,
            array_length(p_events, 1),
            (p_events[1]->>'event_id');
END;
$$;

COMMENT ON PROCEDURE ingest_event_batch IS
  'Bulk event ingestion. Deduplicates entire batch in one set operation, then bulk-inserts new events. Returns inserted and skipped counts.';


-- ============================================================
-- PROCEDURE 3: purge_dedup_log
--
-- Removes event_dedup rows older than the retention window.
-- Dedup rows are only useful during the re-delivery window
-- (typically 24–48 hours). Keeping them indefinitely wastes
-- space and slows the dedup lookup.
--
-- Parameters:
--   p_retain_hours INT  -- keep rows younger than this (default 48)
--
-- Recommended schedule: run hourly or daily via pg_cron.
-- ============================================================
CREATE OR REPLACE PROCEDURE purge_dedup_log(p_retain_hours INT DEFAULT 48)
LANGUAGE plpgsql
AS $$
DECLARE
    v_deleted INT;
    v_cutoff  TIMESTAMPTZ;
BEGIN
    v_cutoff := NOW() - (p_retain_hours || ' hours')::INTERVAL;

    DELETE FROM event_dedup
    WHERE received_at < v_cutoff;

    GET DIAGNOSTICS v_deleted = ROW_COUNT;

    RAISE NOTICE 'purge_dedup_log: deleted % rows older than % hours (cutoff: %)',
        v_deleted, p_retain_hours, v_cutoff;
END;
$$;

COMMENT ON PROCEDURE purge_dedup_log IS
  'Deletes event_dedup rows older than p_retain_hours. Run periodically to keep the dedup table small and fast.';


-- ============================================================
-- USAGE EXAMPLES (for reference — do not execute as-is)
-- ============================================================

/*

-- Single event ingestion
CALL ingest_event(
    p_event_id   := gen_random_uuid(),
    p_tenant_id  := 'aaaaaaaa-0000-0000-0000-000000000001',
    p_user_id    := 'u123',
    p_event_name := 'purchase',
    p_event_time := NOW(),
    p_properties := '{"amount": 500, "device": "mobile"}',
    p_session_id := 'sess_abc'
);

-- Batch ingestion
DO $$
DECLARE
    v_inserted INT;
    v_skipped  INT;
BEGIN
    CALL ingest_event_batch(
        ARRAY[
            '{"event_id":"11111111-0000-0000-0000-000000000001","tenant_id":"aaaaaaaa-0000-0000-0000-000000000001","user_id":"u1","event_name":"page_view","event_time":"2026-04-01T10:00:00Z","properties":{"page":"/home"}}'::JSONB,
            '{"event_id":"11111111-0000-0000-0000-000000000002","tenant_id":"aaaaaaaa-0000-0000-0000-000000000001","user_id":"u2","event_name":"purchase","event_time":"2026-04-01T10:05:00Z","properties":{"amount":299}}'::JSONB
        ],
        v_inserted,
        v_skipped
    );
    RAISE NOTICE 'Inserted: %, Skipped (duplicates): %', v_inserted, v_skipped;
END;
$$;

-- Purge old dedup rows (keep last 48 hours)
CALL purge_dedup_log(48);

-- pg_cron schedule (uncomment when pg_cron is available)
-- SELECT cron.schedule('purge-dedup', '0 * * * *', 'CALL purge_dedup_log(48)');

*/
