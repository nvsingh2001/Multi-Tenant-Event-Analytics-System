-- ============================================================
-- 02_partitioning.sql
-- Multi-Tenant Event Analytics System
-- Partition automation for the events table
-- ============================================================


-- ============================================================
-- FUNCTION: create_monthly_partition
-- Creates a single monthly partition for the events table
-- if it doesn't already exist.
--
-- Parameters:
--   p_year  INT  -- e.g. 2026
--   p_month INT  -- 1..12
--
-- Idempotent: safe to call even if partition already exists.
-- ============================================================
CREATE OR REPLACE FUNCTION create_monthly_partition(p_year INT, p_month INT)
RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
    v_partition_name TEXT;
    v_start_date     DATE;
    v_end_date       DATE;
BEGIN
    -- Build partition name: events_2026_04
    v_partition_name := FORMAT('events_%s_%s', p_year, LPAD(p_month::TEXT, 2, '0'));

    -- Calculate range bounds
    v_start_date := DATE(FORMAT('%s-%s-01', p_year, LPAD(p_month::TEXT, 2, '0')));
    v_end_date   := v_start_date + INTERVAL '1 month';

    -- Skip silently if partition already exists
    IF EXISTS (
        SELECT 1 FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relname = v_partition_name
          AND n.nspname = current_schema()
    ) THEN
        RAISE NOTICE 'Partition % already exists, skipping.', v_partition_name;
        RETURN;
    END IF;

    -- Create the partition
    EXECUTE FORMAT(
        'CREATE TABLE %I PARTITION OF events
         FOR VALUES FROM (%L) TO (%L)',
        v_partition_name,
        v_start_date,
        v_end_date
    );

    RAISE NOTICE 'Created partition % (% to %)', v_partition_name, v_start_date, v_end_date;
END;
$$;

COMMENT ON FUNCTION create_monthly_partition(INT, INT) IS
  'Creates a monthly partition for the events table if it does not already exist. Idempotent.';


-- ============================================================
-- FUNCTION: create_upcoming_partitions
-- Ensures partitions exist for the current month and the
-- next N months (default 3). Called by cron monthly.
--
-- Parameters:
--   p_months_ahead INT  -- how many future months to pre-create (default 3)
-- ============================================================
CREATE OR REPLACE FUNCTION create_upcoming_partitions(p_months_ahead INT DEFAULT 3)
RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
    v_target_date DATE;
    i             INT;
BEGIN
    FOR i IN 0..p_months_ahead LOOP
        v_target_date := DATE_TRUNC('month', NOW()) + (i || ' months')::INTERVAL;
        PERFORM create_monthly_partition(
            EXTRACT(YEAR  FROM v_target_date)::INT,
            EXTRACT(MONTH FROM v_target_date)::INT
        );
    END LOOP;
END;
$$;

COMMENT ON FUNCTION create_upcoming_partitions(INT) IS
  'Pre-creates partitions for current month + p_months_ahead future months. Run via cron on the 1st of each month.';


-- ============================================================
-- FUNCTION: drop_old_partitions
-- Drops partitions older than a given retention window.
-- Dropping a partition is near-instant (DDL) vs. row-by-row DELETE.
--
-- Parameters:
--   p_retain_months INT  -- keep this many months of data (default 12)
--
-- Safety: prints names before dropping. Set p_dry_run = TRUE
-- to preview without actually dropping anything.
-- ============================================================
CREATE OR REPLACE FUNCTION drop_old_partitions(
    p_retain_months INT  DEFAULT 12,
    p_dry_run       BOOL DEFAULT FALSE
)
RETURNS TABLE (dropped_partition TEXT, action TEXT)
LANGUAGE plpgsql
AS $$
DECLARE
    v_cutoff       DATE;
    v_rec          RECORD;
    v_part_year    INT;
    v_part_month   INT;
    v_part_date    DATE;
BEGIN
    v_cutoff := DATE_TRUNC('month', NOW()) - (p_retain_months || ' months')::INTERVAL;

    FOR v_rec IN
        SELECT c.relname AS partition_name
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        JOIN pg_inherits i  ON i.inhrelid = c.oid
        JOIN pg_class p     ON p.oid = i.inhparent
        WHERE p.relname = 'events'
          AND c.relname ~ '^events_\d{4}_\d{2}$'
          AND n.nspname = current_schema()
        ORDER BY c.relname
    LOOP
        -- Parse year/month from name (events_YYYY_MM)
        v_part_year  := SPLIT_PART(v_rec.partition_name, '_', 2)::INT;
        v_part_month := SPLIT_PART(v_rec.partition_name, '_', 3)::INT;
        v_part_date  := DATE(FORMAT('%s-%s-01', v_part_year, LPAD(v_part_month::TEXT, 2, '0')));

        IF v_part_date < v_cutoff THEN
            IF p_dry_run THEN
                dropped_partition := v_rec.partition_name;
                action := 'DRY RUN — would drop';
                RETURN NEXT;
            ELSE
                EXECUTE FORMAT('DROP TABLE IF EXISTS %I', v_rec.partition_name);
                dropped_partition := v_rec.partition_name;
                action := 'DROPPED';
                RETURN NEXT;
                RAISE NOTICE 'Dropped partition %', v_rec.partition_name;
            END IF;
        END IF;
    END LOOP;
END;
$$;

COMMENT ON FUNCTION drop_old_partitions(INT, BOOL) IS
  'Drops event partitions older than p_retain_months. Use p_dry_run=TRUE to preview. Always run dry first.';


-- ============================================================
-- FUNCTION: list_partitions
-- Utility: list all events partitions with their date ranges
-- and approximate row counts.
-- ============================================================
CREATE OR REPLACE FUNCTION list_partitions()
RETURNS TABLE (
    partition_name TEXT,
    range_start    TEXT,
    range_end      TEXT,
    estimated_rows BIGINT
)
LANGUAGE sql
STABLE
AS $$
    SELECT
        c.relname::TEXT                                             AS partition_name,
        pg_get_expr(c.relpartbound, c.oid)                        AS range_info,
        ''::TEXT                                                   AS range_end,
        c.reltuples::BIGINT                                        AS estimated_rows
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    JOIN pg_inherits i  ON i.inhrelid = c.oid
    JOIN pg_class p     ON p.oid = i.inhparent
    WHERE p.relname = 'events'
      AND n.nspname = current_schema()
    ORDER BY c.relname;
$$;


-- ============================================================
-- BOOTSTRAP: create partitions for current + next 3 months
-- Run once after schema setup.
-- ============================================================
SELECT create_upcoming_partitions(3);


-- ============================================================
-- CRON SCHEDULE (pg_cron — run after installing pg_cron extension)
-- Uncomment once pg_cron is available.
-- ============================================================
-- Create partitions on the 1st of each month at midnight
-- SELECT cron.schedule('create-partitions', '0 0 1 * *', 'SELECT create_upcoming_partitions(3)');

-- Drop old partitions on the 1st of each month at 01:00
-- SELECT cron.schedule('drop-old-partitions', '0 1 1 * *', 'SELECT drop_old_partitions(12)');
