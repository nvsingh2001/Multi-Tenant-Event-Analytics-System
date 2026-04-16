-- 08_lifecycle.sql: Data Lifecycle Management
-- Strategy: Retain 12m hot data, archive to flat table, drop old partitions.
-- Cold storage for events older than retention window.
CREATE TABLE IF NOT EXISTS events_archive (
    event_id uuid NOT NULL,
    tenant_id uuid NOT NULL,
    user_id text NOT NULL,
    event_name text NOT NULL,
    event_time timestamptz NOT NULL,
    properties jsonb NOT NULL DEFAULT '{}',
    session_id text,
    ingested_at timestamptz NOT NULL,
    archived_at timestamptz NOT NULL DEFAULT NOW(),
    PRIMARY KEY (event_id, event_time)
);

CREATE INDEX IF NOT EXISTS idx_archive_tenant_time ON events_archive (tenant_id, event_time DESC);

CREATE INDEX IF NOT EXISTS idx_archive_tenant_user ON events_archive (tenant_id, user_id);

-- Archives a partition to events_archive then drops it.
CREATE OR REPLACE FUNCTION archive_partition (p_partition_name text, p_dry_run bool DEFAULT FALSE)
    RETURNS TABLE (
        partition_name text,
        rows_archived bigint,
        action text)
    LANGUAGE plpgsql
    AS $$
DECLARE
    v_row_count bigint;
    v_archive_count bigint;
BEGIN
    IF NOT EXISTS (
        SELECT
            1
        FROM
            pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE
            c.relname = p_partition_name
            AND n.nspname = current_schema()) THEN
    RAISE EXCEPTION 'Partition % does not exist', p_partition_name;
END IF;
    EXECUTE FORMAT('SELECT COUNT(*) FROM %I', p_partition_name) INTO v_row_count;
    IF p_dry_run THEN
        RETURN QUERY
        SELECT
            p_partition_name,
            v_row_count,
            'DRY RUN'::text;
        RETURN;
    END IF;
    EXECUTE FORMAT('INSERT INTO events_archive (event_id, tenant_id, user_id, event_name, event_time, properties, session_id, ingested_at, archived_at)
         SELECT event_id, tenant_id, user_id, event_name, event_time, properties, session_id, ingested_at, NOW() FROM %I', p_partition_name);
    GET DIAGNOSTICS v_archive_count = ROW_COUNT;
    IF v_archive_count <> v_row_count THEN
        RAISE EXCEPTION 'Count mismatch for %: src=% arch=%', p_partition_name, v_row_count, v_archive_count;
    END IF;
    EXECUTE FORMAT('DROP TABLE %I', p_partition_name);
    RETURN QUERY
    SELECT
        p_partition_name,
        v_archive_count,
        'ARCHIVED AND DROPPED'::text;
END;
$$;

-- Monthly job: Creates upcoming partitions, archives old ones, and purges dedup log.
CREATE OR REPLACE FUNCTION run_lifecycle (p_retain_months int DEFAULT 12, p_archive bool DEFAULT TRUE, p_dry_run bool DEFAULT FALSE)
    RETURNS TABLE (
        step text,
        partition_name text,
        rows_affected bigint,
        action text)
    LANGUAGE plpgsql
    AS $$
DECLARE
    v_cutoff date;
    v_rec RECORD;
    v_part_date date;
BEGIN
    v_cutoff := DATE_TRUNC('month', NOW()) - (p_retain_months || ' months')::interval;
    IF NOT p_dry_run THEN
        PERFORM
            create_upcoming_partitions (3);
    END IF;
    step := 'create_partitions';
    action := CASE WHEN p_dry_run THEN
        'DRY RUN'
    ELSE
        'COMPLETED'
    END;
    RETURN NEXT;
    FOR v_rec IN
    SELECT
        c.relname AS pname
    FROM
        pg_class c
        JOIN pg_inherits i ON i.inhrelid = c.oid
        JOIN pg_class p ON p.oid = i.inhparent
    WHERE
        p.relname = 'events'
        AND c.relname ~ '^events_\d{4}_\d{2}$'
    ORDER BY
        c.relname LOOP
            v_part_date := DATE(FORMAT('%s-%s-01', SPLIT_PART(v_rec.pname, '_', 2), LPAD(SPLIT_PART(v_rec.pname, '_', 3), 2, '0')));
            IF v_part_date < v_cutoff THEN
                IF p_archive THEN
                    SELECT
                        a.partition_name,
                        a.rows_archived,
                        a.action INTO partition_name,
                        rows_affected,
                        action
                    FROM
                        archive_partition (v_rec.pname, p_dry_run) a;
                ELSE
                    IF NOT p_dry_run THEN
                        EXECUTE FORMAT('DROP TABLE IF EXISTS %I', v_rec.pname);
                    END IF;
                    partition_name := v_rec.pname;
                    action := 'DROPPED';
                END IF;
                step := 'lifecycle_partition';
                RETURN NEXT;
            END IF;
        END LOOP;
    step := 'purge_dedup';
    partition_name := 'event_dedup';
    IF NOT p_dry_run THEN
        CALL purge_dedup_log (48);
        action := 'Purged 48h';
    ELSE
        action := 'DRY RUN';
    END IF;
    RETURN NEXT;
END;
$$;

-- Summary of current partition states.
CREATE OR REPLACE FUNCTION lifecycle_status ()
    RETURNS TABLE (
        partition_name text,
        partition_month date,
        estimated_rows bigint,
        size text,
        age_months int,
        status text)
    LANGUAGE sql
    STABLE
    AS $$
    SELECT
        c.relname::text,
        DATE(FORMAT('%s-%s-01', SPLIT_PART(c.relname, '_', 2), LPAD(SPLIT_PART(c.relname, '_', 3), 2, '0'))),
        c.reltuples::bigint,
        pg_size_pretty(pg_relation_size(c.oid)),
        (EXTRACT(YEAR FROM AGE(NOW(), DATE(FORMAT('%s-%s-01', SPLIT_PART(c.relname, '_', 2), LPAD(SPLIT_PART(c.relname, '_', 3), 2, '0'))))) * 12 + EXTRACT(MONTH FROM AGE(NOW(), DATE(FORMAT('%s-%s-01', SPLIT_PART(c.relname, '_', 2), LPAD(SPLIT_PART(c.relname, '_', 3), 2, '0')))))::int),
        CASE WHEN DATE(FORMAT('%s-%s-01', SPLIT_PART(c.relname, '_', 2), LPAD(SPLIT_PART(c.relname, '_', 3), 2, '0'))) < DATE_TRUNC('month', NOW()) - INTERVAL '12 months' THEN
            'ELIGIBLE FOR ARCHIVAL'
        WHEN DATE(FORMAT('%s-%s-01', SPLIT_PART(c.relname, '_', 2), LPAD(SPLIT_PART(c.relname, '_', 3), 2, '0'))) >= DATE_TRUNC('month', NOW()) THEN
            'CURRENT / FUTURE'
        ELSE
            'HOT'
        END
    FROM
        pg_class c
        JOIN pg_inherits i ON i.inhrelid = c.oid
        JOIN pg_class p ON p.oid = i.inhparent
    WHERE
        p.relname = 'events'
        AND c.relname ~ '^events_\d{4}_\d{2}$'
    ORDER BY
        2;
$$;

