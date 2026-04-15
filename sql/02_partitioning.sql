-- Creates a monthly partition if it doesn't already exist
CREATE OR REPLACE FUNCTION create_monthly_partition (p_year int, p_month int)
    RETURNS VOID
    LANGUAGE plpgsql
    AS $$
DECLARE
    v_partition_name text;
    v_start_date date;
    v_end_date date;
BEGIN
    v_partition_name := FORMAT('events_%s_%s', p_year, LPAD(p_month::text, 2, '0'));
    v_start_date := DATE(FORMAT('%s-%s-01', p_year, LPAD(p_month::text, 2, '0')));
    v_end_date := v_start_date + INTERVAL '1 month';
    IF NOT EXISTS (
        SELECT
            1
        FROM
            pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE
            c.relname = v_partition_name
            AND n.nspname = current_schema()) THEN
    EXECUTE FORMAT('CREATE TABLE %I PARTITION OF events FOR VALUES FROM (%L) TO (%L)', v_partition_name, v_start_date, v_end_date);
END IF;
END;
$$;

-- Pre-creates partitions for upcoming months
CREATE OR REPLACE FUNCTION create_upcoming_partitions (p_months_ahead int DEFAULT 3)
    RETURNS VOID
    LANGUAGE plpgsql
    AS $$
DECLARE
    v_target_date date;
    i int;
BEGIN
    FOR i IN 0..p_months_ahead LOOP
        v_target_date := DATE_TRUNC('month', NOW()) + (i || ' months')::interval;
        PERFORM
            create_monthly_partition (EXTRACT(YEAR FROM v_target_date)::int, EXTRACT(MONTH FROM v_target_date)::int);
    END LOOP;
END;
$$;

-- Drops partitions older than specified retention
CREATE OR REPLACE FUNCTION drop_old_partitions (p_retain_months int DEFAULT 12, p_dry_run bool DEFAULT FALSE)
    RETURNS TABLE (
        dropped_partition text,
        action text)
    LANGUAGE plpgsql
    AS $$
DECLARE
    v_cutoff date;
    v_rec RECORD;
    v_part_year int;
    v_part_month int;
    v_part_date date;
BEGIN
    v_cutoff := DATE_TRUNC('month', NOW()) - (p_retain_months || ' months')::interval;
    FOR v_rec IN
    SELECT
        c.relname AS partition_name
    FROM
        pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        JOIN pg_inherits i ON i.inhrelid = c.oid
        JOIN pg_class p ON p.oid = i.inhparent
    WHERE
        p.relname = 'events'
        AND c.relname ~ '^events_\d{4}_\d{2}$'
        AND n.nspname = current_schema()
    ORDER BY
        c.relname LOOP
            v_part_year := SPLIT_PART(v_rec.partition_name, '_', 2)::int;
            v_part_month := SPLIT_PART(v_rec.partition_name, '_', 3)::int;
            v_part_date := DATE(FORMAT('%s-%s-01', v_part_year, LPAD(v_part_month::text, 2, '0')));
            IF v_part_date < v_cutoff THEN
                IF p_dry_run THEN
                    dropped_partition := v_rec.partition_name;
                    action := 'DRY RUN';
                    RETURN NEXT;
                ELSE
                    EXECUTE FORMAT('DROP TABLE IF EXISTS %I', v_rec.partition_name);
                    dropped_partition := v_rec.partition_name;
                    action := 'DROPPED';
                    RETURN NEXT;
                END IF;
            END IF;
        END LOOP;
END;
$$;

-- List current partitions
CREATE OR REPLACE FUNCTION list_partitions ()
    RETURNS TABLE (
        partition_name text,
        range_info text,
        estimated_rows bigint)
    LANGUAGE sql
    STABLE
    AS $$
    SELECT
        c.relname::text,
        pg_get_expr(c.relpartbound, c.oid),
        c.reltuples::bigint
    FROM
        pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        JOIN pg_inherits i ON i.inhrelid = c.oid
        JOIN pg_class p ON p.oid = i.inhparent
    WHERE
        p.relname = 'events'
        AND n.nspname = current_schema()
    ORDER BY
        c.relname;
$$;

-- Initial bootstrap
SELECT
    create_upcoming_partitions (3);

