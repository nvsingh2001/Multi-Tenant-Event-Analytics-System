-- 09_backup.sql: Backup & Restore Verification
-- Strategy: Weekly full, daily schema, on-demand per-tenant exports.
-- Verifies database health (row counts, indexes, partitions, RLS) after restore.
CREATE OR REPLACE PROCEDURE verify_backup_integrity ()
LANGUAGE plpgsql
AS $$
DECLARE
    v_tenant_count int;
    v_user_count int;
    v_event_count bigint;
    v_archive_count bigint;
    v_partition_count int;
    v_invalid_indexes text;
    v_rec RECORD;
    v_ok boolean := TRUE;
BEGIN
    RAISE NOTICE '--- VERIFYING INTEGRITY ---';
    SELECT
        COUNT(*) INTO v_tenant_count
    FROM
        tenants;
    SELECT
        COUNT(*) INTO v_user_count
    FROM
        users;
    SELECT
        COUNT(*) INTO v_event_count
    FROM
        events;
    SELECT
        COUNT(*) INTO v_archive_count
    FROM
        events_archive;
    RAISE NOTICE 'Counts: tenants=%, users=%, events=%, archive=%', v_tenant_count, v_user_count, v_event_count, v_archive_count;
    IF v_tenant_count = 0 THEN
        RAISE WARNING 'Tenants table empty';
        v_ok := FALSE;
    END IF;
    SELECT
        COUNT(*) INTO v_partition_count
    FROM
        pg_inherits
    WHERE
        inhparent = 'events'::regclass;
    RAISE NOTICE 'Partitions: %', v_partition_count;
    IF v_partition_count = 0 THEN
        RAISE WARNING 'No partitions';
        v_ok := FALSE;
    END IF;
    SELECT
        STRING_AGG(indexrelid::regclass::text, ', ') INTO v_invalid_indexes
    FROM
        pg_index
    WHERE
        NOT indisvalid;
    IF v_invalid_indexes IS NOT NULL THEN
        RAISE WARNING 'Invalid indexes: %', v_invalid_indexes;
        v_ok := FALSE;
    END IF;
    FOR v_rec IN
    SELECT
        matviewname,
        ispopulated
    FROM
        pg_matviews
    WHERE
        schemaname = current_schema()
        LOOP
            IF NOT v_rec.ispopulated THEN
                RAISE WARNING 'Matview % not populated', v_rec.matviewname;
                v_ok := FALSE;
            END IF;
        END LOOP;
    FOR v_rec IN
    SELECT
        relname
    FROM
        pg_class
    WHERE
        relname IN ('tenants', 'users', 'events')
        AND relkind = 'r'
        AND NOT relrowsecurity LOOP
            RAISE WARNING 'RLS disabled on %', v_rec.relname;
            v_ok := FALSE;
        END LOOP;
    IF v_ok THEN
        RAISE NOTICE 'RESULT: PASS';
    ELSE
        RAISE WARNING 'RESULT: FAIL';
    END IF;
END;
$$;

-- Exports a single tenant's data to a staging table for GDPR/portability.
CREATE OR REPLACE PROCEDURE export_tenant_data (p_tenant_id uuid, p_table_name text DEFAULT 'tenant_export')
LANGUAGE plpgsql
AS $$
BEGIN
    EXECUTE FORMAT('DROP TABLE IF EXISTS %I', p_table_name);
    EXECUTE FORMAT('CREATE TABLE %I AS SELECT e.*, u.email, u.display_name, u.metadata AS user_metadata 
         FROM events e LEFT JOIN users u ON u.tenant_id = e.tenant_id AND u.user_id = e.user_id 
         WHERE e.tenant_id = %L', p_table_name, p_tenant_id);
    RAISE NOTICE 'Exported tenant % to %', p_tenant_id, p_table_name;
END;
$$;

