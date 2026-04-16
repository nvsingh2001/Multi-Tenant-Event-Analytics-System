#!/usr/bin/env bash
# restore.sh: Restores backups into a temporary DB and verifies integrity.
# Usage: bash restore.sh <backup_file> [--verify-only]

set -euo pipefail

DB_NAME="${DB_NAME:-MultitenantEventAnalyticsDB}"
RESTORE_DB="${RESTORE_DB:-MultitenantEventAnalyticsDB_restore}"
PG_ADMIN="-h localhost -p 5432 -U postgres"
PG_RESTORE="${PG_ADMIN} ${RESTORE_DB}"

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"; }
die() { log "ERROR: $*"; exit 1; }

FILE="${1:-}"
[[ -z "${FILE}" ]] && die "File missing"
[[ -f "${FILE}" ]] || die "Not a file"

# Creates fresh restore DB
setup_db() {
    log "Setup ${RESTORE_DB}..."
    psql ${PG_ADMIN} postgres -c "DROP DATABASE IF EXISTS ${RESTORE_DB};"
    psql ${PG_ADMIN} postgres -c "CREATE DATABASE ${RESTORE_DB};"
}

# Restores the dump
restore() {
    log "Restoring ${FILE}..."
    pg_restore --host=localhost --port=5432 --username=postgres --dbname="${RESTORE_DB}" --no-owner --jobs=4 "${FILE}" 2>/dev/null || true
}

# Runs SQL health checks and refreshes views
verify() {
    log "Verifying..."
    psql ${PG_RESTORE} -c "CALL verify_backup_integrity();" || die "Checks failed"
    psql ${PG_RESTORE} -c "CALL refresh_all_views_full();" || die "Views failed"
    log "Verify complete."
}

main() {
    if [[ "${2:-}" != "--verify-only" ]]; then
        setup_db
        restore
    fi
    verify
    log "SUCCESS. Promote with: ALTER DATABASE ${RESTORE_DB} RENAME TO ${DB_NAME};"
}

main "$@"
