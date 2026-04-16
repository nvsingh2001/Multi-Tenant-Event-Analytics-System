#!/usr/bin/env bash
# backup.sh: Handles full, schema, and per-tenant backups.
# Usage: bash backup.sh [full|schema|tenant]

set -euo pipefail

# Config
DB_NAME="${DB_NAME:-MultitenantEventAnalyticsDB}"
DB_USER="${DB_USER:-postgres}"
BACKUP_DIR="${BACKUP_DIR:-/home/ares/projects/BridgeLabz/multitenant-event-analytics/backups}"
RETAIN_DAYS="${RETAIN_DAYS:-30}"
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
LOG_FILE="${BACKUP_DIR}/backup_${TIMESTAMP}.log"
PG_CONN="-h localhost -p 5432 -U ${DB_USER} ${DB_NAME}"

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "${LOG_FILE}"; }
die() { log "ERROR: $*"; exit 1; }

ensure_dir() { mkdir -p "${BACKUP_DIR}" || die "Dir failed"; }

# Full custom-format backup
backup_full() {
    local outfile="${BACKUP_DIR}/full_${DB_NAME}_${TIMESTAMP}.dump"
    log "FULL backup -> ${outfile}"
    pg_dump --format=custom --compress=9 --no-password ${PG_CONN} --file="${outfile}" 2>>"${LOG_FILE}" || die "pg_dump failed"
    pg_restore --list "${outfile}" >/dev/null 2>>"${LOG_FILE}" || die "Integrity failed"
    log "Done."
}

# Schema-only backup
backup_schema() {
    local outfile="${BACKUP_DIR}/schema_${DB_NAME}_${TIMESTAMP}.sql"
    log "SCHEMA backup -> ${outfile}"
    pg_dump --schema-only --no-password ${PG_CONN} --file="${outfile}" 2>>"${LOG_FILE}" || die "Schema failed"
    gzip "${outfile}"
    log "Done."
}

# Per-tenant export
backup_tenant() {
    local tid="${TENANT_ID:-}"
    [[ -z "${tid}" ]] && die "TENANT_ID missing"
    local outfile="${BACKUP_DIR}/tenant_${tid}_${TIMESTAMP}.dump"
    log "TENANT backup (${tid}) -> ${outfile}"
    psql --no-password ${PG_CONN} -c "CALL export_tenant_data('${tid}', 'tenant_export_staging');" 2>>"${LOG_FILE}" || die "Proc failed"
    pg_dump --format=custom --compress=9 --table=tenant_export_staging --no-password ${PG_CONN} --file="${outfile}" 2>>"${LOG_FILE}" || die "Dump failed"
    psql --no-password ${PG_CONN} -c "DROP TABLE IF EXISTS tenant_export_staging;"
    log "Done."
}

rotate() {
    log "Cleaning old backups..."
    find "${BACKUP_DIR}" -maxdepth 1 \( -name "*.dump" -o -name "*.sql.gz" \) -mtime "+${RETAIN_DAYS}" -delete
}

main() {
    ensure_dir
    log "Starting ${1:-full} backup for ${DB_NAME}"
    case "${1:-full}" in
        full) backup_full; rotate ;;
        schema) backup_schema ;;
        tenant) backup_tenant ;;
        *) die "Invalid mode" ;;
    esac
}

main "$@"
