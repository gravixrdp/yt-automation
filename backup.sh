#!/bin/bash
# backup.sh — Gap #8: Daily backup of credentials, queue DB, and logs.
# Usage: bash backup.sh
# Crontab: 0 2 * * * /path/to/gravix-agent/backup.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BACKUP_DIR="${SCRIPT_DIR}/backups"
RETENTION_DAYS="${BACKUP_RETENTION_DAYS:-7}"
LOG_RETENTION_DAYS="${LOG_RETENTION_DAYS:-7}"
DATE=$(date -u +%Y%m%d_%H%M%S)
BACKUP_FILE="${BACKUP_DIR}/gravix_${DATE}.tar.gz"

mkdir -p "${BACKUP_DIR}"

echo "[$(date -u)] Starting Gravix backup..."

# Build file list
FILES_TO_BACKUP=""

# Credentials (if exists)
if [ -f "${SCRIPT_DIR}/secrets/credentials.json" ]; then
    FILES_TO_BACKUP="${FILES_TO_BACKUP} secrets/credentials.json"
fi

# Queue DB (if exists)
if [ -f "${SCRIPT_DIR}/queue.db" ]; then
    # Use SQLite backup to avoid corruption
    if command -v sqlite3 &>/dev/null; then
        sqlite3 "${SCRIPT_DIR}/queue.db" ".backup '${SCRIPT_DIR}/queue_backup.db'"
        FILES_TO_BACKUP="${FILES_TO_BACKUP} queue_backup.db"
    else
        FILES_TO_BACKUP="${FILES_TO_BACKUP} queue.db"
    fi
fi

# Service account key
if [ -f "${SCRIPT_DIR}/service_account.json" ]; then
    FILES_TO_BACKUP="${FILES_TO_BACKUP} service_account.json"
fi

# Config files
for f in .env sources.yaml scraper_config.py scheduler_config.py; do
    if [ -f "${SCRIPT_DIR}/${f}" ]; then
        FILES_TO_BACKUP="${FILES_TO_BACKUP} ${f}"
    fi
done

# Recent logs (last 2 days only to keep backup small)
LOG_FILES=$(find "${SCRIPT_DIR}/logs" -name "*.log" -mtime -2 2>/dev/null || true)
if [ -n "${LOG_FILES}" ]; then
    for lf in ${LOG_FILES}; do
        rel=$(realpath --relative-to="${SCRIPT_DIR}" "${lf}")
        FILES_TO_BACKUP="${FILES_TO_BACKUP} ${rel}"
    done
fi

if [ -z "${FILES_TO_BACKUP}" ]; then
    echo "[$(date -u)] WARNING: No files to backup."
    exit 0
fi

# Create tar.gz
cd "${SCRIPT_DIR}"
tar -czf "${BACKUP_FILE}" ${FILES_TO_BACKUP} 2>/dev/null
BACKUP_SIZE=$(du -sh "${BACKUP_FILE}" | cut -f1)
echo "[$(date -u)] Backup created: ${BACKUP_FILE} (${BACKUP_SIZE})"

# Clean up temp backup DB
rm -f "${SCRIPT_DIR}/queue_backup.db"

# Delete old backups
DELETED=$(find "${BACKUP_DIR}" -name "gravix_*.tar.gz" -mtime +${RETENTION_DAYS} -delete -print | wc -l)
if [ "${DELETED}" -gt 0 ]; then
    echo "[$(date -u)] Deleted ${DELETED} old backup(s) (>${RETENTION_DAYS} days)."
fi

# Delete old log files
LOG_DELETED=$(find "${SCRIPT_DIR}/logs" -name "*.log" -mtime +${LOG_RETENTION_DAYS} -delete -print 2>/dev/null | wc -l)
JSONL_DELETED=$(find "${SCRIPT_DIR}/logs" -name "*.jsonl" -mtime +${LOG_RETENTION_DAYS} -delete -print 2>/dev/null | wc -l)
TOTAL_LOG_DEL=$((LOG_DELETED + JSONL_DELETED))
if [ "${TOTAL_LOG_DEL}" -gt 0 ]; then
    echo "[$(date -u)] Deleted ${TOTAL_LOG_DEL} old log file(s) (>${LOG_RETENTION_DAYS} days)."
fi

echo "[$(date -u)] Backup complete."
