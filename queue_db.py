"""
queue_db.py — SQLite-backed persistent queue for upload jobs.
Survives restarts and tracks retry counts.
"""

import sqlite3
import logging
from datetime import datetime, timezone

import scheduler_config

logger = logging.getLogger(__name__)

DB_PATH = scheduler_config.QUEUE_DB_PATH


def _get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_db():
    """Create tables if they don't exist."""
    conn = _get_conn()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS upload_queue (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_tab TEXT NOT NULL,
            sheet_row INTEGER NOT NULL,
            row_id INTEGER NOT NULL,
            priority_score INTEGER DEFAULT 0,
            scraped_date TEXT,
            dest_account_id TEXT,
            status TEXT DEFAULT 'QUEUED',
            retry_count INTEGER DEFAULT 0,
            next_attempt_after TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            error_msg TEXT,
            UNIQUE(source_tab, row_id)
        );

        CREATE TABLE IF NOT EXISTS daily_uploads (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            dest_account_id TEXT NOT NULL,
            upload_date TEXT NOT NULL,
            row_id INTEGER NOT NULL,
            uploaded_at TEXT NOT NULL,
            UNIQUE(dest_account_id, upload_date, row_id)
        );

        CREATE TABLE IF NOT EXISTS last_upload_time (
            dest_account_id TEXT PRIMARY KEY,
            last_upload_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS youtube_quota (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            project_id TEXT NOT NULL,
            quota_date TEXT NOT NULL,
            units_used INTEGER DEFAULT 0,
            updated_at TEXT NOT NULL,
            UNIQUE(project_id, quota_date)
        );

        CREATE TABLE IF NOT EXISTS idempotency_keys (
            idem_key TEXT PRIMARY KEY,
            queue_id INTEGER,
            completed_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS destination_cleanup_jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            dest_account_id TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'QUEUED',
            attempt_count INTEGER NOT NULL DEFAULT 0,
            next_attempt_after TEXT,
            last_error TEXT,
            rows_cleared_total INTEGER NOT NULL DEFAULT 0,
            mappings_disabled_total INTEGER NOT NULL DEFAULT 0,
            queue_canceled_total INTEGER NOT NULL DEFAULT 0,
            remove_account_after_cleanup INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_queue_status ON upload_queue(status);
        CREATE INDEX IF NOT EXISTS idx_daily_dest ON daily_uploads(dest_account_id, upload_date);
        CREATE INDEX IF NOT EXISTS idx_quota_project ON youtube_quota(project_id, quota_date);
        CREATE INDEX IF NOT EXISTS idx_cleanup_status ON destination_cleanup_jobs(status, next_attempt_after);
        CREATE UNIQUE INDEX IF NOT EXISTS idx_cleanup_active_dest
            ON destination_cleanup_jobs(dest_account_id)
            WHERE status IN ('QUEUED', 'IN_PROGRESS');
    """)
    conn.commit()
    conn.close()
    logger.info("Queue DB initialized at %s", DB_PATH)


def enqueue(
    source_tab: str, sheet_row: int, row_id: int,
    priority_score: int = 0, scraped_date: str = "",
    dest_account_id: str = "",
) -> bool:
    """Add a row to the upload queue. Returns False if already exists."""
    now = datetime.now(timezone.utc).isoformat()
    conn = _get_conn()
    try:
        conn.execute(
            """INSERT OR IGNORE INTO upload_queue
               (source_tab, sheet_row, row_id, priority_score, scraped_date,
                dest_account_id, status, retry_count, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, 'QUEUED', 0, ?, ?)""",
            (source_tab, sheet_row, row_id, priority_score,
             scraped_date, dest_account_id, now, now),
        )
        conn.commit()
        inserted = conn.total_changes > 0
        return inserted
    finally:
        conn.close()


def get_next_jobs(limit: int = 2) -> list[dict]:
    """
    Get the next jobs to process, ordered by priority (desc), scraped_date (asc).
    Only returns QUEUED jobs whose next_attempt_after has passed.
    """
    now = datetime.now(timezone.utc).isoformat()
    conn = _get_conn()
    try:
        rows = conn.execute(
            """SELECT * FROM upload_queue
               WHERE status = 'QUEUED'
                 AND (next_attempt_after IS NULL OR next_attempt_after <= ?)
               ORDER BY priority_score DESC, scraped_date ASC
               LIMIT ?""",
            (now, limit),
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def get_jobs_snapshot(
    statuses: tuple[str, ...] = ("IN_PROGRESS", "QUEUED"),
    limit: int = 20,
) -> list[dict]:
    """
    Return a compact snapshot of queue jobs for bot visibility/debugging.
    IN_PROGRESS rows are sorted first, then QUEUED rows.
    """
    if not statuses:
        return []
    placeholders = ",".join("?" for _ in statuses)
    sql = (
        f"""SELECT id, source_tab, sheet_row, row_id, dest_account_id, status,
                   retry_count, next_attempt_after, error_msg, created_at, updated_at
              FROM upload_queue
             WHERE status IN ({placeholders})
             ORDER BY CASE status
                        WHEN 'IN_PROGRESS' THEN 0
                        WHEN 'QUEUED' THEN 1
                        ELSE 2
                      END,
                      id ASC
             LIMIT ?"""
    )
    conn = _get_conn()
    try:
        rows = conn.execute(sql, (*statuses, int(limit))).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def mark_in_progress(queue_id: int):
    """Mark a job as in-progress."""
    now = datetime.now(timezone.utc).isoformat()
    conn = _get_conn()
    conn.execute(
        "UPDATE upload_queue SET status='IN_PROGRESS', updated_at=? WHERE id=?",
        (now, queue_id),
    )
    conn.commit()
    conn.close()


def mark_completed(queue_id: int):
    """Mark a job as completed (uploaded)."""
    now = datetime.now(timezone.utc).isoformat()
    conn = _get_conn()
    conn.execute(
        "UPDATE upload_queue SET status='COMPLETED', updated_at=? WHERE id=?",
        (now, queue_id),
    )
    conn.commit()
    conn.close()


def mark_failed(queue_id: int, error_msg: str, max_retries: int = 3):
    """
    Mark a job as failed. Re-queue with backoff if retries remain,
    otherwise mark as FAILED.
    """
    now_dt = datetime.now(timezone.utc)
    now = now_dt.isoformat()
    conn = _get_conn()
    row = conn.execute("SELECT retry_count FROM upload_queue WHERE id=?", (queue_id,)).fetchone()
    if not row:
        conn.close()
        return

    retry_count = row["retry_count"] + 1
    if retry_count < max_retries:
        # Exponential backoff
        from datetime import timedelta
        backoff = scheduler_config.RETRY_BACKOFF_BASE * (3 ** (retry_count - 1))
        next_attempt = (now_dt + timedelta(seconds=backoff)).isoformat()
        conn.execute(
            """UPDATE upload_queue
               SET status='QUEUED', retry_count=?, next_attempt_after=?,
                   error_msg=?, updated_at=?
               WHERE id=?""",
            (retry_count, next_attempt, error_msg, now, queue_id),
        )
    else:
        conn.execute(
            """UPDATE upload_queue
               SET status='FAILED', retry_count=?, error_msg=?, updated_at=?
               WHERE id=?""",
            (retry_count, error_msg, now, queue_id),
        )
    conn.commit()
    conn.close()


def requeue_at(queue_id: int, when_dt_iso: str):
    """Move a job back to QUEUED and set next_attempt_after to a timestamp."""
    now = datetime.now(timezone.utc).isoformat()
    conn = _get_conn()
    conn.execute(
        """UPDATE upload_queue
           SET status='QUEUED',
               next_attempt_after=?,
               updated_at=?
           WHERE id=?""",
        (when_dt_iso, now, queue_id),
    )
    conn.commit()
    conn.close()


def cancel_jobs_for_dest(dest_account_id: str) -> int:
    """
    Mark all queued/in-progress jobs for a destination as FAILED and clear next attempt.
    Returns number of rows affected.
    """
    now = datetime.now(timezone.utc).isoformat()
    conn = _get_conn()
    try:
        cur = conn.execute(
            """UPDATE upload_queue
               SET status='FAILED',
                   error_msg='dest_removed',
                   updated_at=?,
                   next_attempt_after=NULL
               WHERE dest_account_id=? AND status IN ('QUEUED','IN_PROGRESS')""",
            (now, dest_account_id),
        )
        conn.commit()
        return cur.rowcount
    finally:
        conn.close()


# ── Destination cleanup jobs ─────────────────────────────────────

def enqueue_destination_cleanup(
    dest_account_id: str, remove_account_after_cleanup: bool = True,
) -> bool:
    """
    Queue a destination cleanup job.
    Returns False if an active cleanup job for this destination already exists.
    """
    now = datetime.now(timezone.utc).isoformat()
    conn = _get_conn()
    try:
        try:
            conn.execute(
                """INSERT INTO destination_cleanup_jobs
                   (dest_account_id, status, attempt_count, next_attempt_after, last_error,
                    rows_cleared_total, mappings_disabled_total, queue_canceled_total,
                    remove_account_after_cleanup, created_at, updated_at)
                   SELECT ?, 'QUEUED', 0, NULL, NULL, 0, 0, 0, ?, ?, ?
                   WHERE NOT EXISTS (
                       SELECT 1 FROM destination_cleanup_jobs
                       WHERE dest_account_id=?
                         AND status IN ('QUEUED', 'IN_PROGRESS')
                   )""",
                (
                    dest_account_id,
                    1 if remove_account_after_cleanup else 0,
                    now,
                    now,
                    dest_account_id,
                ),
            )
        except sqlite3.IntegrityError:
            return False
        conn.commit()
        return conn.total_changes > 0
    finally:
        conn.close()


def get_next_destination_cleanup_job() -> dict | None:
    """Return the next cleanup job ready to run, or None."""
    now = datetime.now(timezone.utc).isoformat()
    conn = _get_conn()
    try:
        row = conn.execute(
            """SELECT * FROM destination_cleanup_jobs
               WHERE status='QUEUED'
                 AND (next_attempt_after IS NULL OR next_attempt_after <= ?)
               ORDER BY updated_at ASC, id ASC
               LIMIT 1""",
            (now,),
        ).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def get_destination_cleanup_job(job_id: int) -> dict | None:
    """Fetch a cleanup job by id."""
    conn = _get_conn()
    try:
        row = conn.execute(
            "SELECT * FROM destination_cleanup_jobs WHERE id=?",
            (job_id,),
        ).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def mark_destination_cleanup_in_progress(job_id: int) -> None:
    """Mark cleanup job in-progress and increment attempt count."""
    now = datetime.now(timezone.utc).isoformat()
    conn = _get_conn()
    try:
        conn.execute(
            """UPDATE destination_cleanup_jobs
               SET status='IN_PROGRESS',
                   attempt_count=attempt_count + 1,
                   updated_at=?
               WHERE id=?""",
            (now, job_id),
        )
        conn.commit()
    finally:
        conn.close()


def reschedule_destination_cleanup(
    job_id: int, error_msg: str, next_attempt_after: str,
    counters: dict | None = None,
) -> None:
    """Re-queue cleanup job with backoff and cumulative progress counters."""
    counters = counters or {}
    rows_cleared = int(counters.get("rows_cleared", 0) or 0)
    mappings_disabled = int(counters.get("mappings_disabled", 0) or 0)
    queue_canceled = int(counters.get("queue_canceled", 0) or 0)
    now = datetime.now(timezone.utc).isoformat()
    conn = _get_conn()
    try:
        conn.execute(
            """UPDATE destination_cleanup_jobs
               SET status='QUEUED',
                   next_attempt_after=?,
                   last_error=?,
                   rows_cleared_total=rows_cleared_total + ?,
                   mappings_disabled_total=mappings_disabled_total + ?,
                   queue_canceled_total=queue_canceled_total + ?,
                   updated_at=?
               WHERE id=?""",
            (
                next_attempt_after,
                error_msg,
                rows_cleared,
                mappings_disabled,
                queue_canceled,
                now,
                job_id,
            ),
        )
        conn.commit()
    finally:
        conn.close()


def complete_destination_cleanup(job_id: int, counters: dict | None = None) -> None:
    """Mark cleanup job completed and add final progress counters."""
    counters = counters or {}
    rows_cleared = int(counters.get("rows_cleared", 0) or 0)
    mappings_disabled = int(counters.get("mappings_disabled", 0) or 0)
    queue_canceled = int(counters.get("queue_canceled", 0) or 0)
    now = datetime.now(timezone.utc).isoformat()
    conn = _get_conn()
    try:
        conn.execute(
            """UPDATE destination_cleanup_jobs
               SET status='COMPLETED',
                   next_attempt_after=NULL,
                   last_error=NULL,
                   rows_cleared_total=rows_cleared_total + ?,
                   mappings_disabled_total=mappings_disabled_total + ?,
                   queue_canceled_total=queue_canceled_total + ?,
                   updated_at=?
               WHERE id=?""",
            (
                rows_cleared,
                mappings_disabled,
                queue_canceled,
                now,
                job_id,
            ),
        )
        conn.commit()
    finally:
        conn.close()


def get_destination_cleanup_stats(limit: int = 20) -> list[dict]:
    """Return recent destination cleanup jobs for status display."""
    conn = _get_conn()
    try:
        rows = conn.execute(
            """SELECT * FROM destination_cleanup_jobs
               ORDER BY updated_at DESC, id DESC
               LIMIT ?""",
            (limit,),
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def has_pending_destination_cleanup() -> bool:
    """Return True if any destination cleanup job is queued or in-progress."""
    conn = _get_conn()
    try:
        row = conn.execute(
            """SELECT 1 FROM destination_cleanup_jobs
               WHERE status IN ('QUEUED', 'IN_PROGRESS')
               LIMIT 1"""
        ).fetchone()
        return row is not None
    finally:
        conn.close()


def get_destinations_with_queue_jobs(
    statuses: tuple[str, ...] = ("QUEUED", "IN_PROGRESS", "FAILED"),
) -> list[str]:
    """Return distinct destination ids present in upload_queue for given statuses."""
    if not statuses:
        return []
    placeholders = ",".join("?" for _ in statuses)
    conn = _get_conn()
    try:
        rows = conn.execute(
            f"""SELECT DISTINCT dest_account_id
                FROM upload_queue
                WHERE dest_account_id IS NOT NULL
                  AND dest_account_id != ''
                  AND status IN ({placeholders})""",
            statuses,
        ).fetchall()
        return [r["dest_account_id"] for r in rows if r["dest_account_id"]]
    finally:
        conn.close()


# ── Daily upload tracking ─────────────────────────────────────────

def get_uploads_today(dest_account_id: str) -> int:
    """Count how many uploads have been done today for a destination."""
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    conn = _get_conn()
    try:
        row = conn.execute(
            "SELECT COUNT(*) as cnt FROM daily_uploads WHERE dest_account_id=? AND upload_date=?",
            (dest_account_id, today),
        ).fetchone()
        return row["cnt"] if row else 0
    finally:
        conn.close()


def record_upload(dest_account_id: str, row_id: int):
    """Record an upload for daily cap tracking."""
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    now = datetime.now(timezone.utc).isoformat()
    conn = _get_conn()
    try:
        conn.execute(
            """INSERT OR IGNORE INTO daily_uploads
               (dest_account_id, upload_date, row_id, uploaded_at)
               VALUES (?, ?, ?, ?)""",
            (dest_account_id, today, row_id, now),
        )
        # Also update last upload time for spacing
        conn.execute(
            """INSERT OR REPLACE INTO last_upload_time
               (dest_account_id, last_upload_at)
               VALUES (?, ?)""",
            (dest_account_id, now),
        )
        conn.commit()
    finally:
        conn.close()


def get_last_upload_time(dest_account_id: str) -> str | None:
    """Get the last upload timestamp for a destination."""
    conn = _get_conn()
    try:
        row = conn.execute(
            "SELECT last_upload_at FROM last_upload_time WHERE dest_account_id=?",
            (dest_account_id,),
        ).fetchone()
        return row["last_upload_at"] if row else None
    finally:
        conn.close()


def get_last_upload_time_any() -> str | None:
    """Get the most recent upload timestamp across all destinations."""
    conn = _get_conn()
    try:
        row = conn.execute(
            "SELECT MAX(last_upload_at) as latest FROM last_upload_time"
        ).fetchone()
        return row["latest"] if row and row["latest"] else None
    finally:
        conn.close()


def get_queue_stats() -> dict:
    """Get queue statistics for admin display."""
    conn = _get_conn()
    try:
        stats = {}
        for status in ("QUEUED", "IN_PROGRESS", "COMPLETED", "FAILED"):
            row = conn.execute(
                "SELECT COUNT(*) as cnt FROM upload_queue WHERE status=?", (status,)
            ).fetchone()
            stats[status.lower()] = row["cnt"]

        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        row = conn.execute(
            "SELECT COUNT(*) as cnt FROM daily_uploads WHERE upload_date=?", (today,)
        ).fetchone()
        stats["uploaded_today"] = row["cnt"]
        return stats
    finally:
        conn.close()


def reset_stale_jobs(hours: int = 24):
    """Reset jobs stuck IN_PROGRESS for longer than `hours`."""
    from datetime import timedelta
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
    conn = _get_conn()
    updated = conn.execute(
        """UPDATE upload_queue SET status='QUEUED', updated_at=?
           WHERE status='IN_PROGRESS' AND updated_at < ?""",
        (datetime.now(timezone.utc).isoformat(), cutoff),
    ).rowcount
    conn.commit()
    conn.close()
    if updated:
        logger.info("Reset %d stale IN_PROGRESS jobs.", updated)
    return updated


def requeue_failed_status_conflicts() -> int:
    """
    Re-queue rows that failed only due to optimistic-lock status conflicts.
    Useful after transient race conditions or schema-column corrections.
    """
    now = datetime.now(timezone.utc).isoformat()
    conn = _get_conn()
    try:
        cur = conn.execute(
            """UPDATE upload_queue
               SET status='QUEUED',
                   retry_count=0,
                   next_attempt_after=NULL,
                   error_msg=NULL,
                   updated_at=?
               WHERE status='FAILED'
                 AND error_msg LIKE 'status_conflict:%'""",
            (now,),
        )
        conn.commit()
        return cur.rowcount
    finally:
        conn.close()


def cleanup_old_records(days: int = 90):
    """Remove completed/failed records older than `days`."""
    from datetime import timedelta
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
    conn = _get_conn()
    conn.execute(
        "DELETE FROM upload_queue WHERE status IN ('COMPLETED','FAILED') AND updated_at < ?",
        (cutoff,),
    )
    conn.execute("DELETE FROM daily_uploads WHERE uploaded_at < ?", (cutoff,))
    conn.commit()
    conn.close()


# ── YouTube Quota Tracking (#2) ──────────────────────────────────

def record_quota_usage(project_id: str, units: int = 0):
    """Record quota units used for a YouTube project today."""
    if units <= 0:
        units = scheduler_config.YT_QUOTA_UNITS_PER_UPLOAD
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    now = datetime.now(timezone.utc).isoformat()
    conn = _get_conn()
    try:
        conn.execute(
            """INSERT INTO youtube_quota (project_id, quota_date, units_used, updated_at)
               VALUES (?, ?, ?, ?)
               ON CONFLICT(project_id, quota_date)
               DO UPDATE SET units_used = units_used + ?, updated_at = ?""",
            (project_id, today, units, now, units, now),
        )
        conn.commit()
    finally:
        conn.close()


def get_quota_remaining(project_id: str) -> int:
    """Get remaining YouTube quota units for a project today."""
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    limit = int(scheduler_config.YT_QUOTA_LIMIT_PER_PROJECT * scheduler_config.QUOTA_SAFETY_MARGIN)
    conn = _get_conn()
    try:
        row = conn.execute(
            "SELECT units_used FROM youtube_quota WHERE project_id=? AND quota_date=?",
            (project_id, today),
        ).fetchone()
        used = row["units_used"] if row else 0
        return max(0, limit - used)
    finally:
        conn.close()


def get_cheapest_project() -> str | None:
    """Get the YouTube project with the most remaining quota today."""
    projects = scheduler_config.YT_PROJECT_KEYS
    if not projects:
        return "default"  # Single project mode
    best_id = None
    best_remaining = -1
    for pid in projects:
        remaining = get_quota_remaining(pid)
        if remaining > best_remaining:
            best_remaining = remaining
            best_id = pid
    if best_remaining < scheduler_config.YT_QUOTA_UNITS_PER_UPLOAD:
        return None  # All projects exhausted
    return best_id


# ── Idempotency (S1) ─────────────────────────────────────────────

def check_idempotency(idem_key: str) -> bool:
    """Return True if this upload was already completed (prevent doubles)."""
    conn = _get_conn()
    try:
        row = conn.execute(
            "SELECT 1 FROM idempotency_keys WHERE idem_key=?", (idem_key,)
        ).fetchone()
        return row is not None
    finally:
        conn.close()


def record_idempotency(idem_key: str, queue_id: int):
    """Record a completed upload's idempotency key."""
    now = datetime.now(timezone.utc).isoformat()
    conn = _get_conn()
    try:
        conn.execute(
            "INSERT OR IGNORE INTO idempotency_keys (idem_key, queue_id, completed_at) VALUES (?, ?, ?)",
            (idem_key, queue_id, now),
        )
        conn.commit()
    finally:
        conn.close()
