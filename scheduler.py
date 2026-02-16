#!/usr/bin/env python3
"""
scheduler.py — Main upload orchestrator for Part 3.
Polls Google Sheet for READY_TO_UPLOAD rows, enforces daily caps,
queues uploads, and dispatches workers for download → ffmpeg → upload.

Usage:
    python scheduler.py                     # Run polling loop
    python scheduler.py --once              # Single poll cycle
    python scheduler.py --reconcile         # Recover stale rows
    python scheduler.py --stats             # Show queue stats
"""

import argparse
import hashlib
import json
import logging
import re
import signal
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta, date
from pathlib import Path
from zoneinfo import ZoneInfo

import scheduler_config
import sheet_manager
import queue_db
import download_manager
import ffmpeg_worker
import uploader
import oauth_helper

# ── Logging ───────────────────────────────────────────────────────
log_filename = f"scheduler_{datetime.now(timezone.utc).strftime('%Y%m%d')}.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(scheduler_config.LOG_DIR / log_filename, encoding="utf-8"),
    ],
)
logger = logging.getLogger("scheduler")

# JSON structured log
json_logger = logging.getLogger("scheduler.json")
json_handler = logging.FileHandler(
    scheduler_config.LOG_DIR / log_filename.replace(".log", "_structured.jsonl"),
    encoding="utf-8",
)
json_handler.setFormatter(logging.Formatter("%(message)s"))
json_logger.addHandler(json_handler)
json_logger.setLevel(logging.INFO)


def log_event(event_type: str, **kwargs):
    entry = {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "event": event_type,
        "instance_id": scheduler_config.INSTANCE_ID,
        **kwargs,
    }
    json_logger.info(json.dumps(entry, ensure_ascii=False))


_SCHEDULE_NOTE_RE = re.compile(r"schedule_at_utc=([0-9T:\-+\.Z]+)", re.IGNORECASE)
_YT_CATEGORY_MAP = {
    "Film & Animation": "1",
    "Autos & Vehicles": "2",
    "Music": "10",
    "Pets & Animals": "15",
    "Sports": "17",
    "Travel & Events": "19",
    "Gaming": "20",
    "People & Blogs": "22",
    "Comedy": "23",
    "Entertainment": "24",
    "News & Politics": "25",
    "Howto & Style": "26",
    "Education": "27",
    "Science & Technology": "28",
}
_DEFAULT_VIRAL_HASHTAGS = [
    "shorts", "viral", "trending", "fyp", "youtubeShorts",
    "reels", "shortvideo", "mustwatch",
]


def _parse_utc_datetime(value: str) -> datetime | None:
    if not value:
        return None
    raw = value.strip()
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(raw)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _extract_scheduled_at_utc(row: dict) -> datetime | None:
    """Read schedule from explicit columns or notes marker."""
    sched_date = str(row.get("scheduled_date", "") or "").strip()
    sched_time = str(row.get("scheduled_time", "") or "").strip()
    if sched_date and sched_time:
        candidate = _parse_utc_datetime(f"{sched_date}T{sched_time}")
        if candidate:
            return candidate
        candidate = _parse_utc_datetime(f"{sched_date}T{sched_time}Z")
        if candidate:
            return candidate

    notes = str(row.get("notes", "") or "")
    matches = _SCHEDULE_NOTE_RE.findall(notes)
    if matches:
        return _parse_utc_datetime(matches[-1])
    return None


def _slug_words(text: str) -> list[str]:
    words = re.findall(r"[A-Za-z0-9]+", text.lower())
    return [w for w in words if len(w) >= 3]


def _fallback_upload_metadata(row_data: dict) -> dict:
    title = str(row_data.get("original_title", "Trending Short Video") or "").strip()
    if not title:
        title = "Trending Short Video"
    if len(title) > 90:
        title = title[:90].rstrip()

    base_words = _slug_words(title)
    source_tags = str(row_data.get("tags_from_source", "") or "")
    for token in source_tags.split(","):
        base_words.extend(_slug_words(token))
    dedup_words: list[str] = []
    seen = set()
    for w in base_words:
        if w not in seen:
            seen.add(w)
            dedup_words.append(w)

    tags = (dedup_words + ["shorts", "viral", "trending"])[:15]
    hashtags = (dedup_words[:8] + _DEFAULT_VIRAL_HASHTAGS)[:12]
    hashtags = list(dict.fromkeys(hashtags))

    description = (
        f"{title}\n\n"
        "Watch till the end and follow for more viral short videos."
    )
    return {
        "title": title[:100],
        "description": description[:5000],
        "tags": tags,
        "hashtags": hashtags,
        "category": "People & Blogs",
        "source": "fallback",
    }


def _resolve_upload_metadata(row_data: dict) -> dict:
    """Use sheet AI fields first, then Gemini, then local fallback."""
    title = str(row_data.get("ai_title", "") or "").strip()
    description = str(row_data.get("ai_description", "") or "").strip()
    tags = [t.strip() for t in str(row_data.get("ai_tags", "") or "").split(",") if t.strip()]
    hashtags = [h.strip().lstrip("#") for h in str(row_data.get("ai_hashtags", "") or "").split(",") if h.strip()]
    category = str(row_data.get("category", "") or "").strip()

    if title and description and tags:
        return {
            "title": title[:100],
            "description": description[:5000],
            "tags": tags[:15],
            "hashtags": hashtags[:15],
            "category": category or "People & Blogs",
            "source": "sheet",
        }

    try:
        import ai_agent
        ai_data = ai_agent.process_row(row_data)
        title = str(ai_data.get("ai_title", "") or title).strip()
        description = str(ai_data.get("ai_description", "") or description).strip()
        tags_csv = str(ai_data.get("ai_tags", "") or ",".join(tags))
        tags = [t.strip() for t in tags_csv.split(",") if t.strip()]
        if ai_data.get("ai_hashtags"):
            hashtags = [str(h).strip().lstrip("#") for h in ai_data.get("ai_hashtags", []) if str(h).strip()]
        else:
            hashtags_csv = str(ai_data.get("ai_hashtags_csv", "") or "")
            hashtags = [h.strip().lstrip("#") for h in hashtags_csv.split(",") if h.strip()]
        category = str(ai_data.get("category", "") or category).strip()
        if title and description and tags:
            return {
                "title": title[:100],
                "description": description[:5000],
                "tags": tags[:15],
                "hashtags": hashtags[:15],
                "category": category or "People & Blogs",
                "source": "gemini",
            }
    except Exception as e:
        logger.warning("Gemini metadata generation failed for row %s: %s", row_data.get("row_id"), e)

    return _fallback_upload_metadata(row_data)


def _youtube_category_id(category_name: str) -> str:
    if not category_name:
        return "22"
    return _YT_CATEGORY_MAP.get(category_name, _YT_CATEGORY_MAP.get(category_name.title(), "22"))


def _slot_times_for_day(day_local: date) -> list[datetime]:
    """Return list of slot datetimes in UTC for a given local day."""
    tz = ZoneInfo(scheduler_config.DISPLAY_TIMEZONE)
    slots = []
    for hour, minute in scheduler_config.UPLOAD_SLOTS_LOCAL:
        dt_local = datetime(day_local.year, day_local.month, day_local.day, hour, minute, tzinfo=tz)
        slots.append(dt_local.astimezone(timezone.utc))
    return slots


def _next_slot_time(uploads_today: int) -> datetime:
    """Given uploads_today, return the UTC datetime of the required slot (today or tomorrow)."""
    now_local = datetime.now(ZoneInfo(scheduler_config.DISPLAY_TIMEZONE))
    today_slots = _slot_times_for_day(now_local.date())
    max_slots_today = min(len(today_slots), scheduler_config.UPLOADS_PER_DAY_PER_DEST)

    if uploads_today < max_slots_today:
        return today_slots[uploads_today]

    # Move to tomorrow's first slot
    tomorrow = now_local.date() + timedelta(days=1)
    tomorrow_slots = _slot_times_for_day(tomorrow)
    if not tomorrow_slots:
        # Fallback: 24h later
        return datetime.now(timezone.utc) + timedelta(hours=24)
    return tomorrow_slots[0]


# ── Core scheduler logic ─────────────────────────────────────────

def poll_and_enqueue(sheets=None):
    """
    Scan all source tabs for READY_TO_UPLOAD rows and add them to the queue.
    Respects per-destination daily caps.
    """
    sheets = sheets or sheet_manager.get_service()
    ready_rows = sheet_manager.read_ready_rows(sheets)

    enqueued = 0
    skipped_cap = 0
    skipped_flag = 0
    skipped_schedule = 0
    skipped_attempts = 0
    now_utc = datetime.now(timezone.utc)

    # Load dynamic tab-level mappings from Sheet
    global_mappings_list = sheet_manager.get_destination_mappings(sheets)
    global_map = {m["source_tag"]: m["destination_account_id"] for m in global_mappings_list}

    for row in ready_rows:
        row_id = int(row.get("row_id", 0) or 0)
        tab_name = row.get("_tab_name", "")
        sheet_row = row.get("_sheet_row", 0)
        dest = row.get("dest_mapping_tags", "").strip()
        manual_flag = row.get("manual_flag", "").strip()
        priority = 0

        # Safety check: flagged_for_review → skip
        if manual_flag == "review":
            logger.debug("Skipping row %d — flagged for review", row_id)
            skipped_flag += 1
            continue

        try:
            attempts = int(row.get("upload_attempts", 0) or 0)
        except (ValueError, TypeError):
            attempts = 0
        if attempts >= scheduler_config.MAX_UPLOAD_ATTEMPTS:
            logger.debug("Skipping row %d — max attempts reached (%d)", row_id, attempts)
            skipped_attempts += 1
            continue

        scheduled_at = _extract_scheduled_at_utc(row)
        if scheduled_at and scheduled_at > now_utc:
            logger.debug("Skipping row %d — scheduled for future at %s", row_id, scheduled_at.isoformat())
            skipped_schedule += 1
            continue

        # S8: Mapping priority — row-level (col W) > tab-level dynamic > static config fallback
        # Row-level mapping is already in `dest` from sheet.

        # 2. Dynamic Tab Mapping
        if not dest and tab_name in global_map:
            dest = global_map[tab_name]
            logger.debug("Using global mapping for %s → %s", tab_name, dest)

        # 3. Static Config Fallback
        if not dest and tab_name in scheduler_config.STATIC_MAPPINGS:
            dest = scheduler_config.STATIC_MAPPINGS[tab_name]
            logger.debug("Using static mapping for %s → %s", tab_name, dest)

        # Parse priority_score from AI output if present
        try:
            priority = int(row.get("priority_score", 0) or 0)
        except (ValueError, TypeError):
            priority = 0

        # Check destination cap
        if dest:
            today_count = queue_db.get_uploads_today(dest)
            if today_count >= scheduler_config.UPLOADS_PER_DAY_PER_DEST:
                logger.debug(
                    "Skipping row %d — dest %s at daily cap (%d/%d)",
                    row_id, dest, today_count, scheduler_config.UPLOADS_PER_DAY_PER_DEST,
                )
                sheet_manager.append_audit_note(
                    tab_name, sheet_row, f"scheduler: quota_reached_today for {dest}", sheets,
                )
                skipped_cap += 1
                continue

        # Enqueue
        added = queue_db.enqueue(
            source_tab=tab_name,
            sheet_row=sheet_row,
            row_id=row_id,
            priority_score=priority,
            scraped_date=row.get("scraped_date_utc", ""),
            dest_account_id=dest,
        )
        if added:
            enqueued += 1

    logger.info(
        "Poll: %d ready rows, %d enqueued, %d capped, %d flagged, %d scheduled, %d max-attempt.",
        len(ready_rows), enqueued, skipped_cap, skipped_flag, skipped_schedule, skipped_attempts,
    )
    log_event("poll_complete", ready=len(ready_rows), enqueued=enqueued,
              skipped_cap=skipped_cap, skipped_flag=skipped_flag,
              skipped_schedule=skipped_schedule, skipped_attempts=skipped_attempts)
    return enqueued


def process_upload_job(job: dict) -> dict:
    """
    Process a single upload job:
    1. Lock row in sheet
    2. Download media
    3. Verify hash
    4. Apply ffmpeg transform
    5. Upload to platform
    6. Update sheet

    Returns result dict.
    """
    queue_id = job["id"]
    tab_name = job["source_tab"]
    sheet_row = job["sheet_row"]
    row_id = job["row_id"]
    dest_account_id = job["dest_account_id"]

    result = {"queue_id": queue_id, "status": "error", "error": ""}

    logger.info("Processing job %d: row %d in %s → %s", queue_id, row_id, tab_name, dest_account_id)
    log_event("job_start", queue_id=queue_id, row_id=row_id, tab=tab_name, dest=dest_account_id)

    sheets = sheet_manager.get_service()

    # Step 1: Lock row (mark IN_PROGRESS)
    queue_db.mark_in_progress(queue_id)
    try:
        sheet_manager.update_row_status(
            tab_name, sheet_row, "IN_PROGRESS", sheets=sheets, expected_status="READY_TO_UPLOAD"
        )
    except ValueError as e:
        error = f"status_conflict: {e}"
        queue_db.mark_failed(queue_id, error, max_retries=0)
        result["error"] = error
        return result
    sheet_manager.append_audit_note(tab_name, sheet_row,
                                    f"scheduler: upload started (dest={dest_account_id})", sheets)

    # Step 2: Read full row data
    row_data = sheet_manager.read_row(tab_name, sheet_row, sheets)
    if not row_data:
        error = "row_not_found"
        queue_db.mark_failed(queue_id, error)
        sheet_manager.mark_upload_error(tab_name, sheet_row, error, retryable=False, sheets=sheets)
        result["error"] = error
        return result

    source_url = row_data.get("source_url", "").strip()
    expected_hash = row_data.get("content_hash", "").strip()

    if not source_url:
        error = "no_source_url"
        queue_db.mark_failed(queue_id, error)
        sheet_manager.mark_upload_error(tab_name, sheet_row, error, retryable=False, sheets=sheets)
        result["error"] = error
        return result

    if not dest_account_id:
        error = "no_destination_mapped"
        queue_db.mark_failed(queue_id, error)
        sheet_manager.mark_upload_error(tab_name, sheet_row, error, retryable=False, sheets=sheets)
        result["error"] = error
        return result

    try:
        attempts = int(row_data.get("upload_attempts", 0) or 0)
    except (ValueError, TypeError):
        attempts = 0
    if attempts >= scheduler_config.MAX_UPLOAD_ATTEMPTS:
        error = f"max_attempts_reached:{attempts}"
        queue_db.mark_failed(queue_id, error, max_retries=0)
        sheet_manager.update_row_status(tab_name, sheet_row, "ERROR", {
            "error_log": error,
        }, sheets=sheets)
        result["error"] = error
        return result

    # S1: Idempotency check — skip if this content+dest was already uploaded
    idem_key = hashlib.sha256(f"{expected_hash}:{dest_account_id}".encode()).hexdigest()
    if expected_hash and queue_db.check_idempotency(idem_key):
        logger.info("Idempotency hit: row %d already uploaded to %s, skipping", row_id, dest_account_id)
        queue_db.mark_completed(queue_id)
        sheet_manager.update_row_status(tab_name, sheet_row, "SKIPPED_DUPLICATE", {
            "notes": f"idempotency: already uploaded to {dest_account_id}",
        }, sheets)
        result["status"] = "skipped_idempotency"
        return result

    # Step 3: Check for same-destination duplicate
    try:
        recent_hashes = sheet_manager.get_uploaded_hashes_for_dest(dest_account_id, days=30, sheets=sheets)
        if expected_hash and expected_hash in recent_hashes:
            error = "duplicate_for_destination"
            queue_db.mark_failed(queue_id, error)
            sheet_manager.update_row_status(tab_name, sheet_row, "SKIPPED_DUPLICATE", {
                "notes": f"already uploaded to {dest_account_id} in past 30 days",
            }, sheets)
            result["error"] = error
            return result
    except Exception as e:
        logger.warning("Duplicate check failed: %s", e)

    # Step 4: Check upload spacing
    last_upload = queue_db.get_last_upload_time(dest_account_id)
    if last_upload:
        try:
            last_dt = datetime.fromisoformat(last_upload)
            elapsed = (datetime.now(timezone.utc) - last_dt).total_seconds()
            if elapsed < scheduler_config.UPLOAD_SPACING_SECONDS:
                wait = scheduler_config.UPLOAD_SPACING_SECONDS - elapsed
                logger.info("Spacing: waiting %.0fs before next upload to %s", wait, dest_account_id)
                time.sleep(min(wait, 300))  # Cap at 5 min
        except (ValueError, TypeError):
            pass
    # Step 4b: Enforce fixed daily slots & cap
    uploads_today = queue_db.get_uploads_today(dest_account_id)
    slot_time = _next_slot_time(uploads_today)
    now_utc = datetime.now(timezone.utc)
    if now_utc < slot_time:
        queue_db.requeue_at(queue_id, slot_time.isoformat())
        try:
            sheet_manager.append_audit_note(
                tab_name, sheet_row,
                f"scheduler: waiting for slot {slot_time.astimezone(ZoneInfo(scheduler_config.DISPLAY_TIMEZONE)).strftime('%H:%M %Z')}",
                sheets,
            )
        except Exception:
            pass
        result["status"] = "deferred_slot_wait"
        result["slot_time"] = slot_time.isoformat()
        return result
    if uploads_today >= scheduler_config.UPLOADS_PER_DAY_PER_DEST:
        tomorrow_slot = _next_slot_time(uploads_today)
        queue_db.requeue_at(queue_id, tomorrow_slot.isoformat())
        result["status"] = "deferred_slot_cap"
        result["slot_time"] = tomorrow_slot.isoformat()
        return result

    # Step 5: Download media
    dl_result = download_manager.download_video(source_url, expected_hash)
    if not dl_result["success"]:
        error = f"download_failed: {dl_result['error']}"
        queue_db.mark_failed(queue_id, error, max_retries=scheduler_config.MAX_UPLOAD_ATTEMPTS)
        retryable = dl_result["error"] not in ("yt-dlp not installed",)
        sheet_manager.mark_upload_error(tab_name, sheet_row, error, retryable=retryable, sheets=sheets)
        result["error"] = error
        return result

    video_path = dl_result["path"]
    actual_hash = dl_result.get("content_hash", "")

    # Update hash in sheet if different
    if actual_hash and actual_hash != expected_hash:
        sheet_manager.update_row_status(tab_name, sheet_row, "IN_PROGRESS", {
            "notes": f"hash updated: {actual_hash[:16]}",
        }, sheets)

    # Step 6: FFmpeg transform
    suggested_cmd = row_data.get("suggested_ffmpeg_cmd", "").strip()

    # Determine platform for validation
    account = oauth_helper.get_account(dest_account_id)
    platform = account.get("platform", "youtube") if account else "youtube"

    ff_result = ffmpeg_worker.transform_video(
        video_path, suggested_cmd,
        max_duration=scheduler_config.MAX_SHORTS_DURATION if platform == "youtube" else 90,
        dest_account_id=dest_account_id,
    )

    if not ff_result["success"]:
        error = f"ffmpeg_failed: {ff_result['error']}"
        download_manager.cleanup_file(video_path)
        queue_db.mark_failed(queue_id, error, max_retries=scheduler_config.MAX_UPLOAD_ATTEMPTS)
        sheet_manager.mark_upload_error(tab_name, sheet_row, error, retryable=True, sheets=sheets)
        result["error"] = error
        return result

    upload_file = ff_result["output_path"]

    # Step 7: Validate for platform
    validation = ffmpeg_worker.validate_for_upload(upload_file, platform)
    if not validation["valid"]:
        error = f"validation_failed: {validation['error']}"
        download_manager.cleanup_file(video_path)
        download_manager.cleanup_file(upload_file)
        queue_db.mark_failed(queue_id, error, max_retries=scheduler_config.MAX_UPLOAD_ATTEMPTS)
        sheet_manager.mark_upload_error(tab_name, sheet_row, error, retryable=False, sheets=sheets)
        result["error"] = error
        return result

    if validation["warnings"]:
        logger.warning("Upload warnings for row %d: %s", row_id, validation["warnings"])

    # Step 8: Upload
    optimized = _resolve_upload_metadata(row_data)
    ai_title = optimized["title"]
    ai_description = optimized["description"]
    ai_tags = optimized["tags"]
    ai_hashtags = optimized["hashtags"]
    yt_category_id = _youtube_category_id(optimized.get("category", ""))

    # Persist generated metadata where matching columns exist.
    try:
        sheet_manager.update_row_status(tab_name, sheet_row, "IN_PROGRESS", {
            "ai_title": ai_title,
            "ai_description": ai_description,
            "ai_tags": ",".join(ai_tags[:15]),
            "ai_hashtags": ",".join(ai_hashtags[:15]),
            "category": optimized.get("category", ""),
        }, sheets=sheets)
    except Exception:
        pass

    if optimized.get("source") != "sheet":
        sheet_manager.append_audit_note(
            tab_name, sheet_row, f"scheduler: metadata source={optimized.get('source')}", sheets
        )

    upload_adapter = uploader.get_uploader(dest_account_id)
    if not upload_adapter:
        error = f"no_uploader_for_{dest_account_id}"
        download_manager.cleanup_file(video_path)
        download_manager.cleanup_file(upload_file)
        queue_db.mark_failed(queue_id, error)
        sheet_manager.mark_upload_error(tab_name, sheet_row, error, retryable=False, sheets=sheets)
        result["error"] = error
        return result

    # S4: YouTube quota guard — skip if 80% of daily quota reached
    if platform == "youtube":
        project = queue_db.get_cheapest_project()
        if project is None:
            error = "youtube_quota_exhausted"
            logger.warning("YouTube quota exhausted for all projects, skipping upload")
            download_manager.cleanup_file(video_path)
            download_manager.cleanup_file(upload_file)
            queue_db.mark_failed(queue_id, error, max_retries=0)
            sheet_manager.mark_upload_error(tab_name, sheet_row, error, retryable=True, sheets=sheets)
            result["error"] = error
            return result

    upload_result = upload_adapter.upload(
        video_path=upload_file,
        title=ai_title,
        description=ai_description,
        tags=ai_tags,
        hashtags=ai_hashtags,
        category_id=yt_category_id,
    )

    # Step 9: Handle result
    download_manager.cleanup_file(video_path)
    download_manager.cleanup_file(upload_file)

    if upload_result.success:
        queue_db.mark_completed(queue_id)
        queue_db.record_upload(dest_account_id, row_id)
        # S1: Record idempotency key after successful upload
        queue_db.record_idempotency(idem_key, queue_id)
        # S4: Record quota usage for YouTube
        if platform == "youtube":
            queue_db.record_quota_usage(
                project if project else "default",
                scheduler_config.YT_QUOTA_UNITS_PER_UPLOAD,
            )
        sheet_manager.mark_uploaded(
            tab_name, sheet_row, upload_result.uploaded_url,
            platform, dest_account_id, sheets,
        )
        log_event("upload_success", queue_id=queue_id, row_id=row_id,
                   url=upload_result.uploaded_url, dest=dest_account_id)
        result["status"] = "uploaded"
        result["uploaded_url"] = upload_result.uploaded_url
    else:
        if not upload_result.retryable:
            # Non-retryable: permission denied, blocked, etc.
            queue_db.mark_failed(queue_id, upload_result.error, max_retries=0)
            sheet_manager.mark_upload_error(
                tab_name, sheet_row, upload_result.error,
                retryable=False, sheets=sheets,
            )
            # Notify admin if blocked/permission issue
            if "blocked" in upload_result.error.lower() or "permission" in upload_result.error.lower():
                sheet_manager.update_row_status(tab_name, sheet_row, "ERROR", {
                    "manual_flag": "review",
                }, sheets)
                logger.error("ADMIN ALERT: Upload blocked for %s — %s", dest_account_id, upload_result.error)
        else:
            queue_db.mark_failed(queue_id, upload_result.error,
                                 max_retries=scheduler_config.MAX_UPLOAD_ATTEMPTS)
            sheet_manager.mark_upload_error(
                tab_name, sheet_row, upload_result.error,
                retryable=True, sheets=sheets,
            )
        log_event("upload_error", queue_id=queue_id, row_id=row_id,
                   error=upload_result.error, error_type=upload_result.error_type,
                   dest=dest_account_id)
        result["error"] = upload_result.error
        result["error_type"] = upload_result.error_type

    return result


def run_workers():
    """Pull jobs from queue and process them with a thread pool."""
    jobs = queue_db.get_next_jobs(limit=scheduler_config.MAX_CONCURRENT_WORKERS)
    if not jobs:
        return 0

    logger.info("Processing %d upload jobs...", len(jobs))
    results = []

    with ThreadPoolExecutor(max_workers=scheduler_config.MAX_CONCURRENT_WORKERS) as executor:
        futures = {executor.submit(process_upload_job, job): job for job in jobs}
        for future in as_completed(futures):
            try:
                result = future.result()
                results.append(result)
            except Exception as e:
                job = futures[future]
                logger.error("Worker exception for job %d: %s", job["id"], e)
                queue_db.mark_failed(job["id"], str(e))

    uploaded = sum(1 for r in results if r.get("status") == "uploaded")
    errors = sum(1 for r in results if r.get("status") == "error")
    logger.info("Workers complete: %d uploaded, %d errors.", uploaded, errors)
    return len(results)


def reconcile(sheets=None):
    """
    Startup reconciliation:
    - Reset stale IN_PROGRESS rows
    - Clean old temp files and logs
    - Refresh tokens
    - Check sheet health (#5)
    """
    logger.info("Running reconciliation...")
    stale = queue_db.reset_stale_jobs(scheduler_config.STALE_IN_PROGRESS_HOURS)
    queue_db.cleanup_old_records(days=90)
    download_manager.cleanup_old_temp_files()

    # Gap #8: Clean old log files
    _cleanup_old_logs()

    # Gap #5: Check sheet health
    try:
        import sheet_archiver
        health = sheet_archiver.check_sheet_health()
        if health["status"] in ("WARNING", "ALARM"):
            log_event("sheet_health_alert", status=health["status"],
                      total_cells=health["total_cells"])
    except Exception as e:
        logger.warning("Sheet health check failed: %s", e)

    # Refresh tokens proactively
    try:
        oauth_helper.refresh_all_tokens()
    except Exception as e:
        logger.error("Token refresh error: %s", e)

    logger.info("Reconciliation done (reset %d stale jobs).", stale)
    log_event("reconciliation", stale_reset=stale)


def _cleanup_old_logs():
    """Delete log files older than LOG_RETENTION_DAYS."""
    from pathlib import Path
    from datetime import timedelta
    import os
    cutoff = (datetime.now(timezone.utc) - timedelta(days=scheduler_config.LOG_RETENTION_DAYS)).timestamp()
    log_dir = scheduler_config.LOG_DIR
    cleaned = 0
    for f in log_dir.iterdir():
        if f.is_file() and f.suffix in (".log", ".jsonl"):
            if f.stat().st_mtime < cutoff:
                f.unlink()
                cleaned += 1
    if cleaned:
        logger.info("Cleaned %d old log files.", cleaned)


# ── Graceful Shutdown (S6) ────────────────────────────────────────

_shutdown_event = threading.Event()


def _handle_shutdown(signum, frame):
    """Handle SIGTERM/SIGINT — signal workers to finish and exit."""
    sig_name = signal.Signals(signum).name
    logger.info("Received %s — initiating graceful shutdown...", sig_name)
    log_event("shutdown_signal", signal=sig_name)
    _shutdown_event.set()


# ── Main loop ─────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Gravix Scheduler — Upload orchestrator")
    parser.add_argument("--once", action="store_true", help="Single poll cycle then exit")
    parser.add_argument("--reconcile", action="store_true", help="Run reconciliation only")
    parser.add_argument("--stats", action="store_true", help="Show queue stats")
    args = parser.parse_args()

    logger.info("Gravix Scheduler starting (instance: %s)", scheduler_config.INSTANCE_ID)

    # S6: Register signal handlers for graceful shutdown
    signal.signal(signal.SIGTERM, _handle_shutdown)
    signal.signal(signal.SIGINT, _handle_shutdown)

    # Init queue DB
    queue_db.init_db()

    if args.stats:
        stats = queue_db.get_queue_stats()
        print(json.dumps(stats, indent=2))
        return

    if args.reconcile:
        reconcile()
        return

    # Startup reconciliation
    reconcile()

    if args.once:
        # Single cycle
        enqueued = poll_and_enqueue()
        if enqueued:
            run_workers()
        return

    # ── Continuous polling loop ─────────────────────────────────────
    logger.info("Starting polling loop (interval: %ds)...", scheduler_config.POLL_INTERVAL_SECONDS)
    log_event("scheduler_start")

    while not _shutdown_event.is_set():
        try:
            # Poll and enqueue
            enqueued = poll_and_enqueue()

            # Process queued jobs (skip if shutting down)
            if not _shutdown_event.is_set():
                if enqueued or queue_db.get_queue_stats().get("queued", 0) > 0:
                    run_workers()

            # Sleep until next poll (interruptible by shutdown)
            _shutdown_event.wait(timeout=scheduler_config.POLL_INTERVAL_SECONDS)

        except Exception as e:
            logger.error("Poll cycle error: %s", e)
            log_event("poll_error", error=str(e))
            _shutdown_event.wait(timeout=30)  # Back off on errors

    logger.info("Scheduler shut down gracefully.")
    log_event("scheduler_stop")


if __name__ == "__main__":
    main()
