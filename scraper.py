#!/usr/bin/env python3
"""
scraper.py — Main scraper & ingestion orchestrator.
Fetches video metadata from YouTube/Instagram sources, deduplicates,
and writes normalized rows to per-source tabs in the master Google Sheet.

Usage:
    python scraper.py --batch                    # Process all sources
    python scraper.py --source source__my_tab    # Process one source
    python scraper.py --dry-run --limit 3        # Dry run, max 3 per source
"""

import argparse
import json
import logging
import os
import re
import shutil
import subprocess
import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any

import yaml

import scraper_config
import scraper_sheets
import hash_utils

# ── Logging ───────────────────────────────────────────────────────
log_filename = f"scraper_{datetime.now(timezone.utc).strftime('%Y%m%d')}.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(scraper_config.LOG_DIR / log_filename, encoding="utf-8"),
    ],
)
logger = logging.getLogger("scraper")

# JSON-structured logger for structured log lines
json_logger = logging.getLogger("scraper.json")
json_handler = logging.FileHandler(
    scraper_config.LOG_DIR / log_filename.replace(".log", "_structured.jsonl"),
    encoding="utf-8",
)
json_handler.setFormatter(logging.Formatter("%(message)s"))
json_logger.addHandler(json_handler)
json_logger.setLevel(logging.INFO)


def log_event(event_type: str, **kwargs):
    """Write a structured JSON log entry."""
    entry = {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "event": event_type,
        "instance_id": scraper_config.INSTANCE_ID,
        **kwargs,
    }
    json_logger.info(json.dumps(entry, ensure_ascii=False))


def _status_path(tab_name: str) -> Path:
    return scraper_config.SCRAPE_STATUS_DIR / f"{tab_name}.json"


def _lock_path(tab_name: str) -> Path:
    return scraper_config.SCRAPE_STATUS_DIR / f"{tab_name}.lock"


def _write_scrape_status(tab_name: str, data: dict) -> None:
    data["updated_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    path = _status_path(tab_name)
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
    tmp.replace(path)


def _acquire_scrape_lock(tab_name: str) -> bool:
    lock = _lock_path(tab_name)
    if lock.exists():
        age = time.time() - lock.stat().st_mtime
        if age < scraper_config.SCRAPE_LOCK_TIMEOUT_MINUTES * 60:
            return False
        try:
            lock.unlink()
        except OSError:
            return False
    lock.write_text(str(os.getpid()), encoding="utf-8")
    return True


def _release_scrape_lock(tab_name: str) -> None:
    lock = _lock_path(tab_name)
    if lock.exists():
        try:
            lock.unlink()
        except OSError:
            pass


def _parse_utc(value: str) -> datetime | None:
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


# ── Scrapingdog key rotation ─────────────────────────────────────

class KeyRotator:
    """Round-robin Scrapingdog key rotation with per-key health monitoring (#6)."""

    def __init__(self, keys: list[str]):
        self.keys = keys if keys else [""]
        self._index = 0
        self._last_used: dict[str, float] = {}
        self._min_interval = 60.0 / scraper_config.SCRAPINGDOG_RATE_LIMIT_PER_MIN
        # Gap #6: Per-key health tracking
        self._request_log: dict[str, list[bool]] = {k: [] for k in self.keys}
        self._disabled_keys: set[str] = set()

    def next_key(self) -> str:
        """Get the next available key, respecting rate limits and health."""
        if not self.keys or self.keys == [""]:
            return ""

        # Try to find a healthy key
        attempts = 0
        while attempts < len(self.keys):
            key = self.keys[self._index % len(self.keys)]
            self._index += 1
            attempts += 1

            # Skip disabled keys
            if key in self._disabled_keys:
                continue

            now = time.time()
            last = self._last_used.get(key, 0)
            wait = self._min_interval - (now - last)
            if wait > 0:
                time.sleep(wait)
            self._last_used[key] = time.time()
            return key

        # All keys disabled — log alarm and try the least-bad one
        if self._disabled_keys:
            logger.error(
                "ADMIN_ALERT: All %d Scrapingdog keys disabled (error rate > %d%%). "
                "Re-enabling least-errored key as fallback.",
                len(self._disabled_keys),
                int(scraper_config.SCRAPINGDOG_ERROR_THRESHOLD * 100),
            )
            log_event("all_keys_disabled", disabled=list(self._disabled_keys))
            # Re-enable the key with the lowest error rate
            best_key = min(self.keys, key=lambda k: self._error_rate(k))
            self._disabled_keys.discard(best_key)
            return best_key

        return self.keys[0]

    def report_result(self, key: str, success: bool):
        """Record success/failure for a key. Auto-disable if threshold exceeded."""
        if key not in self._request_log:
            self._request_log[key] = []
        window = scraper_config.SCRAPINGDOG_WINDOW_SIZE
        self._request_log[key].append(success)
        # Keep only last N entries
        if len(self._request_log[key]) > window:
            self._request_log[key] = self._request_log[key][-window:]

        # Check error rate after minimum sample size
        if len(self._request_log[key]) >= 10:
            rate = self._error_rate(key)
            if rate > scraper_config.SCRAPINGDOG_ERROR_THRESHOLD:
                if key not in self._disabled_keys:
                    self._disabled_keys.add(key)
                    logger.warning(
                        "Scrapingdog key %s..%s DISABLED (error rate %.1f%% > %.1f%% threshold)",
                        key[:4], key[-4:], rate * 100,
                        scraper_config.SCRAPINGDOG_ERROR_THRESHOLD * 100,
                    )
                    log_event("key_disabled", key_prefix=key[:4], error_rate=round(rate, 3))

    def _error_rate(self, key: str) -> float:
        """Error rate for a key [0.0 .. 1.0]."""
        log = self._request_log.get(key, [])
        if not log:
            return 0.0
        failures = sum(1 for ok in log if not ok)
        return failures / len(log)


# ── Source fetching strategies ────────────────────────────────────

def _fetch_youtube_metadata(source_id: str, max_results: int | None = 50) -> list[dict]:
    """
    Fetch video metadata from a YouTube channel using yt-dlp.
    Falls back to YouTube Data API if YT_API_KEY is set.
    Returns list of video metadata dicts.
    """
    videos = []

    # Strategy 1: YouTube Data API (preferred for metadata)
    if scraper_config.YT_API_KEY:
        try:
            videos = _fetch_youtube_data_api(source_id, max_results)
            if videos:
                return videos
        except Exception as e:
            logger.warning("YouTube Data API failed for %s: %s. Falling back to yt-dlp.", source_id, e)

    # Strategy 2: yt-dlp (fallback)
    try:
        videos = _fetch_youtube_ytdlp(source_id, max_results)
    except Exception as e:
        logger.error("yt-dlp also failed for %s: %s", source_id, e)
        log_event("fetch_error", source_id=source_id, error=str(e))

    return videos


def _fetch_youtube_data_api(source_id: str, max_results: int | None) -> list[dict]:
    """Fetch videos via YouTube Data API v3 with pagination."""
    import requests

    # Step 1: Get uploads playlist
    url = "https://www.googleapis.com/youtube/v3/channels"
    params = {
        "part": "contentDetails",
        "id": source_id,
        "key": scraper_config.YT_API_KEY,
    }
    resp = requests.get(url, params=params, timeout=15)
    resp.raise_for_status()
    data = resp.json()

    items = data.get("items", [])
    if not items:
        # Try as username
        params["forUsername"] = source_id
        del params["id"]
        resp = requests.get(url, params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        items = data.get("items", [])
        if not items:
            logger.warning("No channel found for: %s", source_id)
            return []

    uploads_playlist = items[0]["contentDetails"]["relatedPlaylists"]["uploads"]

    # Step 2: Get all video IDs (pages)
    video_ids = []
    videos_url = "https://www.googleapis.com/youtube/v3/playlistItems"
    next_page_token = None
    
    while True:
        if max_results is None:
            fetch_limit = 50
        else:
            remaining = max_results - len(video_ids)
            if remaining <= 0:
                break
            fetch_limit = min(50, remaining)
        params = {
            "part": "contentDetails",
            "playlistId": uploads_playlist,
            "maxResults": fetch_limit,
            "key": scraper_config.YT_API_KEY,
        }
        if next_page_token:
            params["pageToken"] = next_page_token

        resp = requests.get(videos_url, params=params, timeout=15)
        resp.raise_for_status()
        playlist_data = resp.json()

        for item in playlist_data.get("items", []):
            video_ids.append(item["contentDetails"]["videoId"])

        next_page_token = playlist_data.get("nextPageToken")
        if not next_page_token or not playlist_data.get("items"):
            break
        if max_results is not None and len(video_ids) >= max_results:
            break

    if not video_ids:
        return []

    # Step 3: Get video details in batches of 50
    details_url = "https://www.googleapis.com/youtube/v3/videos"
    videos = []
    
    # Chunk into 50s
    for i in range(0, len(video_ids), 50):
        chunk = video_ids[i:i+50]
        params = {
            "part": "snippet,contentDetails,statistics",
            "id": ",".join(chunk),
            "key": scraper_config.YT_API_KEY,
        }
        try:
            resp = requests.get(details_url, params=params, timeout=15)
            resp.raise_for_status()
            details = resp.json()

            for item in details.get("items", []):
                duration_iso = item["contentDetails"].get("duration", "PT0S")
                duration_sec = _parse_iso_duration(duration_iso)
                # Only include shorts (<=120s)
                if duration_sec > 120:
                    continue
                snippet = item["snippet"]
                stats = item.get("statistics", {})
                videos.append({
                    "source_url": f"https://youtube.com/shorts/{item['id']}",
                    "original_title": snippet.get("title", ""),
                    "duration_seconds": duration_sec,
                    "published_at_utc": snippet.get("publishedAt", ""),
                    "view_count": int(stats.get("viewCount", 0)),
                    "thumbnail_url": snippet.get("thumbnails", {}).get("high", {}).get("url", ""),
                    "tags_from_source": ",".join(snippet.get("tags", [])[:15]),
                    "language_hint": snippet.get("defaultLanguage", "unknown"),
                })
        except Exception as e:
            logger.error("Failed to fetch details for chunk %d: %s", i, e)

    return videos


def _fetch_youtube_ytdlp(source_id: str, max_results: int | None) -> list[dict]:
    """Fetch video list via yt-dlp (no download, metadata only)."""
    channel_url = f"https://www.youtube.com/channel/{source_id}/shorts"
    ytdlp_bin = _resolve_ytdlp_bin()
    if not ytdlp_bin:
        logger.error("yt-dlp not installed. Install with: pip install yt-dlp")
        return []
    cmd = [
        ytdlp_bin,
        "--flat-playlist",
        "--no-playlist" if "/shorts/" not in channel_url else "--yes-playlist",
        "--dump-json",
        "--no-warnings",
        channel_url,
    ]
    if max_results is not None:
        cmd.insert(-1, "--playlist-end")
        cmd.insert(-1, str(max_results))
    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=120,
        )
    except FileNotFoundError:
        logger.error("yt-dlp not installed. Install with: pip install yt-dlp")
        return []
    except subprocess.TimeoutExpired:
        logger.error("yt-dlp timed out for %s", source_id)
        return []

    if result.returncode != 0:
        logger.warning("yt-dlp non-zero exit for %s: %s", source_id, result.stderr[:200])

    videos = []
    for line in result.stdout.strip().split("\n"):
        if not line.strip():
            continue
        try:
            item = json.loads(line)
        except json.JSONDecodeError:
            continue
        vid_id = item.get("id", "")
        duration = int(item.get("duration", 0) or 0)
        if duration > 120:
            continue
        videos.append({
            "source_url": f"https://youtube.com/shorts/{vid_id}",
            "original_title": item.get("title", ""),
            "duration_seconds": duration,
            "published_at_utc": item.get("upload_date", ""),
            "view_count": int(item.get("view_count", 0) or 0),
            "thumbnail_url": item.get("thumbnail", ""),
            "tags_from_source": ",".join((item.get("tags") or [])[:15]),
            "language_hint": item.get("language", "unknown") or "unknown",
        })
    return videos


def _fetch_instagram_metadata(source_id: str, max_results: int | None, key_rotator: KeyRotator) -> list[dict]:
    """
    Fetch video metadata from an Instagram page via Scrapingdog.
    Falls back to basic HTML parsing if no Scrapingdog key.
    """
    import requests

    key = key_rotator.next_key()
    if not key:
        logger.warning("No Scrapingdog key available for Instagram source %s", source_id)
        return []

    profile_url = f"https://www.instagram.com/{source_id}/"
    api_url = "https://api.scrapingdog.com/scrape"
    params = {
        "api_key": key,
        "url": profile_url,
        "dynamic": "true",
    }

    videos = []
    for attempt in range(1, scraper_config.RETRY_MAX_ATTEMPTS + 1):
        try:
            resp = requests.get(api_url, params=params, timeout=30)
            if resp.status_code == 429:
                key_rotator.report_result(key, False)
                wait = scraper_config.RETRY_BACKOFF_BASE * (3 ** (attempt - 1))
                logger.warning("429 from Scrapingdog (attempt %d), backing off %.1fs", attempt, wait)
                time.sleep(wait)
                key = key_rotator.next_key()
                params["api_key"] = key
                continue
            if resp.status_code >= 500:
                key_rotator.report_result(key, False)
                wait = scraper_config.RETRY_BACKOFF_BASE * (3 ** (attempt - 1))
                logger.warning("5xx from Scrapingdog (attempt %d), backing off %.1fs", attempt, wait)
                time.sleep(wait)
                continue
            resp.raise_for_status()
            key_rotator.report_result(key, True)
            html = resp.text
            videos = _parse_instagram_html(html, source_id, max_results)
            break
        except Exception as e:
            key_rotator.report_result(key, False)
            logger.error("Instagram fetch attempt %d failed for %s: %s", attempt, source_id, e)
            if attempt == scraper_config.RETRY_MAX_ATTEMPTS:
                log_event("fetch_error", source_id=source_id, platform="instagram", error=str(e))

    return videos


def _parse_instagram_html(html: str, source_id: str, max_results: int | None) -> list[dict]:
    """Extract video metadata from Instagram HTML (best-effort parsing)."""
    videos = []
    # Look for video URLs in og:video or JSON-LD / shared data
    # Pattern for Instagram reel/video shortcodes
    shortcode_pattern = re.compile(r'/reel/([A-Za-z0-9_-]+)/?')
    matches = shortcode_pattern.findall(html)
    seen = set()

    iterable = matches if max_results is None else matches[:max_results]
    for code in iterable:
        if code in seen:
            continue
        seen.add(code)
        videos.append({
            "source_url": f"https://www.instagram.com/reel/{code}/",
            "original_title": f"Reel by {source_id}",
            "duration_seconds": 0,  # Unknown from HTML
            "published_at_utc": "",
            "view_count": 0,
            "thumbnail_url": "",
            "tags_from_source": "",
            "language_hint": "unknown",
        })

    if not videos:
        logger.info("No videos parsed from Instagram HTML for %s", source_id)

    return videos


# ── Download & hash ───────────────────────────────────────────────

def _resolve_ytdlp_bin() -> str | None:
    """Resolve yt-dlp binary from PATH or local venv."""
    found = shutil.which("yt-dlp")
    if found:
        return found
    venv_bin = Path(__file__).parent / "venv" / "bin" / "yt-dlp"
    if venv_bin.exists():
        return str(venv_bin)
    return None

def _check_disk_space() -> bool:
    """Return True if disk has enough free space."""
    total, _, free = shutil.disk_usage("/tmp")
    free_pct = (free / total) * 100
    if free_pct < scraper_config.DISK_MIN_FREE_PERCENT:
        logger.error(
            "INSTANCE_ALARM: Disk free space %.1f%% < %d%%. Stopping downloads.",
            free_pct, scraper_config.DISK_MIN_FREE_PERCENT,
        )
        log_event("disk_alarm", free_percent=round(free_pct, 1))
        return False
    return True


def _try_download(source_url: str) -> str | None:
    """
    Attempt to download video to temp dir using yt-dlp.
    Returns temp file path or None.
    """
    if not _check_disk_space():
        return None

    output_template = str(scraper_config.TEMP_DIR / "%(id)s.%(ext)s")
    ytdlp_bin = _resolve_ytdlp_bin()
    if not ytdlp_bin:
        logger.debug("yt-dlp not available for download")
        return None
    cmd = [
        ytdlp_bin,
        "--no-playlist",
        "--no-warnings",
        "-o", output_template,
        "--max-filesize", f"{scraper_config.MAX_FILE_SIZE_MB}M",
        source_url,
    ]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
        if result.returncode != 0:
            logger.warning("Download failed for %s: %s", source_url, result.stderr[:200])
            return None
    except FileNotFoundError:
        logger.debug("yt-dlp not available for download")
        return None
    except subprocess.TimeoutExpired:
        logger.warning("Download timed out for %s", source_url)
        return None

    # Find the downloaded file
    for f in scraper_config.TEMP_DIR.iterdir():
        if f.is_file() and f.stat().st_mtime > time.time() - 60:
            logger.info("Downloaded: %s (%.1f MB)", f.name, f.stat().st_size / 1e6)
            return str(f)
    return None


def _cleanup_temp_files():
    """Delete temp files older than TEMP_MAX_AGE_HOURS."""
    cutoff = datetime.now(timezone.utc) - timedelta(hours=scraper_config.TEMP_MAX_AGE_HOURS)
    cutoff_ts = cutoff.timestamp()
    cleaned = 0
    for f in scraper_config.TEMP_DIR.iterdir():
        if f.is_file() and f.stat().st_mtime < cutoff_ts:
            f.unlink()
            cleaned += 1
    if cleaned:
        logger.info("Cleaned up %d old temp files.", cleaned)


def _auto_tags_from_title(title: str) -> list[str]:
    """Extract auto-tags from title using keyword matching."""
    keywords = {
        "cricket": "cricket", "goal": "goal", "funny": "funny",
        "viral": "viral", "amazing": "amazing", "incredible": "incredible",
        "dog": "animals", "cat": "animals", "fail": "fail",
        "dance": "dance", "music": "music", "tech": "tech",
        "phone": "tech", "iphone": "tech", "android": "tech",
        "cooking": "cooking", "recipe": "cooking", "food": "food",
        "car": "automotive", "football": "football", "soccer": "soccer",
        "basketball": "basketball", "anime": "anime",
    }
    title_lower = title.lower()
    tags = set()
    for kw, tag in keywords.items():
        if kw in title_lower:
            tags.add(tag)
    return sorted(tags)


# ── ISO duration parsing ─────────────────────────────────────────

def _parse_iso_duration(duration: str) -> int:
    """Parse ISO 8601 duration (e.g., PT1M30S) to seconds."""
    match = re.match(r"PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?", duration)
    if not match:
        return 0
    hours = int(match.group(1) or 0)
    minutes = int(match.group(2) or 0)
    seconds = int(match.group(3) or 0)
    return hours * 3600 + minutes * 60 + seconds


# ── Main processing ──────────────────────────────────────────────

def process_source(
    source: dict,
    sheets,
    url_cache: set[str],
    hash_cache: set[str],
    key_rotator: KeyRotator,
    dry_run: bool = False,
    limit: int | None = None,
) -> dict[str, int]:
    """
    Process a single source: fetch videos, dedupe, insert rows.
    Returns stats dict with counts.
    """
    tab_name = source["source_tab"]
    source_type = source["source_type"]
    source_id = source["source_id"]
    if limit is not None:
        max_per_run = int(limit)
    else:
        raw_max = source.get("max_new_per_run", None)
        if raw_max in (None, "", 0, "0"):
            max_per_run = None
        else:
            try:
                max_per_run = int(raw_max)
            except (TypeError, ValueError):
                max_per_run = None
    rate_limit = source.get("rate_limit_seconds", scraper_config.PER_SOURCE_RATE_LIMIT_SECONDS)

    stats = {"fetched": 0, "inserted": 0, "skipped_duplicate": 0, "errors": 0}
    started_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    if not dry_run:
        if not _acquire_scrape_lock(tab_name):
            _write_scrape_status(tab_name, {
                "tab": tab_name,
                "state": "already_running",
                "started_at": started_at,
                "fetched": 0,
                "inserted": 0,
                "skipped_duplicate": 0,
                "errors": 0,
            })
            logger.info("Skip %s: scrape already running", tab_name)
            log_event("skip_running", tab=tab_name)
            return stats

        _write_scrape_status(tab_name, {
            "tab": tab_name,
            "state": "running",
            "started_at": started_at,
            "fetched": 0,
            "inserted": 0,
            "skipped_duplicate": 0,
            "errors": 0,
        })

    logger.info("Processing source: %s (%s / %s)", tab_name, source_type, source_id)
    log_event("source_start", tab=tab_name, source_type=source_type, source_id=source_id)

    try:
        # Ensure tab exists
        if not dry_run:
            scraper_sheets.ensure_tab_exists(tab_name, sheets)

        # Enforce per-source scrape interval to prevent re-scrape spam
        interval_min = int(source.get("scrape_interval_minutes", 0) or 0)
        if not dry_run and interval_min > 0:
            entry = scraper_sheets.get_master_index_entry(tab_name, sheets)
            last = _parse_utc(entry.get("last_scraped_at", "") if entry else "")
            if last:
                elapsed = (datetime.now(timezone.utc) - last).total_seconds()
                if elapsed < interval_min * 60:
                    next_allowed = (last + timedelta(minutes=interval_min)).strftime("%Y-%m-%dT%H:%M:%SZ")
                    _write_scrape_status(tab_name, {
                        "tab": tab_name,
                        "state": "cooldown",
                        "started_at": started_at,
                        "fetched": 0,
                        "inserted": 0,
                        "skipped_duplicate": 0,
                        "errors": 0,
                        "next_allowed": next_allowed,
                    })
                    logger.info("Skip %s: cooldown until %s", tab_name, next_allowed)
                    log_event("skip_cooldown", tab=tab_name, next_allowed=next_allowed)
                    return stats

        # Fetch videos
        if source_type == "youtube":
            videos = _fetch_youtube_metadata(source_id, max_per_run)
        elif source_type == "instagram":
            videos = _fetch_instagram_metadata(source_id, max_per_run, key_rotator)
        else:
            logger.error("Unknown source_type: %s", source_type)
            return stats

        stats["fetched"] = len(videos)
        logger.info("Fetched %d videos from %s", len(videos), tab_name)
        if not dry_run:
            _write_scrape_status(tab_name, {
                "tab": tab_name,
                "state": "running",
                "started_at": started_at,
                "fetched": stats["fetched"],
                "inserted": stats["inserted"],
                "skipped_duplicate": stats["skipped_duplicate"],
                "errors": stats["errors"],
            })

        if not videos:
            return stats

        # Get current tab URLs for dedupe (unless dry-run)
        if not dry_run:
            tab_urls = scraper_sheets.get_existing_urls(tab_name, sheets)
        else:
            tab_urls = url_cache

        # Get next row_id
        if not dry_run:
            next_id = scraper_sheets.get_next_row_id(tab_name, sheets)
        else:
            next_id = 1

        last_request_time = 0.0
        last_status_time = 0.0

        def maybe_status_update():
            nonlocal last_status_time
            if dry_run:
                return
            now = time.time()
            if now - last_status_time >= scraper_config.SCRAPE_STATUS_THROTTLE_SECONDS:
                _write_scrape_status(tab_name, {
                    "tab": tab_name,
                    "state": "running",
                    "started_at": started_at,
                    "fetched": stats["fetched"],
                    "inserted": stats["inserted"],
                    "skipped_duplicate": stats["skipped_duplicate"],
                    "errors": stats["errors"],
                })
                last_status_time = now

        iter_videos = videos if max_per_run is None else videos[:max_per_run]
        for video in iter_videos:
            # Rate limiting
            now = time.time()
            elapsed = now - last_request_time
            if elapsed < rate_limit:
                time.sleep(rate_limit - elapsed)
            last_request_time = time.time()

            # Step 1: Normalize URL and check URL dedupe
            normalized_url = hash_utils.normalize_url(video["source_url"])
            if normalized_url in tab_urls or normalized_url in url_cache:
                logger.debug("SKIPPED_DUPLICATE (URL): %s", normalized_url)
                log_event("skip_duplicate", reason="url", url=normalized_url)
                stats["skipped_duplicate"] += 1
                maybe_status_update()
                continue

            # Step 2: Attempt download + hashing
            content_hash = None
            hash_method = "metadata_hash"
            temp_path = None

            try:
                temp_path = _try_download(normalized_url)
            except Exception as e:
                logger.warning("Download error for %s: %s", normalized_url, e)

            if temp_path:
                try:
                    content_hash, hash_method = hash_utils.compute_file_hash(temp_path)
                except Exception as e:
                    logger.warning("Hash error for %s: %s", temp_path, e)
                    content_hash = None

            if not content_hash:
                # Fallback to metadata hash
                content_hash, hash_method = hash_utils.compute_metadata_hash(
                    video.get("original_title", ""),
                    video.get("duration_seconds", 0),
                    video.get("published_at_utc", ""),
                    normalized_url,
                )

            # Step 3: Cross-tab hash dedupe
            if content_hash in hash_cache:
                logger.debug("SKIPPED_DUPLICATE (hash): %s", normalized_url)
                log_event("skip_duplicate", reason="hash", url=normalized_url, hash=content_hash[:16])
                stats["skipped_duplicate"] += 1
                # Clean up temp file
                if temp_path and Path(temp_path).exists():
                    Path(temp_path).unlink()
                maybe_status_update()
                continue

            # Step 4: Build row
            now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            auto_tags = _auto_tags_from_title(video.get("original_title", ""))
            existing_tags = video.get("tags_from_source", "")
            if auto_tags:
                if existing_tags:
                    existing_tags += "," + ",".join(auto_tags)
                else:
                    existing_tags = ",".join(auto_tags)

            notes = ""
            if hash_method == "metadata_hash" and temp_path is None:
                notes = "download_failed, metadata_hash used"

            row_data = {
                "row_id": next_id,
                "scraped_date_utc": now_utc,
                "source_channel": source.get("source_id", ""),
                "source_channel_tab": tab_name,
                "source_url": normalized_url,
                "original_title": video.get("original_title", ""),
                "duration_seconds": video.get("duration_seconds", 0),
                "published_at_utc": video.get("published_at_utc", ""),
                "view_count": video.get("view_count", 0),
                "thumbnail_url": video.get("thumbnail_url", ""),
                "local_temp_path": temp_path or "",
                "content_hash": content_hash,
                "content_hash_method": hash_method,
                "scraped_by": scraper_config.INSTANCE_ID,
                "status": "PENDING",
                "upload_attempts": 0,
                "last_attempt_time_utc": "",
                "notes": notes,
                "error_log": "",
                "tags_from_source": existing_tags,
                "language_hint": video.get("language_hint", "unknown"),
                "manual_flag": "none",
                "dest_mapping_tags": "",
            }

            if dry_run:
                print(f"\n{'='*60}")
                print(f"DRY RUN — Row {next_id} [{tab_name}]")
                print(f"{'='*60}")
                print(json.dumps(row_data, indent=2, ensure_ascii=False))
                stats["inserted"] += 1
            else:
                try:
                    scraper_sheets.append_row(tab_name, row_data, sheets)
                    stats["inserted"] += 1
                    log_event("row_inserted", tab=tab_name, row_id=next_id, url=normalized_url)
                except Exception as e:
                    logger.error("Failed to insert row %d: %s", next_id, e)
                    stats["errors"] += 1
                    log_event("insert_error", tab=tab_name, row_id=next_id, error=str(e))

            # Update caches
            url_cache.add(normalized_url)
            hash_cache.add(content_hash)
            next_id += 1

            # Clean up temp file
            if temp_path and Path(temp_path).exists():
                Path(temp_path).unlink()

            maybe_status_update()

        # Update master_index
        if not dry_run:
            try:
                scraper_sheets.update_master_index(tab_name, source_type, source_id, sheets)
            except Exception as e:
                logger.error("Failed to update master_index: %s", e)

        log_event("source_complete", tab=tab_name, stats=stats)
        if not dry_run:
            _write_scrape_status(tab_name, {
                "tab": tab_name,
                "state": "done",
                "started_at": started_at,
                "fetched": stats["fetched"],
                "inserted": stats["inserted"],
                "skipped_duplicate": stats["skipped_duplicate"],
                "errors": stats["errors"],
            })
        return stats
    except Exception as e:
        if not dry_run:
            _write_scrape_status(tab_name, {
                "tab": tab_name,
                "state": "error",
                "started_at": started_at,
                "fetched": stats["fetched"],
                "inserted": stats["inserted"],
                "skipped_duplicate": stats["skipped_duplicate"],
                "errors": stats["errors"] + 1,
                "error": str(e),
            })
        raise
    finally:
        if not dry_run:
            _release_scrape_lock(tab_name)


# ── CLI ───────────────────────────────────────────────────────────

def load_sources() -> list[dict]:
    """Load source definitions from sources.yaml."""
    if not scraper_config.SOURCES_YAML.exists():
        logger.error("sources.yaml not found at %s", scraper_config.SOURCES_YAML)
        sys.exit(1)
    with open(scraper_config.SOURCES_YAML, "r") as f:
        sources = yaml.safe_load(f)
    if not sources:
        logger.error("No sources defined in sources.yaml")
        sys.exit(1)
    return sources


def main():
    parser = argparse.ArgumentParser(
        description="Gravix Scraper — Ingest video metadata into Google Sheets",
    )
    parser.add_argument("--batch", action="store_true", help="Process all sources")
    parser.add_argument("--source", type=str, default=None, help="Process a specific source tab")
    parser.add_argument("--dry-run", action="store_true", help="Fetch but don't write to sheet")
    parser.add_argument("--limit", type=int, default=None, help="Max videos per source")
    parser.add_argument("--setup-only", action="store_true", help="Only create tabs, no scraping")
    args = parser.parse_args()

    if not any([args.batch, args.source, args.setup_only]):
        parser.print_help()
        print("\nError: Please specify --batch, --source, or --setup-only.")
        sys.exit(1)

    logger.info("Gravix Scraper starting (instance: %s)", scraper_config.INSTANCE_ID)

    # ── Validate credentials ───────────────────────────────────────
    if not os.path.isfile(scraper_config.GOOGLE_SVC_JSON):
        logger.error(
            "Service account file not found: %s\n"
            "Set GOOGLE_SVC_JSON in .env or place service_account.json in project dir.",
            scraper_config.GOOGLE_SVC_JSON,
        )
        sys.exit(1)

    # ── Connect to Sheets ─────────────────────────────────────────
    sheets = scraper_sheets.get_service()

    # ── Ensure global tabs ────────────────────────────────────────
    logger.info("Ensuring global tabs exist...")
    scraper_sheets.ensure_global_tabs(sheets)

    # ── Setup-only mode ───────────────────────────────────────────
    if args.setup_only:
        sources = load_sources()
        for src in sources:
            scraper_sheets.ensure_tab_exists(src["source_tab"], sheets)
            logger.info("Tab ensured: %s", src["source_tab"])
        logger.info("Setup complete. %d tabs ready.", len(sources))
        return

    # ── Load sources ──────────────────────────────────────────────
    sources = load_sources()
    if args.source:
        sources = [s for s in sources if s["source_tab"] == args.source]
        if not sources:
            logger.error("Source '%s' not found in sources.yaml", args.source)
            sys.exit(1)

    # ── Build dedupe caches ───────────────────────────────────────
    logger.info("Loading content hash cache...")
    if not args.dry_run:
        hash_cache = scraper_sheets.get_all_content_hashes(sheets)
    else:
        hash_cache = set()
    url_cache: set[str] = set()

    # ── Key rotator ───────────────────────────────────────────────
    key_rotator = KeyRotator(scraper_config.SCRAPINGDOG_KEYS)

    # ── Clean old temp files ──────────────────────────────────────
    _cleanup_temp_files()

    # ── Process sources ───────────────────────────────────────────
    total_stats = {"fetched": 0, "inserted": 0, "skipped_duplicate": 0, "errors": 0}

    for source in sources:
        try:
            stats = process_source(
                source, sheets, url_cache, hash_cache, key_rotator,
                dry_run=args.dry_run,
                limit=args.limit,
            )
            for key in total_stats:
                total_stats[key] += stats.get(key, 0)
        except Exception as e:
            logger.error("Fatal error processing %s: %s", source["source_tab"], e)
            total_stats["errors"] += 1

    # ── Summary ───────────────────────────────────────────────────
    logger.info(
        "Scrape complete: %d fetched, %d inserted, %d duplicates, %d errors.",
        total_stats["fetched"],
        total_stats["inserted"],
        total_stats["skipped_duplicate"],
        total_stats["errors"],
    )
    log_event("scrape_complete", stats=total_stats)

    if total_stats["errors"]:
        sys.exit(1)


if __name__ == "__main__":
    main()
