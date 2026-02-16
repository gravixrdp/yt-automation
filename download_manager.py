"""
download_manager.py — Safe media download with hash verification.
"""

import logging
import shutil
import subprocess
import time
from pathlib import Path

import scheduler_config
import hash_utils

logger = logging.getLogger(__name__)


def _resolve_ytdlp_bin() -> str | None:
    """Resolve yt-dlp from PATH first, then project venv."""
    found = shutil.which("yt-dlp")
    if found:
        return found
    venv_bin = Path(__file__).parent / "venv" / "bin" / "yt-dlp"
    if venv_bin.exists():
        return str(venv_bin)
    return None


def check_disk_space() -> bool:
    """Return True if disk has enough free space for downloads."""
    total, _, free = shutil.disk_usage("/tmp")
    free_pct = (free / total) * 100
    if free_pct < scheduler_config.DISK_MIN_FREE_PERCENT:
        logger.error(
            "Disk free %.1f%% < %d%%. Downloads paused.",
            free_pct, scheduler_config.DISK_MIN_FREE_PERCENT,
        )
        return False
    return True


def download_video(source_url: str, expected_hash: str = "") -> dict:
    """
    Download video from source_url to temp directory using yt-dlp.
    Returns dict with keys: success, path, content_hash, hash_method, error.
    """
    result = {
        "success": False, "path": None,
        "content_hash": None, "hash_method": None, "error": None,
    }

    if not check_disk_space():
        result["error"] = "insufficient_disk_space"
        return result

    scheduler_config.TEMP_DIR.mkdir(parents=True, exist_ok=True)
    output_template = str(scheduler_config.TEMP_DIR / "%(id)s.%(ext)s")
    ytdlp_bin = _resolve_ytdlp_bin()
    if not ytdlp_bin:
        result["error"] = "yt-dlp not installed"
        return result

    cmd = [
        ytdlp_bin,
        "--no-playlist",
        "--no-warnings",
        "-o", output_template,
        source_url,
    ]

    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=180)
        if proc.returncode != 0:
            result["error"] = f"yt-dlp exit {proc.returncode}: {proc.stderr[:200]}"
            return result
    except FileNotFoundError:
        result["error"] = "yt-dlp not installed"
        return result
    except subprocess.TimeoutExpired:
        result["error"] = "download_timeout"
        return result

    # Find the downloaded file (most recently modified)
    downloaded = None
    best_mtime = 0
    for f in scheduler_config.TEMP_DIR.iterdir():
        if f.is_file() and f.stat().st_mtime > best_mtime:
            best_mtime = f.stat().st_mtime
            downloaded = f

    if not downloaded:
        result["error"] = "no_file_after_download"
        return result

    # Compute hash
    try:
        content_hash, hash_method = hash_utils.compute_file_hash(str(downloaded))
    except Exception as e:
        result["error"] = f"hash_error: {e}"
        result["path"] = str(downloaded)
        return result

    result["path"] = str(downloaded)
    result["content_hash"] = content_hash
    result["hash_method"] = hash_method
    result["success"] = True

    # Verify against expected hash if provided
    if expected_hash and expected_hash != content_hash:
        logger.warning(
            "Hash mismatch for %s: expected=%s got=%s (hash_method=%s). "
            "Will update sheet with new hash.",
            source_url, expected_hash[:16], content_hash[:16], hash_method,
        )
        result["hash_mismatch"] = True

    size_mb = downloaded.stat().st_size / (1024 * 1024)
    logger.info("Downloaded %s (%.1f MB, hash=%s)", downloaded.name, size_mb, content_hash[:16])
    return result


def cleanup_file(path: str | None):
    """Delete a temp file immediately."""
    if path and Path(path).exists():
        try:
            Path(path).unlink()
            logger.debug("Cleaned up: %s", path)
        except OSError as e:
            logger.warning("Failed to clean up %s: %s", path, e)


def cleanup_old_temp_files():
    """Delete temp files older than configured max age."""
    from datetime import datetime, timezone, timedelta
    cutoff = (
        datetime.now(timezone.utc) -
        timedelta(hours=6)
    ).timestamp()

    cleaned = 0
    for f in scheduler_config.TEMP_DIR.iterdir():
        if f.is_file() and f.stat().st_mtime < cutoff:
            f.unlink()
            cleaned += 1
    if cleaned:
        logger.info("Cleaned %d old temp files.", cleaned)
