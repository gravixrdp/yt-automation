"""
hash_utils.py — Content hashing for deduplication.
Supports full SHA256, head+tail SHA256, and metadata-based hashing.
"""

import hashlib
from pathlib import Path

import scraper_config


def compute_file_hash(file_path: str | Path) -> tuple[str, str]:
    """
    Compute SHA256 of a file.
    For files > HASH_HEADTAIL_THRESHOLD_MB, uses head+tail method.
    Returns (hex_hash, method) where method is 'full' or 'headtail'.
    """
    file_path = Path(file_path)
    file_size = file_path.stat().st_size
    threshold = scraper_config.HASH_HEADTAIL_THRESHOLD_MB * 1024 * 1024

    if file_size <= threshold:
        # Full file hash
        h = hashlib.sha256()
        with open(file_path, "rb") as f:
            while chunk := f.read(8192):
                h.update(chunk)
        return h.hexdigest(), "full"
    else:
        # Head + tail hash (first 10MB + last 10MB)
        chunk_size = 10 * 1024 * 1024  # 10MB
        h = hashlib.sha256()
        with open(file_path, "rb") as f:
            # Read first 10MB
            head = f.read(chunk_size)
            h.update(head)
            # Seek to last 10MB
            f.seek(-chunk_size, 2)
            tail = f.read(chunk_size)
            h.update(tail)
        return h.hexdigest(), "headtail"


def compute_metadata_hash(
    title: str,
    duration: int | str,
    published_at: str,
    source_url: str,
) -> tuple[str, str]:
    """
    Compute a metadata-based fingerprint when file download is not available.
    Returns (hex_hash, 'metadata_hash').
    """
    payload = f"{title}|{duration}|{published_at}|{source_url}"
    h = hashlib.sha256(payload.encode("utf-8"))
    return h.hexdigest(), "metadata_hash"


def normalize_url(url: str) -> str:
    """
    Normalize a video URL to a canonical form for dedupe.
    Strips query params, trailing slashes, and standardizes domain.
    """
    import re
    from urllib.parse import urlparse, urlunparse

    url = url.strip()
    parsed = urlparse(url)

    # Normalize YouTube URLs
    if "youtube.com" in parsed.netloc or "youtu.be" in parsed.netloc:
        # Extract video ID
        if "youtu.be" in parsed.netloc:
            video_id = parsed.path.strip("/")
        elif "/shorts/" in parsed.path:
            video_id = parsed.path.split("/shorts/")[-1].strip("/")
        elif "v=" in (parsed.query or ""):
            match = re.search(r"v=([A-Za-z0-9_-]+)", parsed.query)
            video_id = match.group(1) if match else parsed.path.strip("/")
        else:
            video_id = parsed.path.strip("/")
        # Standardize to shorts URL
        return f"https://youtube.com/shorts/{video_id}"

    # Normalize Instagram URLs
    if "instagram.com" in parsed.netloc:
        # Strip query params and trailing slashes
        path = parsed.path.rstrip("/")
        return f"https://www.instagram.com{path}"

    # Generic: strip query, fragment, trailing slash
    clean = urlunparse((
        parsed.scheme, parsed.netloc, parsed.path.rstrip("/"),
        "", "", "",
    ))
    return clean


def clean_tab_name(channel_id: str) -> str:
    """
    Generate a clean sheet tab name from a channel ID.
    Format: source__{clean_id} (lowercase, alphanum, hyphen, underscore, max 40).
    """
    import re
    clean = channel_id.lower().strip()
    clean = re.sub(r"[^a-z0-9_-]", "_", clean)
    clean = re.sub(r"_+", "_", clean).strip("_")
    clean = clean[:40]
    return f"source__{clean}"
