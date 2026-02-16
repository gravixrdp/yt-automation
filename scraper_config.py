"""
scraper_config.py — Configuration specific to the scraper & ingestion system.
"""

import os
import uuid
from pathlib import Path
from dotenv import load_dotenv

# ── Load environment ──────────────────────────────────────────────
load_dotenv(Path(__file__).parent / ".env")

# ── Credentials ───────────────────────────────────────────────────
GOOGLE_SVC_JSON = os.getenv(
    "GOOGLE_SVC_JSON",
    os.getenv("SERVICE_ACCOUNT_FILE", str(Path(__file__).parent / "service_account.json")),
)
SCRAPINGDOG_KEYS = [
    k.strip() for k in os.getenv("SCRAPINGDOG_KEYS", "").split(",") if k.strip()
]
YT_API_KEY = os.getenv("YT_API_KEY", "")
INSTANCE_ID = os.getenv("INSTANCE_ID", f"scraper_{uuid.uuid4().hex[:8]}")

# ── Google Sheet ──────────────────────────────────────────────────
SPREADSHEET_ID = "127jRbWlGE4D9CQbi0ZmvUY6VZHdZOuwZeCb5lTf_N5Y"
SHEETS_SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# ── Scraper sheet schema (columns A–W) ────────────────────────────
SCRAPER_HEADERS = [
    "row_id",               # A
    "scraped_date_utc",     # B
    "source_channel",       # C
    "source_channel_tab",   # D
    "source_url",           # E
    "original_title",       # F
    "duration_seconds",     # G
    "published_at_utc",     # H
    "view_count",           # I
    "thumbnail_url",        # J
    "local_temp_path",      # K
    "content_hash",         # L
    "content_hash_method",  # M
    "scraped_by",           # N
    "status",               # O
    "upload_attempts",      # P
    "last_attempt_time_utc",# Q
    "notes",                # R
    "error_log",            # S
    "tags_from_source",     # T
    "language_hint",        # U
    "manual_flag",          # V
    "dest_mapping_tags",    # W
]

# Global tab names
TAB_MASTER_INDEX = "master_index"
TAB_DESTINATIONS_MAPPING = "destinations_mapping"

MASTER_INDEX_HEADERS = [
    "source_tab", "source_type", "source_id",
    "last_scraped_at", "row_count", "status",
]
DESTINATIONS_MAPPING_HEADERS = [
    "source_tag", "destination_account_id", "platform", "active", "notes",
]

# ── Rate limiting ─────────────────────────────────────────────────
PER_SOURCE_RATE_LIMIT_SECONDS = 3       # 1 request per 3s per source
SCRAPINGDOG_RATE_LIMIT_PER_MIN = 10     # per key
SCRAPINGDOG_ERROR_THRESHOLD = float(os.getenv("SCRAPINGDOG_ERROR_THRESHOLD", "0.20"))
SCRAPINGDOG_WINDOW_SIZE = int(os.getenv("SCRAPINGDOG_WINDOW_SIZE", "100"))
RETRY_MAX_ATTEMPTS = 3
RETRY_BACKOFF_BASE = 2.0                # seconds: 2, 6, 18
SCRAPER_MAX_WORKERS = int(os.getenv("SCRAPER_MAX_WORKERS", "3"))

# ── Temp files ────────────────────────────────────────────────────
TEMP_DIR = Path(f"/tmp/shorts_ingest/{INSTANCE_ID}")
TEMP_DIR.mkdir(parents=True, exist_ok=True)
TEMP_MAX_AGE_HOURS = 6
DISK_MIN_FREE_PERCENT = 10             # stop downloads if below

# ── Download limits ───────────────────────────────────────────────
MAX_FILE_SIZE_MB = 100                  # skip files larger than this
HASH_HEADTAIL_THRESHOLD_MB = 50        # use headtail if > this

# ── Logging ───────────────────────────────────────────────────────
LOG_DIR = Path(__file__).parent / "logs"
LOG_DIR.mkdir(exist_ok=True)

# ── Scrape status / locking ──────────────────────────────────────
SCRAPE_STATUS_DIR = LOG_DIR / "scrape_status"
SCRAPE_STATUS_DIR.mkdir(exist_ok=True)
SCRAPE_LOCK_TIMEOUT_MINUTES = int(os.getenv("SCRAPE_LOCK_TIMEOUT_MINUTES", "120"))
SCRAPE_STATUS_THROTTLE_SECONDS = float(os.getenv("SCRAPE_STATUS_THROTTLE_SECONDS", "5"))

# ── Sources config file ──────────────────────────────────────────
SOURCES_YAML = Path(__file__).parent / "sources.yaml"

# ── Dedupe cache ──────────────────────────────────────────────────
HASH_CACHE_MAX_SIZE = 10_000
