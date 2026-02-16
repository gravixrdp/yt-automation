"""
scheduler_config.py — Configuration for Part 3: Scheduler, Uploader & Telegram Control.
Includes all production-hardening constants (10 gaps).
"""

import json
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent / ".env")

# ── Credentials ───────────────────────────────────────────────────
GOOGLE_SVC_JSON = os.getenv(
    "GOOGLE_SVC_JSON",
    os.getenv("SERVICE_ACCOUNT_FILE", str(Path(__file__).parent / "service_account.json")),
)
SHEETS_SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
SPREADSHEET_ID = "127jRbWlGE4D9CQbi0ZmvUY6VZHdZOuwZeCb5lTf_N5Y"

# ── Telegram ──────────────────────────────────────────────────────
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
ADMIN_TELEGRAM_IDS = [
    int(x.strip()) for x in os.getenv("ADMIN_TELEGRAM_IDS", "").split(",")
    if x.strip().isdigit()
]
TELEGRAM_RATE_LIMIT_PER_MIN = int(os.getenv("TELEGRAM_RATE_LIMIT_PER_MIN", "20"))

# ── OAuth / Credentials ──────────────────────────────────────────
CREDENTIALS_KEY = os.getenv("CREDENTIALS_KEY", "")  # AES-GCM master key
CREDENTIALS_FILE = Path(__file__).parent / "secrets" / "credentials.json"
CREDENTIALS_FILE.parent.mkdir(parents=True, exist_ok=True)

# Instagram Graph API
INSTAGRAM_APP_ID = os.getenv("INSTAGRAM_APP_ID", "")
INSTAGRAM_APP_SECRET = os.getenv("INSTAGRAM_APP_SECRET", "")

# YouTube OAuth
YOUTUBE_CLIENT_ID = os.getenv("YOUTUBE_CLIENT_ID", "")
YOUTUBE_CLIENT_SECRET = os.getenv("YOUTUBE_CLIENT_SECRET", "")
YOUTUBE_REDIRECT_URI = os.getenv("YOUTUBE_REDIRECT_URI", "http://localhost:8090/oauth/callback")
OAUTH_REQUIRE_HTTPS = os.getenv("OAUTH_REQUIRE_HTTPS", "true").lower() == "true"

# ── YouTube Quota (#2) ───────────────────────────────────────────
YT_QUOTA_LIMIT_PER_PROJECT = int(os.getenv("YT_QUOTA_LIMIT_PER_PROJECT", "10000"))
YT_QUOTA_UNITS_PER_UPLOAD = int(os.getenv("YT_QUOTA_UNITS_PER_UPLOAD", "1600"))
QUOTA_SAFETY_MARGIN = float(os.getenv("QUOTA_SAFETY_MARGIN", "0.8"))
# JSON dict: {"project1": {"client_id": "...", "client_secret": "..."}, ...}
YT_PROJECT_KEYS = json.loads(os.getenv("YT_PROJECT_KEYS", "{}"))

# ── Instance ──────────────────────────────────────────────────────
INSTANCE_ID = os.getenv("INSTANCE_ID", "scheduler_01")

# ── Scheduler ─────────────────────────────────────────────────────
POLL_INTERVAL_SECONDS = 60          # how often to check sheet
MAX_CONCURRENT_WORKERS = 2          # concurrent upload workers
UPLOADS_PER_DAY_PER_DEST = 2       # daily cap per destination account
UPLOAD_SPACING_SECONDS = 600       # 10 min between uploads to same dest
STALE_IN_PROGRESS_HOURS = 2        # reset rows stuck IN_PROGRESS after 2h
UPLOAD_SLOTS_LOCAL = [
    tuple(map(int, t.split(":")))
    for t in os.getenv("UPLOAD_SLOTS_LOCAL", "09:00,12:00,15:00,18:00").split(",")
    if ":" in t
]  # daily fixed slot times in DISPLAY_TIMEZONE (hour:minute)

# ── Retry / Backoff ──────────────────────────────────────────────
MAX_UPLOAD_ATTEMPTS = 3
RETRY_BACKOFF_BASE = 2.0           # seconds: 2, 6, 18

# ── Paths ─────────────────────────────────────────────────────────
TEMP_DIR = Path(f"/tmp/shorts_upload/{INSTANCE_ID}")
TEMP_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR = Path(__file__).parent / "logs"
LOG_DIR.mkdir(exist_ok=True)
QUEUE_DB_PATH = Path(__file__).parent / "queue.db"
BACKUP_DIR = Path(__file__).parent / "backups"
BACKUP_DIR.mkdir(exist_ok=True)

# ── ffmpeg defaults ───────────────────────────────────────────────
FFMPEG_DEFAULT_CMD = (
    '/usr/bin/ffmpeg -y -i "{input}" -ss 0.5 -threads 1 -c:v libx264 -preset veryfast '
    '-crf 23 -c:a aac -b:a 96k "{output}"'
)
FFMPEG_FALLBACK_CMD = (
    '/usr/bin/ffmpeg -y -i "{input}" -threads 1 -c:v libx264 -preset veryfast '
    '-crf 23 -c:a aac -b:a 96k "{output}"'
)

MAX_SHORTS_DURATION = 60            # YouTube Shorts limit

# ── Video Branding (#3) ──────────────────────────────────────────
WATERMARK_DIR = Path(__file__).parent / "assets" / "watermarks"
WATERMARK_DIR.mkdir(parents=True, exist_ok=True)
BRANDING_INTRO = os.getenv("BRANDING_INTRO", "")   # path to intro clip
BRANDING_OUTRO = os.getenv("BRANDING_OUTRO", "")   # path to outro clip
CROP_VARIATION_ENABLED = os.getenv("CROP_VARIATION_ENABLED", "true").lower() == "true"
CROP_MAX_PX = int(os.getenv("CROP_MAX_PX", "8"))

# ── Disk ──────────────────────────────────────────────────────────
DISK_MIN_FREE_PERCENT = 10

# ── Scrapingdog Key Health (#6) ──────────────────────────────────
SCRAPINGDOG_ERROR_THRESHOLD = float(os.getenv("SCRAPINGDOG_ERROR_THRESHOLD", "0.20"))
SCRAPINGDOG_WINDOW_SIZE = int(os.getenv("SCRAPINGDOG_WINDOW_SIZE", "100"))

# ── Display Timezone (#7) ────────────────────────────────────────
DISPLAY_TIMEZONE = os.getenv("DISPLAY_TIMEZONE", "Asia/Kolkata")

# ── Backup (#8) ──────────────────────────────────────────────────
BACKUP_RETENTION_DAYS = int(os.getenv("BACKUP_RETENTION_DAYS", "7"))
LOG_RETENTION_DAYS = int(os.getenv("LOG_RETENTION_DAYS", "7"))

# ── Sheet Archiving (#5) ─────────────────────────────────────────
ARCHIVE_AFTER_DAYS = int(os.getenv("ARCHIVE_AFTER_DAYS", "30"))
SHEET_CELL_WARN_THRESHOLD = 5_000_000   # warn at 5M cells
SHEET_CELL_ALARM_THRESHOLD = 8_000_000  # alarm at 8M cells

# ── Static Mappings (#10) ────────────────────────────────────────
# JSON dict: {"source__cricket_shorts": "yt_cricket_channel", ...}
STATIC_MAPPINGS = json.loads(os.getenv("STATIC_MAPPINGS", "{}"))
