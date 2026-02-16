"""
config.py — Central configuration for the Gravix AI Content Agent.
Loads credentials from .env and defines constants.
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# ── Load environment ──────────────────────────────────────────────
load_dotenv(Path(__file__).parent / ".env")

# ── Credentials ───────────────────────────────────────────────────
SERVICE_ACCOUNT_FILE = os.getenv(
    "SERVICE_ACCOUNT_FILE",
    str(Path(__file__).parent / "service_account.json"),
)
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")

# ── Google Sheet ──────────────────────────────────────────────────
SPREADSHEET_ID = "127jRbWlGE4D9CQbi0ZmvUY6VZHdZOuwZeCb5lTf_N5Y"
SHEET_NAME = "Sheet1"

# ── Column layout (0-indexed for API, display letter for reference) ─
HEADERS = [
    "row_id",               # A
    "source_channel",       # B
    "source_channel_tab",   # C
    "source_url",           # D
    "original_title",       # E
    "duration_seconds",     # F
    "view_count",           # G
    "ai_title",             # H
    "ai_description",       # I
    "ai_hashtags",          # J
    "ai_tags",              # K
    "category",             # L
    "priority_score",       # M
    "priority_reason",      # N
    "suggested_ffmpeg_cmd", # O
    "ffmpeg_reason",        # P
    "flagged_for_review",   # Q
    "review_reasons",       # R
    "notes",                # S
    "thumbnail_url",        # T
    "content_hash",         # U
    "status",               # V
    "processed_at",         # W
    "agent_version",        # X
    "error_log",            # Y
]

# Column letter helper (A=0, B=1 … Z=25)
COL_INDEX = {name: idx for idx, name in enumerate(HEADERS)}

def col_letter(name: str) -> str:
    """Return the spreadsheet column letter for a header name."""
    idx = COL_INDEX[name]
    return chr(ord("A") + idx)

# ── AI settings ───────────────────────────────────────────────────
GEMINI_MODEL = "gemini-2.0-flash"
GEMINI_TEMPERATURE = 0.1
GEMINI_MAX_TOKENS = 700

# ── Rate limiting & retries ───────────────────────────────────────
RATE_LIMIT_RPS = 5          # max requests per second
MAX_RETRIES = 3
RETRY_BACKOFF_BASE = 1.0    # seconds (exponential: 1, 2, 4)

# ── Logging ───────────────────────────────────────────────────────
LOG_DIR = Path(__file__).parent / "logs"
LOG_DIR.mkdir(exist_ok=True)

# ── Scopes ────────────────────────────────────────────────────────
SHEETS_SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
