"""
telegram_bot.py — Admin Telegram bot for Gravix scheduler control.
Provides inline keyboard flows for mapping, uploads, stats, and OAuth.
Includes rate limiting (#9), timezone display (#7), and /mappings command (#10).
"""

import collections
import json
import logging
import os
from pathlib import Path
import re
import sys
import time as _time
from datetime import datetime, timezone, timedelta
from typing import Any
from zoneinfo import ZoneInfo

import yaml
import scheduler_config
import sheet_manager
import queue_db
import oauth_helper
import scraper_sheets
import ai_agent
import scraper_config

logger = logging.getLogger(__name__)

# Simple in-memory state for guided flows (per-admin).
_pending_actions: dict[int, dict[str, Any]] = {}


# ── Timezone display helper (#7) ─────────────────────────────────

def _to_display_tz(utc_str: str) -> str:
    """Convert a UTC timestamp string to local display timezone."""
    if not utc_str:
        return "N/A"
    try:
        if utc_str.endswith("Z"):
            utc_str = utc_str[:-1] + "+00:00"
        dt = datetime.fromisoformat(utc_str).replace(tzinfo=timezone.utc)
        local = dt.astimezone(ZoneInfo(scheduler_config.DISPLAY_TIMEZONE))
        return local.strftime("%Y-%m-%d %H:%M %Z")
    except Exception:
        return utc_str[:16]


def _parse_utc_iso(value: str) -> datetime | None:
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


def _extract_scheduled_utc(row_or_notes: dict | str) -> datetime | None:
    if isinstance(row_or_notes, dict):
        sched_date = str(row_or_notes.get("scheduled_date", "") or "").strip()
        sched_time = str(row_or_notes.get("scheduled_time", "") or "").strip()
        if sched_date and sched_time:
            parsed = _parse_utc_iso(f"{sched_date}T{sched_time}")
            if parsed:
                return parsed
            parsed = _parse_utc_iso(f"{sched_date}T{sched_time}Z")
            if parsed:
                return parsed
        notes = str(row_or_notes.get("notes", "") or "")
    else:
        notes = str(row_or_notes or "")

    if not notes:
        return None
    matches = re.findall(r"schedule_at_utc=([0-9T:\\-+\\.Z]+)", notes, flags=re.IGNORECASE)
    if not matches:
        return None
    return _parse_utc_iso(matches[-1])


def _find_row_by_row_id(row_id: str, sheets) -> tuple[str, dict] | tuple[None, None]:
    tabs = sheet_manager.get_all_source_tabs(sheets)
    for tab in tabs:
        rows = sheets.values().get(
            spreadsheetId=scheduler_config.SPREADSHEET_ID,
            range=f"'{tab}'!A:AZ",
        ).execute().get("values", [])
        if len(rows) < 2:
            continue
        headers = rows[0]
        for i, row in enumerate(rows[1:], start=2):
            padded = row + [""] * (len(headers) - len(row))
            if padded and padded[0] == row_id:
                found_row = dict(zip(headers, padded))
                found_row["_sheet_row"] = i
                found_row["_tab_name"] = tab
                return tab, found_row
    return None, None


def _schedule_picker_keyboard(tab: str, sheet_row: int):
    now = datetime.now(timezone.utc)
    options = [
        ("+10m", now + timedelta(minutes=10)),
        ("+30m", now + timedelta(minutes=30)),
        ("+1h", now + timedelta(hours=1)),
        ("+3h", now + timedelta(hours=3)),
        ("+6h", now + timedelta(hours=6)),
        ("+12h", now + timedelta(hours=12)),
    ]
    keyboard = []
    row = []
    for label, dt in options:
        ts = int(dt.timestamp())
        row.append(InlineKeyboardButton(label, callback_data=f"set_sched:{tab}:{sheet_row}:{ts}"))
        if len(row) == 2:
            keyboard.append(row)
            row = []
    if row:
        keyboard.append(row)
    keyboard.append([InlineKeyboardButton("🗑 Clear", callback_data=f"clear_sched:{tab}:{sheet_row}")])
    keyboard.append([InlineKeyboardButton("Cancel", callback_data="cancel")])
    return InlineKeyboardMarkup(keyboard)


def _schedule_stamp(schedule_utc: datetime) -> str:
    return schedule_utc.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _set_row_schedule(tab: str, sheet_row: int, schedule_utc: datetime, sheets) -> None:
    schedule_utc = schedule_utc.astimezone(timezone.utc)
    sheet_manager.update_row_status(tab, sheet_row, "READY_TO_UPLOAD", {
        "scheduled_date": schedule_utc.strftime("%Y-%m-%d"),
        "scheduled_time": schedule_utc.strftime("%H:%M:%S"),
    }, sheets=sheets)
    sheet_manager.append_audit_note(
        tab, sheet_row,
        f"schedule_at_utc={_schedule_stamp(schedule_utc)} via Telegram",
        sheets,
    )


def _clear_row_schedule(tab: str, sheet_row: int, sheets) -> None:
    # Set marker to a safe past timestamp so scheduler treats row as ready now.
    marker = datetime(1970, 1, 1, tzinfo=timezone.utc)
    sheet_manager.update_row_status(tab, sheet_row, "READY_TO_UPLOAD", {
        "scheduled_date": "",
        "scheduled_time": "",
    }, sheets=sheets)
    sheet_manager.append_audit_note(
        tab, sheet_row,
        f"schedule_cleared schedule_at_utc={_schedule_stamp(marker)} via Telegram",
        sheets,
    )


def _normalize_source_tab(raw: str) -> str:
    cleaned = re.sub(r"[^a-zA-Z0-9_]+", "_", raw.strip()).strip("_").lower()
    if not cleaned:
        cleaned = "new_source"
    if not cleaned.startswith("source__"):
        cleaned = f"source__{cleaned}"
    return cleaned[:90]


def _normalize_source_id(source_type: str, raw: str) -> str:
    value = raw.strip()
    if source_type == "youtube":
        m = re.search(r"youtube\.com/channel/([A-Za-z0-9_-]+)", value, flags=re.IGNORECASE)
        if m:
            return m.group(1)
        m = re.search(r"youtube\.com/@([A-Za-z0-9._-]+)", value, flags=re.IGNORECASE)
        if m:
            return f"@{m.group(1)}"
        if value.startswith("@"):
            return value
        return value
    if source_type == "instagram":
        m = re.search(r"instagram\.com/([A-Za-z0-9._-]+)/?", value, flags=re.IGNORECASE)
        if m:
            return m.group(1)
    return value


def _sources_yaml_path() -> Path:
    return Path(__file__).parent / "sources.yaml"


def _read_sources_yaml() -> list[dict]:
    path = _sources_yaml_path()
    if not path.exists():
        return []
    with open(path, "r", encoding="utf-8") as f:
        loaded = yaml.safe_load(f) or []
    if not isinstance(loaded, list):
        return []
    return [item for item in loaded if isinstance(item, dict)]


def _write_sources_yaml(rows: list[dict]) -> None:
    path = _sources_yaml_path()
    with open(path, "w", encoding="utf-8") as f:
        yaml.safe_dump(rows, f, sort_keys=False, allow_unicode=False)


# ── Rate limiter (#9) ────────────────────────────────────────────

_rate_log: dict[int, list[float]] = collections.defaultdict(list)


def _rate_limit_check(user_id: int) -> bool:
    """Returns True if the user is within rate limits, False if exceeded."""
    now = _time.time()
    window = 60.0  # 1 minute
    limit = scheduler_config.TELEGRAM_RATE_LIMIT_PER_MIN
    # Prune old entries
    _rate_log[user_id] = [t for t in _rate_log[user_id] if now - t < window]
    if len(_rate_log[user_id]) >= limit:
        logger.warning("Rate limit exceeded for user %d (%d/%d per min)", user_id, len(_rate_log[user_id]), limit)
        return False
    _rate_log[user_id].append(now)
    return True


def _clear_pending(uid: int):
    """Drop any stored multi-step flow state for a user."""
    _pending_actions.pop(uid, None)


def _read_scrape_status(tab: str) -> dict | None:
    try:
        path = Path(scraper_config.SCRAPE_STATUS_DIR) / f"{tab}.json"
        if not path.exists():
            return None
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def _format_scrape_status(tab: str, status: dict) -> str:
    state = status.get("state", "unknown")
    fetched = status.get("fetched", 0)
    inserted = status.get("inserted", 0)
    skipped = status.get("skipped_duplicate", 0)
    errors = status.get("errors", 0)
    updated = status.get("updated_at", "N/A")
    return (
        f"📥 *{tab}* — {state}\n"
        f"Fetched: {fetched} | Inserted: {inserted} | Skipped: {skipped} | Errors: {errors}\n"
        f"Updated: {updated}"
    )

# ── Check if python-telegram-bot is available ─────────────────────
try:
    from telegram import (
        Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup,
        BotCommand,
    )
    from telegram.ext import (
        Application, CommandHandler, CallbackQueryHandler,
        ContextTypes, MessageHandler, filters,
    )
    HAS_TELEGRAM = True
except ImportError:
    HAS_TELEGRAM = False
    logger.warning("python-telegram-bot not installed. Run: pip install python-telegram-bot")


def is_admin(user_id: int) -> bool:
    """Check if a Telegram user is an authorized admin."""
    if not scheduler_config.ADMIN_TELEGRAM_IDS:
        logger.warning("ADMIN_TELEGRAM_IDS is empty; refusing admin access by default.")
        return False
    return user_id in scheduler_config.ADMIN_TELEGRAM_IDS


def admin_only(func):
    """Decorator to restrict commands to admin users with rate limiting."""
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
        uid = update.effective_user.id
        if not is_admin(uid):
            await update.message.reply_text("Unauthorized. Admin access required.")
            return
        if not _rate_limit_check(uid):
            await update.message.reply_text("⏱ Please slow down. Rate limit exceeded.")
            return
        return await func(update, context)
    return wrapper


# ── Menu Helpers ──────────────────────────────────────────────────

def get_main_menu_keyboard():
    """Return the main menu inline keyboard."""
    keyboard = [
        [
            InlineKeyboardButton("📊 Status", callback_data="status"),
            InlineKeyboardButton("🏥 Health", callback_data="health"),
        ],
        [
            InlineKeyboardButton("📥 Sources", callback_data="sources"),
            InlineKeyboardButton("📤 Destinations", callback_data="destinations"),
        ],
        [
            InlineKeyboardButton("❌ Errors", callback_data="view_errors"),
            InlineKeyboardButton("❓ Help", callback_data="help"),
        ],
        [
            InlineKeyboardButton("➕ Add Source", callback_data="src_add"),
            InlineKeyboardButton("🧠 AI Titles", callback_data="ai_menu"),
        ],
    ]
    return InlineKeyboardMarkup(keyboard)



def get_sticky_keyboard():
    keyboard = [
        ["📊 Status", "🏥 Health"],
        ["📥 Sources", "📤 Destinations"],
        ["❌ Errors", "❓ Help"],
        ["🔄 Scrape Now", "⚙️ Services"],
        ["➕ Add Source", "🧠 AI Titles"],
        ["🧾 Scrape Status"],
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)


# ── Command Handlers ──────────────────────────────────────────────

@admin_only
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/start — Welcome message with main menu."""
    user = update.effective_user
    await update.message.reply_text(
        f"👋 *Hi {user.first_name}!*\n\n"
        f"**Gravix Control Panel**\n"
        f"Select an action from the menu below.\n"
        f"Bot is running 24/7 on VPS.",
        reply_markup=get_sticky_keyboard(),
        parse_mode="Markdown",
    )


@admin_only
async def handle_reply_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle text messages from ReplyKeyboardMarkup."""
    text = update.message.text
    
    if text == "📊 Status":
        await cmd_status(update, context)
    elif text == "🏥 Health":
        await cmd_health(update, context)
    elif text == "📥 Sources":
        await cmd_sources(update, context)
    elif text == "📤 Destinations":
        await cmd_destinations(update, context)
    elif text == "❌ Errors":
        await cmd_errors(update, context)
    elif text == "❓ Help":
        await cmd_help(update, context)
    elif text == "🔄 Scrape Now":
        await cmd_scrape_now(update, context)
    elif text == "⚙️ Services":
        context.args = ["status", "all"] # Hack to reuse logic? No, create new context or call function
        # Calling cmd_services directly needs context.args
        # We can construct a mock context or just call logic?
        # Better: cmd_services(update, context) expects context.args.
        # We can manually set context.args.
        context.args = ["status", "all"]
        await cmd_services(update, context)
    elif text == "➕ Add Source":
        await cmd_add_source_prompt(update, context)
    elif text == "🧠 AI Titles":
        await cmd_ai_prompt(update, context)
    elif text == "🧾 Scrape Status":
        await cmd_scrape_status(update, context)
    elif update.effective_user and update.effective_user.id in _pending_actions:
        await handle_pending_flow(update, context)
    else:
        # Unknown text
        pass


@admin_only
async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/help — Show usage instructions."""
    await update.effective_message.reply_text(cmd_help_text(), parse_mode="Markdown", reply_markup=get_sticky_keyboard())



@admin_only
async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/status — Show overall system stats."""
    stats = queue_db.get_queue_stats()
    accounts = oauth_helper.get_all_accounts()
    active_accounts = [a for a in accounts if a.get("token_valid")]
    now_display = _to_display_tz(datetime.now(timezone.utc).isoformat())

    msg = (
        f"📊 *Gravix Scheduler Status*\n"
        f"🕐 {now_display}\n\n"
        f"*Queue:*\n"
        f"  ⏳ Queued: {stats.get('queued', 0)}\n"
        f"  🔄 In Progress: {stats.get('in_progress', 0)}\n"
        f"  ✅ Completed: {stats.get('completed', 0)}\n"
        f"  ❌ Failed: {stats.get('failed', 0)}\n"
        f"  📤 Uploaded Today: {stats.get('uploaded_today', 0)}\n"
        f"  (Quota resets 00:00 UTC / 05:30 IST)\n\n"
        f"*Destinations:*\n"
        f"  🔗 Connected: {len(active_accounts)}/{len(accounts)}\n"
    )

    for acc in accounts:
        status_icon = "✅" if acc.get("token_valid") else "❌"
        msg += f"  {status_icon} {acc['account_name']} ({acc['platform']})\n"

    await update.effective_message.reply_text(msg, parse_mode="Markdown")


@admin_only
async def cmd_sources(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/sources — List source tabs with action buttons."""
    try:
        sheets = sheet_manager.get_service()
        tabs = sheet_manager.get_all_source_tabs(sheets)
    except Exception as e:
        await update.effective_message.reply_text(f"Error reading tabs: {e}")
        return

    if not tabs:
        await update.effective_message.reply_text("No source tabs found.")
        return

    keyboard = []
    for tab in tabs[:10]:
        keyboard.append([
            InlineKeyboardButton(f"📁 {tab}", callback_data=f"src_info:{tab}"),
        ])
        keyboard.append([
            InlineKeyboardButton("🔗 Map", callback_data=f"src_map:{tab}"),
            InlineKeyboardButton("🔄 Scrape", callback_data=f"src_scrape:{tab}"),
            InlineKeyboardButton("⏸ Pause", callback_data=f"src_pause:{tab}"),
        ])

    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.effective_message.reply_text(
        f"📁 *Source Tabs* ({len(tabs)} total):",
        reply_markup=reply_markup,
        parse_mode="Markdown",
    )


@admin_only
async def cmd_destinations(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/destinations — List connected destination accounts."""
    accounts = oauth_helper.get_all_accounts()
    if not accounts:
        await update.effective_message.reply_text(
            "No destinations connected. Use /connect to add one."
        )
        return

    keyboard = []
    for acc in accounts:
        status_icon = "✅" if acc.get("token_valid") else "⚠️"
        keyboard.append([
            InlineKeyboardButton(
                f"{status_icon} {acc['account_name']} ({acc['platform']})",
                callback_data=f"dest_info:{acc['account_id']}",
            ),
        ])

    keyboard.append([
        InlineKeyboardButton("➕ Connect New", callback_data="connect_new"),
    ])

    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.effective_message.reply_text(
        "🎯 *Destination Accounts:*",
        reply_markup=reply_markup,
        parse_mode="Markdown",
    )


@admin_only
async def cmd_connect(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/connect — Start OAuth flow to register a new destination."""
    keyboard = [
        [
            InlineKeyboardButton("▶️ YouTube", callback_data="oauth_start:youtube"),
            InlineKeyboardButton("📷 Instagram", callback_data="oauth_start:instagram"),
        ],
        [InlineKeyboardButton("❌ Cancel", callback_data="cancel")],
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.effective_message.reply_text(
        "🔐 *Connect a New Destination*\n"
        "1. Click a button below to get an OAuth link.\n"
        "2. Authorize the app in your browser.\n"
        "3. You will be redirected to `localhost`. It may fail to load.\n"
        "4. *Copy the full URL* from your browser address bar.\n"
        "5. Send here: `/auth pasted_url`\n\n"
        "Choose platform:",
        reply_markup=reply_markup,
        parse_mode="Markdown",
    )


@admin_only
async def cmd_row(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/row <row_id> — Show row details with action buttons."""
    args = context.args
    if not args:
        await update.effective_message.reply_text("Usage: /row <row_id>")
        return

    row_id = args[0]
    # Search all tabs for this row_id
    try:
        sheets = sheet_manager.get_service()
        found_tab, found_row = _find_row_by_row_id(row_id, sheets)
    except Exception as e:
        await update.effective_message.reply_text(f"Error: {e}")
        return

    if not found_row:
        await update.effective_message.reply_text(f"Row {row_id} not found.")
        return

    scheduled_dt = _extract_scheduled_utc(found_row.get("notes", ""))
    scheduled_txt = _to_display_tz(scheduled_dt.isoformat()) if scheduled_dt else "Not Scheduled"

    msg = (
        f"📋 *Row {row_id}* in `{found_tab}`\n\n"
        f"*Title:* {found_row.get('original_title', 'N/A')[:60]}\n"
        f"*URL:* {found_row.get('source_url', 'N/A')}\n"
        f"*Status:* {found_row.get('status', 'N/A')}\n"
        f"*Duration:* {found_row.get('duration_seconds', '?')}s\n"
        f"*Views:* {found_row.get('view_count', '?')}\n"
        f"*Dest:* {found_row.get('dest_mapping_tags', 'unmapped')}\n"
        f"*Schedule:* {scheduled_txt}\n"
        f"*Errors:* {found_row.get('error_log', 'none')[:100]}\n"
    )

    keyboard = [
        [
            InlineKeyboardButton("🚀 Force Upload", callback_data=f"force_upload:{found_tab}:{found_row['_sheet_row']}"),
            InlineKeyboardButton("🔗 Map To...", callback_data=f"row_map:{found_tab}:{found_row['_sheet_row']}"),
        ],
        [
            InlineKeyboardButton("🔍 Mark Review", callback_data=f"mark_review:{found_tab}:{found_row['_sheet_row']}"),
            InlineKeyboardButton("📝 View Notes", callback_data=f"view_notes:{found_tab}:{found_row['_sheet_row']}"),
        ],
        [
            InlineKeyboardButton("⏰ Schedule", callback_data=f"row_schedule:{found_tab}:{found_row['_sheet_row']}"),
        ],
    ]

    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.effective_message.reply_text(msg, reply_markup=reply_markup, parse_mode="Markdown")


@admin_only
async def cmd_schedule(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/schedule <row_id> <YYYY-MM-DD> <HH:MM> [Timezone] — schedule upload."""
    args = context.args
    if len(args) < 3:
        await update.effective_message.reply_text(
            "Usage: /schedule <row_id> <YYYY-MM-DD> <HH:MM> [Timezone]\n"
            f"Default timezone: {scheduler_config.DISPLAY_TIMEZONE}"
        )
        return

    row_id, date_s, time_s = args[0], args[1], args[2]
    tz_name = args[3] if len(args) >= 4 else scheduler_config.DISPLAY_TIMEZONE
    try:
        tz = ZoneInfo(tz_name)
        local_dt = datetime.fromisoformat(f"{date_s}T{time_s}").replace(tzinfo=tz)
        schedule_utc = local_dt.astimezone(timezone.utc)
    except Exception:
        await update.effective_message.reply_text(
            "Invalid date/time. Use: /schedule 123 2026-02-20 21:30 Asia/Kolkata"
        )
        return

    try:
        sheets = sheet_manager.get_service()
        tab, row = _find_row_by_row_id(row_id, sheets)
        if not row:
            await update.effective_message.reply_text(f"Row {row_id} not found.")
            return

        sheet_row = int(row["_sheet_row"])
        sheet_manager.update_row_status(tab, sheet_row, "READY_TO_UPLOAD", sheets=sheets)
        sheet_manager.append_audit_note(
            tab, sheet_row, f"schedule_at_utc={schedule_utc.strftime('%Y-%m-%dT%H:%M:%SZ')}", sheets
        )
        await update.effective_message.reply_text(
            f"✅ Scheduled row {row_id} for {_to_display_tz(schedule_utc.isoformat())}.\n"
            "Scheduler will upload when time is reached."
        )
    except Exception as e:
        await update.effective_message.reply_text(f"Schedule error: {e}")


@admin_only
async def cmd_map_source(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/map_source — Interactive source→destination mapping."""
    try:
        sheets = sheet_manager.get_service()
        tabs = sheet_manager.get_all_source_tabs(sheets)
    except Exception as e:
        await update.effective_message.reply_text(f"Error: {e}")
        return

    keyboard = []
    for tab in tabs[:10]:
        keyboard.append([
            InlineKeyboardButton(f"📁 {tab}", callback_data=f"map_select_src:{tab}"),
        ])
    keyboard.append([InlineKeyboardButton("❌ Cancel", callback_data="cancel")])

    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.effective_message.reply_text(
        "🔗 *Map Source → Destination*\nSelect source tab:",
        reply_markup=reply_markup,
        parse_mode="Markdown",
    )

@admin_only
async def cmd_mappings(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/mappings — Show all current source→destination mappings."""
    msg = "🗺 *Current Mappings*\n\n"

    # Static mappings from config (#10)
    static = scheduler_config.STATIC_MAPPINGS
    if static:
        msg += "*Static (config-driven):*\n"
        for src, dest in static.items():
            msg += f"  📌 `{src}` → `{dest}`\n"
        msg += "\n"
    else:
        msg += "*Static:* None configured\n\n"

    # Dynamic mappings from sheet
    try:
        sheets = sheet_manager.get_service()
        dynamic = sheet_manager.get_destination_mappings(sheets)
        if dynamic:
            msg += "*Dynamic (per-tab overrides):*\n"
            for m in dynamic:
                src = m.get("source_tag", "?")
                dest = m.get("destination_account_id", "?")
                platform = m.get("platform", "?")
                msg += f"  🔗 `{src}` → `{dest}` ({platform})\n"
        else:
            msg += "*Dynamic:* None set\n"
    except Exception as e:
        msg += f"*Dynamic:* Error reading: {e}\n"

    msg += "\n_Per-row overrides (col W) take priority over all._"
    await update.effective_message.reply_text(msg, parse_mode="Markdown")


@admin_only
async def cmd_health(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/health — System health check: DB, tokens, disk, queue."""
    import shutil

    checks = []

    # 1. Queue DB reachable + stats
    try:
        stats = queue_db.get_queue_stats()
        checks.append(f"✅ *Queue DB*: {stats.get('queued', 0)} queued, "
                       f"{stats.get('in_progress', 0)} in-progress, "
                       f"{stats.get('completed', 0)} completed, "
                       f"{stats.get('failed', 0)} failed")
    except Exception as e:
        checks.append(f"❌ *Queue DB*: unreachable — {e}")

    # 2. OAuth tokens status
    try:
        accounts = oauth_helper.get_all_accounts()
        valid = sum(1 for a in accounts if a.get("token_valid"))
        total = len(accounts)
        icon = "✅" if valid == total else ("⚠️" if valid > 0 else "❌")
        checks.append(f"{icon} *Tokens*: {valid}/{total} valid")
    except Exception as e:
        checks.append(f"❌ *Tokens*: error — {e}")

    # 3. Disk space
    try:
        usage = shutil.disk_usage("/")
        free_pct = (usage.free / usage.total) * 100
        free_gb = usage.free / (1024 ** 3)
        icon = "✅" if free_pct > 20 else ("⚠️" if free_pct > 10 else "❌")
        checks.append(f"{icon} *Disk*: {free_gb:.1f} GB free ({free_pct:.0f}%)")
    except Exception:
        checks.append("❌ *Disk*: check failed")

    # 4. Last upload time
    try:
        last = queue_db.get_last_upload_time_any()
        if last:
            checks.append(f"📤 *Last Upload*: {_to_display_tz(last)}")
        else:
            checks.append("📤 *Last Upload*: none recorded")
    except Exception:
        checks.append("📤 *Last Upload*: unknown")

    msg = "🏥 *System Health*\n\n" + "\n".join(checks)
    await update.effective_message.reply_text(msg, parse_mode="Markdown")


@admin_only
async def cmd_errors(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/errors — Show recent ERROR rows."""
    try:
        sheets = sheet_manager.get_service()
        tabs = sheet_manager.get_all_source_tabs(sheets)
        error_rows = []
        for tab in tabs:
            rows = sheet_manager.read_rows_by_status(tab, "ERROR", sheets)
            error_rows.extend(rows[:5])
    except Exception as e:
        await update.effective_message.reply_text(f"Error: {e}")
        return

    if not error_rows:
        await update.effective_message.reply_text("✅ No error rows found.")
        return

    msg = "❌ *Recent Errors:*\n\n"
    for row in error_rows[:10]:
        msg += (
            f"• Row {row.get('row_id', '?')} in `{row.get('_tab_name', '?')}`\n"
            f"  {row.get('error_log', 'no details')[:80]}\n\n"
        )

    await update.effective_message.reply_text(msg, parse_mode="Markdown")


# ── Callback Query Handler ───────────────────────────────────────

async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle inline keyboard button presses."""
    query = update.callback_query
    await query.answer()

    if not is_admin(query.from_user.id):
        await query.edit_message_text("Unauthorized.")
        return
    if not _rate_limit_check(query.from_user.id):
        await query.edit_message_text("⏱ Please slow down. Rate limit exceeded.")
        return

    data = query.data

    if data == "cancel":
        _clear_pending(query.from_user.id)
        await query.edit_message_text("Cancelled.")
        return

    # ── Main Menu Routing ─────────────────────────────────────
    if data == "status":
        await cmd_status(update, context)
    elif data == "health":
        await cmd_health(update, context)
    elif data == "sources":
        await cmd_sources(update, context)
    elif data == "destinations":
        await cmd_destinations(update, context)
    elif data == "view_errors":
        await cmd_errors(update, context)
    elif data == "help":
        await cmd_help(update, context)
    elif data == "ai_menu":
        _pending_actions[query.from_user.id] = {"action": "ai_row"}
        await query.edit_message_text("Send the row_id here to generate AI metadata.")
    elif data == "src_add":
        await cmd_add_source_prompt(update, context)

    # ── Source info ────────────────────────────────────────────
    elif data.startswith("src_info:"):
        tab = data.split(":", 1)[1]
        try:
            sheets = sheet_manager.get_service()
            pending = len(sheet_manager.read_rows_by_status(tab, "PENDING", sheets))
            ready = len(sheet_manager.read_rows_by_status(tab, "READY_TO_UPLOAD", sheets))
            uploaded = len(sheet_manager.read_rows_by_status(tab, "UPLOADED", sheets))
            errors = len(sheet_manager.read_rows_by_status(tab, "ERROR", sheets))
        except Exception as e:
            await query.edit_message_text(f"Error: {e}")
            return

        await query.edit_message_text(
            f"📁 *{tab}*\n\n"
            f"⏳ PENDING: {pending}\n"
            f"🚀 READY_TO_UPLOAD: {ready}\n"
            f"✅ UPLOADED: {uploaded}\n"
            f"❌ ERROR: {errors}",
            parse_mode="Markdown",
        )

    # ── Source mapping ────────────────────────────────────────
    elif data.startswith("src_map:") or data.startswith("map_select_src:"):
        tab = data.split(":", 1)[1]
        accounts = oauth_helper.get_all_accounts()
        if not accounts:
            await query.edit_message_text("No destination accounts. Use /connect first.")
            return

        keyboard = []
        for acc in accounts:
            if acc.get("token_valid"):
                keyboard.append([InlineKeyboardButton(
                    f"DEST: {acc['account_name']} ({acc['platform']})",
                    callback_data=f"map_dest:{tab}:{acc['account_id']}",
                )])
        keyboard.append([
            InlineKeyboardButton("All PENDING", callback_data=f"map_apply_all:{tab}"),
            InlineKeyboardButton("Cancel", callback_data="cancel"),
        ])

        await query.edit_message_text(
            f"🔗 Mapping `{tab}` → Select destination:",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode="Markdown",
        )

    elif data.startswith("map_apply_all:"):
        # Backward-compatible callback from older buttons.
        tab = data.split(":", 1)[1]
        accounts = oauth_helper.get_all_accounts()
        if not accounts:
            await query.edit_message_text("No destination accounts. Use /connect first.")
            return
        keyboard = []
        for acc in accounts:
            if acc.get("token_valid"):
                keyboard.append([InlineKeyboardButton(
                    f"DEST: {acc['account_name']} ({acc['platform']})",
                    callback_data=f"map_dest:{tab}:{acc['account_id']}",
                )])
        keyboard.append([InlineKeyboardButton("Cancel", callback_data="cancel")])
        await query.edit_message_text(
            f"🔗 Mapping `{tab}` → Select destination:",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode="Markdown",
        )

    # ── Apply mapping to destination ──────────────────────────
    elif data.startswith("map_dest:"):
        parts = data.split(":", 2)
        tab, dest_id = parts[1], parts[2]
        keyboard = [
            [
                InlineKeyboardButton("All PENDING", callback_data=f"map_exec_all:{tab}:{dest_id}"),
                InlineKeyboardButton("Cancel", callback_data="cancel"),
            ],
        ]
        await query.edit_message_text(
            f"Map `{tab}` → `{dest_id}`\nApply to:",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode="Markdown",
        )

    # ── Execute mapping on all PENDING rows ───────────────────
    elif data.startswith("map_exec_all:"):
        parts = data.split(":", 2)
        tab, dest_id = parts[1], parts[2]
        try:
            sheets = sheet_manager.get_service()
            pending_rows = sheet_manager.read_rows_by_status(tab, "PENDING", sheets)
            ready_rows = sheet_manager.read_rows_by_status(tab, "READY_TO_UPLOAD", sheets)
            error_rows = sheet_manager.read_rows_by_status(tab, "ERROR", sheets)
            row_numbers = sorted({r["_sheet_row"] for r in (pending_rows + ready_rows + error_rows)})
            if row_numbers:
                sheet_manager.write_dest_mapping(tab, row_numbers, dest_id, sheets)

            # Also write to global mapping
            account = oauth_helper.get_account(dest_id)
            platform = account.get("platform", "unknown") if account else "unknown"
            sheet_manager.write_global_mapping(tab, dest_id, platform, sheets)

            await query.edit_message_text(
                f"✅ Mapped `{tab}` → `{dest_id}`\n"
                f"Updated {len(row_numbers)} rows (PENDING/READY/ERROR, col W)\n"
                f"Range: `{tab}!W2:W{max(row_numbers) if row_numbers else 2}`\n"
                f"Global mapping also written to destinations_mapping.",
                parse_mode="Markdown",
            )
        except Exception as e:
            await query.edit_message_text(f"Error applying mapping: {e}")

    # ── Force scrape ──────────────────────────────────────────
    elif data.startswith("src_scrape:"):
        tab = data.split(":", 1)[1]
        # If already running, show status instead of re-trigger
        status = _read_scrape_status(tab)
        if status and status.get("state") == "running":
            await query.edit_message_text(
                _format_scrape_status(tab, status),
                parse_mode="Markdown",
            )
            return
        # Create a trigger file that the scraper loop can watch for
        trigger_file = scheduler_config.TEMP_DIR / f"trigger_scrape_{tab}.flag"
        trigger_file.write_text(datetime.now(timezone.utc).isoformat())
        msg = f"🔄 Scrape queued for `{tab}`."
        if status:
            msg += "\n\n" + _format_scrape_status(tab, status)
        await query.edit_message_text(msg, parse_mode="Markdown")

    # ── Force upload ──────────────────────────────────────────
    elif data.startswith("force_upload:"):
        parts = data.split(":", 2)
        tab, sheet_row = parts[1], int(parts[2])
        try:
            sheets = sheet_manager.get_service()
            sheet_manager.update_row_status(tab, sheet_row, "READY_TO_UPLOAD", sheets=sheets)
            sheet_manager.append_audit_note(tab, sheet_row, "admin: force upload via Telegram", sheets)
            await query.edit_message_text(
                f"🚀 Row {sheet_row} in `{tab}` set to READY_TO_UPLOAD.\n"
                f"Scheduler will pick it up on next poll.",
                parse_mode="Markdown",
            )
        except Exception as e:
            await query.edit_message_text(f"Error: {e}")

    # ── Row mapping ───────────────────────────────────────────
    elif data.startswith("row_map:"):
        parts = data.split(":", 2)
        tab, sheet_row = parts[1], parts[2]
        accounts = oauth_helper.get_all_accounts()
        keyboard = []
        for acc in accounts:
            if acc.get("token_valid"):
                keyboard.append([InlineKeyboardButton(
                    f"{acc['account_name']} ({acc['platform']})",
                    callback_data=f"row_map_exec:{tab}:{sheet_row}:{acc['account_id']}",
                )])
        keyboard.append([InlineKeyboardButton("Cancel", callback_data="cancel")])
        await query.edit_message_text(
            f"Map row {sheet_row} in `{tab}` to:",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode="Markdown",
        )

    # ── Row scheduling ─────────────────────────────────────────
    elif data.startswith("row_schedule:"):
        parts = data.split(":")
        tab, sheet_row = parts[1], int(parts[2])
        keyboard = _schedule_picker_keyboard(tab, sheet_row)
        await query.edit_message_text(
            f"Pick schedule for row {sheet_row} in `{tab}`:",
            reply_markup=keyboard,
            parse_mode="Markdown",
        )

    elif data.startswith("set_sched:"):
        _, tab, sheet_row, ts = data.split(":")
        dt = datetime.fromtimestamp(int(ts), tz=timezone.utc)
        try:
            sheets = sheet_manager.get_service()
            _set_row_schedule(tab, int(sheet_row), dt, sheets)
            await query.edit_message_text(
                f"✅ Scheduled for {_to_display_tz(dt.isoformat())}",
                parse_mode="Markdown",
            )
        except Exception as e:
            await query.edit_message_text(f"Error setting schedule: {e}")

    elif data.startswith("clear_sched:"):
        _, tab, sheet_row = data.split(":")
        try:
            sheets = sheet_manager.get_service()
            _clear_row_schedule(tab, int(sheet_row), sheets)
            await query.edit_message_text("🗑 Schedule cleared; will upload ASAP.")
        except Exception as e:
            await query.edit_message_text(f"Error clearing schedule: {e}")

    elif data.startswith("row_map_exec:"):
        parts = data.split(":", 3)
        tab, sheet_row, dest_id = parts[1], int(parts[2]), parts[3]
        try:
            sheets = sheet_manager.get_service()
            sheet_manager.write_dest_mapping(tab, [sheet_row], dest_id, sheets)
            await query.edit_message_text(
                f"✅ Row {sheet_row} in `{tab}` mapped to `{dest_id}`.\n"
                f"Updated cell `{tab}!W{sheet_row}`.",
                parse_mode="Markdown",
            )
        except Exception as e:
            await query.edit_message_text(f"Error: {e}")

    # ── Mark for review ───────────────────────────────────────
    elif data.startswith("mark_review:"):
        parts = data.split(":", 2)
        tab, sheet_row = parts[1], int(parts[2])
        try:
            sheets = sheet_manager.get_service()
            sheet_manager.update_row_status(tab, sheet_row, "PENDING", {
                "manual_flag": "review",
            }, sheets)
            sheet_manager.append_audit_note(tab, sheet_row, "admin: marked for review", sheets)
            await query.edit_message_text(f"🔍 Row {sheet_row} marked for review.")
        except Exception as e:
            await query.edit_message_text(f"Error: {e}")

    # ── Pause source ──────────────────────────────────────────
    elif data.startswith("src_pause:"):
        tab = data.split(":", 1)[1]
        try:
            sheets = sheet_manager.get_service()
            # Write PAUSED marker to master_index
            result = sheets.values().get(
                spreadsheetId=scheduler_config.SPREADSHEET_ID,
                range=f"'master_index'!A:F",
            ).execute()
            rows = result.get("values", [])
            for i, row in enumerate(rows[1:], start=2):
                if row and row[0] == tab:
                    sheets.values().update(
                        spreadsheetId=scheduler_config.SPREADSHEET_ID,
                        range=f"'master_index'!F{i}",
                        valueInputOption="RAW",
                        body={"values": [["PAUSED"]]},
                    ).execute()
                    break
            await query.edit_message_text(f"⏸ Source `{tab}` paused in master_index.")
        except Exception as e:
            await query.edit_message_text(f"Error: {e}")

    # ── OAuth start ───────────────────────────────────────────
    elif data.startswith("oauth_start:"):
        platform = data.split(":", 1)[1]
        try:
            if platform == "youtube":
                url, state = oauth_helper.generate_youtube_oauth_url()
            elif platform == "instagram":
                url, state = oauth_helper.generate_instagram_oauth_url()
            else:
                await query.edit_message_text("Unknown platform.")
                return

            await query.edit_message_text(
                f"🔐 {platform.title()} OAuth\n\n"
                f"Open this link to authorize:\n{url}\n\n"
                f"State token: {state[:16]}...\n"
                f"After completing, copy the failed URL and use /auth <url>.",
                parse_mode=None,
            )
        except Exception as e:
            await query.edit_message_text(f"OAuth error: {e}")

    # ── Connect new (from destinations list) ──────────────────
    elif data == "connect_new":
        keyboard = [
            [
                InlineKeyboardButton("▶️ YouTube", callback_data="oauth_start:youtube"),
                InlineKeyboardButton("📷 Instagram", callback_data="oauth_start:instagram"),
            ],
            [InlineKeyboardButton("❌ Cancel", callback_data="cancel")],
        ]
        await query.edit_message_text(
            "🔐 *Connect New Account*\nChoose platform:",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode="Markdown",
        )

    # ── Destination info ──────────────────────────────────────
    elif data.startswith("dest_info:"):
        account_id = data.split(":", 1)[1]
        account = oauth_helper.get_account(account_id)
        if not account:
            await query.edit_message_text("Account not found.")
            return

        uploads_today = queue_db.get_uploads_today(account_id)
        msg = (
            f"🎯 *{account.get('account_name', account_id)}*\n\n"
            f"Platform: {account.get('platform')}\n"
            f"Status: {account.get('status')}\n"
            f"Token Valid: {'✅' if account.get('token_valid') else '❌'}\n"
            f"Connected: {account.get('connected_at', 'N/A')[:10]}\n"
            f"Last Refresh: {account.get('last_refresh', 'N/A')[:10]}\n"
            f"Uploads Today: {uploads_today}/{scheduler_config.UPLOADS_PER_DAY_PER_DEST}\n"
        )

        keyboard = [
            [
                InlineKeyboardButton("🔄 Refresh Token", callback_data=f"refresh_token:{account_id}"),
                InlineKeyboardButton("❌ Remove", callback_data=f"remove_account:{account_id}"),
            ],
        ]
        await query.edit_message_text(
            msg,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode="Markdown",
        )

    # ── Refresh token ─────────────────────────────────────────
    elif data.startswith("refresh_token:"):
        account_id = data.split(":", 1)[1]
        account = oauth_helper.get_account(account_id)
        if not account:
            await query.edit_message_text("Account not found.")
            return
        try:
            platform = account.get("platform")
            if platform == "youtube":
                success = oauth_helper.refresh_youtube_token(account_id)
            elif platform == "instagram":
                success = oauth_helper.refresh_instagram_token(account_id)
            else:
                success = False
            if success:
                await query.edit_message_text(f"✅ Token refreshed for {account_id}.")
            else:
                await query.edit_message_text(f"❌ Token refresh failed. Reconnect required.")
        except Exception as e:
            await query.edit_message_text(f"Error: {e}")

    # ── Remove account ────────────────────────────────────────
    elif data.startswith("remove_account:"):
        account_id = data.split(":", 1)[1]
        oauth_helper.remove_account(account_id)
        try:
            counts = sheet_manager.deactivate_destination(account_id)
            canceled = queue_db.cancel_jobs_for_dest(account_id)
            msg = (
                f"Account `{account_id}` removed.\n"
                f"Mappings disabled: {counts.get('mappings_disabled',0)}\n"
                f"Rows unmapped: {counts.get('rows_cleared',0)}\n"
                f"Queued jobs canceled: {canceled}"
            )
        except Exception as e:
            msg = f"Account `{account_id}` removed, but sheet cleanup failed: {e}"
        await query.edit_message_text(msg)

    # ── Add source flow (callbacks) ────────────────────────────
    elif data.startswith("src_add_choose:"):
        platform = data.split(":", 1)[1]
        _pending_actions[query.from_user.id] = {"action": "add_source", "platform": platform}
        await query.edit_message_text(
            f"Send the channel URL or @handle for {platform.title()}."
        )

    elif data.startswith("src_add_dest:"):
        dest_id = data.split(":", 1)[1]
        state = _pending_actions.get(query.from_user.id, {})
        if state.get("action") != "add_source":
            await query.edit_message_text("Flow expired. Tap Add Source again.")
            return
        platform = state.get("platform")
        raw_id = state.get("raw_id", "")
        tab_name = state.get("tab_name")
        if not raw_id:
            await query.edit_message_text("Missing channel link. Start again.")
            _clear_pending(query.from_user.id)
            return
        await _complete_add_source(update, platform, raw_id, tab_name, dest_id or None)

    # ── Apply AI metadata ───────────────────────────────────────
    elif data.startswith("apply_ai:"):
        _, tab, sheet_row = data.split(":")
        try:
            sheets = sheet_manager.get_service()
            row = sheet_manager.read_row(tab, int(sheet_row), sheets)
            if not row:
                await query.edit_message_text("Row not found.")
                return
            ai_data = ai_agent.process_row(row)
            hashtags_csv = ",".join(ai_data.get("ai_hashtags", []))
            update_fields = {
                "ai_title": ai_data.get("ai_title", ""),
                "ai_description": ai_data.get("ai_description", ""),
                "ai_hashtags": hashtags_csv,
                "ai_hashtags_csv": hashtags_csv,
                "ai_tags": ai_data.get("ai_tags", ""),
                "category": ai_data.get("category", ""),
                "priority_score": ai_data.get("priority_score", 0),
                "suggested_ffmpeg_cmd": ai_data.get("suggested_ffmpeg_cmd", ""),
                "notes": ai_data.get("notes", ""),
                "manual_flag": "review" if ai_data.get("flagged_for_review") else "",
            }
            sheet_manager.update_row_status(tab, int(sheet_row), "READY_TO_UPLOAD", update_fields, sheets=sheets)
            sheet_manager.append_audit_note(tab, int(sheet_row), "ai: metadata applied via bot", sheets)
            await query.edit_message_text("✅ AI metadata applied and row set READY_TO_UPLOAD.")
        except Exception as e:
            await query.edit_message_text(f"Error applying AI data: {e}")



@admin_only
async def cmd_auth(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/auth <url> — Manually complete OAuth by pasting the redirect URL."""
    args = context.args
    if not args:
        await update.effective_message.reply_text("Usage: /auth <redirect_url_with_code>")
        return

    url = args[0]
    from urllib.parse import urlparse, parse_qs
    try:
        parsed = urlparse(url)
        params = parse_qs(parsed.query)
        code = params.get("code", [None])[0]
        state = params.get("state", [None])[0]

        if not code or not state:
            await update.effective_message.reply_text("❌ Invalid URL. Missing 'code' or 'state'.")
            return

        # Determine platform from state storage
        # We need to peek at stored states or try both?
        # oauth_helper.exchange_... functions check state validity.
        
        # Try YouTube first
        res = oauth_helper.exchange_youtube_code(code, state)
        if "error" not in res:
             await update.effective_message.reply_text(f"✅ Connected YouTube: {res.get('account_name')}")
             return
        elif res["error"] == "invalid_state":
             # Try Instagram
             res = oauth_helper.exchange_instagram_code(code, state)
             if "error" not in res:
                 await update.effective_message.reply_text(f"✅ Connected Instagram: {res.get('account_name')}")
                 return
        
        await update.effective_message.reply_text(f"❌ Failed: {res.get('error')}")

    except Exception as e:
        await update.effective_message.reply_text(f"Error parsing URL: {e}")



@admin_only
async def cmd_set_interval(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/set_interval <time> — Set scraper loop interval (e.g. 5d, 1h, 30m)."""
    args = context.args
    if not args:
        # Show current
        try:
            with open("/home/ubuntu/gravix-agent/scraper_interval.txt", "r") as f:
                val = f.read().strip()
                secs = int(val) if val.isdigit() else 3600
        except FileNotFoundError:
            secs = 3600
        
        hours = secs / 3600
        await update.effective_message.reply_text(f"⏱ Current Interval: {secs} seconds (~{hours:.1f} hours)\nUsage: /set_interval 5d")
        return

    val = args[0].lower()
    seconds = 3600
    try:
        if val.endswith("d"):
            seconds = int(val[:-1]) * 86400
        elif val.endswith("h"):
            seconds = int(val[:-1]) * 3600
        elif val.endswith("m"):
            seconds = int(val[:-1]) * 60
        elif val.isdigit():
            seconds = int(val)
        else:
            await update.effective_message.reply_text("❌ Invalid format. Use 5d, 1h, 30m.")
            return
    except ValueError:
        await update.effective_message.reply_text("❌ Invalid number.")
        return

    # Write to file
    with open("/home/ubuntu/gravix-agent/scraper_interval.txt", "w") as f:
        f.write(str(seconds))

    await update.effective_message.reply_text(
        f"✅ Interval set to {seconds} seconds (~{seconds/3600:.1f} hours).\n"
        "Effect will take place after current sleep cycle ends.\n"
        "To apply immediately: /scrape_now"
    )


# ── Add Source (inline + command) ─────────────────────────────────


def _format_source_summary(tab: str, platform: str, source_id: str) -> str:
    return (
        f"📥 *New Source Ready*\n"
        f"Tab: `{tab}`\n"
        f"Platform: {platform}\n"
        f"Source ID: `{source_id}`"
    )


@admin_only
async def cmd_add_source_prompt(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Open inline menu to add a new source."""
    _clear_pending(update.effective_user.id)
    keyboard = [
        [
            InlineKeyboardButton("▶️ YouTube", callback_data="src_add_choose:youtube"),
            InlineKeyboardButton("📷 Instagram", callback_data="src_add_choose:instagram"),
        ],
        [InlineKeyboardButton("❌ Cancel", callback_data="cancel")],
    ]
    await update.effective_message.reply_text(
        "Choose platform for new source:", reply_markup=InlineKeyboardMarkup(keyboard)
    )


@admin_only
async def cmd_add_source(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/add_source <platform> <channel_url_or_id> [tab_name]"""
    args = context.args
    if len(args) < 2:
        await update.effective_message.reply_text(
            "Usage: /add_source <youtube|instagram> <channel_url_or_id> [tab_name]"
        )
        return
    platform = args[0].lower()
    raw = args[1]
    tab_name = args[2] if len(args) >= 3 else ""
    await _complete_add_source(update, platform, raw, tab_name, dest_id=None)


async def handle_pending_flow(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle free-text replies when a multi-step flow is active."""
    uid = update.effective_user.id
    state = _pending_actions.get(uid, {})
    action = state.get("action")
    text = (update.message.text or "").strip()

    if action == "add_source":
        platform = state.get("platform")
        if not platform:
            await update.effective_message.reply_text("Platform missing. Tap Add Source again.")
            _clear_pending(uid)
            return
        # Store raw id and proposed tab, then prompt for destination mapping
        tab_name = _normalize_source_tab(text)
        _pending_actions[uid] = {
            "action": "add_source",
            "platform": platform,
            "raw_id": text,
            "tab_name": tab_name,
        }
        accounts = oauth_helper.get_all_accounts()
        kb_rows = []
        for acc in accounts:
            if acc.get("token_valid"):
                kb_rows.append([InlineKeyboardButton(
                    f"Map to {acc['account_name']} ({acc['platform']})",
                    callback_data=f"src_add_dest:{acc['account_id']}",
                )])
        if not kb_rows:
            await _complete_add_source(update, platform, text, tab_name, dest_id=None)
            return
        kb_rows.append([InlineKeyboardButton("Skip mapping", callback_data="src_add_dest:")])
        kb_rows.append([InlineKeyboardButton("Cancel", callback_data="cancel")])
        await update.effective_message.reply_text(
            f"Source detected.\nTab: `{tab_name}`\nPlatform: {platform}\nNow pick destination (optional):",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(kb_rows),
        )
        return

    if action == "ai_row":
        try:
            row_id = int(text)
        except ValueError:
            await update.effective_message.reply_text("Send a numeric row_id, e.g., 123")
            return
        await _run_ai_for_row(update, context, row_id)
        _clear_pending(uid)
        return

    # Unknown/expired
    await update.effective_message.reply_text("No active action. Use the menu buttons.")


async def _complete_add_source(update: Update, platform: str, raw_id: str, tab_override: str | None, dest_id: str | None):
    """Create sources.yaml entry, ensure sheet tab, and optionally map destination."""
    platform = platform.lower()
    if platform not in ("youtube", "instagram"):
        await update.effective_message.reply_text("Platform must be youtube or instagram.")
        return

    source_id = _normalize_source_id(platform, raw_id)
    tab_name = _normalize_source_tab(tab_override or source_id)

    try:
        rows = _read_sources_yaml()
    except Exception as e:
        await update.effective_message.reply_text(f"Could not read sources.yaml: {e}")
        return

    if any(r.get("source_tab") == tab_name for r in rows):
        await update.effective_message.reply_text(f"Tab `{tab_name}` already exists in sources.yaml.")
        _clear_pending(update.effective_user.id)
        return

    new_entry = {
        "source_tab": tab_name,
        "source_type": platform,
        "source_id": source_id,
        "scrape_interval_minutes": 360,
        "max_new_per_run": 0,
        "rate_limit_seconds": 3,
    }
    rows.append(new_entry)
    try:
        _write_sources_yaml(rows)
        sheets = scraper_sheets.get_service()
        scraper_sheets.ensure_global_tabs(sheets)
        scraper_sheets.ensure_tab_exists(tab_name, sheets)
        scraper_sheets.update_master_index(tab_name, platform, source_id, sheets)
    except Exception as e:
        await update.effective_message.reply_text(f"Created entry but failed sheet setup: {e}")
        return

    # Optional mapping
    if dest_id:
        try:
            account = oauth_helper.get_account(dest_id)
            platform_name = account.get("platform", "unknown") if account else "unknown"
            sheet_manager.write_global_mapping(tab_name, dest_id, platform_name, sheets)
        except Exception as e:
            await update.effective_message.reply_text(f"Source added but mapping failed: {e}")
            _clear_pending(update.effective_user.id)
            return

    msg = _format_source_summary(tab_name, platform, source_id)
    if dest_id:
        msg += f"\nMapped to `{dest_id}`"
    else:
        msg += "\nMapping: not set (use /mappings or buttons)."
    msg += "\n\nRun /scrape_now to ingest immediately."
    await update.effective_message.reply_text(msg, parse_mode="Markdown")
    _clear_pending(update.effective_user.id)


# ── AI Metadata helper ───────────────────────────────────────────

@admin_only
async def cmd_ai_prompt(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Ask for row id to run AI generator."""
    _pending_actions[update.effective_user.id] = {"action": "ai_row"}
    await update.effective_message.reply_text("Send the row_id to generate fresh AI title/description/hashtags.")


@admin_only
async def cmd_ai(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/ai <row_id> — Run Gemini to produce metadata and preview."""
    if not context.args:
        await cmd_ai_prompt(update, context)
        return
    try:
        row_id = int(context.args[0])
    except ValueError:
        await update.effective_message.reply_text("Row id must be a number.")
        return
    await _run_ai_for_row(update, context, row_id)


async def _run_ai_for_row(update: Update, context: ContextTypes.DEFAULT_TYPE, row_id: int):
    """Generate AI metadata for a sheet row and show preview with apply button."""
    try:
        sheets = sheet_manager.get_service()
        tab, row = _find_row_by_row_id(str(row_id), sheets)
    except Exception as e:
        await update.effective_message.reply_text(f"Error reading sheet: {e}")
        return

    if not row:
        await update.effective_message.reply_text(f"Row {row_id} not found.")
        return

    try:
        ai_data = ai_agent.process_row(row)
    except Exception as e:
        await update.effective_message.reply_text(f"Gemini error: {e}")
        return

    hashtags = ", ".join(ai_data.get("ai_hashtags", [])[:12])
    desc = ai_data.get("ai_description", "")[:420]
    preview = (
        f"🧠 *AI Preview for Row {row_id}* (`{tab}`)\n"
        f"Title: {ai_data.get('ai_title','')[:90]}\n"
        f"Desc: {desc}\n"
        f"Tags: {ai_data.get('ai_tags','')[:120]}\n"
        f"Hashtags: {hashtags}\n"
        f"Category: {ai_data.get('category','')} | Priority: {ai_data.get('priority_score',0)}\n"
        f"Flagged: {ai_data.get('flagged_for_review')}\n"
        f"FFmpeg: {ai_data.get('suggested_ffmpeg_cmd','')}"
    )
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Apply to Sheet", callback_data=f"apply_ai:{tab}:{row['_sheet_row']}")],
        [InlineKeyboardButton("❌ Cancel", callback_data="cancel")],
    ])
    await update.effective_message.reply_text(preview, parse_mode="Markdown", reply_markup=keyboard)

@admin_only
async def cmd_scrape_now(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/scrape_now — Restart scraper service to trigger run immediately."""
    await update.effective_message.reply_text("🔄 Restarting scraper service...")
    
    # Execute systemctl
    import subprocess
    try:
        subprocess.run(["systemctl", "--user", "restart", "gravix-scraper"], check=True)
        await update.effective_message.reply_text("✅ Scraper service restarted. It should start running now.")
    except Exception as e:
        await update.effective_message.reply_text(f"❌ Failed to restart service: {e}")


@admin_only
async def cmd_scrape_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/scrape_status [source_tab] — Show scraper progress/status."""
    tabs = []
    if context.args:
        tabs = [context.args[0]]
    else:
        try:
            sheets = sheet_manager.get_service()
            tabs = sheet_manager.get_all_source_tabs(sheets)
        except Exception:
            tabs = []

    if not tabs:
        await update.effective_message.reply_text("No source tabs found.")
        return

    lines = ["🧾 *Scrape Status*"]
    for tab in tabs[:10]:
        status = _read_scrape_status(tab)
        if status:
            lines.append(_format_scrape_status(tab, status))
        else:
            lines.append(f"📥 *{tab}* — no status yet")
    await update.effective_message.reply_text("\n\n".join(lines), parse_mode="Markdown")



@admin_only
async def cmd_services(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/services [action] [target] — Control system services."""
    args = context.args
    if not args or len(args) < 1:
        await update.effective_message.reply_text(
            "Usage: /services <action> [target]\n"
            "Actions: status, start, stop, restart\n"
            "Targets: all, scraper, scheduler, bot"
        )
        return

    action = args[0].lower()
    target = args[1].lower() if len(args) > 1 else "all"

    map_name = {
        "scraper": "gravix-scraper",
        "scheduler": "gravix-scheduler",
        "bot": "gravix-bot",
        "all": ["gravix-scraper", "gravix-scheduler", "gravix-bot"]
    }

    targets = []
    if target in map_name:
        val = map_name[target]
        targets = val if isinstance(val, list) else [val]
    else:
        await update.effective_message.reply_text("❌ Unknown target. Use: scraper, scheduler, bot, all")
        return

    # Execute
    import subprocess
    try:
        if action == "status":
            msg = "📊 *Service Status:*\n"
            for t in targets:
                res = subprocess.run(["systemctl", "--user", "is-active", t], capture_output=True, text=True)
                status = res.stdout.strip()
                icon = "✅" if status == "active" else "🔴"
                msg += f"{icon} `{t}`: {status}\n"
            await update.effective_message.reply_text(msg, parse_mode="Markdown")
            return

        elif action in ["start", "stop", "restart"]:
            msg = f"⚙️ *{action.title()}* {target}...\n"
            await update.effective_message.reply_text(msg, parse_mode="Markdown")
            for t in targets:
                subprocess.run(["systemctl", "--user", action, t], check=True)
            await update.effective_message.reply_text(f"✅ Action completed.")
            
        else:
             await update.effective_message.reply_text("❌ Unknown action.")

    except Exception as e:
        await update.effective_message.reply_text(f"❌ Error: {e}")


# ── Notify Admin Helper ───────────────────────────────────────────




async def send_admin_alert(app, message: str):
    """Send an alert to all admin Telegram users."""
    bot = app.bot
    for admin_id in scheduler_config.ADMIN_TELEGRAM_IDS:
        try:
            await bot.send_message(chat_id=admin_id, text=f"⚠️ {message}")
        except Exception as e:
            logger.error("Failed to send alert to admin %d: %s", admin_id, e)


# ── Bot setup & run ───────────────────────────────────────────────


# ── Main ──────────────────────────────────────────────────────────

def cmd_help_text():
    return (
        "🤖 *Gravix Bot Help*\n\n"
        "*/start* — Main Menu\n"
        "*/status* — Queue stats\n"
        "*/health* — System health\n"
        "\n"
        "*Configuration & Control:*\n"
        "*/set_interval <time>* — Use '5d', '1h'\n"
        "*/scrape_now* — Trigger scraper immediately\n"
        "*/scrape_status [tab]* — Scraper progress\n"
        "*/services <action> <target>* — Manage system\n"
        "*/add_source <platform> <channel_url_or_id>* — Quick add YouTube/Instagram source\n"
        "*/ai <row_id>* — Generate AI title/desc/hashtags for a row\n"
        "\n"
        "*Mapping:*\n"
        "*/sources* — List tabs\n"
        "*/destinations* — List accounts\n"
        "*/connect* — Add destination\n"
        "*/auth <url>* — Manual connect\n"
    )

async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.effective_message.reply_text(cmd_help_text(), parse_mode="Markdown")


def create_bot_app():
    """Create and configure the Telegram bot application."""
    if not HAS_TELEGRAM:
        logger.error("python-telegram-bot not installed.")
        return None

    if not scheduler_config.TELEGRAM_BOT_TOKEN:
        logger.error("TELEGRAM_BOT_TOKEN not set in .env")
        return None

    app = Application.builder().token(scheduler_config.TELEGRAM_BOT_TOKEN).build()

    # Register command handlers
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("help", cmd_help))
    app.add_handler(CommandHandler("auth", cmd_auth))
    app.add_handler(CommandHandler("status", cmd_status))
    app.add_handler(CommandHandler("sources", cmd_sources))
    app.add_handler(CommandHandler("destinations", cmd_destinations))
    app.add_handler(CommandHandler("connect", cmd_connect))
    app.add_handler(CommandHandler("row", cmd_row))
    app.add_handler(CommandHandler("map_source", cmd_map_source))
    app.add_handler(CommandHandler("errors", cmd_errors))
    app.add_handler(CommandHandler("mappings", cmd_mappings))
    app.add_handler(CommandHandler("health", cmd_health))
    app.add_handler(CommandHandler("add_source", cmd_add_source))
    app.add_handler(CommandHandler("ai", cmd_ai))

    # New Control Commands
    app.add_handler(CommandHandler("set_interval", cmd_set_interval))
    app.add_handler(CommandHandler("scrape_now", cmd_scrape_now))
    app.add_handler(CommandHandler("scrape_status", cmd_scrape_status))
    app.add_handler(CommandHandler("services", cmd_services))
    
    app.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), handle_reply_message))
    app.add_handler(CallbackQueryHandler(handle_callback))

    return app


def run_bot():
    """Start the Telegram bot in polling mode."""
    app = create_bot_app()
    if not app:
        logger.error("Cannot start bot. Check config.")
        return

    logger.info("Telegram bot starting...")
    queue_db.init_db()
    
    # Note: run_polling() is blocking.
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    logging.basicConfig(
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        level=logging.INFO
    )
    run_bot()


if __name__ == "__main__":
    logging.basicConfig(
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        level=logging.INFO
    )
    run_bot()
