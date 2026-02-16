"""
sheets_client.py — Google Sheets read/write operations.
Uses the Google Sheets API v4 with a service account.
"""

import logging
from typing import Any

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

import config

logger = logging.getLogger(__name__)


def get_service():
    """Authenticate with the service account and return a Sheets API service."""
    creds = Credentials.from_service_account_file(
        config.SERVICE_ACCOUNT_FILE,
        scopes=config.SHEETS_SCOPES,
    )
    service = build("sheets", "v4", credentials=creds, cache_discovery=False)
    return service.spreadsheets()


def setup_headers(sheets=None):
    """Write the header row to the sheet if it is empty."""
    sheets = sheets or get_service()
    # Check if row 1 is already populated
    result = sheets.values().get(
        spreadsheetId=config.SPREADSHEET_ID,
        range=f"{config.SHEET_NAME}!A1:Y1",
    ).execute()
    existing = result.get("values", [])
    if existing and any(cell.strip() for cell in existing[0]):
        logger.info("Headers already exist — skipping.")
        return False

    sheets.values().update(
        spreadsheetId=config.SPREADSHEET_ID,
        range=f"{config.SHEET_NAME}!A1",
        valueInputOption="RAW",
        body={"values": [config.HEADERS]},
    ).execute()
    logger.info("Wrote %d headers to row 1.", len(config.HEADERS))
    return True


def insert_sample_rows(sheets=None):
    """Insert sample data rows for testing."""
    sheets = sheets or get_service()
    sample_rows = [
        [
            1,
            "MrShortsExample",
            "mrshorts_tab",
            "https://youtube.com/shorts/xyz123",
            "Incredible last-minute cricket catch!",
            18,
            152300,
            "", "", "", "", "",    # H-L  (AI output cols — empty)
            "", "", "", "", "",    # M-Q  (AI output cols — empty)
            "", "",               # R-S  (AI output cols — empty)
            "https://i.ytimg.com/vi/xyz123/hqdefault.jpg",
            "sha256:abcd1234567890",
            "PENDING",            # V  status
            "",                   # W  processed_at
            "",                   # X  agent_version
            "",                   # Y  error_log
        ],
        [
            2,
            "FunnyViralClips",
            "funnyvirals_tab",
            "https://youtube.com/shorts/abc456",
            "Dog does the funniest thing ever 😂",
            12,
            520000,
            "", "", "", "", "",
            "", "", "", "", "",
            "", "",
            "https://i.ytimg.com/vi/abc456/hqdefault.jpg",
            "sha256:efgh5678901234",
            "PENDING",
            "",
            "",
            "",
        ],
        [
            3,
            "TechMinute",
            "techminute_tab",
            "https://youtube.com/shorts/def789",
            "New iPhone feature you MUST try!",
            45,
            89000,
            "", "", "", "", "",
            "", "", "", "", "",
            "", "",
            "https://i.ytimg.com/vi/def789/hqdefault.jpg",
            "sha256:ijkl9012345678",
            "PENDING",
            "",
            "",
            "",
        ],
    ]

    # Convert all values to strings for the sheet
    str_rows = [[str(cell) for cell in row] for row in sample_rows]

    start_row = 2
    end_row = start_row + len(str_rows) - 1
    range_str = f"{config.SHEET_NAME}!A{start_row}:Y{end_row}"

    sheets.values().update(
        spreadsheetId=config.SPREADSHEET_ID,
        range=range_str,
        valueInputOption="RAW",
        body={"values": str_rows},
    ).execute()
    logger.info("Inserted %d sample rows.", len(str_rows))


def read_all_rows(sheets=None) -> list[dict[str, Any]]:
    """Read all data rows (skip header) and return list of dicts."""
    sheets = sheets or get_service()
    result = sheets.values().get(
        spreadsheetId=config.SPREADSHEET_ID,
        range=f"{config.SHEET_NAME}!A:Y",
    ).execute()
    all_rows = result.get("values", [])
    if len(all_rows) < 2:
        return []

    headers = all_rows[0]
    data_rows = []
    for i, row in enumerate(all_rows[1:], start=2):
        # Pad short rows
        padded = row + [""] * (len(headers) - len(row))
        row_dict = dict(zip(headers, padded))
        row_dict["_sheet_row"] = i  # 1-indexed row in sheet
        data_rows.append(row_dict)
    return data_rows


def read_pending_rows(sheets=None) -> list[dict[str, Any]]:
    """Read rows where status is PENDING or empty."""
    all_rows = read_all_rows(sheets)
    pending = []
    for row in all_rows:
        status = row.get("status", "").strip().upper()
        if status in ("", "PENDING"):
            pending.append(row)
    return pending


def write_row_results(sheet_row: int, data: dict[str, Any], sheets=None):
    """
    Write AI outputs and agent metadata to a specific row.
    sheet_row is 1-indexed (the actual row number in the spreadsheet).
    """
    sheets = sheets or get_service()

    # Build the update for columns H–Y (indices 7–24)
    update_values = [
        str(data.get("ai_title", "")),
        str(data.get("ai_description", "")),
        str(data.get("ai_hashtags_csv", "")),
        str(data.get("ai_tags", "")),
        str(data.get("category", "")),
        str(data.get("priority_score", "")),
        str(data.get("priority_reason", "")),
        str(data.get("suggested_ffmpeg_cmd", "") or ""),
        str(data.get("ffmpeg_reason", "") or ""),
        str(data.get("flagged_for_review", "")),
        str(", ".join(data.get("review_reasons", [])) if data.get("review_reasons") else ""),
        str(data.get("notes", "") or ""),
        # Skip T (thumbnail_url) and U (content_hash) — those are input cols
    ]

    # Write H–S (columns 8–19 → H:S)
    range_hs = f"{config.SHEET_NAME}!H{sheet_row}:S{sheet_row}"
    sheets.values().update(
        spreadsheetId=config.SPREADSHEET_ID,
        range=range_hs,
        valueInputOption="RAW",
        body={"values": [update_values]},
    ).execute()

    # Write V–Y (status, processed_at, agent_version, error_log)
    meta_values = [
        str(data.get("status", "DONE")),
        str(data.get("processed_at", "")),
        str(data.get("agent_version", "")),
        str(data.get("error_log", "")),
    ]
    range_vy = f"{config.SHEET_NAME}!V{sheet_row}:Y{sheet_row}"
    sheets.values().update(
        spreadsheetId=config.SPREADSHEET_ID,
        range=range_vy,
        valueInputOption="RAW",
        body={"values": [meta_values]},
    ).execute()

    logger.info("Wrote results to row %d (status=%s).", sheet_row, data.get("status", "DONE"))


def write_error(sheet_row: int, error_msg: str, sheets=None):
    """Mark a row as ERROR and write the error message."""
    sheets = sheets or get_service()
    from datetime import datetime, timezone

    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    range_vy = f"{config.SHEET_NAME}!V{sheet_row}:Y{sheet_row}"
    sheets.values().update(
        spreadsheetId=config.SPREADSHEET_ID,
        range=range_vy,
        valueInputOption="RAW",
        body={"values": [["ERROR", now, config.HEADERS[-2], error_msg]]},
    ).execute()
    logger.warning("Marked row %d as ERROR: %s", sheet_row, error_msg[:80])
