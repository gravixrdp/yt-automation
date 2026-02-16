"""
scraper_sheets.py — Google Sheets operations for the scraper.
Manages per-source tabs, auto-creation, master_index, and dedupe lookups.
"""

import logging
from datetime import datetime, timezone
from typing import Any

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

import scraper_config

logger = logging.getLogger(__name__)


def get_service():
    """Authenticate and return a Sheets API spreadsheets resource."""
    creds = Credentials.from_service_account_file(
        scraper_config.GOOGLE_SVC_JSON,
        scopes=scraper_config.SHEETS_SCOPES,
    )
    service = build("sheets", "v4", credentials=creds, cache_discovery=False)
    return service.spreadsheets()


# ── Tab management ────────────────────────────────────────────────

def _get_existing_tabs(sheets) -> list[str]:
    """Return list of existing tab names in the spreadsheet."""
    meta = sheets.get(spreadsheetId=scraper_config.SPREADSHEET_ID).execute()
    return [s["properties"]["title"] for s in meta.get("sheets", [])]


def _create_tab(tab_name: str, sheets):
    """Create a new tab in the spreadsheet."""
    body = {
        "requests": [{
            "addSheet": {
                "properties": {"title": tab_name}
            }
        }]
    }
    sheets.batchUpdate(
        spreadsheetId=scraper_config.SPREADSHEET_ID, body=body
    ).execute()
    logger.info("Created tab: %s", tab_name)


def _write_headers(tab_name: str, headers: list[str], sheets):
    """Write header row to column A of the given tab and freeze row 1."""
    sheets.values().update(
        spreadsheetId=scraper_config.SPREADSHEET_ID,
        range=f"'{tab_name}'!A1",
        valueInputOption="RAW",
        body={"values": [headers]},
    ).execute()

    # Get the sheet ID for freeze request
    meta = sheets.get(spreadsheetId=scraper_config.SPREADSHEET_ID).execute()
    sheet_id = None
    for s in meta.get("sheets", []):
        if s["properties"]["title"] == tab_name:
            sheet_id = s["properties"]["sheetId"]
            break

    if sheet_id is not None:
        sheets.batchUpdate(
            spreadsheetId=scraper_config.SPREADSHEET_ID,
            body={"requests": [{
                "updateSheetProperties": {
                    "properties": {
                        "sheetId": sheet_id,
                        "gridProperties": {"frozenRowCount": 1},
                    },
                    "fields": "gridProperties.frozenRowCount",
                }
            }]},
        ).execute()
    logger.info("Wrote %d headers + froze row 1 on '%s'.", len(headers), tab_name)


def ensure_tab_exists(tab_name: str, sheets=None) -> bool:
    """
    Ensure a source tab exists with correct headers.
    Returns True if the tab was newly created.
    """
    sheets = sheets or get_service()
    existing = _get_existing_tabs(sheets)
    if tab_name in existing:
        return False
    _create_tab(tab_name, sheets)
    _write_headers(tab_name, scraper_config.SCRAPER_HEADERS, sheets)
    return True


def ensure_global_tabs(sheets=None):
    """Create destinations_mapping and master_index tabs if missing."""
    sheets = sheets or get_service()
    existing = _get_existing_tabs(sheets)

    if scraper_config.TAB_MASTER_INDEX not in existing:
        _create_tab(scraper_config.TAB_MASTER_INDEX, sheets)
        _write_headers(
            scraper_config.TAB_MASTER_INDEX,
            scraper_config.MASTER_INDEX_HEADERS,
            sheets,
        )

    if scraper_config.TAB_DESTINATIONS_MAPPING not in existing:
        _create_tab(scraper_config.TAB_DESTINATIONS_MAPPING, sheets)
        _write_headers(
            scraper_config.TAB_DESTINATIONS_MAPPING,
            scraper_config.DESTINATIONS_MAPPING_HEADERS,
            sheets,
        )


# ── Dedupe lookups ────────────────────────────────────────────────

def get_existing_urls(tab_name: str, sheets=None) -> set[str]:
    """
    Return a set of all source_url values in a tab (column E).
    """
    sheets = sheets or get_service()
    try:
        result = sheets.values().get(
            spreadsheetId=scraper_config.SPREADSHEET_ID,
            range=f"'{tab_name}'!E:E",
        ).execute()
    except HttpError as e:
        if e.resp.status == 400:
            return set()
        raise
    rows = result.get("values", [])
    # Skip header row
    return {row[0].strip() for row in rows[1:] if row and row[0].strip()}


def get_all_content_hashes(sheets=None) -> set[str]:
    """
    Return a set of content_hash values across ALL source tabs.
    Capped at HASH_CACHE_MAX_SIZE entries.
    """
    sheets = sheets or get_service()
    existing_tabs = _get_existing_tabs(sheets)
    source_tabs = [t for t in existing_tabs if t.startswith("source__")]

    all_hashes: set[str] = set()
    for tab in source_tabs:
        try:
            result = sheets.values().get(
                spreadsheetId=scraper_config.SPREADSHEET_ID,
                range=f"'{tab}'!L:L",  # content_hash column
            ).execute()
        except HttpError:
            continue
        rows = result.get("values", [])
        for row in rows[1:]:
            if row and row[0].strip():
                all_hashes.add(row[0].strip())
                if len(all_hashes) >= scraper_config.HASH_CACHE_MAX_SIZE:
                    logger.warning(
                        "Hash cache reached max size (%d). Some hashes may be missed.",
                        scraper_config.HASH_CACHE_MAX_SIZE,
                    )
                    return all_hashes
    logger.info("Loaded %d content hashes from %d tabs.", len(all_hashes), len(source_tabs))
    return all_hashes


# ── Row operations ────────────────────────────────────────────────

def get_next_row_id(tab_name: str, sheets=None) -> int:
    """
    Get the next auto-increment row_id for a tab.
    Reads column A (row_id) and returns max + 1.
    """
    sheets = sheets or get_service()
    try:
        result = sheets.values().get(
            spreadsheetId=scraper_config.SPREADSHEET_ID,
            range=f"'{tab_name}'!A:A",
        ).execute()
    except HttpError:
        return 1
    rows = result.get("values", [])
    max_id = 0
    for row in rows[1:]:
        if row and row[0].strip():
            try:
                val = int(row[0])
                if val > max_id:
                    max_id = val
            except ValueError:
                continue
    return max_id + 1


def get_row_count(tab_name: str, sheets=None) -> int:
    """Return the number of data rows (excluding header) in a tab."""
    sheets = sheets or get_service()
    try:
        result = sheets.values().get(
            spreadsheetId=scraper_config.SPREADSHEET_ID,
            range=f"'{tab_name}'!A:A",
        ).execute()
    except HttpError:
        return 0
    rows = result.get("values", [])
    # Subtract header row, count non-empty
    return max(0, sum(1 for r in rows[1:] if r and r[0].strip()))


def append_row(tab_name: str, row_data: dict[str, Any], sheets=None):
    """
    Append a single row to a source tab.
    row_data keys should match SCRAPER_HEADERS.
    """
    sheets = sheets or get_service()
    values = [str(row_data.get(h, "")) for h in scraper_config.SCRAPER_HEADERS]

    sheets.values().append(
        spreadsheetId=scraper_config.SPREADSHEET_ID,
        range=f"'{tab_name}'!A:W",
        valueInputOption="RAW",
        insertDataOption="INSERT_ROWS",
        body={"values": [values]},
    ).execute()
    logger.debug("Appended row %s to '%s'.", row_data.get("row_id", "?"), tab_name)


def update_row_status(
    tab_name: str, sheet_row: int, status: str, error_msg: str = "", sheets=None,
):
    """Update status (col O) and error_log (col S) for a specific row."""
    sheets = sheets or get_service()
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    # Update O (status) and S (error_log)
    sheets.values().batchUpdate(
        spreadsheetId=scraper_config.SPREADSHEET_ID,
        body={
            "valueInputOption": "RAW",
            "data": [
                {
                    "range": f"'{tab_name}'!O{sheet_row}",
                    "values": [[status]],
                },
                {
                    "range": f"'{tab_name}'!Q{sheet_row}",
                    "values": [[now]],
                },
                {
                    "range": f"'{tab_name}'!S{sheet_row}",
                    "values": [[error_msg]],
                },
            ],
        },
    ).execute()


# ── Master index ──────────────────────────────────────────────────

def update_master_index(tab_name: str, source_type: str, source_id: str, sheets=None):
    """
    Update or insert a row in master_index for the given source tab.
    """
    sheets = sheets or get_service()
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    row_count = get_row_count(tab_name, sheets)

    # Read existing master_index to find if tab already listed
    try:
        result = sheets.values().get(
            spreadsheetId=scraper_config.SPREADSHEET_ID,
            range=f"'{scraper_config.TAB_MASTER_INDEX}'!A:F",
        ).execute()
    except HttpError:
        result = {"values": []}

    rows = result.get("values", [])
    target_row = None
    for i, row in enumerate(rows[1:], start=2):
        if row and row[0] == tab_name:
            target_row = i
            break

    new_values = [tab_name, source_type, source_id, now, str(row_count), "ACTIVE"]

    if target_row:
        # Update existing row
        sheets.values().update(
            spreadsheetId=scraper_config.SPREADSHEET_ID,
            range=f"'{scraper_config.TAB_MASTER_INDEX}'!A{target_row}:F{target_row}",
            valueInputOption="RAW",
            body={"values": [new_values]},
        ).execute()
    else:
        # Append new row
        sheets.values().append(
            spreadsheetId=scraper_config.SPREADSHEET_ID,
            range=f"'{scraper_config.TAB_MASTER_INDEX}'!A:F",
            valueInputOption="RAW",
            insertDataOption="INSERT_ROWS",
            body={"values": [new_values]},
        ).execute()
    logger.info(
        "Updated master_index: %s → %d rows, scraped at %s",
        tab_name, row_count, now,
    )
