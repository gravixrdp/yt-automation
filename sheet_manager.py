"""
sheet_manager.py — Unified Google Sheets read/write wrapper for Part 3.
Provides safe, centralized access with audit logging.
"""

import logging
from datetime import datetime, timezone
from typing import Any

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

import scheduler_config

logger = logging.getLogger(__name__)
ROW_READ_RANGE = "A:AZ"


def get_service():
    """Authenticate and return Sheets API spreadsheets resource."""
    creds = Credentials.from_service_account_file(
        scheduler_config.GOOGLE_SVC_JSON,
        scopes=scheduler_config.SHEETS_SCOPES,
    )
    service = build("sheets", "v4", credentials=creds, cache_discovery=False)
    return service.spreadsheets()


def _now_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _col_to_letter(col_index_1_based: int) -> str:
    """Convert 1-based column index to A1 notation letters (1 -> A, 27 -> AA)."""
    result = ""
    n = col_index_1_based
    while n > 0:
        n, rem = divmod(n - 1, 26)
        result = chr(ord("A") + rem) + result
    return result


def _get_header_col_map(tab_name: str, sheets) -> dict[str, str]:
    """Return mapping like {'status': 'O', ...} by reading tab headers."""
    try:
        hdr = sheets.values().get(
            spreadsheetId=scheduler_config.SPREADSHEET_ID,
            range=f"'{tab_name}'!A1:AZ1",
        ).execute().get("values", [[]])[0]
    except HttpError:
        return {}
    out: dict[str, str] = {}
    for idx, name in enumerate(hdr, start=1):
        if name:
            out[name] = _col_to_letter(idx)
    return out


# ── Tab discovery ─────────────────────────────────────────────────

def get_all_source_tabs(sheets=None) -> list[str]:
    """Return all tab names that start with 'source__'."""
    sheets = sheets or get_service()
    meta = sheets.get(spreadsheetId=scheduler_config.SPREADSHEET_ID).execute()
    return [s["properties"]["title"] for s in meta.get("sheets", [])
            if s["properties"]["title"].startswith("source__")]


# ── Row reading ───────────────────────────────────────────────────

def read_rows_by_status(tab_name: str, status: str, sheets=None) -> list[dict]:
    """Read all rows in a tab with a given status (col O for scraper tabs)."""
    sheets = sheets or get_service()
    try:
        result = sheets.values().get(
            spreadsheetId=scheduler_config.SPREADSHEET_ID,
            range=f"'{tab_name}'!{ROW_READ_RANGE}",
        ).execute()
    except HttpError as e:
        logger.error("Failed to read tab %s: %s", tab_name, e)
        return []

    all_rows = result.get("values", [])
    if len(all_rows) < 2:
        return []

    headers = all_rows[0]
    matching = []
    for i, row in enumerate(all_rows[1:], start=2):
        padded = row + [""] * (len(headers) - len(row))
        row_dict = dict(zip(headers, padded))
        row_dict["_sheet_row"] = i
        row_dict["_tab_name"] = tab_name
        if row_dict.get("status", "").strip().upper() == status.upper():
            matching.append(row_dict)
    return matching


def read_ready_rows(sheets=None) -> list[dict]:
    """Read all READY_TO_UPLOAD rows across all source tabs."""
    sheets = sheets or get_service()
    tabs = get_all_source_tabs(sheets)
    all_ready = []
    for tab in tabs:
        rows = read_rows_by_status(tab, "READY_TO_UPLOAD", sheets)
        all_ready.extend(rows)
    logger.info("Found %d READY_TO_UPLOAD rows across %d tabs.", len(all_ready), len(tabs))
    return all_ready


def read_row(tab_name: str, sheet_row: int, sheets=None) -> dict | None:
    """Read a single row by tab name and row number."""
    sheets = sheets or get_service()
    try:
        result = sheets.values().get(
            spreadsheetId=scheduler_config.SPREADSHEET_ID,
            range=f"'{tab_name}'!A{sheet_row}:AZ{sheet_row}",
        ).execute()
    except HttpError:
        return None

    rows = result.get("values", [])
    if not rows:
        return None

    # Get headers
    hdr = sheets.values().get(
        spreadsheetId=scheduler_config.SPREADSHEET_ID,
        range=f"'{tab_name}'!A1:AZ1",
    ).execute().get("values", [[]])[0]

    padded = rows[0] + [""] * (len(hdr) - len(rows[0]))
    row_dict = dict(zip(hdr, padded))
    row_dict["_sheet_row"] = sheet_row
    row_dict["_tab_name"] = tab_name
    return row_dict


# ── Row updating ──────────────────────────────────────────────────

def update_row_status(
    tab_name: str, sheet_row: int, status: str,
    extra_fields: dict | None = None, sheets=None,
    expected_status: str | None = None,
):
    """
    Update status (col O) and optional extra fields for a row.
    If expected_status is set, performs optimistic locking: only writes
    if current status matches expected_status.

    Raises ValueError("STATUS_CONFLICT") if a conflict is detected.
    """
    sheets = sheets or get_service()
    now = _now_utc()

    # Gap #4: Optimistic locking — read-before-write
    if expected_status is not None:
        try:
            result = sheets.values().get(
                spreadsheetId=scheduler_config.SPREADSHEET_ID,
                range=f"'{tab_name}'!O{sheet_row}",
            ).execute()
            current = (result.get("values", [[""]])[0][0] or "").strip().upper()
            if current and current != expected_status.upper():
                logger.warning(
                    "STATUS_CONFLICT on %s row %d: expected %s, found %s",
                    tab_name, sheet_row, expected_status, current,
                )
                raise ValueError(f"STATUS_CONFLICT: expected={expected_status}, actual={current}")
        except HttpError as e:
            logger.warning("Lock check failed for %s row %d: %s", tab_name, sheet_row, e)
            # Proceed anyway on API errors to avoid blocking

    updates = [{"range": f"'{tab_name}'!O{sheet_row}", "values": [[status]]}]
    updates.append({"range": f"'{tab_name}'!Q{sheet_row}", "values": [[now]]})

    if extra_fields:
        # Base map for legacy scraper schema + dynamic map from actual headers.
        col_map = {
            "status": "O",
            "upload_attempts": "P",
            "last_attempt_time_utc": "Q",
            "notes": "R",
            "error_log": "S",
            "manual_flag": "V",
            "dest_mapping_tags": "W",
            # Backward compatibility when uploaded_url column doesn't exist.
            "uploaded_url": "R",
        }
        dynamic_map = _get_header_col_map(tab_name, sheets)
        col_map.update(dynamic_map)
        for field, value in extra_fields.items():
            if field in col_map:
                updates.append({
                    "range": f"'{tab_name}'!{col_map[field]}{sheet_row}",
                    "values": [[str(value)]],
                })

    sheets.values().batchUpdate(
        spreadsheetId=scheduler_config.SPREADSHEET_ID,
        body={"valueInputOption": "RAW", "data": updates},
    ).execute()
    logger.debug("Updated row %d in %s: status=%s", sheet_row, tab_name, status)


def append_audit_note(tab_name: str, sheet_row: int, note: str, sheets=None):
    """Append a timestamped audit note to the notes column (R)."""
    sheets = sheets or get_service()
    now = _now_utc()
    # Read existing notes
    try:
        result = sheets.values().get(
            spreadsheetId=scheduler_config.SPREADSHEET_ID,
            range=f"'{tab_name}'!R{sheet_row}",
        ).execute()
        existing = result.get("values", [[""]])[0][0] if result.get("values") else ""
    except (HttpError, IndexError):
        existing = ""

    new_note = f"[{now}] {note}"
    if existing:
        combined = f"{existing}; {new_note}"
    else:
        combined = new_note

    sheets.values().update(
        spreadsheetId=scheduler_config.SPREADSHEET_ID,
        range=f"'{tab_name}'!R{sheet_row}",
        valueInputOption="RAW",
        body={"values": [[combined]]},
    ).execute()


def mark_uploaded(
    tab_name: str, sheet_row: int, uploaded_url: str,
    platform: str, dest_account: str, sheets=None,
):
    """Mark a row as successfully uploaded with full metadata."""
    sheets = sheets or get_service()
    now = _now_utc()

    # Read current upload_attempts
    try:
        result = sheets.values().get(
            spreadsheetId=scheduler_config.SPREADSHEET_ID,
            range=f"'{tab_name}'!P{sheet_row}",
        ).execute()
        attempts = int(result.get("values", [["0"]])[0][0] or 0)
    except (HttpError, ValueError, IndexError):
        attempts = 0

    update_row_status(tab_name, sheet_row, "UPLOADED", {
        "upload_attempts": attempts + 1,
        "uploaded_url": uploaded_url,
        "notes": f"uploaded to {platform} {dest_account}: {uploaded_url}",
    }, sheets)
    append_audit_note(
        tab_name, sheet_row,
        f"uploader: uploaded to {platform} {dest_account}",
        sheets,
    )


def mark_upload_error(
    tab_name: str, sheet_row: int, error_msg: str,
    retryable: bool = True, sheets=None,
):
    """Mark a row with upload error. Set back to READY_TO_UPLOAD if retryable."""
    sheets = sheets or get_service()
    try:
        result = sheets.values().get(
            spreadsheetId=scheduler_config.SPREADSHEET_ID,
            range=f"'{tab_name}'!P{sheet_row}",
        ).execute()
        attempts = int(result.get("values", [["0"]])[0][0] or 0)
    except (HttpError, ValueError, IndexError):
        attempts = 0

    attempts += 1
    if retryable and attempts < scheduler_config.MAX_UPLOAD_ATTEMPTS:
        new_status = "READY_TO_UPLOAD"
    else:
        new_status = "ERROR"

    update_row_status(tab_name, sheet_row, new_status, {
        "upload_attempts": attempts,
        "error_log": error_msg,
    }, sheets)
    append_audit_note(tab_name, sheet_row, f"uploader error: {error_msg[:100]}", sheets)


def write_dest_mapping(
    tab_name: str, sheet_rows: list[int], dest_account_id: str, sheets=None,
):
    """Write destination mapping tags for multiple rows."""
    sheets = sheets or get_service()
    if not sheet_rows:
        return
    data = [
        {
            "range": f"'{tab_name}'!W{row_num}",
            "values": [[dest_account_id]],
        }
        for row_num in sheet_rows
    ]
    sheets.values().batchUpdate(
        spreadsheetId=scheduler_config.SPREADSHEET_ID,
        body={"valueInputOption": "RAW", "data": data},
    ).execute()
    logger.info("Wrote dest_mapping '%s' to %d rows in %s.", dest_account_id, len(sheet_rows), tab_name)


def write_global_mapping(source_tab: str, dest_account_id: str, platform: str, sheets=None):
    """Write a mapping entry to the destinations_mapping global tab."""
    sheets = sheets or get_service()
    values = [source_tab, dest_account_id, platform, "TRUE", ""]
    sheets.values().append(
        spreadsheetId=scheduler_config.SPREADSHEET_ID,
        range="'destinations_mapping'!A:E",
        valueInputOption="RAW",
        insertDataOption="INSERT_ROWS",
        body={"values": [values]},
    ).execute()
    logger.info("Added global mapping: %s -> %s (%s)", source_tab, dest_account_id, platform)


def get_destination_mappings(sheets=None) -> list[dict]:
    """Read all active destination mappings from the global tab."""
    sheets = sheets or get_service()
    try:
        result = sheets.values().get(
            spreadsheetId=scheduler_config.SPREADSHEET_ID,
            range="'destinations_mapping'!A:E",
        ).execute()
    except HttpError:
        return []

    rows = result.get("values", [])
    if len(rows) < 2:
        return []

    headers = rows[0]
    mappings = []
    for row in rows[1:]:
        padded = row + [""] * (len(headers) - len(row))
        d = dict(zip(headers, padded))
        if d.get("active", "").upper() == "TRUE":
            mappings.append(d)
    return mappings


def get_uploaded_hashes_for_dest(dest_account_id: str, days: int = 30, sheets=None) -> set[str]:
    """Get content hashes uploaded to a destination in the past N days."""
    sheets = sheets or get_service()
    from datetime import timedelta
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
    tabs = get_all_source_tabs(sheets)
    hashes = set()
    for tab in tabs:
        rows = read_rows_by_status(tab, "UPLOADED", sheets)
        for row in rows:
            if row.get("dest_mapping_tags", "").strip() == dest_account_id:
                if row.get("last_attempt_time_utc", "") >= cutoff:
                    h = row.get("content_hash", "").strip()
                    if h:
                        hashes.add(h)
    return hashes


def deactivate_destination(dest_account_id: str, sheets=None) -> dict:
    """
    Disable mappings pointing to a destination and clear per-row mapping tags.
    Returns counts: {"rows_cleared": int, "mappings_disabled": int}
    """
    sheets = sheets or get_service()
    result = {"rows_cleared": 0, "mappings_disabled": 0}

    # 1) Mark global mappings inactive
    try:
        resp = sheets.values().get(
            spreadsheetId=scheduler_config.SPREADSHEET_ID,
            range="'destinations_mapping'!A:E",
        ).execute()
        rows = resp.get("values", [])
    except HttpError:
        rows = []

    if rows:
        header = rows[0]
        updates = []
        for idx, row in enumerate(rows[1:], start=2):
            padded = row + [""] * (len(header) - len(row))
            if len(padded) >= 2 and padded[1] == dest_account_id and padded[3].upper() == "TRUE":
                updates.append({
                    "range": f"'destinations_mapping'!D{idx}",
                    "values": [["FALSE"]],
                })
        if updates:
            sheets.values().batchUpdate(
                spreadsheetId=scheduler_config.SPREADSHEET_ID,
                body={"valueInputOption": "RAW", "data": updates},
            ).execute()
            result["mappings_disabled"] = len(updates)

    # 2) Clear dest_mapping_tags from source tabs
    tabs = get_all_source_tabs(sheets)
    updates: list[dict] = []
    note_updates: list[dict] = []
    for tab in tabs:
        try:
            col = sheets.values().get(
                spreadsheetId=scheduler_config.SPREADSHEET_ID,
                range=f"'{tab}'!W2:W",
            ).execute().get("values", [])
            notes_col = sheets.values().get(
                spreadsheetId=scheduler_config.SPREADSHEET_ID,
                range=f"'{tab}'!R2:R",
            ).execute().get("values", [])
        except HttpError:
            continue
        for i, cell in enumerate(col, start=2):
            if cell and cell[0].strip() == dest_account_id:
                updates.append({
                    "range": f"'{tab}'!W{i}",
                    "values": [[""]],
                })
                existing_note = notes_col[i-2][0] if i-2 < len(notes_col) and notes_col[i-2] else ""
                note_val = (existing_note + "; " if existing_note else "") + "dest_removed"
                note_updates.append({
                    "range": f"'{tab}'!R{i}",
                    "values": [[note_val]],
                })
    if updates:
        sheets.values().batchUpdate(
            spreadsheetId=scheduler_config.SPREADSHEET_ID,
            body={"valueInputOption": "RAW", "data": updates},
        ).execute()
        result["rows_cleared"] = len(updates)
    if note_updates:
        sheets.values().batchUpdate(
            spreadsheetId=scheduler_config.SPREADSHEET_ID,
            body={"valueInputOption": "RAW", "data": note_updates},
        ).execute()

    logger.info("Deactivated dest %s: %s", dest_account_id, result)
    return result
