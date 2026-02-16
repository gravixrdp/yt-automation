#!/usr/bin/env python3
"""
sheet_archiver.py — Gap #5: Archive old rows and monitor sheet health.
Moves UPLOADED/ERROR rows older than N days to archive tabs.
Checks total cell count and warns/alarms at thresholds.

Usage:
    python sheet_archiver.py --archive --days 30
    python sheet_archiver.py --health-check
"""

import argparse
import logging
import sys
from datetime import datetime, timezone, timedelta

import scheduler_config

logger = logging.getLogger("sheet_archiver")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


def get_service():
    from google.oauth2.service_account import Credentials
    from googleapiclient.discovery import build
    creds = Credentials.from_service_account_file(
        scheduler_config.GOOGLE_SVC_JSON,
        scopes=scheduler_config.SHEETS_SCOPES,
    )
    service = build("sheets", "v4", credentials=creds, cache_discovery=False)
    return service.spreadsheets()


def get_all_source_tabs(sheets) -> list[str]:
    meta = sheets.get(spreadsheetId=scheduler_config.SPREADSHEET_ID).execute()
    return [s["properties"]["title"] for s in meta.get("sheets", [])
            if s["properties"]["title"].startswith("source__")]


def check_sheet_health(sheets=None) -> dict:
    """
    Count total cells across all tabs. Warn at 5M, alarm at 8M.
    Returns dict with total_cells, status, tab_counts.
    """
    sheets = sheets or get_service()
    meta = sheets.get(spreadsheetId=scheduler_config.SPREADSHEET_ID).execute()

    total_cells = 0
    tab_counts = {}
    for s in meta.get("sheets", []):
        props = s["properties"]
        title = props["title"]
        grid = props.get("gridProperties", {})
        rows = grid.get("rowCount", 0)
        cols = grid.get("columnCount", 0)
        cells = rows * cols
        tab_counts[title] = {"rows": rows, "cols": cols, "cells": cells}
        total_cells += cells

    status = "ok"
    if total_cells >= scheduler_config.SHEET_CELL_ALARM_THRESHOLD:
        status = "ALARM"
        logger.error(
            "SHEET ALARM: %d cells (threshold: %d). Archive immediately!",
            total_cells, scheduler_config.SHEET_CELL_ALARM_THRESHOLD,
        )
    elif total_cells >= scheduler_config.SHEET_CELL_WARN_THRESHOLD:
        status = "WARNING"
        logger.warning(
            "Sheet cell count warning: %d cells (threshold: %d).",
            total_cells, scheduler_config.SHEET_CELL_WARN_THRESHOLD,
        )
    else:
        logger.info("Sheet health OK: %d total cells.", total_cells)

    return {"total_cells": total_cells, "status": status, "tab_counts": tab_counts}


def _ensure_archive_tab(year: int, sheets) -> str:
    """Ensure an archive_YYYY tab exists with proper headers."""
    tab_name = f"archive_{year}"
    meta = sheets.get(spreadsheetId=scheduler_config.SPREADSHEET_ID).execute()
    existing = [s["properties"]["title"] for s in meta.get("sheets", [])]
    if tab_name not in existing:
        sheets.batchUpdate(
            spreadsheetId=scheduler_config.SPREADSHEET_ID,
            body={"requests": [{"addSheet": {"properties": {"title": tab_name}}}]},
        ).execute()
        # Copy headers from first source tab
        source_tabs = [t for t in existing if t.startswith("source__")]
        if source_tabs:
            hdr_result = sheets.values().get(
                spreadsheetId=scheduler_config.SPREADSHEET_ID,
                range=f"'{source_tabs[0]}'!A1:W1",
            ).execute()
            headers = hdr_result.get("values", [[]])
            if headers:
                sheets.values().update(
                    spreadsheetId=scheduler_config.SPREADSHEET_ID,
                    range=f"'{tab_name}'!A1:W1",
                    valueInputOption="RAW",
                    body={"values": headers},
                ).execute()
        logger.info("Created archive tab: %s", tab_name)
    return tab_name


def archive_completed_rows(days_old: int = 30, sheets=None) -> dict:
    """
    Move UPLOADED and ERROR rows older than `days_old` to archive_YYYY tabs.
    Returns stats dict.
    """
    sheets = sheets or get_service()
    cutoff = datetime.now(timezone.utc) - timedelta(days=days_old)
    cutoff_str = cutoff.strftime("%Y-%m-%dT%H:%M:%SZ")
    year = datetime.now(timezone.utc).year

    stats = {"archived": 0, "tabs_processed": 0, "errors": 0}
    source_tabs = get_all_source_tabs(sheets)

    for tab in source_tabs:
        try:
            result = sheets.values().get(
                spreadsheetId=scheduler_config.SPREADSHEET_ID,
                range=f"'{tab}'!A:W",
            ).execute()
            all_rows = result.get("values", [])
            if len(all_rows) < 2:
                continue

            headers = all_rows[0]
            status_idx = headers.index("status") if "status" in headers else None
            time_idx = headers.index("last_attempt_time_utc") if "last_attempt_time_utc" in headers else None
            if status_idx is None:
                continue

            # Find rows to archive (scan from bottom to avoid index shifts)
            rows_to_archive = []
            for i, row in enumerate(all_rows[1:], start=2):
                padded = row + [""] * (len(headers) - len(row))
                status = padded[status_idx].strip().upper()
                if status not in ("UPLOADED", "ERROR"):
                    continue
                if time_idx is not None:
                    ts = padded[time_idx].strip()
                    if ts and ts > cutoff_str:
                        continue  # Too recent
                rows_to_archive.append((i, padded))

            if not rows_to_archive:
                continue

            # Ensure archive tab
            archive_tab = _ensure_archive_tab(year, sheets)

            # Append rows to archive
            archive_values = [row for _, row in rows_to_archive]
            sheets.values().append(
                spreadsheetId=scheduler_config.SPREADSHEET_ID,
                range=f"'{archive_tab}'!A:W",
                valueInputOption="RAW",
                insertDataOption="INSERT_ROWS",
                body={"values": archive_values},
            ).execute()

            # Delete from source (bottom-up to preserve indices)
            delete_requests = []
            sheet_id = None
            meta = sheets.get(spreadsheetId=scheduler_config.SPREADSHEET_ID).execute()
            for s in meta.get("sheets", []):
                if s["properties"]["title"] == tab:
                    sheet_id = s["properties"]["sheetId"]
                    break
            if sheet_id is not None:
                for row_num, _ in sorted(rows_to_archive, reverse=True):
                    delete_requests.append({
                        "deleteDimension": {
                            "range": {
                                "sheetId": sheet_id,
                                "dimension": "ROWS",
                                "startIndex": row_num - 1,  # 0-indexed
                                "endIndex": row_num,
                            }
                        }
                    })
                if delete_requests:
                    sheets.batchUpdate(
                        spreadsheetId=scheduler_config.SPREADSHEET_ID,
                        body={"requests": delete_requests},
                    ).execute()

            stats["archived"] += len(rows_to_archive)
            stats["tabs_processed"] += 1
            logger.info("Archived %d rows from %s to %s.", len(rows_to_archive), tab, archive_tab)

        except Exception as e:
            logger.error("Error archiving tab %s: %s", tab, e)
            stats["errors"] += 1

    logger.info("Archive complete: %d rows archived from %d tabs.", stats["archived"], stats["tabs_processed"])
    return stats


def main():
    parser = argparse.ArgumentParser(description="Gravix Sheet Archiver")
    parser.add_argument("--archive", action="store_true", help="Archive old completed rows")
    parser.add_argument("--days", type=int, default=30, help="Archive rows older than N days")
    parser.add_argument("--health-check", action="store_true", help="Check sheet cell count")
    args = parser.parse_args()

    if args.health_check:
        result = check_sheet_health()
        print(f"Total cells: {result['total_cells']:,}")
        print(f"Status: {result['status']}")
        for tab, info in result["tab_counts"].items():
            print(f"  {tab}: {info['rows']} rows × {info['cols']} cols = {info['cells']:,} cells")
        return

    if args.archive:
        stats = archive_completed_rows(days_old=args.days)
        print(f"Archived: {stats['archived']} rows from {stats['tabs_processed']} tabs")
        if stats["errors"]:
            print(f"Errors: {stats['errors']}")
        return

    parser.print_help()


if __name__ == "__main__":
    main()
