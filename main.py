"""
main.py — Orchestrator for the Gravix AI Content Agent.
CLI entry point with rate limiting, retries, and logging.
"""

import argparse
import json
import logging
import sys
import time
from datetime import datetime, timezone

import config
import sheets_client
import ai_agent

# ── Logging setup ─────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(config.LOG_DIR / "agent.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger("gravix-agent")


def process_single_row(row: dict, sheets, dry_run: bool = False) -> dict | None:
    """
    Process one row with retries and error handling.
    Returns the AI result dict or None on failure.
    """
    row_id = row.get("row_id", "?")
    sheet_row = row.get("_sheet_row", 0)

    for attempt in range(1, config.MAX_RETRIES + 1):
        try:
            logger.info(
                "Processing row %s (sheet row %d) — attempt %d/%d",
                row_id, sheet_row, attempt, config.MAX_RETRIES,
            )
            result = ai_agent.process_row(row)

            # Inject agent metadata
            result["status"] = "DONE"
            result["processed_at"] = datetime.now(timezone.utc).strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            )

            if dry_run:
                print("\n" + "=" * 60)
                print(f"DRY RUN — Row {row_id} (sheet row {sheet_row})")
                print("=" * 60)
                print(json.dumps(result, indent=2, ensure_ascii=False))
                print("=" * 60 + "\n")
            else:
                sheets_client.write_row_results(sheet_row, result, sheets)
                logger.info("✓ Row %s written successfully.", row_id)

            return result

        except Exception as e:
            logger.error(
                "Attempt %d failed for row %s: %s", attempt, row_id, e
            )
            if attempt < config.MAX_RETRIES:
                wait = config.RETRY_BACKOFF_BASE * (2 ** (attempt - 1))
                logger.info("Retrying in %.1fs...", wait)
                time.sleep(wait)
            else:
                error_msg = f"All {config.MAX_RETRIES} attempts failed. Last error: {e}"
                logger.error(error_msg)
                if not dry_run:
                    sheets_client.write_error(sheet_row, error_msg, sheets)
                return None


def main():
    parser = argparse.ArgumentParser(
        description="Gravix AI Content Agent — Process video metadata from Google Sheets",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Process rows but print results instead of writing to sheet",
    )
    parser.add_argument(
        "--row-id",
        type=int,
        default=None,
        help="Process only a specific row_id",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Process all PENDING rows",
    )
    parser.add_argument(
        "--setup",
        action="store_true",
        help="Initialize sheet headers and insert sample data",
    )
    args = parser.parse_args()

    # ── Setup mode ─────────────────────────────────────────────────
    if args.setup:
        logger.info("Setting up Google Sheet...")
        sheets = sheets_client.get_service()
        sheets_client.setup_headers(sheets)
        sheets_client.insert_sample_rows(sheets)
        logger.info("Sheet setup complete.")
        return

    # ── Validate credentials ───────────────────────────────────────
    if not config.GEMINI_API_KEY or config.GEMINI_API_KEY == "your_gemini_api_key_here":
        logger.error(
            "GEMINI_API_KEY not set. Edit .env file with your API key."
        )
        sys.exit(1)

    import os
    if not os.path.isfile(config.SERVICE_ACCOUNT_FILE):
        logger.error(
            "Service account file not found: %s\n"
            "Download it from GCP Console and save it to the project directory.",
            config.SERVICE_ACCOUNT_FILE,
        )
        sys.exit(1)

    # ── Read rows ──────────────────────────────────────────────────
    sheets = sheets_client.get_service()
    pending = sheets_client.read_pending_rows(sheets)

    if args.row_id is not None:
        pending = [r for r in pending if str(r.get("row_id", "")) == str(args.row_id)]
        if not pending:
            logger.warning("No pending row found with row_id=%d", args.row_id)
            sys.exit(0)

    if not pending:
        logger.info("No pending rows to process.")
        sys.exit(0)

    logger.info("Found %d pending row(s) to process.", len(pending))

    # ── Process rows with rate limiting ────────────────────────────
    min_interval = 1.0 / config.RATE_LIMIT_RPS
    processed = 0
    errors = 0
    last_call = 0.0

    for row in pending:
        # Rate limiting
        now = time.time()
        elapsed = now - last_call
        if elapsed < min_interval:
            time.sleep(min_interval - elapsed)

        last_call = time.time()
        result = process_single_row(row, sheets, dry_run=args.dry_run)
        if result:
            processed += 1
        else:
            errors += 1

    # ── Summary ────────────────────────────────────────────────────
    logger.info(
        "Processing complete: %d succeeded, %d failed out of %d total.",
        processed, errors, len(pending),
    )
    if errors > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
