#!/usr/bin/env python3
"""
setup_sheet.py — One-time script to initialize the Google Sheet.
Writes headers and inserts sample test rows.
"""

import logging
import sys

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("setup_sheet")

import sheets_client


def main():
    logger.info("Connecting to Google Sheets...")
    sheets = sheets_client.get_service()

    logger.info("Setting up headers...")
    created = sheets_client.setup_headers(sheets)
    if created:
        logger.info("Headers created successfully.")
    else:
        logger.info("Headers already existed.")

    logger.info("Inserting sample rows...")
    sheets_client.insert_sample_rows(sheets)
    logger.info("Done! Check your Google Sheet.")


if __name__ == "__main__":
    main()
