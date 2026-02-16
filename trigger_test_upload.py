
import sys
sys.path.append("/home/ubuntu/gravix-agent")
from dotenv import load_dotenv
from pathlib import Path
load_dotenv("/home/ubuntu/gravix-agent/.env")
import sheet_manager
import scheduler_config
import oauth_helper

# Get Service
try:
    sheets = sheet_manager.get_service()
    dest_id = "yt_UCblQ1oHhx6jUrqp5uqKf5Rg"
    tab_name = "source__modox_recap"

    # 1. Map Globally
    print(f"Mapping {tab_name} -> {dest_id}...")
    sheet_manager.write_global_mapping(tab_name, dest_id, "youtube", sheets)

    # 2. Find ERROR Row (retry)
    print("Reading ERROR rows...")
    rows = sheet_manager.read_rows_by_status(tab_name, "ERROR", sheets)
    if not rows:
        print("No ERROR rows found! Checking PENDING...")
        rows = sheet_manager.read_rows_by_status(tab_name, "PENDING", sheets)
    
    if not rows:
        print("No rows found to trigger.")
        sys.exit(1)

    target_row = rows[0]
    row_num = target_row["_sheet_row"]
    title = target_row.get("Title", "Unknown")

    print(f"Selecting Row {row_num}: {title}")

    # 3. Update to READY_TO_UPLOAD
    print(f"Setting Row {row_num} to READY_TO_UPLOAD...")
    sheet_manager.update_row_status(tab_name, row_num, "READY_TO_UPLOAD", sheets=sheets)
    sheet_manager.append_audit_note(tab_name, row_num, "admin: test upload via script", sheets)

    print("Done. Scheduler should pick it up.")
except Exception as e:
    print(f"Error: {e}")
    sys.exit(1)
