
import sys
sys.path.append("/home/ubuntu/gravix-agent")
from dotenv import load_dotenv
load_dotenv("/home/ubuntu/gravix-agent/.env")
import sheet_manager
import logging

logging.basicConfig(level=logging.INFO)

try:
    sheets = sheet_manager.get_service()
    tab_name = "source__modox_recap"
    row_num = 3 # Row 3
    
    print(f"Resetting Row {row_num} in {tab_name} to READY_TO_UPLOAD...")
    # Fix: use update_row_status and pass extra_fields dict
    sheet_manager.update_row_status(
        tab_name, 
        row_num, 
        "READY_TO_UPLOAD", 
        extra_fields={"error_log": ""},
        sheets=sheets
    )
    print("Done.")

except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()
