
import sys
sys.path.append("/home/ubuntu/gravix-agent")
from dotenv import load_dotenv
load_dotenv("/home/ubuntu/gravix-agent/.env")
import sheet_manager
import scheduler_config

try:
    sheets = sheet_manager.get_service()
    tab_name = "source__modox_recap"
    # Row 3 (the one previously pending or set to ready)
    # Check rows 2, 3, 4
    for r_num in [2, 3, 4]:
        row = sheet_manager.read_row(tab_name, r_num, sheets)
        if row:
            print(f"Row {r_num}: Status={row.get('Status')} Error={row.get('error_log')}")
except Exception as e:
    print(f"Error: {e}")
