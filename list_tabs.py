
import sys
sys.path.append("/home/ubuntu/gravix-agent")
from dotenv import load_dotenv
load_dotenv("/home/ubuntu/gravix-agent/.env")
import sheet_manager
import scheduler_config

try:
    sheets = sheet_manager.get_service()
    meta = sheets.get(spreadsheetId=scheduler_config.SPREADSHEET_ID).execute()
    print("Tabs:")
    for sheet in meta.get("sheets", []):
        print(f"- {sheet['properties']['title']}")
except Exception as e:
    print(f"Error: {e}")
