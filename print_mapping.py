
import sys
sys.path.append("/home/ubuntu/gravix-agent")
from dotenv import load_dotenv
load_dotenv("/home/ubuntu/gravix-agent/.env")
import sheet_manager
import scheduler_config

try:
    sheets = sheet_manager.get_service()
    result = sheets.values().get(
        spreadsheetId=scheduler_config.SPREADSHEET_ID,
        range="'destinations_mapping'!A:E",
    ).execute()
    rows = result.get("values", [])
    print("Destinations Mapping Content:")
    for row in rows:
        print(row)
except Exception as e:
    print(f"Error: {e}")
