
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
import scraper_config

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
creds = Credentials.from_service_account_file(scraper_config.GOOGLE_SVC_JSON, scopes=SCOPES)
service = build('sheets', 'v4', credentials=creds)
sheets = service.spreadsheets()

# 1. Clear source__apna_scenario
try:
    # Clear range A2:Z
    sheets.values().clear(
        spreadsheetId=scraper_config.SPREADSHEET_ID,
        range="source__apna_scenario!A2:Z"
    ).execute()
    print("✅ Cleared source__apna_scenario content (kept header)")
except Exception as e:
    print(f"⚠️ Failed to clear tab: {e}")

# 2. Reset master_index
try:
    result = sheets.values().get(
        spreadsheetId=scraper_config.SPREADSHEET_ID,
        range="'master_index'!A:F"
    ).execute()
    rows = result.get("values", [])
    
    for i, row in enumerate(rows):
        if i == 0: continue
        if row and row[0] == "source__apna_scenario":
            # Update row i+1
            # Reset LastScraped(D) -> "", VideoCount(E) -> "0", Status(F) -> "ACTIVE"
            # In A1 notation: D{i+1}:F{i+1}
            range_name = f"'master_index'!D{i+1}:F{i+1}"
            sheets.values().update(
                spreadsheetId=scraper_config.SPREADSHEET_ID,
                range=range_name,
                valueInputOption="RAW",
                body={"values": [["", "0", "ACTIVE"]]}
            ).execute()
            print(f"✅ Reset master_index row {i+1} for source__apna_scenario")
            break
except Exception as e:
    print(f"❌ Failed to update master_index: {e}")
