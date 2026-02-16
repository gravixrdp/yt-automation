from google.oauth2.service_account import Credentials
import sys

print('Starting key validation...')
sys.stdout.flush()

try:
    creds = Credentials.from_service_account_file(
        './service_account.json', 
        scopes=['https://www.googleapis.com/auth/spreadsheets']
    )
    print(f'✅ Key validation passed: {creds.service_account_email}')
except Exception as e:
    print(f'❌ Key validation failed: {e}')
