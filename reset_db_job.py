
import sys
sys.path.append("/home/ubuntu/gravix-agent")
from dotenv import load_dotenv
load_dotenv("/home/ubuntu/gravix-agent/.env")
import queue_db

conn = queue_db._get_conn()
try:
    print("Deleting ALL jobs from upload_queue...")
    conn.execute("DELETE FROM upload_queue")
    conn.execute("DELETE FROM idempotency_keys")
    conn.commit()
    print(f"Deleted {conn.total_changes} rows (combined).")
finally:
    conn.close()
