
import sys
sys.path.append("/home/ubuntu/gravix-agent")
from dotenv import load_dotenv
load_dotenv("/home/ubuntu/gravix-agent/.env")
import queue_db

conn = queue_db._get_conn()
try:
    rows = conn.execute("SELECT * FROM upload_queue").fetchall()
    print(f"Total rows in queue: {len(rows)}")
    for r in rows:
        print(dict(r))
finally:
    conn.close()
