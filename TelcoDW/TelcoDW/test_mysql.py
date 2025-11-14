# test_mysql.py
import sys
from sqlalchemy import create_engine, text

URL = sys.argv[1] if len(sys.argv) > 1 else "mysql+pymysql://root:password@localhost:3306/telco_dw"

print(f"Connecting to: {URL}")
engine = create_engine(URL, future=True)

try:
    with engine.begin() as conn:
        res = conn.execute(text("SELECT VERSION()")).scalar()
        print("✅ Connected. MySQL version:", res)
except Exception as e:
    print("❌ Connection failed:")
    print(type(e).__name__, e)
    raise
