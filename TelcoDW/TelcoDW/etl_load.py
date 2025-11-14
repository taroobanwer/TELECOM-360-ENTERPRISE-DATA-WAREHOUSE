from sqlalchemy import create_engine, text
from pathlib import Path

def load_to_mysql(df_fact, mysql_url):
    print(f"[INFO] Connecting to MySQL...")
    engine = create_engine(mysql_url, future=True)
    ddl_file = Path(__file__).resolve().parent / "telco_dw_ddl.sql"

    print("[INFO] Running DDL...")
    with engine.begin() as conn:
        ddl_sql = ddl_file.read_text(encoding="utf-8")
        for stmt in ddl_sql.split(";"):
            s = stmt.strip()
            if s:
                conn.execute(text(s))
    print("[INFO] DDL done.")

    if df_fact.empty:
        print("[WARN] Empty fact dataset. Nothing to load.")
        return

    cols = ["customer_id","monthly_charges","total_charges","tenure_months","churn_flag","churn_label"]
    missing = [c for c in cols if c not in df_fact.columns]
    if missing:
        raise ValueError(f"Missing required columns in dataset: {missing}")

    print(f"[INFO] Loading {len(df_fact)} rows into stg_churn...")
    # Load into staging table (column names must match stg_churn)
    df_fact[cols].to_sql("stg_churn", engine, if_exists="append", index=False)
    print("✅ Data load into staging complete.")
