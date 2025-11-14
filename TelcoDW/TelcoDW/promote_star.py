"""
Promote from staging -> star schema (dims + fact with surrogate keys).

Run from terminal (NOT inside this file):
  python promote_star.py --mysql "mysql+pymysql://root:pass@localhost:3306/telco_dw"
"""

import argparse
import pandas as pd
import numpy as np
from pathlib import Path
from sqlalchemy import create_engine, text

BASE = Path(__file__).resolve().parent

FILES = {
    "core":     BASE / "Telco_customer_churn.xlsx",
    "fallback": BASE / "CustomerChurn.xlsx",
    "demo":     BASE / "Telco_customer_churn_demographics.xlsx",
    "loc":      BASE / "Telco_customer_churn_location.xlsx",
    "pop":      BASE / "Telco_customer_churn_population.xlsx",
    "svc":      BASE / "Telco_customer_churn_services.xlsx",
}

def _load_xlsx(p: Path) -> pd.DataFrame:
    try:
        return pd.read_excel(p)
    except Exception:
        return pd.DataFrame()

def _find_col(df: pd.DataFrame, candidates):
    norm = {c.lower().replace(" ",""): c for c in df.columns}
    for cand in candidates:
        key = cand.lower().replace(" ","")
        if key in norm:
            return norm[key]
    return None

def _norm_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [c.strip() for c in df.columns]
    return df

def upsert_ignore(engine, df: pd.DataFrame, table: str):
    """
    Load df into a temporary table, then INSERT IGNORE into target table.
    Execute INSERT and DROP in *separate* statements (no multi-statement).
    """
    if df.empty:
        return

    tmp = f"tmp_{table}"
    df.to_sql(tmp, engine, if_exists="replace", index=False)
    cols = ", ".join(f"`{c}`" for c in df.columns)

    insert_sql = f"INSERT IGNORE INTO `{table}` ({cols}) SELECT {cols} FROM `{tmp}`"
    drop_sql   = f"DROP TABLE `{tmp}`"

    # Run as two separate executes to avoid 1064 near 'DROP TABLE ...'
    with engine.begin() as conn:
        conn.execute(text(insert_sql))
        conn.execute(text(drop_sql))

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mysql", required=True, help="mysql+pymysql://user:pass@host:3306/telco_dw")
    args = ap.parse_args()
    eng = create_engine(args.mysql, future=True)

    # -------- Load sources --------
    df_core = _load_xlsx(FILES["core"])
    if df_core.empty:
        df_core = _load_xlsx(FILES["fallback"])
    df_demo = _load_xlsx(FILES["demo"])
    df_loc  = _load_xlsx(FILES["loc"])
    df_pop  = _load_xlsx(FILES["pop"])
    df_svc  = _load_xlsx(FILES["svc"])

    for name in ["df_core","df_demo","df_loc","df_pop","df_svc"]:
        obj = locals()[name]
        if not obj.empty:
            locals()[name] = _norm_cols(obj)

    # -------- dim_customer (all attrs optional, no KeyErrors) --------
    cid_core = _find_col(df_core, ["CustomerID","Customer ID","customer_id","Id"])
    cid_demo = _find_col(df_demo, ["CustomerID","Customer ID","customer_id","Id"])

    customer_ids = pd.Series(dtype=object)
    if cid_core: customer_ids = df_core[cid_core]
    elif cid_demo: customer_ids = df_demo[cid_demo]

    dim_customer = pd.DataFrame({"customer_id": customer_ids.dropna().astype(str).drop_duplicates()})

    def pick(src: pd.DataFrame, want: str, candidates):
        if src.empty: return None
        c = _find_col(src, candidates)
        return src[c] if c else None

    # collect optional attributes from demo/core
    gender = pick(df_demo, "gender", ["gender"])
    senior = pick(df_demo, "senior_citizen", ["SeniorCitizen","senior_citizen"])
    partner = pick(df_demo, "partner", ["Partner","partner"])
    depend = pick(df_demo, "dependents", ["Dependents","dependents"])
    agegrp = pick(df_demo, "age_group", ["AgeGroup","age_group"])

    # Merge by detected key
    for series, candidates in [
        (gender,   ["CustomerID","Customer ID","customer_id","Id"]),
        (senior,   ["CustomerID","Customer ID","customer_id","Id"]),
        (partner,  ["CustomerID","Customer ID","customer_id","Id"]),
        (depend,   ["CustomerID","Customer ID","customer_id","Id"]),
        (agegrp,   ["CustomerID","Customer ID","customer_id","Id"]),
    ]:
        if series is not None:
            key = _find_col(df_demo, candidates)
            if key:
                tmp = pd.DataFrame({"customer_id": df_demo[key].astype(str), series.name if series.name else "val": series})
                colname = series.name if series.name else "val"
                # rename to our canonical if needed
                rename = {
                    "SeniorCitizen":"senior_citizen",
                    "Dependents":"dependents",
                    "Partner":"partner",
                    "AgeGroup":"age_group",
                }
                colname_out = rename.get(colname, colname.lower())
                tmp = tmp.rename(columns={colname: colname_out})
                dim_customer = dim_customer.merge(tmp[["customer_id", colname_out]], on="customer_id", how="left")

    # normalize booleans to 0/1 where applicable
    for b in ["senior_citizen","partner","dependents"]:
        if b in dim_customer.columns:
            dim_customer[b] = dim_customer[b].astype(str).str.lower().map(
                {"yes":1,"y":1,"true":1,"1":1,"no":0,"n":0,"false":0,"0":0}
            )

    # ensure all expected columns exist (fill with None if missing)
    for col in ["gender","senior_citizen","partner","dependents","age_group"]:
        if col not in dim_customer.columns:
            dim_customer[col] = None

    # -------- dim_services --------
    svc_cols = ["phone_service","multiple_lines","internet_service","online_security","online_backup",
                "device_protection","tech_support","streaming_tv","streaming_movies",
                "paperless_billing","contract","payment_method"]

    def extract_services(df: pd.DataFrame) -> pd.DataFrame:
        if df.empty: return pd.DataFrame(columns=svc_cols)
        cols = {}
        for c in df.columns:
            k = c.strip().lower().replace(" ","_")
            if k in svc_cols:
                cols[k] = df[c]
        if not cols: return pd.DataFrame(columns=svc_cols)
        out = pd.DataFrame(cols)
        # use map (not applymap) to avoid FutureWarning
        for c in out.columns:
            out[c] = out[c].map(lambda x: str(x).strip() if pd.notna(x) else x)
        return out

    dim_services = pd.concat([extract_services(df_svc), extract_services(df_core)], axis=0, ignore_index=True)
    dim_services = dim_services.drop_duplicates() if not dim_services.empty else dim_services

    # -------- dim_geography --------
    geo_cols = ["state","city","zip_code","county","latitude","longitude"]
    dim_geography = pd.DataFrame(columns=geo_cols)
    if not df_loc.empty:
        rename = {}
        for c in df_loc.columns:
            k = c.strip().lower().replace(" ","_")
            if k in geo_cols: rename[c] = k
        dim_geography = df_loc.rename(columns=rename)
        dim_geography = dim_geography[[c for c in geo_cols if c in dim_geography.columns]].drop_duplicates()

    # -------- dim_population --------
    pop_cols = ["city","state","population","population_density","median_income","unemployment_rate"]
    dim_population = pd.DataFrame(columns=pop_cols)
    if not df_pop.empty:
        rename = {}
        for c in df_pop.columns:
            k = c.strip().lower().replace(" ","_")
            if k in pop_cols: rename[c] = k
        dim_population = df_pop.rename(columns=rename)
        dim_population = dim_population[[c for c in pop_cols if c in dim_population.columns]].drop_duplicates()

    # -------- dim_time (today) --------
    with eng.begin() as conn:
        conn.execute(text("""
            INSERT IGNORE INTO dim_time (date_sk, d_date, year, quarter, month, day)
            SELECT DATE_FORMAT(CURDATE(), '%Y%m%d') + 0,
                   CURDATE(),
                   YEAR(CURDATE()),
                   QUARTER(CURDATE()),
                   MONTH(CURDATE()),
                   DAY(CURDATE());
        """))

    # -------- Upserts (INSERT IGNORE) --------
    # dim_customer needs only (customer_id, gender, senior_citizen, partner, dependents, age_group)
    upsert_ignore(eng, dim_customer[["customer_id","gender","senior_citizen","partner","dependents","age_group"]], "dim_customer")
    if not dim_geography.empty:
        upsert_ignore(eng, dim_geography, "dim_geography")
    if not dim_population.empty:
        upsert_ignore(eng, dim_population, "dim_population")
    if not dim_services.empty:
        upsert_ignore(eng, dim_services, "dim_services")

        # -------- Build fact from staging + lookups --------
    stg = pd.read_sql("SELECT * FROM stg_churn", eng)
    if stg.empty:
        print("[WARN] stg_churn is empty. Run main_etl.py first.")
        return

    date_sk = int(pd.Timestamp.today().strftime("%Y%m%d"))
    stg["date_sk"] = date_sk

    d_customer = pd.read_sql("SELECT customer_sk, customer_id FROM dim_customer", eng)
    stg = stg.merge(d_customer, on="customer_id", how="left")

    # services FK best-effort: try to match by any overlapping service columns present in core file
    service_sk = pd.Series([np.nan]*len(stg), name="service_sk")
    stg["service_sk"] = service_sk  # keep as nullable; full mapping can be added later

    # geo/pop not mapped yet (needs clear keys in your files)
    stg["geo_sk"] = np.nan
    stg["population_sk"] = np.nan

    fact_cols = ["customer_sk","geo_sk","population_sk","service_sk","date_sk",
                 "tenure_months","monthly_charges","total_charges","churn_flag","churn_label"]
    for c in fact_cols:
        if c not in stg.columns:
            stg[c] = np.nan if c != "churn_label" else None

    fact = stg[fact_cols].copy()
    fact.to_sql("fact_churn", eng, if_exists="append", index=False)
    print(f"[DONE] Inserted {len(fact)} rows into fact_churn.")

if __name__ == "__main__":
    main()
