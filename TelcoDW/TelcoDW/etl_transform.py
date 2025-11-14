import pandas as pd
import numpy as np

def _find_col(df, candidates):
    norm = {c.lower().replace(" ", ""): c for c in df.columns}
    for cand in candidates:
        key = cand.lower().replace(" ", "")
        if key in norm:
            return norm[key]
    return None

def transform(data: dict) -> pd.DataFrame:
    df_core = data.get("TelcoCustomerChurn")
    if df_core is None or df_core.empty:
        df_core = data.get("CustomerChurn")
    if df_core is None or df_core.empty:
        raise ValueError("No core churn dataset found.")

    df_core = df_core.copy()
    df_core.columns = [c.strip() for c in df_core.columns]

    # Accept common variants from your files
    cust_id = _find_col(df_core, ["CustomerID","Customer ID","customer_id","Id"])
    monthly = _find_col(df_core, ["MonthlyCharges","Monthly Charges","monthly_charges"])
    total   = _find_col(df_core, ["TotalCharges","Total Charges","total_charges"])
    tenure  = _find_col(df_core, ["tenure","Tenure","tenure_months","Tenure Months","TenureMonths"])
    churn   = _find_col(df_core, ["Churn Label","Churn","ChurnLabel","churn_label"])

    out = pd.DataFrame()
    out["customer_id"]     = df_core[cust_id] if cust_id else pd.NA
    out["monthly_charges"] = pd.to_numeric(df_core[monthly], errors="coerce").fillna(0.0) if monthly else 0.0
    out["total_charges"]   = pd.to_numeric(df_core[total],   errors="coerce").fillna(0.0) if total   else 0.0
    out["tenure_months"]   = pd.to_numeric(df_core[tenure],  errors="coerce").fillna(0).astype(int) if tenure  else 0

    if churn:
        out["churn_label"] = df_core[churn].astype(str)
        out["churn_flag"]  = out["churn_label"].str.strip().str.lower().isin(["yes","true","churned","1"]).astype(int)
    else:
        out["churn_label"] = "Unknown"
        out["churn_flag"]  = 0

    out["monthly_charges"] = out["monthly_charges"].clip(lower=0)
    out["total_charges"]   = out["total_charges"].clip(lower=0)
    out["tenure_months"]   = out["tenure_months"].clip(lower=0)

    print("[TRANSFORM] column mapping:",
          {"customer_id": cust_id, "monthly": monthly, "total": total, "tenure": tenure, "churn": churn})
    return out
