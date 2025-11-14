import pandas as pd
from pathlib import Path

BASE = Path(__file__).resolve().parent

FILES = {
    "CustomerChurn": BASE / "CustomerChurn.xlsx",
    "TelcoCustomerChurn": BASE / "Telco_customer_churn.xlsx",
    "Demographics": BASE / "Telco_customer_churn_demographics.xlsx",
    "Location": BASE / "Telco_customer_churn_location.xlsx",
    "Population": BASE / "Telco_customer_churn_population.xlsx",
    "Services": BASE / "Telco_customer_churn_services.xlsx",
    "Status": BASE / "Telco_customer_churn_status.xlsx",
}

def load_excel(name):
    path = FILES[name]
    try:
        df = pd.read_excel(path)
        print(f"Loaded {name} ({len(df)} rows)")
        return df
    except Exception as e:
        print(f"⚠️ Could not load {name}: {e}")
        return pd.DataFrame()

def extract_all():
    data = {name: load_excel(name) for name in FILES}
    return data

if __name__ == "__main__":
    data = extract_all()
    for name, df in data.items():
        print(f"{name}: {df.shape}")
