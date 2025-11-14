"""
Main ETL runner for Telco DW
Usage:
  python main_etl.py --mysql "mysql+pymysql://root:pass@localhost:3306/telco_dw"
"""

import argparse
from etl_extract import extract_all
from etl_transform import transform
from etl_load import load_to_mysql

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mysql", required=True, help="MySQL connection string")
    args = parser.parse_args()

    print("\n--- EXTRACT ---")
    data = extract_all()

    print("\n--- TRANSFORM ---")
    df_fact = transform(data)

    print("\n--- LOAD ---")
    load_to_mysql(df_fact, args.mysql)

if __name__ == "__main__":
    main()
