
**Telco Customer Churn – Data Warehouse (ETL + Star Schema)**

A complete **Data Warehouse project** built using **Python, Pandas, and MySQL**, designed to integrate multiple Telco datasets into a unified **star schema** for churn analytics and BI reporting.

This project implements a full **ETL pipeline**, data cleaning, schema creation, staging layer, dimensional modeling, and loading into MySQL.

---

**1. Project Overview**

The goal of this project is to build a scalable, analytics-ready **Data Warehouse** for the Telco Customer Churn dataset.
The warehouse consolidates data from multiple Excel sources—including customer churn, demographics, services, population, and location—into a well-structured star schema.

**Objectives**

* Build an end-to-end ETL pipeline
* Implement a star schema for analytical reporting
* Clean and normalize data across multiple sources
* Load fact and dimension tables into MySQL
* Enable KPI reporting like churn rate, revenue, and retention insights

---
**2. Tools & Technologies Used**

**Programming:** Python (Pandas, SQLAlchemy)
**Database:** MySQL 8.0
**Modeling:** Star Schema + Dimensional Modeling
**ETL Scripts:** Custom Python pipeline
**Visualization:** Power BI / Tableau (optional)

---

**3. Business Problem**

Telecom companies experience high churn rates, affecting revenue and long-term customer value.
The goal is to build a warehouse that provides:

* Churn rate analysis
* Revenue impact measurement
* Customer segmentation
* Service usage patterns
* Geographic and demographic insights

**Key KPIs**

* Monthly Churn Rate (%)
* Average Monthly Charges
* Total Revenue Lost from Churn
* Customer Tenure Analysis
* Churn by Demographics & Location

---

**4. Data Modeling – Star Schema**

The warehouse follows a **classic star schema**:

### **Fact Table**

**fact_churn**
Contains metrics related to churn behavior, revenue (charges), tenure, and foreign keys to all dimensions.

### **Dimension Tables**

| Dimension          | Description                                             |
| ------------------ | ------------------------------------------------------- |
| **dim_customer**   | Customer profile, gender, dependents, age group         |
| **dim_geography**  | City, state, ZIP, latitude & longitude                  |
| **dim_population** | City-level population statistics                        |
| **dim_services**   | Internet, security, phone, streaming, and contract info |
| **dim_time**       | Date dimension for time-based analytics                 |

A detailed SQL schema is included in `telco_dw_ddl.sql`.

---

**5. Repository Structure**

```
📁 Telco-DW/
│
├── etl_extract.py           # Extract raw datasets
├── etl_transform.py         # Transform & clean data
├── etl_load.py              # Load staging & initialize schema
├── promote_star.py          # Populate dimensions and fact tables
├── main_etl.py              # Main ETL pipeline
│
├── telco_dw_ddl.sql         # MySQL schema for DW
│
└── data/                    # (Optional) Raw Excel data files
```

---

**6. ETL Pipeline Steps**

### **(1) Extract**

* Reads all Excel files
* Validates file formats
* Prints loaded rows and data columns
* Handles missing files gracefully

### **(2) Transform**

* Cleans column names
* Normalizes variants (e.g., “Total Charges” vs “TotalCharges”)
* Converts numeric fields
* Creates churn flag
* Prepares standardized fact dataset

### **(3) Load**

* Creates database schema using SQL file
* Loads data into staging (`stg_churn`)
* Populates fact and dimension tables

---

## ▶️ **7. How to Run**

### **Install Dependencies**

```bash
pip install -r requirements.txt
```

### **Run Full ETL**

```bash
python main_etl.py --mysql "mysql+pymysql://root:password@localhost:3306/telco_dw"
```

### **Promote to Star Schema**

```bash
python promote_star.py --mysql "mysql+pymysql://root:password@localhost:3306/telco_dw"
```

All tables (dim + fact) will be populated in MySQL.

---
**8. Data Quality Measures**

* Removed missing & inconsistent values
* Standardized categorical fields
* Converted numeric columns with coercion
* Trimmed whitespace & fixed typos
* Checked row counts at each stage
* Ensured referential integrity in fact table

---

**9. Deployment**

The warehouse is deployed on:

**MySQL Server (Local Instance)**

* Schema: `telco_dw`
* Tables: Dimensions + Fact + Staging
* BI tools can connect via MySQL ODBC/JDBC

---

**10. Analytics & BI Reporting**

The warehouse supports:

* Churn analysis dashboards
* Revenue KPIs
* Customer segmentation
* Time-series churn trends
* Location-based churn patterns

A ready-made KPI view is included:

```sql
SELECT * FROM vw_churn_kpis;
```
