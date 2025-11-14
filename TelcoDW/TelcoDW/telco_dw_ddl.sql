-- Telco DW - MySQL DDL (Star Schema) - FINAL

CREATE DATABASE IF NOT EXISTS telco_dw;
USE telco_dw;

-- ============== STAGING ==============
CREATE TABLE IF NOT EXISTS stg_churn (
  customer_id      VARCHAR(64),
  monthly_charges  DECIMAL(10,2),
  total_charges    DECIMAL(12,2),
  tenure_months    INT,
  churn_flag       TINYINT,
  churn_label      VARCHAR(32)
);

-- ============== DIMENSIONS ==============
CREATE TABLE IF NOT EXISTS dim_customer (
  customer_sk    INT AUTO_INCREMENT PRIMARY KEY,
  customer_id    VARCHAR(64) UNIQUE,
  gender         VARCHAR(16),
  senior_citizen TINYINT,
  partner        VARCHAR(8),
  dependents     VARCHAR(8),
  age_group      VARCHAR(32),
  -- SCD helpers (no function defaults to avoid syntax errors):
  effective_from DATE NULL,
  effective_to   DATE NULL,
  is_current     TINYINT DEFAULT 1
);

CREATE TABLE IF NOT EXISTS dim_geography (
  geo_sk    INT AUTO_INCREMENT PRIMARY KEY,
  state     VARCHAR(64),
  city      VARCHAR(128),
  zip_code  VARCHAR(16),
  county    VARCHAR(128),
  latitude  DECIMAL(9,6),
  longitude DECIMAL(9,6),
  UNIQUE KEY uk_geo (state, city, zip_code)
);

CREATE TABLE IF NOT EXISTS dim_population (
  population_sk     INT AUTO_INCREMENT PRIMARY KEY,
  city              VARCHAR(128),
  state             VARCHAR(64),
  population        INT,
  population_density DECIMAL(12,4),
  median_income     DECIMAL(12,2),
  unemployment_rate DECIMAL(5,2),
  UNIQUE KEY uk_pop (state, city)
);

CREATE TABLE IF NOT EXISTS dim_services (
  service_sk         INT AUTO_INCREMENT PRIMARY KEY,
  phone_service      VARCHAR(8),
  multiple_lines     VARCHAR(16),
  internet_service   VARCHAR(32),
  online_security    VARCHAR(16),
  online_backup      VARCHAR(16),
  device_protection  VARCHAR(16),
  tech_support       VARCHAR(16),
  streaming_tv       VARCHAR(16),
  streaming_movies   VARCHAR(16),
  paperless_billing  VARCHAR(16),
  contract           VARCHAR(32),
  payment_method     VARCHAR(64),
  UNIQUE KEY uk_services (
    phone_service, multiple_lines, internet_service, online_security, online_backup,
    device_protection, tech_support, streaming_tv, streaming_movies,
    paperless_billing, contract, payment_method
  )
);

CREATE TABLE IF NOT EXISTS dim_time (
  date_sk INT PRIMARY KEY,
  d_date  DATE,
  year    INT,
  quarter INT,
  month   INT,
  day     INT
);

-- ============== FACT ==============
CREATE TABLE IF NOT EXISTS fact_churn (
  fact_sk          BIGINT AUTO_INCREMENT PRIMARY KEY,
  customer_sk      INT,
  geo_sk           INT,
  population_sk    INT,
  service_sk       INT,
  date_sk          INT,
  tenure_months    INT,
  monthly_charges  DECIMAL(10,2),
  total_charges    DECIMAL(12,2),
  churn_flag       TINYINT,
  churn_label      VARCHAR(32),
  FOREIGN KEY (customer_sk)   REFERENCES dim_customer(customer_sk),
  FOREIGN KEY (geo_sk)        REFERENCES dim_geography(geo_sk),
  FOREIGN KEY (population_sk) REFERENCES dim_population(population_sk),
  FOREIGN KEY (service_sk)    REFERENCES dim_services(service_sk),
  FOREIGN KEY (date_sk)       REFERENCES dim_time(date_sk)
);

-- ============== VIEW ==============
CREATE OR REPLACE VIEW vw_churn_kpis AS
SELECT
  t.year, t.month,
  SUM(f.churn_flag) AS churned_customers,
  COUNT(*)          AS total_customers,
  ROUND(SUM(f.churn_flag)/NULLIF(COUNT(*),0)*100, 2) AS churn_rate_pct,
  ROUND(AVG(f.monthly_charges), 2) AS avg_monthly_charges
FROM fact_churn f
JOIN dim_time t ON f.date_sk = t.date_sk
GROUP BY t.year, t.month
ORDER BY t.year, t.month;
