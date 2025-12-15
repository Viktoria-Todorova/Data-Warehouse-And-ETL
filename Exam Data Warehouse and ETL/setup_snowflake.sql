-- Create data_engineer role with admin privileges and assign to user
CREATE ROLE data_engineer;
GRANT ROLE ACCOUNTADMIN TO ROLE data_engineer;
SELECT CURRENT_ROLE();
GRANT ROLE data_engineer TO USER crazytori;
USE ROLE data_engineer;

-- Create warehouse
CREATE WAREHOUSE IF NOT EXISTS EXAM_WH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE;

--Create database with layered architecture: STAGING → CLEANSED → BUSINESS → PRESENTATION
CREATE DATABASE IF NOT EXISTS AIRFLOW_EXAM_DEC;

CREATE SCHEMA AIRFLOW_EXAM_DEC.STAGING_LAYER;
CREATE SCHEMA AIRFLOW_EXAM_DEC.CLEANSED_LAYER;
CREATE SCHEMA AIRFLOW_EXAM_DEC.BUSINESS_LAYER;
CREATE SCHEMA AIRFLOW_EXAM_DEC.PRESENTATION;

-- Define CSV file format for S3 data ingestion
CREATE FILE FORMAT AIRFLOW_EXAM_DEC.STAGING_LAYER.CSV_FORMAT
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1;

-- Create external stage pointing to S3 bucket with Airflow outputs
CREATE OR REPLACE STAGE  AIRFLOW_EXAM_DEC.STAGING_LAYER.RETAIL_S3_STAGE
    URL = 's3://course-data-warehouse-viki/OutputExam/'
    CREDENTIALS = (
        AWS_KEY_ID = '...'
        AWS_SECRET_KEY = '....'
    )
    FILE_FORMAT = AIRFLOW_EXAM_DEC.STAGING_LAYER.CSV_FORMAT;

-- Create Tables for cleaned data from Airflow ETL pipeline

CREATE OR REPLACE TABLE AIRFLOW_EXAM_DEC.CLEANSED_LAYER.CLEANED_SALES (
    sales_id        INT,
    product_id      INT,
    region          STRING,
    quantity        INT,
    price           NUMERIC(10, 2),
    timestamp       DATE,
    discount     NUMERIC(10, 2),
    order_status STRING
);

CREATE OR REPLACE TABLE AIRFLOW_EXAM_DEC.CLEANSED_LAYER.CLEANED_PRODUCTS (
    product_id      INT,
    category        STRING,
    brand           STRING,
    rating          NUMERIC(10, 2),
    in_stock        BOOLEAN,
    launch_date     DATE
);

-- merged product sales table
CREATE or replace TABLE AIRFLOW_EXAM_DEC.CLEANSED_LAYER.PRODUCT_SALES (
    sales_id        INT,
    product_id      INT,
    region          STRING,
    quantity        INT,
    price           NUMERIC(10, 2),
    timestamp       DATE,
    discount     NUMERIC(10, 2),
    order_status STRING,
    category        STRING,
    brand           STRING,
    rating          NUMERIC(10, 2),
    in_stock        BOOLEAN,
    launch_date     DATE
   );

-- Load cleaned sales/product/merged-aggregated data from Airflow output

COPY INTO AIRFLOW_EXAM_DEC.CLEANSED_LAYER.CLEANED_SALES
FROM @AIRFLOW_EXAM_DEC.STAGING_LAYER.RETAIL_S3_STAGE
FILES = ('cleaned_sales.csv'),
ON_ERROR = 'CONTINUE';

SELECT * FROM AIRFLOW_EXAM_DEC.CLEANSED_LAYER.CLEANED_SALES;


COPY INTO AIRFLOW_EXAM_DEC.CLEANSED_LAYER.CLEANED_PRODUCTS
FROM @AIRFLOW_EXAM_DEC.STAGING_LAYER.RETAIL_S3_STAGE
FILES = ('cleaned_products.csv'),
ON_ERROR = 'CONTINUE';
select * from AIRFLOW_EXAM_DEC.CLEANSED_LAYER.CLEANED_PRODUCTS

COPY INTO AIRFLOW_EXAM_DEC.CLEANSED_LAYER.PRODUCT_SALES
FROM @AIRFLOW_EXAM_DEC.STAGING_LAYER.RETAIL_S3_STAGE
FILES = ('merged_products_sales.csv'),
ON_ERROR = 'CONTINUE';
select * from AIRFLOW_EXAM_DEC.CLEANSED_LAYER.PRODUCT_SALES


-- ============================================================================
-- STAR SCHEMA
-- ============================================================================

CREATE OR REPLACE TABLE AIRFLOW_EXAM_DEC.BUSINESS_LAYER.DIM_DATE (
    date_key INT PRIMARY KEY,
    full_date DATE NOT NULL,
    day_name STRING NOT NULL,
    month INT NOT NULL,
    month_name STRING NOT NULL,
    quarter INT NOT NULL,
    year INT NOT NULL
);

INSERT INTO AIRFLOW_EXAM_DEC.BUSINESS_LAYER.DIM_DATE
SELECT DISTINCT
    TO_CHAR(timestamp, 'YYYYMMDD')::INT AS date_key,
    timestamp::DATE AS full_date,
    DAYNAME(timestamp)                 AS day_name,
    MONTH(timestamp) AS month,
    MONTHNAME(timestamp) AS month_name,
    QUARTER(timestamp) AS quarter,
    YEAR(timestamp) AS year
FROM AIRFLOW_EXAM_DEC.CLEANSED_LAYER.CLEANED_SALES;

SELECT * FROM AIRFLOW_EXAM_DEC.BUSINESS_LAYER.DIM_DATE;

CREATE OR REPLACE TABLE AIRFLOW_EXAM_DEC.BUSINESS_LAYER.DIM_PRODUCT (
    product_id INT PRIMARY KEY,
    brand STRING NOT NULL,
    category STRING NOT NULL,
    rating NUMBER(4,2),
    in_stock        BOOLEAN,
    launch_date     DATE
);

INSERT INTO AIRFLOW_EXAM_DEC.BUSINESS_LAYER.DIM_PRODUCT
SELECT DISTINCT
    product_id,
    brand,
    category,
    rating,
    in_stock,
    launch_date
FROM AIRFLOW_EXAM_DEC.CLEANSED_LAYER.CLEANED_PRODUCTS;

SELECT * FROM AIRFLOW_EXAM_DEC.BUSINESS_LAYER.DIM_PRODUCT;


CREATE OR REPLACE TABLE AIRFLOW_EXAM_DEC.BUSINESS_LAYER.FACT_SALES (
    sales_id INT PRIMARY KEY,
    product_id INT NOT NULL,
    date_key INT NOT NULL,
    region STRING NOT NULL,
    quantity INT NOT NULL,
    price NUMERIC(10,2) NOT NULL,
    discount NUMERIC(10,2) DEFAULT 0,
    total_sales NUMERIC(10,2) NOT NULL,
    order_status STRING NOT NULL,
    FOREIGN KEY (product_id) REFERENCES AIRFLOW_EXAM_DEC.BUSINESS_LAYER.DIM_PRODUCT(product_id),
    FOREIGN KEY (date_key) REFERENCES AIRFLOW_EXAM_DEC.BUSINESS_LAYER.DIM_DATE(date_key)
);

INSERT INTO AIRFLOW_EXAM_DEC.BUSINESS_LAYER.FACT_SALES
SELECT
    s.sales_id,
    s.product_id,
    TO_CHAR(s.timestamp, 'YYYYMMDD')::INT AS date_key,
    s.region,
    s.quantity,
    s.price,
    s.discount,
    s.quantity * s.price * (1 - COALESCE(s.discount,0)) AS total_sales,
    s.order_status
FROM AIRFLOW_EXAM_DEC.CLEANSED_LAYER.CLEANED_SALES s;


SELECT * FROM AIRFLOW_EXAM_DEC.BUSINESS_LAYER.FACT_SALES

SHOW TABLES IN SCHEMA AIRFLOW_EXAM_DEC.BUSINESS_LAYER;


-- Create Materialized Analytical Views
-- Create materialized views under PRESENTATION schema: MV_SALES_BY_REGION_MONTH, MV_TOP_PRODUCTS_BY_REVENUE, MV_REVENUE_TREND, MV_CATEGORY_PERFORMANCE.


--  Sales aggregated by region and month
CREATE OR REPLACE MATERIALIZED VIEW AIRFLOW_EXAM_DEC.PRESENTATION.MV_SALES_BY_REGION_MONTH AS
SELECT
    region,
    MONTHNAME(TO_DATE(date_key::STRING, 'YYYYMMDD')) AS month,
    YEAR(TO_DATE(date_key::STRING, 'YYYYMMDD')) AS year,
    COUNT(sales_id) AS total_orders,
    SUM(total_sales) AS total_revenue
FROM AIRFLOW_EXAM_DEC.BUSINESS_LAYER.FACT_SALES
GROUP BY
     region,
     month,
     year;


SELECT * FROM AIRFLOW_EXAM_DEC.PRESENTATION.MV_SALES_BY_REGION_MONTH
ORDER BY YEAR DESC, MONTH;

-- Products categorized by revenue tiers

CREATE OR REPLACE MATERIALIZED VIEW AIRFLOW_EXAM_DEC.PRESENTATION.MV_TOP_PRODUCTS_BY_REVENUE AS
SELECT
    product_id,
    sum(total_sales) AS total_revenue,
    CASE
        WHEN SUM(total_sales) > 25000 THEN 'TOP REVENUE'
        WHEN SUM(total_sales) BETWEEN 15000 AND 25000 THEN 'MIDDLE REVENUE'
        ELSE 'LOW REVENUE'
    END AS producst_by_revenue
FROM AIRFLOW_EXAM_DEC.BUSINESS_LAYER.FACT_SALES
GROUP BY product_id;


SELECT * FROM AIRFLOW_EXAM_DEC.PRESENTATION.MV_TOP_PRODUCTS_BY_REVENUE
order by total_revenue desc;

-- Revenue trend over time (monthly)

CREATE OR REPLACE MATERIALIZED VIEW AIRFLOW_EXAM_DEC.PRESENTATION.MV_REVENUE_TREND AS
SELECT
    YEAR(TO_DATE(date_key::STRING, 'YYYYMMDD')) AS year,
    MONTHNAME(TO_DATE(date_key::STRING, 'YYYYMMDD')) AS month,
    SUM(total_sales) AS total_revenue
FROM AIRFLOW_EXAM_DEC.BUSINESS_LAYER.FACT_SALES
group by year, month;

 -- DAY(TO_DATE(date_key::STRING, 'YYYYMMDD')) AS DAY,    day

SELECT * FROM AIRFLOW_EXAM_DEC.PRESENTATION.MV_REVENUE_TREND
ORDER BY year, month;


-- Category performance over time

CREATE OR REPLACE MATERIALIZED VIEW AIRFLOW_EXAM_DEC.PRESENTATION.MV_CATEGORY_PERFORMANCE AS
SELECT
    category,
    YEAR(timestamp) AS year,
    MONTHNAME(timestamp) AS month,
    sum(quantity) as total_quantity,
    SUM(quantity * price * (1 - COALESCE(discount, 0))) AS total_sales
from AIRFLOW_EXAM_DEC.CLEANSED_LAYER.PRODUCT_SALES
group by category, year,month;

SELECT * FROM AIRFLOW_EXAM_DEC.PRESENTATION.MV_CATEGORY_PERFORMANCE;