-- Warehouse for loading
CREATE WAREHOUSE IF NOT EXISTS ETL_WH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE;

-- Database
CREATE DATABASE IF NOT EXISTS RETAIL_DB_NOV;

-- Schemas
CREATE SCHEMA RETAIL_DB_NOV.STAGING_LAYER;
CREATE SCHEMA RETAIL_DB_NOV.CLEANSED;
CREATE SCHEMA RETAIL_DB_NOV.BUSINESS;
CREATE SCHEMA RETAIL_DB_NOV.PRESENTATION;

CREATE FILE FORMAT RETAIL_DB_NOV.STAGING_LAYER.CSV_FORMAT
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1;

CREATE STAGE RETAIL_DB_NOV.STAGING_LAYER.RETAIL_S3_STAGE
    URL = 's3://course-data-warehouse-viki/'
    CREDENTIALS = (
        AWS_KEY_ID = ''
        AWS_SECRET_KEY = ''
    )
    FILE_FORMAT = RETAIL_DB_NOV.STAGING_LAYER.CSV_FORMAT;



CREATE TABLE RETAIL_DB_NOV.CLEANSED.CLEANED_SALES (
    sales_id        INT,
    product_id      INT,
    region          STRING,
    price           NUMERIC(10, 2),
    quantity        INT,
    timestamp       DATE,
    total_sales     NUMERIC(10, 2)
);

CREATE TABLE RETAIL_DB_NOV.CLEANSED.CLEANED_PRODUCTS (
    product_id      INT,
    brand           STRING,
    category        STRING,
    rating          NUMERIC(10, 2)
);

CREATE TABLE RETAIL_DB_NOV.BUSINESS.MERGED_SALES_PRODUCTS (
    order_id        INT,
    product_id      INT,
    region          STRING,
    price           NUMERIC(10, 2),
    quantity        INT,
    timestamp       DATE,
    total_sales     NUMERIC(10, 2),
    brand           STRING,
    category        STRING,
    rating          NUMERIC(10, 2)
);

CREATE TABLE RETAIL_DB_NOV.BUSINESS.ENRICHED_SALES (
    order_id        INT,
    product_id      INT,
    region          STRING,
    quantity        INT,
    price           NUMERIC(10, 2),
    timestamp       DATE,
    total_sales     NUMERIC(10, 2),
    category        STRING,
    brand           STRING,
    rating          NUMERIC(10, 2),
    week            INT,
    month           STRING,
    weekday         STRING,
    hour            INT,
    sales_bucket    STRING
);
CREATE OR REPLACE TABLE RETAIL_DB_NOV.PRESENTATION.HOURLY_SALES_TREND (
    region              STRING,
    category            STRING,
    hour                NUMBER(2,0),
    hourly_total_sales  NUMBER(10,2)
);

CREATE OR REPLACE TABLE RETAIL_DB_NOV.PRESENTATION.PRODUCT_SALES_RANKING (
    product_id  INT,
    category    STRING,
    brand       STRING,
    rating      NUMBER(4,2),
    revenue     NUMBER(10,2),
    sales_count NUMBER(38,0),
    value_bucket STRING
);

CREATE OR REPLACE TABLE RETAIL_DB_NOV.PRESENTATION.SEASONAL_SALES_PATTERNS (
    quarter     NUMBER(1,0),
    category    STRING,
    total_sales NUMBER(10,2)
);

CREATE OR REPLACE TABLE RETAIL_DB_NOV.PRESENTATION.REVENUE_CONCENTRATION (
    region            STRING,
    region_revenue    NUMBER(10,2),
    revenue_share     FLOAT,
    cumulative_share  FLOAT
);

COPY INTO RETAIL_DB_NOV.CLEANSED.CLEANED_SALES
FROM @RETAIL_DB_NOV.STAGING_LAYER.RETAIL_S3_STAGE/Outputs/
FILES = ('cleaned_sales.csv'),
ON_ERROR = 'CONTINUE';

COPY INTO RETAIL_DB_NOV.CLEANSED.CLEANED_PRODUCTS
FROM @RETAIL_DB_NOV.STAGING_LAYER.RETAIL_S3_STAGE/Outputs/
FILES = ('cleaned_products.csv'),
ON_ERROR = 'CONTINUE';

SELECT * FROM RETAIL_DB_NOV.CLEANSED.CLEANED_PRODUCTS;

COPY INTO RETAIL_DB_NOV.BUSINESS.MERGED_SALES_PRODUCTS
FROM @RETAIL_DB_NOV.STAGING_LAYER.RETAIL_S3_STAGE/Outputs/
FILES = ('merge.csv'),
ON_ERROR = 'CONTINUE';

COPY INTO RETAIL_DB_NOV.BUSINESS.ENRICHED_SALES
FROM @RETAIL_DB_NOV.STAGING_LAYER.RETAIL_S3_STAGE/Outputs/
FILES = ('enrich.csv'),
ON_ERROR = 'CONTINUE';

COPY INTO RETAIL_DB_NOV.PRESENTATION.HOURLY_SALES_TREND
FROM @RETAIL_DB_NOV.STAGING_LAYER.RETAIL_S3_STAGE/Analytics/
FILES = ('hourly_sales_trend.csv'),
ON_ERROR = 'CONTINUE';

COPY INTO RETAIL_DB_NOV.PRESENTATION.PRODUCT_SALES_RANKING
FROM @RETAIL_DB_NOV.STAGING_LAYER.RETAIL_S3_STAGE/Analytics/
FILES = ('product_sales_ranking.csv'),
ON_ERROR = 'CONTINUE';

SELECT * FROM  RETAIL_DB_NOV.PRESENTATION.PRODUCT_SALES_RANKING

COPY INTO RETAIL_DB_NOV.PRESENTATION.REVENUE_CONCENTRATION
FROM @RETAIL_DB_NOV.STAGING_LAYER.RETAIL_S3_STAGE/Analytics/
FILES = ('revenue_concentration.csv'),
ON_ERROR = 'CONTINUE';



COPY INTO RETAIL_DB_NOV.PRESENTATION.SEASONAL_SALES_PATTERNS
FROM @RETAIL_DB_NOV.STAGING_LAYER.RETAIL_S3_STAGE/Analytics/
FILES = ('seasonal_Sales_pattern.csv'),
ON_ERROR = 'CONTINUE';

