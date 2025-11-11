-- Exercise: Snowflake Fundamentals
-- This document includes Snowflake Fundamentals exercise, enhancing data transformation, performance optimization.
-- 1.	View your current user and region

SELECT
    CURRENT_USER(),
    CURRENT_REGION()

-- 2.	View session details (user, role, warehouse, region)
-- Confirm your active user, role, warehouse, and region.

SELECT
    CURRENT_USER(),
    CURRENT_REGION(),
    CURRENT_WAREHOUSE(),
    CURRENT_ROLE()

-- 3.	Display your organization, account name, and cloud provider
-- Discover your Snowflake organization and account-level metadata.

SELECT
    CURRENT_ORGANIZATION_NAME(),
    CURRENT_ACCOUNT_NAME(),
    CURRENT_REGION(),
    SYSTEM$GET_SNOWFLAKE_PLATFORM_INFO()

-- 4.	List all available warehouses and check their state
-- View all existing compute resources and their states.

SHOW WAREHOUSES;

-- 5.	Create a compute warehouse
-- Provision compute to run queries and transformations.


CREATE WAREHOUSE SNOWFLAKE_WAREHOUSE_NOVEMBER
    WITH WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 180 --SECONDS
    AUTO_RESUME = TRUE;

    --TO CHANGE THE WAREHOUS

ALTER WAREHOUSE SNOWFLAKE_WAREHOUSE_NOVEMBER
    SET WAREHOUSE_SIZE = 'SMALL'
        AUTO_SUSPEND = 90
        AUTO_RESUME = TRUE
        MIN_CLUSTER_COUNT = 1
        MAX_CLUSTER_COUNT = 3 ;

-- 6.	See all databases available to your role
-- Find which databases your current role can access.

SHOW DATABASES;

-- 7.	Create schemas
-- Design your multi-layer Snowflake schema structure.

         CREATE DATABASE SALES_DB_NOV;
    CREATE SCHEMA STAGING_LAYER;
    CREATE SCHEMA RAW_LAYER; --RAW DATA
    CREATE SCHEMA CLEANSED_LAYER; --CLEARED DATA
    CREATE SCHEMA BUSINESS_LAYER; --
    CREATE SCHEMA PRESENTATION_LAYER; --USED FOR BI ETC

-- 8.	View login history (great for audit/compliance intro)
-- Check who accessed Snowflake and when — great for auditing.

SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.LOGIN_HISTORY   --DB SNOWFLAKE
    ORDER BY EVENT_TIMESTAMP DESC;


    SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.FILE_FORMATS;


-- 9.	List any currently executing queries
-- View currently executing workloads.

SELECT *
FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY());
-- WE SEE QUERY_IS, QIERIES, DB_NAME, USERNAME, ROLE, WAREHOUSE, EXECUTION_STATUS

-- WE CAN SEE THE RUNNING QUERIES
--WHERE EXECUTION_STATUS = 'RUNNING'


-- 10.	Create Database
-- Establish a centralized database for all sales data.

    CREATE DATABASE SALES_DB_NOV;
-- 11.	Create Table
-- Define your base transactional fact table structure.

CREATE OR REPLACE TABLE SALES_DB_NOV.RAW_LAYER.SALES_ORDERS (
    ORDER_ID STRING PRIMARY KEY ,
    CUSTOMER_ID STRING NOT NULL,
    PRODUCT_ID STRING NOT NULL,
    ORDER_DATE DATE NOT NULL,
    AMOUNT DECIMAL(10,2),
    PROFIT DECIMAL(10,2),
    QUANTITY INT,
    CATEGORY STRING,
    SUBCATEGORY STRING
);
-- 12.	Create a role, assign user, grant access
-- Secure your database and table using roles.
-- 13.	Assign Currect User to role analyst_role
-- Simulate working as a limited-access analyst.
-- 14.	View Your Current Role Privileges
-- Understand what your role can do.
-- 15.	Revert to full access
-- Return to high-privilege access for further administration.
--



