# Retail ETL Pipeline Project

## Overview
End-to-end ETL pipeline that extracts retail sales data from S3, transforms it using Pandas with Pandera validation, and loads enriched datasets into Snowflake for analytics.

## Architecture
**Extract** → S3 (CSV/JSON) → **Transform** → Pandas + Pandera → **Load** → S3 → Snowflake

## Project Phases

### Phase 1: Project Setup
- Create `retail_etl_project` directory
- Initialize virtual environment
- Install dependencies: `boto3`, `pandas`, `pandera`, `snowflake-connector-python`, `pyyaml`, `apache-airflow`

### Phase 2: Configuration Management
- Define credentials and paths in `config.yaml`
- Manage AWS keys, Snowflake connection strings, S3 paths

### Phase 3: Data Extraction
- Use `boto3` to connect to S3
- Create reusable extract functions for CSV and JSON files
- Read raw sales and product metadata

### Phase 4: Input Validation
- Define Pandera schemas for raw input data
- Validate data quality before transformation

### Phase 5: Data Transformation
#### Sales Cleaning
- Normalize columns to snake_case
- Clean region values (lowercase, strip whitespace)
- Drop missing region/timestamp
- Remove invalid prices (≤0) and quantities (≤0)
- Convert timestamp to datetime
- Recalculate total_sales

#### Product Cleaning
- Normalize columns to snake_case
- Standardize brand (uppercase) and category (lowercase)
- Drop missing product_id/rating
- Remove duplicates

#### Data Enrichment
- **Merge**: Join sales with products on product_id
- **Enrich**: Extract temporal features (month, weekday, hour)
- Create sales_bucket using `pd.cut()` on total_sales

#### Analytics Outputs
1. **Hourly Sales Trend**: Peak sales hours by region and category
2. **Product Sales Ranking**: Rank products by revenue and categorize (Bestseller/Average/Low Performer)
3. **Seasonal Sales Patterns**: Quarterly sales analysis by category
4. **Revenue Concentration**: Regional revenue distribution and cumulative share analysis

### Phase 6: Data Loading
- Upload transformed DataFrames to S3
- Load from S3 into Snowflake tables:
  - `CLEANSED` schema: cleaned_sales, cleaned_products
  - `BUSINESS` schema: merged_sales_products, enriched_sales
  - `PRESENTATION` schema: hourly_sales_trend, product_sales_ranking, seasonal_sales_patterns, revenue_concentration

### Phase 7: Orchestration
- Create Airflow DAG to automate the pipeline
- Schedule and monitor ETL jobs

## Snowflake Schema Structure
```
RETAIL_DB_NOV
├── STAGING_LAYER (S3 stage)
├── CLEANSED (cleaned data)
├── BUSINESS (merged & enriched)
└── PRESENTATION (analytics)
```

## Validation Strategy
All transformations include Pandera schema validation to ensure data quality at each stage:
- Pre-transform validation (input)
- Post-transform validation (output)
- Analytics validation (final)

## Key Technologies
- **Extraction**: boto3
- **Transformation**: Pandas, Pandera
- **Loading**: Snowflake Connector
- **Orchestration**: Apache Airflow (Astro CLI)
- **Configuration**: PyYAML

## Getting Started
```bash
# Create project
mkdir retail_etl_project && cd retail_etl_project

# Set up environment
python -m venv venv
source venv/bin/activate  # or venv\Scripts\activate on Windows

# Install dependencies
pip install boto3 pandas pandera snowflake-connector-python pyyaml apache-airflow

# Configure credentials
cp config.yaml.example config.yaml
# Edit config.yaml with your credentials
```

## Project Structure
```
retail_etl_project/
├── venv/                      # Virtual environment (ignore)
├── dags/
│   ├── retail_etl_dag.py
│   └── .airflowignore
├── include/
│   ├── config.yaml
│   ├── etl/
│   │   ├── extract_data.py
│   │   ├── load_data.py
│   │   └── transform.py
│   └── validations/
│       ├── sales_schema.py
│       ├── product_schema.py
│       ├── enrich_schema.py
│       ├── hourly_sales.py
│       ├── product_sales_schema.py
│       ├── revenue_shema.py
│       └── seasonal_sales_schema.py
├── plugins/                   # Empty
└── tests/
    └── dags/
        └── test_dag_example.py
```

## Security Notes
- Never commit AWS credentials to version control
- Use environment variables or secret managers
- Rotate credentials regularly
