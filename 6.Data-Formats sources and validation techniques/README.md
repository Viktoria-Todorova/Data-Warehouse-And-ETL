# 📦 File Formats Lab — ETL Pipeline


A modular **ETL (Extract–Transform–Load)** pipeline demonstrating data engineering concepts using CSV, Parquet, and AWS S3.

This project is part of the **SoftUni Data Warehouse & ETL** course and showcases real-world data ingestion, transformation, validation, and output generation.


---

## 📁 Project Structure

```
File Formats-Lab/
│
├── etl_pipeline.py
│
├── extract/
│   ├── extract_data.py
│   └── __init__.py
│
├── transform/
│   ├── transform_data.py
│   └── __init__.py
│
├── validate/
│   ├── pandera_validation.py
│   └── __init__.py
│
├── load/
│   ├── load_data.py
│   └── __init__.py
│
├── data/
│   ├── input/
│   │   ├── employees.csv
│   │   ├── sales/
│   │   └── parquet/
│   │       └── sales.parquet
│   │
│   └── output/
│       ├── transformed/
│       └── aggregated/
│
├── resources/
│   └── ddl/
│       ├── sales_schema.sql
│       └── employees_schema.sql
│
├── tests/
│   ├── test_extract.py
│   └── test_transform.py
│
├── requirements.txt
└── README.md
```


---

## 🚀 Overview

This project implements a full ETL workflow:

### ✔ Extract
- Read CSV and Parquet files  
- Download raw data from AWS S3  
- Convert S3 byte streams to DataFrames  

### ✔ Transform
- Clean and normalize data  
- Fix column types and naming  
- Merge datasets  
- Compute new metrics  

### ✔ Validate
Uses **Pandera** schemas to ensure:
- Required columns exist  
- Correct types  
- Valid ranges  
- Invalid rows are rejected  

### ✔ Load
Final processed data is saved into:

```
data/output/transformed/
data/output/aggregated/
```

---

## 🔄 Pipeline Flow

```
        +------------------+
        |     Raw Data     |
        | CSV | Parquet | S3
        +----------+-------+
                   |
                   v
        +------------------+
        |     EXTRACT      |
        +----------+-------+
                   |
                   v
        +------------------+
        |    TRANSFORM     |
        +----------+-------+
                   |
                   v
        +------------------+
        |    VALIDATE      |
        +----------+-------+
                   |
                   v
        +------------------+
        |      LOAD        |
        +------------------+
```


---

## 🛠 Installation & Setup

### 1️⃣ Clone the repository

```bash
git clone <repository-url>
cd File Formats-Lab
```

### 2️⃣ Create a virtual environment

```bash
python -m venv .venv
```

Activate it:

**Windows**
```bash
.venv\Scripts\activate
```

**macOS/Linux**
```bash
source .venv/bin/activate
```

### 3️⃣ Install required packages

```bash
pip install -r requirements.txt
```

### 4️⃣ Install Parquet support (required)

```bash
pip install pyarrow
```

or

```bash
pip install fastparquet
```

Without it you will get:

```
ImportError: Unable to find a usable engine
```


---

## ☁ AWS S3 Configuration (Optional)


```bash
set AWS_ACCESS_KEY_ID=your_key
set AWS_SECRET_ACCESS_KEY=your_secret
set AWS_REGION=your_region
```


---

## ▶ Running the ETL Pipeline

```bash
python etl_pipeline.py
```

Outputs will appear inside:

```
data/output/
```


---



---


## 📄 Technologies Used

- Python 3.11+  
- Pandas  
- PyArrow / Fastparquet  
- Boto3  
- Pandera  
- Pytest  


---

## 🎯 Purpose

This project demonstrates:

- Working with CSV and Parquet formats  
- Modular ETL architecture  
- Cloud extraction from AWS S3  
- Data validation with schemas  
- Building reliable, testable pipelines  



