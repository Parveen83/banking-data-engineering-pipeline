# Banking Data Engineering Platform - Version 2

## Overview

This project demonstrates a basic data engineering pipeline that loads banking transaction data into PostgreSQL and exports it into multiple file formats using Apache Airflow.

Version 1 focuses on the **Table → File** data engineering pattern.

## v1 Architecture

```text
PaySim Dataset (CSV)
        ↓
      Airflow
        ↓
   PostgreSQL
        ↓
 ┌───────────────┐
 │ transactions  │
 │    table      │
 └───────────────┘
        ↓
 ┌─────────┬──────────┐
 │   CSV   │ Parquet  │
 └─────────┴──────────┘
```

## v2 Architecture
```text
CSV
CSV
JSON
PARQUET
     ↓
Airflow
     ↓
Validation
     ↓
Standardization
     ↓
raw.transactions
```

## Objectives

* Load transaction data into PostgreSQL
* Export table data to CSV
* Export table data to Parquet
* Orchestrate workflow using Apache Airflow
* Containerize the platform using Docker

## Tech Stack

* Python
* Apache Airflow
* PostgreSQL
* Pandas
* SQLAlchemy
* PyArrow
* Docker
* Docker Compose

## Dataset

Dataset Used: PaySim Mobile Money Transactions Dataset

Columns used:

| Source Column  | Target Column           |
| -------------- | ----------------------- |
| step           | step                    |
| type           | transaction_type        |
| amount         | amount                  |
| nameOrig       | source_account          |
| oldbalanceOrg  | source_old_balance      |
| newbalanceOrig | source_new_balance      |
| nameDest       | destination_account     |
| oldbalanceDest | destination_old_balance |
| newbalanceDest | destination_new_balance |
| isFraud        | is_fraud                |
| isFlaggedFraud | is_flagged_fraud        |

## Project Structure

```text
banking-data-engineering-pipeline/
│
├── airflow/
│   └── dags/
│       ├── banking_pipeline.py
│       └── v2_multi_file_ingestion.py
│
├── sql/
│   ├── create_schemas.sql
│   └── create_transactions_table.sql
│
├── data/
│   ├── input/
│   │   ├── transactions_jan.csv
│   │   ├── transactions_feb.parquet
│   │   ├── transactions_mar.json
│   │   └── transactions_apr.csv
│   │
│   ├── raw/
│   │   └── paysim.csv
│   │
│   └── exports/
│       ├── transactions.csv
│       └── transactions.parquet
│
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
└── README.md
```

## Database Design

### Schema

```sql
raw
```

### Table

```sql
raw.transactions
```

Columns:

* step
* transaction_type
* amount
* source_account
* source_old_balance
* source_new_balance
* destination_account
* destination_old_balance
* destination_new_balance
* is_fraud
* is_flagged_fraud

## Airflow Workflow

### Task 1 - Create Schema

Creates the raw schema if it does not exist.

### Task 2 - Create Table

Creates the raw.transactions table.

### Task 3 - Load CSV

Loads PaySim transaction data into PostgreSQL.

### Task 4 - Export CSV

Exports PostgreSQL table data to:

```text
data/exports/transactions.csv
```

### Task 5 - Export Parquet

Exports PostgreSQL table data to:

```text
data/exports/transactions.parquet
```

## DAG Flow

```text
### Version 1

create_schema
      ↓
create_table
      ↓
load_csv
      ↓
 ┌─────────────┐
 │             │
 ↓             ↓
export_csv export_parquet


### Version 2

create_schema
      ↓
create_table
      ↓
load_transaction_files
      ↓
validate_load
```

## How to Run

### Start Services

```bash
docker compose up -d
```

### Verify Airflow

Open:

```text
http://localhost:8080
```

### Trigger DAG

Airflow UI

```text
banking_pipeline
    ↓
Trigger DAG
```

## Output

Generated files:

```text
data/exports/transactions.csv
data/exports/transactions.parquet
```

## Key Learnings

* PostgreSQL data loading
* Apache Airflow orchestration
* SQL execution from Airflow
* CSV export
* Parquet export
* Dockerized data engineering workflows
* Batch processing concepts

## Successful DAG Runs

### Version 1

![V1 DAG](docs/images/banking_pipeline_dag_v1.png)

### Version 2

![V2 DAG](docs/images/banking_pipeline_dag_v2.png)

## Project Versions

### Version 1
- Single CSV ingestion
- PostgreSQL loading
- CSV export
- Parquet export

### Version 2
- Multiple file ingestion
- Supports CSV, JSON and Parquet
- Dynamic file discovery
- Schema validation
- Data type standardization
- Loads all files into raw.transactions

Version 2 extends the pipeline to support ingestion from multiple file formats.

Supported formats:
- CSV
- JSON
- Parquet

The pipeline automatically:
- Discovers input files
- Validates the schema
- Standardizes column names
- Standardizes data types
- Loads all records into PostgreSQL

## Data Validation

The pipeline validates:

- Required columns
- Supported file formats
- Numeric data types
- Empty input directory

### Version 3

Single Table → Partitioned Files

```text
transactions
      ↓
year=2025/
year=2026/
```

### Version 4+

* JSON Processing
* PDF Statement Extraction
* dbt Modeling
* Snowflake Migration
* Databricks Lakehouse Architecture


## Successful DAG Run

![DAG Success](docs/images/banking_pipeline_dag_v1.png)