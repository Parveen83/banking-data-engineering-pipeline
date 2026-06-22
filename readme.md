# Banking Data Engineering Platform - Version 1

## Overview

This project demonstrates a basic data engineering pipeline that loads banking transaction data into PostgreSQL and exports it into multiple file formats using Apache Airflow.

Version 1 focuses on the **Table → File** data engineering pattern.

## Architecture

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
│       └── banking_pipeline.py
│
├── sql/
│   ├── create_schema.sql
│   └── create_transactions_table.sql
│
├── data/
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
create_schema
      ↓
create_table
      ↓
load_csv
      ↓
 ┌───────────────┐
 │               │
 ↓               ↓
export_csv   export_parquet
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

## Future Enhancements

### Version 2

Multiple Files → Single Table

```text
transactions_jan.csv
transactions_feb.csv
transactions_mar.csv
          ↓
       Airflow
          ↓
 PostgreSQL Table
```

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