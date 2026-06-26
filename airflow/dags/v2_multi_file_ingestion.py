from datetime import datetime

import glob
import pandas as pd
from sqlalchemy import create_engine, text

from airflow import DAG
from airflow.operators.python import PythonOperator


DB_CONN = "postgresql://postgres:postgres@banking-postgres:5432/banking"
SQL_PATH = "/opt/airflow/sql"

EXPECTED_COLUMNS = {
    "step",
    "type",
    "amount",
    "nameOrig",
    "oldbalanceOrg",
    "newbalanceOrig",
    "nameDest",
    "oldbalanceDest",
    "newbalanceDest",
    "isFraud",
    "isFlaggedFraud",
}


def execute_sql_file(file_path):
    engine = create_engine(DB_CONN)

    with open(file_path, "r") as f:
        sql = f.read()

    with engine.begin() as conn:
        conn.execute(text(sql))


def create_schema():
    execute_sql_file(
        f"{SQL_PATH}/create_schemas.sql"
    )


def create_table():
    execute_sql_file(
        f"{SQL_PATH}/create_transactions_table.sql"
    )


def read_file(file_path):

    if file_path.endswith(".csv"):
        return pd.read_csv(file_path)

    elif file_path.endswith(".parquet"):
        return pd.read_parquet(file_path)

    elif file_path.endswith(".json"):
        return pd.read_json(file_path)

    else:
        raise ValueError(
            f"Unsupported file format: {file_path}"
        )


def validate_schema(df):

    actual_columns = set(df.columns)

    if actual_columns != EXPECTED_COLUMNS:
        raise ValueError(
            f"Schema mismatch.\n"
            f"Expected: {EXPECTED_COLUMNS}\n"
            f"Actual: {actual_columns}"
        )


def standardize_datatypes(df):

    numeric_columns = [
        "step",
        "amount",
        "oldbalanceOrg",
        "newbalanceOrig",
        "oldbalanceDest",
        "newbalanceDest",
        "isFraud",
        "isFlaggedFraud",
    ]

    for col in numeric_columns:
        df[col] = pd.to_numeric(df[col])

    return df


def standardize_columns(df):

    return df.rename(
        columns={
            "type": "transaction_type",
            "nameOrig": "source_account",
            "oldbalanceOrg": "source_old_balance",
            "newbalanceOrig": "source_new_balance",
            "nameDest": "destination_account",
            "oldbalanceDest": "destination_old_balance",
            "newbalanceDest": "destination_new_balance",
            "isFraud": "is_fraud",
            "isFlaggedFraud": "is_flagged_fraud",
        }
    )


def load_transaction_files():

    files = glob.glob(
        "/opt/airflow/data/input/*"
    )

    if not files:
        raise ValueError(
            "No files found in /opt/airflow/data/input"
        )

    print(f"Files discovered: {files}")

    engine = create_engine(DB_CONN)

    with engine.begin() as conn:
        conn.execute(
            text(
                "TRUNCATE TABLE raw.transactions"
            )
        )

    total_rows_loaded = 0

    for file in files:

        print(f"Loading file: {file}")

        df = read_file(file)

        validate_schema(df)

        df = standardize_datatypes(df)

        df = standardize_columns(df)

        df.to_sql(
            name="transactions",
            con=engine,
            schema="raw",
            if_exists="append",
            index=False,
            chunksize=10000,
        )

        rows_loaded = len(df)

        total_rows_loaded += rows_loaded

        print(
            f"Loaded {rows_loaded} rows from {file}"
        )

    print(
        f"Total rows loaded: {total_rows_loaded}"
    )


def validate_load():

    engine = create_engine(DB_CONN)

    result = pd.read_sql(
        """
        SELECT COUNT(*) AS total_rows
        FROM raw.transactions
        """,
        con=engine,
    )

    print(result)


with DAG(
    dag_id="v2_multi_file_ingestion",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["banking", "version2"],
) as dag:

    create_schema_task = PythonOperator(
        task_id="create_schema",
        python_callable=create_schema,
    )

    create_table_task = PythonOperator(
        task_id="create_table",
        python_callable=create_table,
    )

    load_files_task = PythonOperator(
        task_id="load_transaction_files",
        python_callable=load_transaction_files,
    )

    validate_load_task = PythonOperator(
        task_id="validate_load",
        python_callable=validate_load,
    )

    (
        create_schema_task
        >> create_table_task
        >> load_files_task
        >> validate_load_task
    )