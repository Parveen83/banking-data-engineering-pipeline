from datetime import datetime

import glob
import pandas as pd
import logging
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

VALID_TRANSACTION_TYPES = {
    "PAYMENT",
    "TRANSFER",
    "CASH_OUT",
    "CASH_IN",
    "DEBIT",
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

def validate_datatypes(df):

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
        try:
            pd.to_numeric(df[col], errors="raise")
        except Exception:
            raise ValueError(
                f"Invalid values found in column '{col}'"
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
def validate_nulls(df):

    null_counts = df.isnull().sum()

    if null_counts.any():

        raise ValueError(
            f"Null values found:\n{null_counts[null_counts > 0]}"
        )

    logging.info("Null validation successful")

def validate_duplicates(df):
    
    duplicate_count = df.duplicated().sum()

    if duplicate_count > 0:
        raise ValueError(
            f"Duplicate rows found: {duplicate_count}"
        )

    logging.info("Duplicate validation successful")

def validate_amount(df):

    invalid_rows = df[df["amount"] < 0]

    if not invalid_rows.empty:

        raise ValueError(
            f"Negative amounts found: {len(invalid_rows)}"
        )

    logging.info("Amount validation successful")

def validate_transaction_type(df):

    invalid = df[
        ~df["type"].isin(VALID_TRANSACTION_TYPES)
    ]

    if not invalid.empty:

        raise ValueError(
            f"Invalid transaction types found:\n"
            f"{invalid['type'].unique()}"
        )

    logging.info("Transaction type validation successful")

def validate_fraud_flag(df):

    invalid = df[
        ~df["isFraud"].isin([0, 1])
    ]

    if not invalid.empty:

        raise ValueError(
            "Invalid fraud flag found"
        )

    logging.info("Fraud flag validation successful")

def load_transaction_files():

    files = glob.glob(
        "/opt/airflow/data/input/*"
    )
    
    if not files:
        raise ValueError(
            "No files found in /opt/airflow/data/input"
        )

    logging.info(f"Files discovered: {files}")

    engine = create_engine(DB_CONN)

    with engine.begin() as conn:
        conn.execute(
            text(
                "TRUNCATE TABLE raw.transactions"
            )
        )

    total_rows_loaded = 0

    for file in files:
        try:
            logging.info("Loading file: %s", file)

            df = read_file(file)

            validate_schema(df)

            validate_nulls(df)

            validate_duplicates(df)

            validate_amount(df)
            validate_transaction_type(df)
            validate_fraud_flag(df)
            #validate_datatypes(df)

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

            logging.info("Loaded %s rows from %s", len(df), file)

        except Exception:
            logging.exception("Failed to process file: %s", file)
            

def validate_load():

    engine = create_engine(DB_CONN)

    result = pd.read_sql(
        """
        SELECT COUNT(*) AS total_rows
        FROM raw.transactions
        """,
        con=engine,
    )

    logging.info(f"Load validation result: {result.iloc[0]['total_rows']}")

def validate_staging():

    engine = create_engine(DB_CONN)

    raw_count = pd.read_sql(
        """
        SELECT COUNT(*) AS total_rows
        FROM raw.transactions
        """,
        con=engine,
    ).iloc[0]["total_rows"]

    staging_count = pd.read_sql(
        """
        SELECT COUNT(*) AS total_rows
        FROM staging.transactions
        """,
        con=engine,
    ).iloc[0]["total_rows"]

    logging.info(f"Raw rows      : {raw_count}")
    logging.info(f"Staging rows  : {staging_count}")

    if raw_count != staging_count:
        raise ValueError(
            "Row count mismatch"
        )

    print("Validation Successful")


with DAG(
    dag_id="banking_pipeline_v3",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["banking", "version3"],
) as dag:

    def create_schema():
        execute_sql_file(
            f"{SQL_PATH}/create_schemas.sql"
        )
    def create_staging_schema():
        execute_sql_file(
            f"{SQL_PATH}/create_staging_schema.sql"
        )

    def create_staging_table():
        execute_sql_file(
            f"{SQL_PATH}/create_staging_table.sql"
        )

    def transform_to_staging():
        execute_sql_file(
            f"{SQL_PATH}/transform_to_staging.sql"
        )
    create_schema_task = PythonOperator(
        task_id="create_schema",
        python_callable=create_schema,
    )

    create_table_task = PythonOperator(
        task_id="create_table",
        python_callable=create_table,
    )

    create_staging_schema_task = PythonOperator(
        task_id="create_staging_schema",
        python_callable=create_staging_schema,
    )

    create_staging_table_task = PythonOperator(
        task_id="create_staging_table",
        python_callable=create_staging_table,
    )

    load_files_task = PythonOperator(
        task_id="load_transaction_files",
        python_callable=load_transaction_files,
    )

    validate_load_task = PythonOperator(
        task_id="validate_load",
        python_callable=validate_load,
    )   

    transform_to_staging_task = PythonOperator(
        task_id="transform_to_staging",
        python_callable=transform_to_staging,
    )

    validate_staging_task = PythonOperator(
        task_id="validate_staging",
        python_callable=validate_staging,
    )    

    (
        create_schema_task
        >> create_table_task
        >> create_staging_schema_task
        >> create_staging_table_task
        >> load_files_task
        >> validate_load_task
        >> transform_to_staging_task
        >> validate_staging_task
    )