from datetime import datetime

import pandas as pd
from sqlalchemy import create_engine, text

from airflow import DAG
from airflow.operators.python import PythonOperator


DB_CONN = "postgresql://postgres:postgres@banking-postgres:5432/banking"
SQL_PATH = "/opt/airflow/sql"

def execute_sql_file(file_path):
    engine = create_engine(DB_CONN)

    with open(file_path, "r") as f:
        sql = f.read()

    with engine.begin() as conn:
        conn.execute(text(sql))

def create_schema():
    execute_sql_file(f"{SQL_PATH}/create_schemas.sql")

def create_table():
    execute_sql_file(f"{SQL_PATH}/create_transactions_table.sql")

def load_csv():
    df = pd.read_csv("/opt/airflow/data/raw/paysim.csv")

    df = df.rename(
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

    engine = create_engine(DB_CONN)

    # Clear old data before reload
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE raw.transactions"))

    df.to_sql(
        name="transactions",
        con=engine,
        schema="raw",
        if_exists="append",
        index=False,
        chunksize=10000,
    )


# def export_csv():
#     engine = create_engine(DB_CONN)

#     df = pd.read_sql(
#         "SELECT * FROM raw.transactions",
#         con=engine,
#     )

#     df.to_csv(
#         "/opt/airflow/data/exports/transactions.csv",
#         index=False,
#     )

def export_csv():
    engine = create_engine(DB_CONN)

    sql = """
    COPY (
        SELECT *
        FROM raw.transactions
    )
    TO STDOUT WITH CSV HEADER
    """

    conn = engine.raw_connection()

    try:
        cursor = conn.cursor()

        with open(
            "/opt/airflow/data/exports/transactions.csv",
            "w"
        ) as f:
            cursor.copy_expert(sql, f)

        cursor.close()

    finally:
        conn.close()
# def export_parquet():
#     engine = create_engine(DB_CONN)

#     df = pd.read_sql(
#         "SELECT * FROM raw.transactions",
#         con=engine,
#     )

#     df.to_parquet(
#         "/opt/airflow/data/exports/transactions.parquet",
#         index=False,
#     )

def export_parquet():
    engine = create_engine(DB_CONN)

    chunks = []

    for chunk in pd.read_sql(
        "SELECT * FROM raw.transactions",
        con=engine,
        chunksize=100000
    ):
        chunks.append(chunk)

    df = pd.concat(chunks)

    df.to_parquet(
        "/opt/airflow/data/exports/transactions.parquet",
        index=False
    )

with DAG(
    dag_id="banking_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["banking", "version1"],
) as dag:

    create_schema_task = PythonOperator(
        task_id="create_schema",
        python_callable=create_schema,
    )

    create_table_task = PythonOperator(
        task_id="create_table",
        python_callable=create_table,
    )

    load_csv_task = PythonOperator(
        task_id="load_csv",
        python_callable=load_csv,
    )

    export_csv_task = PythonOperator(
        task_id="export_csv",
        python_callable=export_csv,
    )

    export_parquet_task = PythonOperator(
        task_id="export_parquet",
        python_callable=export_parquet,
    )

    (
        create_schema_task
        >> create_table_task
        >> load_csv_task
        >> [export_csv_task, export_parquet_task]
    )