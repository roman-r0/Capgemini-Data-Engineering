import logging
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


_logger = logging.getLogger(__name__)
table_name = "airbnb_listings"


def read_csv_file(**kwargs):
    """Function to read CSV file into Pandas DataFrame."""
    file_path = kwargs.get("file_path")

    if not file_path:
        raise ValueError(f"Wrong arg {file_path=}")

    file_path = Path(file_path)

    df = pd.read_csv(file_path, sep=",")

    if len(df) <= 1:
        raise ValueError(f"{file_path=} is an invalid CSV file!")

    kwargs["ti"].xcom_push(key="raw_data", value=df.to_dict())
    _logger.info("File successfully read and DataFrame created.")


def transform_data(**kwargs):
    """Function to transform data from xcom and save it as a file."""
    file_path = kwargs.get("file_path")

    if not file_path:
        raise ValueError(f"Wrong arg {file_path=}")

    file_path = Path(file_path)

    ti = kwargs["ti"]
    raw_data = ti.xcom_pull(task_ids="extract_data", key="raw_data")

    if raw_data is None:
        raise ValueError("No data found in XCom.")

    df = pd.DataFrame(raw_data)

    df = df[df["price"] > 0]
    df["last_review"] = pd.to_datetime(df["last_review"], errors="coerce")

    if df["last_review"].isnull().any():
        earliest_date = (
            df["last_review"].min()
            if not df["last_review"].isnull().all()
            else pd.Timestamp("1970-01-01")
        )
        df["last_review"].fillna(earliest_date, inplace=True)

    df["reviews_per_month"].fillna(0, inplace=True)
    df["reviews_per_month"] = df["reviews_per_month"].round(2)

    df.dropna(subset=["latitude", "longitude"], inplace=True)

    df.to_csv(file_path, index=False)
    _logger.info(f"Transformed data saved to {file_path=}")


def perform_data_quality_checks(**kwargs):
    """Function to validate/check quality of inserted data to DB table."""
    transformed_file_path = kwargs.get("transformed_file_path")
    postgres_conn_id = kwargs.get("postgres_conn_id")
    hook = PostgresHook(postgres_conn_id=postgres_conn_id)

    if not transformed_file_path:
        raise ValueError(f"Missing {transformed_file_path=}")

    if not postgres_conn_id:
        raise ValueError(f"Missing {postgres_conn_id=}")

    df_transformed = pd.read_csv(transformed_file_path)
    expected_record_count = len(df_transformed)

    with hook.get_conn() as conn:
        with conn.cursor() as cursor:
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                actual_record_count = cursor.fetchone()[0]

                if actual_record_count != expected_record_count:
                    _logger.error(
                        f"Record count mismatch: expected {expected_record_count}, got {actual_record_count}"
                    )
                    return "data_quality_failed"

                # Check for NULL values in specific columns
                for column in ["price", "minimum_nights", "availability_365"]:
                    cursor.execute(
                        f"SELECT COUNT(*) FROM {table_name} WHERE {column} IS NULL"
                    )
                    null_count = cursor.fetchone()[0]

                    if null_count > 0:
                        _logger.error(
                            f"NULL values found in column {column}: {null_count} NULL values"
                        )
                        return "data_quality_failed"

                _logger.info("Data quality checks passed.")
                return "next_task"
            except Exception as e:
                _logger.error(f"Error during data quality checks: {e}")
                raise


def log_error_and_stop(**kwargs):
    """Function to log and raise exception for failure."""
    _logger.error("Data quality check failed. Stopping further processing.")
    raise ValueError("Data quality check failed.")


def failure_callback(context):
    """Function to callback in case of failure."""
    task_id = context.get("task_instance").task_id
    dag_id = context.get("task_instance").dag_id
    execution_date = context.get("execution_date")
    exception = context.get("exception")

    log_message = (
        f"Task {task_id} in DAG {dag_id} failed.\n"
        f"Execution Date: {execution_date}\n"
        f"Exception: {exception}\n"
    )

    with open("/opt/airflow/logs/task_failures.log", "a") as f:
        f.write(log_message)

    _logger.error(f"Task {task_id} failed. Details have been logged.")


with DAG(
    dag_id="nyc_airbnb_etl",
    start_date=datetime(2024, 8, 31),
    schedule_interval="@daily",
    catchup=False,
    on_failure_callback=failure_callback,
    default_args={
        "owner": "airflow",
        "retries": 5,
        "retry_delay": timedelta(minutes=5),
        "email_on_failure": False,
    },
):
    raw_file_path = "/opt/airflow/raw/AB_NYC_2019.csv"
    transformed_file_path = "/opt/airflow/transformed/AB_NYC_2019_transformed.csv"
    postgres_connection_id = "airflow_etl_postgres"

    create_airbnb_listing_table = PostgresOperator(
        task_id="create_airbnb_listing_table",
        postgres_conn_id=postgres_connection_id,
        sql=f"""
            DROP TABLE IF EXISTS airbnb_listings;
            CREATE TABLE {table_name} (
                 id SERIAL PRIMARY KEY,
                 name TEXT,
                 host_id INTEGER,
                 host_name TEXT,
                 neighbourhood_group TEXT,
                 neighbourhood TEXT,
                 latitude DECIMAL(9,6),
                 longitude DECIMAL(9,6),
                 room_type TEXT,
                 price INTEGER,
                 minimum_nights INTEGER,
                 number_of_reviews INTEGER,
                 last_review DATE,
                 reviews_per_month DECIMAL,
                 calculated_host_listings_count INTEGER,
                 availability_365 INTEGER
            );
        """,
    )

    extract_data_task = PythonOperator(
        task_id="extract_data",
        python_callable=read_csv_file,
        op_kwargs={"file_path": raw_file_path},
        provide_context=True,
    )

    transform_data_task = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data,
        op_kwargs={"file_path": transformed_file_path},
        provide_context=True,
    )

    load_data_to_airbnb_listing_table = PostgresOperator(
        task_id="load_data_to_airbnb_listing_table",
        postgres_conn_id=postgres_connection_id,
        sql=f"""
             COPY {table_name} FROM '{transformed_file_path}' WITH CSV HEADER;
        """,
    )

    data_quality_checks_task = PythonOperator(
        task_id="data_quality_checks",
        python_callable=perform_data_quality_checks,
        op_kwargs={
            "transformed_file_path": transformed_file_path,
            "postgres_conn_id": postgres_connection_id,
        },
        provide_context=True,
    )

    error_logging_task = PythonOperator(
        task_id="log_error_and_stop",
        python_callable=log_error_and_stop,
        provide_context=True,
    )

    next_task = DummyOperator(task_id="next_task")

    branch_task = BranchPythonOperator(
        task_id="branch_task",
        python_callable=lambda **kwargs: kwargs["ti"].xcom_pull(
            task_ids="data_quality_checks"
        )
        or "log_error_and_stop",
        provide_context=True,
    )

    (
        create_airbnb_listing_table
        >> extract_data_task
        >> transform_data_task
        >> load_data_to_airbnb_listing_table
        >> data_quality_checks_task
    )
    data_quality_checks_task >> branch_task
    branch_task >> [next_task, error_logging_task]
