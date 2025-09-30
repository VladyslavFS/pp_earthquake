import logging

import duckdb
import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

OWNER = "VladyslavFS"
DAG_ID = "raw_from_api_to_s3"

LAYER = "raw"
SOURCE = "earthquake"

ACCESS_KEY = Variable.get("accessKey")
SECRET_KEY = Variable.get("secretKey")

args = {
    "owner": OWNER,
    "start_date": pendulum.datetime(2025, 8, 1, tz="Europe/Kyiv"),
    "catchup": True,
    "retries": 3,
    "retry_delay": pendulum.duration(hours=1),
}

def get_dates(**context) -> tuple[str, str]:
    """"""
    start_date = context["data_interval_start"].format("YYYY-MM-DD")
    end_date = context["data_interval_end"].format("YYYY-MM-DD")

    return start_date, end_date

def get_and_transfer_api_data_to_s3_func(**context):
    """"""

    start_date, end_date = get_dates(**context)
    logging.info(f'Start load for dates: {start_date}/{end_date}')
    conn = duckdb.connect()

    conn.sql(
        f"""
        SET TIMEZONE='UTC';
        INSTALL httpfs;
        LOAD httpfs;
        SET s3_url_style = 'path';
        SET s3_endpoint = 'minio:9000';
        SET s3_access_key_id = '{ACCESS_KEY}';
        SET s3_secret_access_key = '{SECRET_KEY}';
        SET s3_use_ssl = FALSE;

        COPY
        (
            SELECT
                *
            FROM
                read_csv_auto('https://earthquake.usgs.gov/fdsnws/event/1/query?format=csv&starttime={start_date}&endtime={end_date}') AS res
        ) TO 's3://prod/{LAYER}/{SOURCE}/{start_date}/{start_date}_00-00-00.gz.parquet';
        """,
    )

    conn.close()
    logging.info(f"Download for date success: {start_date}")

dag = DAG(
    dag_id=DAG_ID,
    schedule_interval="0 5 * * *",
    default_args=args,
    tags=["s3", "raw"],
    concurrency=1,
    max_active_tasks=1,
    max_active_runs=1,
)

start = EmptyOperator(task_id="start", dag=dag,)

get_and_transfer_api_data_to_s3_task = PythonOperator(
    task_id="get_and_transfer_api_data_to_s3",
    python_callable=get_and_transfer_api_data_to_s3_func,
    dag=dag,
)

end = EmptyOperator(task_id="end", dag=dag,)

start >> get_and_transfer_api_data_to_s3_task >> end