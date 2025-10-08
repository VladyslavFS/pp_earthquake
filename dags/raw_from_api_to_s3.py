import logging
import os
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

# AWS credentials
AWS_ACCESS_KEY = Variable.get("aws_access_key_id", default_var=os.getenv("AWS_ACCESS_KEY_ID"))
AWS_SECRET_KEY = Variable.get("aws_secret_access_key", default_var=os.getenv("AWS_SECRET_ACCESS_KEY"))
AWS_REGION = Variable.get("aws_region", default_var=(os.getenv("AWS_REGION"), "eu-north-1"))
S3_BUCKET = Variable.get("s3_bucket_name", default_var=os.getenv("S3_BUCKET_NAME"))

args = {
    "owner": OWNER,
    "start_date": pendulum.datetime(2025, 9, 1, tz="Europe/Kyiv"),
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
        SET s3_region = '{AWS_REGION}';
        SET s3_access_key_id = '{AWS_ACCESS_KEY}';
        SET s3_secret_access_key = '{AWS_SECRET_KEY}';
        SET s3_use_ssl = TRUE;

        COPY
        (
            SELECT
                *
            FROM
                read_csv_auto('https://earthquake.usgs.gov/fdsnws/event/1/query?format=csv&starttime={start_date}&endtime={end_date}') AS res
        ) TO 's3://{S3_BUCKET}/{LAYER}/{SOURCE}/{start_date}/{start_date}_00-00-00.gz.parquet';
        """,
    )

    conn.close()
    logging.info(f"Download for date success: {start_date}")

dag = DAG(
    dag_id=DAG_ID,
    schedule_interval="0 5 * * *",
    default_args=args,
    tags=["s3", "raw", "aws"],
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