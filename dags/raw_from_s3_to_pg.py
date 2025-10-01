import logging

import duckdb
import pendulum
import datetime
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

OWNER = "VladyslavFS"
DAG_ID = "raw_from_s3_to_pg"

LAYER = "raw"
SOURCE = "earthquake"
SCHEMA = "ods"
TARGET_TABLE = "fct_earthquake"

# AWS credentials
AWS_ACCESS_KEY = Variable.get("aws_access_key_id")
AWS_SECRET_KEY = Variable.get("aws_secret_access_key")
AWS_REGION = Variable.get("aws_region", default_var="eu-north-1")
S3_BUCKET = Variable.get("s3_bucket_name")

DB_PASSWORD = Variable.get("pg_password")

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

def get_and_transfer_raw_data_to_ods_func(**context):
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

        CREATE SECRET dwh_postgres (
            TYPE postgres,
            HOST 'postgres_dwh',
            PORT 5432,
            DATABASE postgres,
            USER 'postgres',
            PASSWORD '{DB_PASSWORD}'
        );

        ATTACH '' AS dwh_postgres_db (TYPE postgres, SECRET dwh_postgres);

        INSERT INTO dwh_postgres_db.{SCHEMA}.{TARGET_TABLE}
        (
            time,
            latitude,
            longitude,
            depth,
            mag,
            mag_type,
            nst,
            gap,
            dmin,
            rms,
            net,
            id,
            updated,
            place,
            type,
            horizontal_error,
            depth_error,
            mag_error,
            mag_nst,
            status,
            location_source,
            mag_source
        )
        SELECT
            time,
            latitude,
            longitude,
            depth,
            mag,
            magType AS mag_type,
            nst,
            gap,
            dmin,
            rms,
            net,
            id,
            updated,
            place,
            type,
            horizontalError AS horizontal_error,
            depthError AS depth_error,
            magError AS mag_error,
            magNst AS mag_nst,
            status,
            locationSource AS location_source,
            magSource AS mag_source
        FROM 's3://{S3_BUCKET}/{LAYER}/{SOURCE}/{start_date}/{start_date}_00-00-00.gz.parquet';
        """,
    )

    conn.close()
    logging.info(f"Download for date success: {start_date}")

dag = DAG(
    dag_id=DAG_ID,
    schedule_interval="0 5 * * *",
    default_args=args,
    tags=["s3", "ods", "pg", "aws"],
    concurrency=1,
    max_active_tasks=1,
    max_active_runs=1,
)

start = EmptyOperator(task_id="start", dag=dag,)

sensor_on_s3 = S3KeySensor(
    task_id="sensor_on_s3",
    bucket_key="raw/earthquake/{{ ds }}/{{ ds }}_00-00-00.gz.parquet",
    bucket_name=S3_BUCKET,
    aws_conn_id="aws_default",
    poke_interval=60,
    timeout=360_000,
    mode="reschedule",
    dag=dag,
)

get_and_transfer_raw_data_to_ods_task = PythonOperator(
    task_id="get_and_transfer_raw_data_to_ods",
    python_callable=get_and_transfer_raw_data_to_ods_func,
    dag=dag,
)

end = EmptyOperator(task_id="end", dag=dag,)

start >> get_and_transfer_raw_data_to_ods_task >> end