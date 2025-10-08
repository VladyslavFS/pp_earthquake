import logging
import os
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
AWS_ACCESS_KEY = Variable.get("aws_access_key_id", default_var=os.getenv("AWS_ACCESS_KEY_ID"))
AWS_SECRET_KEY = Variable.get("aws_secret_access_key", default_var=os.getenv("AWS_SECRET_ACCESS_KEY"))
AWS_REGION = Variable.get("aws_region", default_var=os.getenv("AWS_REGION", "eu-north-1"))
S3_BUCKET = Variable.get("s3_bucket_name", default_var=os.getenv("S3_BUCKET_NAME"))

# Postgres credentials
DB_HOST = Variable.get("rds_endpoint", default_var=os.getenv("RDS_ENDPOINT"))
DB_PORT = Variable.get("rds_port", default_var=os.getenv("RDS_PORT", "5432"))
DB_NAME = Variable.get("pg_db_name", default_var=os.getenv("RDS_DATABASE", "postgres"))
DB_USER = Variable.get("pg_user", default_var=os.getenv("RDS_USERNAME", "postgres"))
DB_PASSWORD = Variable.get("pg_password", default_var=os.getenv("RDS_PASSWORD"))

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
            HOST '{DB_HOST}',
            PORT {DB_PORT},
            DATABASE {DB_NAME},
            USER {DB_USER},
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