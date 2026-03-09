from airflow import DAG, Dataset
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.exceptions import AirflowSkipException
from datetime import datetime, timedelta
import pandas as pd
import logging

logger = logging.getLogger(__name__)
BUCKET_NAME = "production-log"
MINIO_CONN_ID = "MINIO_CONN"


def connect_minio_and_verify_bucket_existence(**kwargs):
    minio_conn = S3Hook(aws_conn_id=MINIO_CONN_ID)
    try:
        conn = minio_conn.get_conn()
        buckets = conn.list_buckets()
        if BUCKET_NAME in [b['Name'] for b in buckets.get('Buckets', [])]:
            logger.info(f"連線成功！Bucket '{BUCKET_NAME}' 存在於 MinIO 中。")
            return
        else:
            logger.error(f"連線成功，但 Bucket '{BUCKET_NAME}' 不存在於 MinIO 中。")
            raise Exception(f"Bucket '{BUCKET_NAME}' 不存在")
    except Exception as e:
        logger.error(f"連線失敗，錯誤原因：{str(e)}")
        raise AirflowSkipException(
            "Failed to connect to MinIO, skipping downstream tasks.")


def verify_key_existence(**kwargs):
    minio_conn = S3Hook(aws_conn_id=MINIO_CONN_ID)

    exec_date = datetime.now()
    year = exec_date.strftime("%Y")
    month = exec_date.strftime("%m")
    day = exec_date.strftime("%d")

    prefix = f"bronze/daily-merged/year={year}/month={month}/day={day}/"

    has_parquet = minio_conn.get_key(
        f"{prefix}/{year}-{month}-{day}.parquet", bucket_name=BUCKET_NAME)

    if has_parquet:
        logger.info(
            f"找到 Parquet 檔案：{BUCKET_NAME}/{prefix}/{year}-{month}-{day}.parquet")
        kwargs['ti'].xcom_push(key='bronze_parquet_key',
                               value=f"{prefix}/{year}-{month}-{day}.parquet")
        return {"status": "success", "file_type": "parquet", "key": f"{prefix}/{year}-{month}-{day}.parquet"}

    else:
        logger.error(
            f"在 {BUCKET_NAME}/{prefix} 下沒有找到 Parquet 檔案。")
        raise FileNotFoundError(
            f"檔案不存在：{prefix}/{year}-{month}-{day}.parquet")


def refine_bronze_data_to_silver(**kwargs):
    minio_conn = S3Hook(aws_conn_id=MINIO_CONN_ID)

    bronze_key = kwargs['ti'].xcom_pull(key='bronze_parquet_key')
    file_obj = minio_conn.get_key(bronze_key, bucket_name=BUCKET_NAME).get()[
        "Body"].read()
    df = pd.read_parquet(file_obj.get()["Body"])
    columns = df.columns.tolist()
    logger.info(f"讀取到的欄位有：{columns}")

    # 計算新欄位

    # 移除欄位不合理值

    # 補齊空值

    # JOIN 其他維度表補齊靜態資料

    # 寫回 Silver 路徑


with DAG(
    "silver_daily_etl",
    start_date=datetime(2026, 3, 9),
    schedule=[Dataset("s3://production-log/bronze/daily-merged/")],
    catchup=False,
    tags=["production-log", "daily"],
    dagrun_timeout=timedelta(minutes=60),
) as dag:
    connect_minio_task = PythonOperator(
        task_id="connect_minio",
        python_callable=connect_minio_and_verify_bucket_existence,
        retries=3,
        retry_delay=timedelta(seconds=30))

    verify_key_existence_task = PythonOperator(
        task_id="verify_key_existence",
        python_callable=verify_key_existence
    )

    refine_data_task = PythonOperator(
        task_id="refine_data",
        python_callable=refine_bronze_data_to_silver,
    )

    connect_minio_task >> verify_key_existence_task
