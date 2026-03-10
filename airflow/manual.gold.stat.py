import io

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

    prefix = f"silver/daily-refined/year={year}/month={month}/day={day}"

    has_parquet = minio_conn.get_key(
        f"{prefix}/{year}-{month}-{day}.parquet", bucket_name=BUCKET_NAME)

    if has_parquet:
        logger.info(
            f"找到 Parquet 檔案：{BUCKET_NAME}/{prefix}/{year}-{month}-{day}.parquet")
        kwargs['ti'].xcom_push(key='silver_parquet_key',
                               value=f"{prefix}/{year}-{month}-{day}.parquet")
        return {"status": "success", "file_type": "parquet", "key": f"{prefix}/{year}-{month}-{day}.parquet"}

    else:
        logger.error(
            f"在 {BUCKET_NAME}/{prefix} 下沒有找到 Parquet 檔案。")
        raise FileNotFoundError(
            f"檔案不存在：{prefix}/{year}-{month}-{day}.parquet")


def stat_to_golden(**kwargs):
    minio_conn = S3Hook(aws_conn_id=MINIO_CONN_ID)

    silver_key = kwargs['ti'].xcom_pull(
        task_ids='verify_key_existence', key='silver_parquet_key')
    file_obj = minio_conn.get_key(silver_key, bucket_name=BUCKET_NAME).get()[
        "Body"].read()
    df = pd.read_parquet(io.BytesIO(file_obj))
    columns = df.columns.tolist()
    logger.info(f"讀取到的欄位有：{columns}")

    report_df = (
        df.groupby("machine_id")
        .agg(total_count=("status", "count"), fail_count=("is_fail", "sum"))
        .reset_index()
    )

    report_df["fail_rate"] = report_df["fail_count"] / report_df["total_count"]

    logger.info(f"計算完成的統計報表：\n{report_df.head()}")

    # 寫回 Golden 路徑
    buffer = io.BytesIO()
    report_df.to_parquet(buffer, index=False)
    buffer.seek(0)

    golden_key = f"golden/daily-statistics/{silver_key.split('/')[-1]}"
    minio_conn.load_file_obj(buffer, key=golden_key,
                             bucket_name=BUCKET_NAME, replace=True)

    logger.info(f"成功增加檔案到 {golden_key}")


with DAG(
    "golden_stat_dag",
    start_date=datetime(2026, 3, 9),
    schedule=[Dataset("s3://production-log/silver/daily-refined/")],
    catchup=False,
    tags=["production-log"],
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

    stat_to_golden_task = PythonOperator(
        task_id="stat_to_golden",
        python_callable=stat_to_golden,
    )

    connect_minio_task >> verify_key_existence_task >> stat_to_golden_task
