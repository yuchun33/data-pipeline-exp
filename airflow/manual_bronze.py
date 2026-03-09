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
    minio_hook = S3Hook(aws_conn_id=MINIO_CONN_ID)
    try:
        conn = minio_hook.get_conn()
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
    minio_hook = S3Hook(aws_conn_id=MINIO_CONN_ID)

    exec_date = kwargs.get("logical_date") or kwargs.get("execution_date")

    year = exec_date.strftime("%Y")
    month = exec_date.strftime("%m")
    day = exec_date.strftime("%d")

    source_prefix = f"topics/production-log/year={year}/month={month}/day={day}/"
    target_key = f"bronze/daily-merged/year={year}/month={month}/day={day}/{exec_date.strftime('%Y-%m-%d')}.parquet"

    logger.info(f"開始合併 {year}-{month}-{day} 的日誌")
    logger.info(f"來源路徑：{source_prefix}")
    logger.info(f"目標路徑：{target_key}")

    keys = minio_hook.list_keys(bucket_name=BUCKET_NAME, prefix=source_prefix)
    if not keys:
        logger.warning(f"在 {BUCKET_NAME}/{source_prefix} 下沒有找到任何檔案。")
        raise AirflowSkipException(
            "No files found, skipping downstream tasks.")

    kwargs['ti'].xcom_push(key='total_files', value=len(keys))
    kwargs['ti'].xcom_push(key='source_prefix', value=source_prefix)
    kwargs['ti'].xcom_push(key='target_key', value=target_key)

    return {"status": "success", "total_files": len(keys), "source_prefix": source_prefix, "target_key": target_key}


def merge_daily_logs(**kwargs):
    minio_hook = S3Hook(aws_conn_id=MINIO_CONN_ID)

    ti = kwargs['ti']

    total_files = ti.xcom_pull(
        task_ids='verify_key_existence', key='total_files')
    source_prefix = ti.xcom_pull(
        task_ids='verify_key_existence', key='source_prefix')
    target_key = ti.xcom_pull(
        task_ids='verify_key_existence', key='target_key')

    keys = minio_hook.list_keys(bucket_name=BUCKET_NAME, prefix=source_prefix)

    all_data = []
    failed_files = []
    for idx, key in enumerate(keys, 1):
        try:
            file_obj = minio_hook.get_key(key, bucket_name=BUCKET_NAME).get()[
                "Body"].read()
            if key.endswith(".parquet"):
                df = pd.read_parquet(io.BytesIO(file_obj))
            if key.endswith(".json"):
                df = pd.read_json(io.BytesIO(file_obj), lines=True)
            else:
                logger.warning(f"不支援的檔案格式：{key}，跳過此檔案。")
                continue

            if df.empty:
                logger.warning(f"檔案 {key} 為空，跳過此檔案。")
                continue

            all_data.append(df)
            logger.debug(
                f"已讀取 {idx}/{total_files} 個檔案：{key}，目前累積 {len(all_data)} 個 DataFrame。")

        except Exception as e:
            failed_files.append(key)
            logger.debug(f"讀取檔案 {key} 失敗，錯誤原因：{str(e)}")
            continue

    if not all_data:
        logger.warning("沒有成功讀取任何檔案，無法進行合併。")
        raise AirflowSkipException(
            "No valid files to merge, skipping downstream tasks.")

    merged_df = pd.concat(all_data, ignore_index=True)
    total_rows = len(merged_df)
    logger.info(f"成功合併 {len(all_data)} 個檔案，共 {total_rows} 筆記錄")

    null_counts = merged_df.isnull().sum().sum()
    if null_counts > 0:
        logger.warning(
            f"合併後的 DataFrame 中有缺失值，總計 {null_counts} 個缺失值。")

    try:
        buffer = io.BytesIO()
        merged_df.to_parquet(buffer, index=False, compression="snappy")
        buffer.seek(0)
        file_size = buffer.getbuffer().nbytes
        logger.info(f"合併後的 Parquet 檔案大小：{file_size / 1024 / 1024:.2f} MB")

        minio_hook.load_file_obj(
            file_obj=buffer, key=target_key, bucket_name=BUCKET_NAME, replace=True)
        logger.info(f"成功上傳合併檔案到 {target_key}")

        kwargs['ti'].xcom_push(key='target_key', value=target_key)
        kwargs['ti'].xcom_push(key='expect_rows', value=total_rows)

        return {"status": "success", "target_key": target_key, "expect_rows": total_rows}
    except Exception as e:
        logger.error(f"上傳合併檔案失敗，錯誤原因：{str(e)}")
        raise AirflowSkipException(
            "Failed to upload merged file, skipping downstream tasks.")


def verify_mergerd_file(**kwargs):
    minio_hook = S3Hook(aws_conn_id=MINIO_CONN_ID)

    ti = kwargs['ti']
    target_key = ti.xcom_pull(
        task_ids='merge_daily_logs', key='target_key')
    expect_rows = ti.xcom_pull(
        task_ids='merge_daily_logs', key='expect_rows')

    logger.info(f"開始驗證檔案：{target_key}")
    logger.info(f"預期筆數：{expect_rows}")

    file_obj = minio_hook.get_key(target_key, bucket_name=BUCKET_NAME).get()[
        "Body"].read()
    df_check = pd.read_parquet(io.BytesIO(file_obj))
    actual_rows = len(df_check)

    if actual_rows == expect_rows:
        columns = df_check.columns.tolist()
        logger.info(f"驗證成功！實際筆數與預期筆數一致。預期：{expect_rows} == {actual_rows}")
        logger.info(f"✓ 檔案路徑：{target_key}")
        logger.info(f"✓ 資料欄位：{columns}")
        logger.info(f"✓ 資料形狀：{df_check.shape[0]} 行 × {df_check.shape[1]} 列")
        logger.info(f"✓ 資料類型：{df_check.dtypes.to_dict()}")

        kwargs['ti'].xcom_push(key='columns', value=columns)
        kwargs['ti'].xcom_push(key='actual_rows', value=actual_rows)
        kwargs['ti'].xcom_push(key='file_key', value=target_key)

        return {"status": "success", "file_key": target_key, "actual_rows": actual_rows, "columns": columns}
    else:
        logger.error(f"驗證失敗！實際筆數與預期筆數不一致。預期：{expect_rows}，實際：{actual_rows}")
        raise


with DAG(
    "bronze_daily_compaction",
    tags=["production-log"],
    start_date=datetime(2026, 1, 1, 23, 0, 0),
    schedule=timedelta(days=1),
    catchup=False,
    dagrun_timeout=timedelta(minutes=10),
    description="每日 23:00 觸發的 DAG，用於合併每日生產日誌並驗證結果",
) as dag:

    connect_minio_task = PythonOperator(
        task_id="connect_minio",
        python_callable=connect_minio_and_verify_bucket_existence,
        retries=3,
        retry_delay=timedelta(seconds=10)
    )

    verify_key_existence_task = PythonOperator(
        task_id="verify_key_existence",
        python_callable=verify_key_existence)

    merge_daily_logs_task = PythonOperator(
        task_id="merge_daily_logs",
        python_callable=merge_daily_logs)

    verify_mergerd_file_task = PythonOperator(
        task_id="verify_merged_file",
        python_callable=verify_mergerd_file,
        outlets=[Dataset("s3://production-log/bronze/daily-merged/")],)

    connect_minio_task >> verify_key_existence_task >> merge_daily_logs_task >> verify_mergerd_file_task
