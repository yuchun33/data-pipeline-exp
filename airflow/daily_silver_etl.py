from airflow import DAG, Dataset
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.standard.operators.python import PythonOperator
import logging
from datetime import datetime, timedelta
import pandas as pd
import io

logger = logging.getLogger(__name__)


def transform_to_silver(**kwargs):
    s3_hook = S3Hook(aws_conn_id="MINIO")

    try:
        # 1. 測試連線底層物件 (boto3 client)
        conn = s3_hook.get_conn()

        # 2. 嘗試列出 Bucket（最實務的測試）
        buckets = conn.list_buckets()
        logger.info(
            f"連線成功！目前擁有的 Buckets: {[b['Name'] for b in buckets.get('Buckets', [])]}"
        )

    except Exception as e:
        logger.error(f"連線失敗，錯誤原因：{str(e)}")
        raise  # 拋出異常讓 Task 標記為 Failed

    bucket = "production-log"

    # 1. 取得剛剛 Bronze 合併完的路徑
    exec_date = kwargs.get("logical_date") or kwargs.get("execution_date")

    if not exec_date:
        # 如果還是抓不到，從 ds 字串轉換（最後的保險）
        ds = kwargs.get("ds")
        exec_date = datetime.strptime(ds, "%Y-%m-%d")

    year = exec_date.strftime("%Y")
    month = exec_date.strftime("%m")
    day = exec_date.strftime("%d")

    bronze_key = f"archive/production-log/year={year}/month={month}/day={day}/daily_merged.parquet"
    silver_key = (
        f"silver/production-log/year={year}/month={month}/day={day}/refined_log.parquet"
    )

    # 2. 讀取 Bronze 資料
    obj = s3_hook.get_key(bronze_key, bucket)
    df = pd.read_parquet(io.BytesIO(obj.get()["Body"].read()))

    logger.info(f"讀取到的欄位有：{df.columns.tolist()}")

    # 3. 補充資料與清洗
    # (A) 時間轉換：Unix -> Datetime
    df["event_time"] = (
        pd.to_datetime(df["timestamp"], unit="s")
        .dt.tz_localize("UTC")
        .dt.tz_convert("Asia/Taipei")
    )
    df["hour"] = df["event_time"].dt.hour

    # (B) 靜態維度補充 (實務上這可以從資料庫或另一份 S3 檔案讀取)
    # 這裡示範用 Map 方式簡單補充
    machine_map = {"M01": "Line_A_Packer", "M02": "Line_B_Tester"}
    df["machine_name"] = df["machine_id"].map(machine_map).fillna("Unknown")

    # (C) 狀態標籤優化
    df["is_error"] = df["status"].apply(lambda x: 1 if x == "FAIL" else 0)

    # 4. 存回 Silver 區 (通常會依據需求做一些欄位篩選)
    final_df = df[
        [
            "event_time",
            "hour",
            "machine_id",
            "machine_name",
            "module",
            "slot",
            "order",
            "status",
            "is_error",
        ]
    ]

    buffer = io.BytesIO()
    print(final_df.head())
    final_df.to_parquet(buffer, index=False)
    buffer.seek(0)
    s3_hook.load_file_obj(buffer, key=silver_key, bucket_name=bucket, replace=True)

    logger.info(f"成功增加檔案到 {silver_key}")

    print(f"Silver layer processed: {len(final_df)} rows.")


with DAG(
    "daily_ETL_production_log",
    start_date=datetime(2026, 1, 1),
    schedule=timedelta(days=1),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=1),
    description="每日合併 production logs 為 parquet 檔案並驗證",
) as dag:

    compact_task = PythonOperator(
        task_id="transform_to_silver",
        python_callable=transform_to_silver,
        retries=2,
        retry_delay=timedelta(minutes=5),
        outlets=[Dataset("s3://production-log/silver")],
    )

    compact_task
