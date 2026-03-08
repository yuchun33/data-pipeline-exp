from airflow import DAG, Dataset
from airflow.providers.standard.operators.python import PythonOperator
import pandas as pd
import io
from datetime import datetime, timedelta
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import logging

logger = logging.getLogger(__name__)


def generate_golden_error_report(**kwargs):
    s3_hook = S3Hook(aws_conn_id="MINIO")
    bucket = "production-log"

    # 取得日期資訊
    exec_date = kwargs.get("run_after") or kwargs.get("execution_date")

    if not exec_date:
        exec_date = datetime.now()

    year = exec_date.strftime("%Y")
    month = exec_date.strftime("%m")
    day = exec_date.strftime("%d")

    # 來源路徑 (從之前合併好的 Bronze/Silver 讀取)
    source_key = (
        f"silver/production-log/year={year}/month={month}/day={day}/refined_log.parquet"
    )
    # 黃金層輸出路徑
    golden_key = f"golden/daily_machine_health/year={year}/month={month}/day={day}/top_errors.parquet"

    # 1. 讀取資料
    obj = s3_hook.get_key(source_key, bucket)
    df = pd.read_parquet(io.BytesIO(obj.get()["Body"].read()))

    # 2. 計算指標
    # 建立失敗標記：1 為 FAIL，0 為 OK
    df["is_fail"] = (df["status"] == "FAIL").astype(int)

    # 按機器聚合
    report_df = (
        df.groupby("machine_id")
        .agg(total_count=("status", "count"), fail_count=("is_fail", "sum"))
        .reset_index()
    )

    # 計算 Error Rate (百分比格式)
    report_df["error_rate_%"] = (
        report_df["fail_count"] / report_df["total_count"] * 100
    ).round(2)

    # 3. 排序 (將錯誤率最高的排在最前面)
    report_df = report_df.sort_values(by="error_rate_%", ascending=False)

    # 4. 寫回 MinIO
    buffer = io.BytesIO()
    report_df.to_parquet(buffer, index=False)
    buffer.seek(0)
    s3_hook.load_file_obj(
        file_obj=buffer, key=golden_key, bucket_name=bucket, replace=True
    )

    # 在 Airflow Log 中印出前五名異常機器
    print(f"Top 5 Problematic Machines for {year}-{month}-{day}:")
    print(report_df.head(5).to_string(index=False))


with DAG(
    "daily_production_report",
    start_date=datetime(2026, 1, 1),
    schedule=[Dataset("s3://production-log/silver")],
    catchup=False,
) as dag:

    gen_golden_report_task = PythonOperator(
        task_id="daily_production_report",
        python_callable=generate_golden_error_report,
        retries=2,
        retry_delay=timedelta(minutes=5),
    )

    gen_golden_report_task
