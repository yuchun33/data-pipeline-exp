from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
import pandas as pd
import io
import logging

logger = logging.getLogger(__name__)


def compact_daily_logs(**kwargs):
    """
    合併當日所有的 production logs 為單一 parquet 檔案
    """
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

    exec_date = kwargs.get("logical_date") or kwargs.get("execution_date")

    if not exec_date:
        # 如果還是抓不到，從 ds 字串轉換（最後的保險）
        ds = kwargs.get("ds")
        exec_date = datetime.strptime(ds, "%Y-%m-%d")

    year = exec_date.strftime("%Y")
    month = exec_date.strftime("%m")
    day = exec_date.strftime("%d")

    # 定義來源與輸出路徑
    source_prefix = f"topics/production-log/year={year}/month={month}/day={day}/"
    target_key = f"archive/production-log/year={year}/month={month}/day={day}/daily_merged.parquet"

    logger.info(f"開始合併 {year}-{month}-{day} 的日誌")
    logger.info(f"來源路徑：{source_prefix}")
    logger.info(f"目標路徑：{target_key}")

    # 1. 獲取該天目錄下所有檔案
    try:
        keys = s3_hook.list_keys(bucket_name=bucket, prefix=source_prefix)
    except Exception as e:
        logger.error(f"無法列出 {bucket}/{source_prefix} 下的檔案：{str(e)}")
        raise

    if not keys:
        logger.warning(f"No logs found for {year}-{month}-{day}")
        return None

    logger.info(f"找到 {len(keys)} 個檔案待合併")

    # 2. 批次讀取與合併
    all_data = []
    failed_files = []

    for idx, key in enumerate(keys, 1):
        try:
            file_obj = s3_hook.get_key(key, bucket_name=bucket).get()["Body"].read()

            # 根據檔案副檔名決定讀取方式
            if key.endswith(".parquet"):
                df = pd.read_parquet(io.BytesIO(file_obj))
            elif key.endswith(".json"):
                df = pd.read_json(io.BytesIO(file_obj), lines=True)
            else:
                logger.warning(f"跳過不支援的檔案格式：{key}")
                continue

            # 驗證 DataFrame 不為空
            if df.empty:
                logger.warning(f"檔案 {key} 為空，已跳過")
                continue

            # --- 新增：強制 Schema 檢查 ---
            expected_cols = ["timestamp", "machine_id"]  # 定義你一定要有的欄位

            # 狀況 A：如果是讀到數字欄位 (例如 ['0'])
            if "0" in df.columns:
                logger.error(
                    f"檔案 {key} 格式異常，欄位被解析為 '0'。內容預覽：{df.iloc[0].to_dict()}"
                )
                continue  # 或者 raise

            # 狀況 B：檢查關鍵欄位是否存在
            missing = [c for c in expected_cols if c not in df.columns]
            if missing:
                logger.warning(
                    f"檔案 {key} 缺少欄位 {missing}，已跳過。現有欄位：{df.columns.tolist()}"
                )
                continue

            all_data.append(df)
            logger.debug(f"[{idx}/{len(keys)}] 成功讀取 {key}，共 {len(df)} 筆記錄")

        except Exception as e:
            logger.error(f"讀取檔案 {key} 失敗：{str(e)}")
            failed_files.append(key)
            continue

    if not all_data:
        logger.error("無法讀取任何有效的日誌檔案")
        raise ValueError(f"無法合併 {year}-{month}-{day} 的日誌：沒有有效的檔案")

    if failed_files:
        logger.warning(f"有 {len(failed_files)} 個檔案讀取失敗：{failed_files}")

    try:
        # 3. 合併所有 DataFrame
        merged_df = pd.concat(all_data, ignore_index=True)
        total_rows = len(merged_df)
        logger.info(f"成功合併 {len(all_data)} 個檔案，共 {total_rows} 筆記錄")

        # 驗證合併結果
        if merged_df.isnull().any().any():
            null_count = merged_df.isnull().sum().sum()
            logger.warning(f"合併後的資料包含 {null_count} 個 NULL 值")

        # 4. 轉為 Parquet 並回傳至 MinIO
        buffer = io.BytesIO()
        merged_df.to_parquet(buffer, index=False, compression="snappy")
        buffer.seek(0)

        file_size = buffer.getbuffer().nbytes
        logger.info(f"Parquet 檔案大小：{file_size / 1024 / 1024:.2f} MB")

        # 上傳至 MinIO
        s3_hook.load_file_obj(
            file_obj=buffer, key=target_key, bucket_name=bucket, replace=True
        )
        logger.info(f"成功上傳合併檔案到 {target_key}")

        # 將結果傳遞給下一個 task
        kwargs["ti"].xcom_push(key="merged_file_key", value=target_key)
        kwargs["ti"].xcom_push(key="total_rows", value=total_rows)

        return {"status": "success", "file_key": target_key, "rows": total_rows}

    except Exception as e:
        logger.error(f"合併或上傳檔案失敗：{str(e)}")
        raise


def verify_parquet(**kwargs):
    """
    驗證合併後的 Parquet 檔案
    - 檢查檔案是否存在
    - 驗證實際筆數是否與合併筆數一致
    """
    s3_hook = S3Hook(aws_conn_id="MINIO")
    bucket = "production-log"

    # 從上一個 task 取得資訊
    ti = kwargs["ti"]
    merged_file_key = ti.xcom_pull(task_ids="compact_daily_logs", key="merged_file_key")
    expected_rows = ti.xcom_pull(task_ids="compact_daily_logs", key="total_rows")

    if not merged_file_key:
        logger.error("無法從上一個 task 獲取合併檔案路徑")
        raise ValueError("Missing merged_file_key from compact_daily_logs task")

    logger.info(f"開始驗證檔案：{merged_file_key}")
    logger.info(f"預期筆數：{expected_rows}")

    try:
        # 獲取檔案內容
        file_obj = s3_hook.get_key(merged_file_key, bucket_name=bucket)
        if not file_obj:
            raise FileNotFoundError(f"檔案不存在：{merged_file_key}")

        file_content = file_obj.get()["Body"].read()
        file_size = len(file_content)
        logger.info(f"檔案大小：{file_size / 1024 / 1024:.2f} MB")

        # 讀取 Parquet 檔案
        df_check = pd.read_parquet(io.BytesIO(file_content))

        actual_rows = len(df_check)
        columns = df_check.columns.tolist()

        logger.info(f"✓ 驗證成功！")
        logger.info(f"  檔案路徑：{merged_file_key}")
        logger.info(f"  實際筆數：{actual_rows}")
        logger.info(f"  資料欄位：{columns}")

        # 驗證筆數是否一致
        if expected_rows and actual_rows != expected_rows:
            error_msg = f"筆數不符！預期：{expected_rows}，實際：{actual_rows}"
            logger.error(error_msg)
            raise ValueError(error_msg)

        logger.info(f"✓ 筆數驗證通過：{actual_rows} 筆 == {expected_rows} 筆")

        # 檢查是否有 NULL 值
        null_counts = df_check.isnull().sum()
        if null_counts.sum() > 0:
            logger.warning(
                f"資料包含 NULL 值：{null_counts[null_counts > 0].to_dict()}"
            )
        else:
            logger.info(f"✓ 無 NULL 值")

        # 基本的資料品質檢查
        logger.info(f"✓ 資料形狀：{df_check.shape[0]} 行 × {df_check.shape[1]} 列")
        logger.info(f"✓ 資料類型：{df_check.dtypes.to_dict()}")

        # 推送驗證結果
        ti.xcom_push(key="verification_status", value="passed")
        ti.xcom_push(key="actual_rows", value=actual_rows)

        return {
            "status": "verification_passed",
            "expected_rows": expected_rows,
            "actual_rows": actual_rows,
            "columns": columns,
            "file_key": merged_file_key,
        }

    except FileNotFoundError as e:
        logger.error(f"檔案不存在：{str(e)}")
        raise
    except Exception as e:
        logger.error(f"驗證 Parquet 檔案失敗：{str(e)}")
        raise


with DAG(
    "minio_daily_compaction",
    start_date=datetime(2026, 1, 1),
    schedule=timedelta(days=1),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=1),
    description="每日合併 production logs 為 parquet 檔案並驗證",
) as dag:

    compact_task = PythonOperator(
        task_id="compact_daily_logs",
        python_callable=compact_daily_logs,
        retries=2,
        retry_delay=timedelta(minutes=5),
    )

    verify_task = PythonOperator(
        task_id="verify_parquet",
        python_callable=verify_parquet,
        retries=1,
        retry_delay=timedelta(minutes=2),
    )

    compact_task >> verify_task
