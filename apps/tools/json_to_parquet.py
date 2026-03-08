import sys

import pandas as pd
import json
import glob
import os


def json_to_parquet(source_folder, output_filename):
    # 1. 取得資料夾內所有 .json 檔案的路徑
    # 使用 glob 可以輕鬆過濾副檔名
    json_files = glob.glob(os.path.join(source_folder, "*.json"))

    if not json_files:
        print("找不到任何 JSON 檔案。")
        return

    all_data = []

    # 2. 逐一讀取 JSON 檔案
    for file_path in json_files:
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                df = pd.read_json(f, lines=True)
                print(df.head())
                all_data.append(df)
        except Exception as e:
            print(f"讀取 {file_path} 時出錯：{e}")

    if all_data:
        combined_df = pd.concat(all_data, ignore_index=True)
        # 4. 存成 Parquet
        # engine='pyarrow' 是目前效能最好的選擇
        combined_df.to_parquet(output_filename, engine="pyarrow", index=False)
        print(f"成功合併 {len(all_data)} 筆資料，已儲存至：{output_filename}")


if __name__ == "__main__":
    # 設定你的路徑
    output_file = "merged_data.parquet"  # 輸出的檔名

    input_dir = sys.argv[1] if len(sys.argv) > 1 else "source"

    json_to_parquet(input_dir, output_file)
