import pandas as pd
import sys

def read_and_verify_parquet(file_path: str) -> pd.DataFrame:
    """
    Read a parquet file and verify its columns.
    
    Args:
        file_path: Path to the parquet file
        
    Returns:
        DataFrame with the parquet data
    """
    try:
        df = pd.read_parquet(file_path)
        
        print(f"File: {file_path}")
        print(f"Shape: {df.shape}")
        print(f"\nColumns ({len(df.columns)}):")
        for col in df.columns:
            print(f"  - {col}: {df[col].dtype}")
        
        print(f"\nFirst few rows:")
        print(df.head())
        
        return df
    except Exception as e:
        print(f"Error reading parquet file: {e}")
        return None


if __name__ == "__main__":
    # Example usage
    parquet_file = sys.argv[1] if len(sys.argv) > 1 else "your_file.parquet"
    df = read_and_verify_parquet(parquet_file)