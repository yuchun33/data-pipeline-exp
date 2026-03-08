import pandas as pd
import json
from pathlib import Path
from typing import Optional, List, Dict, Any


class JsonCrud:
    def __init__(self, filepath: str):
        self.filepath = Path(filepath)

    def create(self, data: List[Dict[str, Any]]) -> None:
        """Write data to JSON file"""
        df = pd.DataFrame(data)
        df.to_json(self.filepath, orient="records", indent=2)

    def read(self) -> pd.DataFrame:
        """Read data from JSON file"""
        if not self.filepath.exists():
            return pd.DataFrame()
        return pd.read_json(self.filepath)

    def update(self, index: int, data: Dict[str, Any]) -> None:
        """Update a record by index"""
        df = self.read()
        if index < len(df):
            for key, value in data.items():
                df.at[index, key] = value
            df.to_json(self.filepath, orient="records", indent=2)

    def delete(self, index: int) -> None:
        """Delete a record by index"""
        df = self.read()
        df = df.drop(index).reset_index(drop=True)
        df.to_json(self.filepath, orient="records", indent=2)

    def modify_column(self, column: str, func) -> None:
        """Modify a column using a function"""
        df = self.read()
        df[column] = df[column].apply(func)
        df.to_json(self.filepath, orient="records", indent=2)

    def add_column_from_existing(self, source_col: str, new_col: str, func) -> None:
        """Add a new column based on an existing column"""
        df = self.read()
        df[new_col] = df[source_col].apply(func)
        df.to_json(self.filepath, orient="records", indent=2)


# Example usage
if __name__ == "__main__":
    crud = JsonCrud("data.json")

    # Create
    crud.create([{"id": 1, "name": "John"}, {"id": 2, "name": "Jane"}])

    # Read
    print(crud.read())

    # Update
    crud.update(0, {"name": "John Doe"})

    # Delete
    crud.delete(1)
