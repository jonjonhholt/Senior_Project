import pandas as pd
import numpy as np
import os
import logging
from sqlalchemy import create_engine

logging.basicConfig(level=logging.INFO)

DATASETS = {
    "sustainable_projects": {
        "file": "Sustainable Connections Projects.xlsx",
        "table": "sustainable_connections_projects",
        "columns": [
            "year", "government", "business",
            "nonprofit", "tribal_government", "other", "total"
        ],
        "primary_key": ["year"],
        "types": {
            "year": "int",
            "government": "int",
            "business": "int",
            "nonprofit": "int",
            "tribal_government": "int",
            "other": "int",
            "total": "int",
        }
    },
}

def get_engine():
    return create_engine(
        f"mssql+pymssql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
        f"@{os.getenv('DB_SERVER')}:1433/{os.getenv('DB_NAME')}",
        isolation_level="AUTOCOMMIT"
    )

def load_excel(file_path, columns):
    df = pd.read_excel(file_path)

    df.columns = columns

    df = df[pd.to_numeric(df["year"], errors="coerce").notna()]
    df["year"] = df["year"].astype(int)

    df = df.replace({np.nan: None})

    int_cols = df.columns.drop("year")
    df[int_cols] = df[int_cols].astype("Int64")

    return df

def upsert_data(df, table_name, engine):
    with engine.begin() as conn:
        years = tuple(df["year"].unique())

        conn.execute(
            f"DELETE FROM {table_name} WHERE year IN {years}"
        )

        df.to_sql(
            table_name,
            conn,
            if_exists="append",
            index=False
        )

def main():
    engine = get_engine()

    columns = [
        "year",
        "government",
        "business",
        "nonprofit",
        "tribal_government",
        "other",
        "total",
    ]

    df = load_excel(
        "/Users/skyler/Downloads/Sustainable Connections Projects.xlsx",
        columns
    )

    upsert_data(df, "sustainable_connections_projects", engine)

    logging.info("Upload complete")

if __name__ == "__main__":
    main()