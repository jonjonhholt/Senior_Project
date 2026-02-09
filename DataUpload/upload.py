import pandas as pd
import numpy as np
from sqlalchemy import create_engine

server = "ciac-dashboard-db.database.windows.net"
database = "dashboard-main"
username = "ciac"
password = "Dashboard!"

engine = create_engine(
    f"mssql+pymssql://{username}:{password}@{server}:1433/{database}",
    isolation_level="AUTOCOMMIT"
)

df = pd.read_excel(
    "/Users/skyler/Downloads/Sustainable Connections Projects.xlsx"
)


df.columns = [
    "year",
    "government",
    "business",
    "nonprofit",
    "tribal_government",
    "other",
    "total",
]


df = df[pd.to_numeric(df["year"], errors="coerce").notna()]
df["year"] = df["year"].astype(int)

df = df.replace({np.nan: None})

int_cols = df.columns.drop("year")
df[int_cols] = df[int_cols].astype("Int64")

df.to_sql(
    "sustainable_connections_projects",
    engine,
    if_exists="append",
    index=False
)

print("Upload complete")
