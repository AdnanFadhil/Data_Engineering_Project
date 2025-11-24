import pandas as pd

from ..database.log import info, error

def save_csv(df, folder, name):
    path = folder / f"{name}.csv"
    df.to_csv(path, index=False)
    info(f"CSV berhasil disimpan: {path}")

def save_table(df, engine, schema, name):
    df.to_sql(name, engine, schema=schema, if_exists="replace", index=False, method="multi")
    info(f"Table berhasil disimpan: {schema}.{name}")
