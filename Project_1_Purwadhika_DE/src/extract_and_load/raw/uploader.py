import os
import shutil
import pandas as pd
from datetime import datetime
from .logger import log
from .db_utils import ensure_table_exists, is_file_already_inserted
from . import config

def upload_and_archive(cfg, engine, month=None, chunksize=100000):
    """
    Upload semua CSV harian ke RAW table dan arsipkan file.
    Csv files di folder: input_folder/<month>/MM_DD_YYYY.csv
    """
    if not month:
        log("Month not specified for upload, skipping")
        return

    input_folder = os.path.join(cfg["input"], month)
    old_folder = os.path.join(cfg["old"], month)
    failed_folder = os.path.join(cfg["failed"], month)
    table_name = cfg["table"]
    schema = config.SCHEMA_RAW

    os.makedirs(old_folder, exist_ok=True)
    os.makedirs(failed_folder, exist_ok=True)
    os.makedirs(input_folder, exist_ok=True)

    full_table = f'"{schema}"."{table_name}"'

    csv_files = sorted(f for f in os.listdir(input_folder) if f.endswith(".csv"))
    if not csv_files:
        log(f"No CSV files found in {input_folder}")
        return

    for file in csv_files:
        path = os.path.join(input_folder, file)
        log(f"Uploading {file} into table {full_table}")

        try:
            df = pd.read_csv(path)
            if df.empty:
                log(f"Empty file, skipping {file}")
                shutil.move(path, os.path.join(failed_folder, file))
                continue

            ensure_table_exists(df.head(5), table_name, engine)

            source_files = df["source_file"].unique()
            if all(is_file_already_inserted(table_name, sf, engine) for sf in source_files):
                log(f"Skipping {file}, all rows already inserted based on source_file.")
                shutil.move(path, os.path.join(old_folder, file))
                continue

            total_rows = 0
            for chunk in pd.read_csv(path, chunksize=chunksize):
                if chunk.empty:
                    continue

                chunk["entry_time"] = datetime.now()

                if "source_file" not in chunk.columns:
                    chunk["source_file"] = file

                chunk.to_sql(
                    table_name,
                    engine,
                    schema=schema,
                    if_exists="append",
                    index=False
                )
                total_rows += len(chunk)

            log(f"Inserted {total_rows:,} rows from {file}")
            shutil.move(path, os.path.join(old_folder, file))

        except Exception as e:
            log(f"Error inserting {file}: {e}", "ERROR")
            shutil.move(path, os.path.join(failed_folder, file))

    log(f"Finished uploading table {full_table}")
