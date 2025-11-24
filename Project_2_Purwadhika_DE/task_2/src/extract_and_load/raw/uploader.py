import os
import shutil
import pandas as pd
from datetime import datetime
from .logger import log
from .db_utils import ensure_table_exists, is_file_already_inserted
from . import config

def get_csv_files(input_folder):
    """
    Ambil semua file CSV dari folder input.

    Parameters:
    - input_folder (str): Folder tempat file CSV berada.

    Returns:
    - list: List file CSV (full path) yang diurutkan.
    """
    if not os.path.exists(input_folder):
        return []
    return sorted(f for f in os.listdir(input_folder) if f.endswith(".csv"))

def prepare_table(df_sample, table_name, engine):
    """
    Pastikan tabel ada di database.

    Parameters:
    - df_sample (pd.DataFrame): Sample data untuk membuat schema.
    - table_name (str): Nama tabel.
    - engine: SQLAlchemy engine.

    Returns:
    - None
    """
    ensure_table_exists(df_sample, table_name, engine)

def check_duplicate(df, table_name, engine):
    """
    Cek apakah semua source_file di DataFrame sudah ada di DB.

    Parameters:
    - df (pd.DataFrame)
    - table_name (str)
    - engine

    Returns:
    - bool: True jika semua file sudah ada di DB.
    """
    source_files = df["source_file"].unique()
    return all(is_file_already_inserted(table_name, sf, engine) for sf in source_files)

def insert_csv_in_chunks(path, table_name, engine, chunksize=100000):
    """
    Insert CSV ke table DB per chunk.

    Parameters:
    - path (str): File CSV.
    - table_name (str)
    - engine
    - chunksize (int): Jumlah baris per batch insert.

    Returns:
    - int: Total rows inserted.
    """
    total_rows = 0
    for chunk in pd.read_csv(path, chunksize=chunksize):
        if chunk.empty:
            continue
        chunk["entry_time"] = datetime.now()
        if "source_file" not in chunk.columns:
            chunk["source_file"] = os.path.basename(path)
        chunk.to_sql(table_name, engine, schema=config.SCHEMA_RAW, if_exists="append", index=False)
        total_rows += len(chunk)
    return total_rows

def move_file(path, target_folder):
    """
    Pindahkan file ke folder tujuan.

    Parameters:
    - path (str): File path.
    - target_folder (str): Folder tujuan.

    Returns: None
    """
    os.makedirs(target_folder, exist_ok=True)
    shutil.move(path, os.path.join(target_folder, os.path.basename(path)))

def upload_and_archive(cfg, engine, month=None, chunksize=100000):
    """
    Upload semua CSV harian ke RAW table dan arsipkan file.

    Parameters:
    - cfg (dict): Konfigurasi folder dan tabel.
      Contoh: {"input": "input_folder", "old": "old_folder", "failed": "failed_folder", "table": "table_name"}
    - engine: SQLAlchemy engine.
    - month (str): Bulan format YYYY-MM.
    - chunksize (int): Jumlah baris per insert chunk.

    Returns: None
    """
    if not month:
        log("Month not specified for upload, skipping")
        return

    input_folder = os.path.join(cfg["input"], month)
    old_folder = os.path.join(cfg["old"], month)
    failed_folder = os.path.join(cfg["failed"], month)
    table_name = cfg["table"]

    csv_files = get_csv_files(input_folder)
    if not csv_files:
        log(f"No CSV files found in {input_folder}")
        return

    full_table = f'"{config.SCHEMA_RAW}"."{table_name}"'

    for file in csv_files:
        path = os.path.join(input_folder, file)
        log(f"Uploading {file} into table {full_table}")

        try:
            df = pd.read_csv(path)
            if df.empty:
                log(f"Empty file, skipping {file}")
                move_file(path, failed_folder)
                continue

            prepare_table(df.head(5), table_name, engine)

            if check_duplicate(df, table_name, engine):
                log(f"Skipping {file}, all rows already inserted based on source_file.")
                move_file(path, old_folder)
                continue

            total_rows = insert_csv_in_chunks(path, table_name, engine, chunksize)
            log(f"Inserted {total_rows:,} rows from {file}")
            move_file(path, old_folder)

        except Exception as e:
            log(f"Error inserting {file}: {e}", "ERROR")
            move_file(path, failed_folder)

    log(f"Finished uploading table {full_table}")
