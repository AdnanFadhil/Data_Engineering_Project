import os
import glob
import pandas as pd
from .logger import log
from . import config

def load_parquet_files(download_dir):
    """
    Load semua file parquet dari folder ke dictionary {basename: DataFrame}.

    Parameters:
    - download_dir (str): Folder tempat file parquet berada.

    Returns:
    - dict: Dictionary dengan key = nama file parquet, value = pd.DataFrame.
      Jika tidak ada file, mengembalikan dictionary kosong.
    """
    parquet_files = glob.glob(os.path.join(download_dir, "*.parquet"))
    if not parquet_files:
        log(f"No parquet files found in {download_dir}", level="ERROR")
        return {}

    dfs = {}
    for file in parquet_files:
        try:
            log(f"Reading {file} ...")
            df = pd.read_parquet(file)
            dfs[os.path.basename(file)] = df
            log(f"Loaded {len(df):,} rows from {os.path.basename(file)}")
        except Exception as e:
            log(f"Failed read parquet file {file} — {e}", level="ERROR")
    return dfs


def process_parquet_file(df, color, datetime_col, basename):
    """
    Proses DataFrame parquet: konversi datetime, drop NA, buat kolom date dan source_file.

    Parameters:
    - df (pd.DataFrame): DataFrame yang akan diproses.
    - color (str): Warna taxi ('yellow' atau 'green') untuk menentukan kolom datetime.
    - datetime_col (str): Nama kolom datetime pickup di DataFrame.
    - basename (str): Nama file parquet asli (digunakan untuk membuat kolom source_file).

    Returns:
    - pd.DataFrame: DataFrame yang sudah diproses dan siap displit per tanggal.
      Jika gagal, mengembalikan None.
    """
    if df is None or df.empty:
        log(f"No data for {color} in {basename} — skipping", level="ERROR")
        return None

    try:
        df[datetime_col] = pd.to_datetime(df[datetime_col], errors="coerce")
        df = df.dropna(subset=[datetime_col])
        df["date"] = df[datetime_col].dt.date.astype(str)

        month_year_part = basename.split("_")[-1].replace(".parquet", "")
        year_part, month_part = month_year_part.split("-")
        df["source_file"] = "parquet" + month_part + year_part + "_" + df[datetime_col].dt.strftime("%d_%m_%Y")
        return df
    except Exception as e:
        log(f"Failed processing datetime for {color} — {e}", level="ERROR")
        return None


def save_daily_csvs(df, color, month_folder):
    """
    Split DataFrame per tanggal dan simpan sebagai CSV di folder yellow/green.

    Parameters:
    - df (pd.DataFrame): DataFrame yang sudah diproses.
    - color (str): Warna taxi ('yellow' atau 'green').
    - month_folder (str): Nama folder bulan (YYYY-MM) untuk menyimpan CSV.

    Returns:
    - None: Semua data disimpan ke file CSV harian. Tidak ada nilai yang dikembalikan.
    """
    if df is None or df.empty:
        return

    target_dir = os.path.join(config.YELLOW_DIR if color=="yellow" else config.GREEN_DIR, month_folder)
    os.makedirs(target_dir, exist_ok=True)
    log(f"Ensured CSV folder exists: {target_dir}")

    for date_str, group in df.groupby("date"):
        path = os.path.join(target_dir, f"{date_str}.csv")
        try:
            group.to_csv(path, index=False)
            log(f"Saved {path} ({len(group):,} rows)")
        except Exception as e:
            log(f"Failed writing CSV {path} — {e}", level="ERROR")


def split_parquet_files(download_dir=None):
    """
    Function utama: load, process, dan split semua parquet menjadi daily CSV.

    Parameters:
    - download_dir (str, optional): Folder tempat file parquet berada. 
      Default: config.DOWNLOAD_DIR.

    Returns:
    - None: Semua file CSV harian akan disimpan di folder yellow/green sesuai bulan.
    """
    try:
        if not download_dir:
            download_dir = config.DOWNLOAD_DIR
        log(f"Processing parquet files from {download_dir}...")

        dfs = load_parquet_files(download_dir)
        if not dfs:
            return

        os.makedirs(config.YELLOW_DIR, exist_ok=True)
        os.makedirs(config.GREEN_DIR, exist_ok=True)

        month_folder = os.path.basename(download_dir)
        for basename, df in dfs.items():
            color = "yellow" if "yellow" in basename.lower() else "green"
            datetime_col = "tpep_pickup_datetime" if color=="yellow" else "lpep_pickup_datetime"

            processed_df = process_parquet_file(df, color, datetime_col, basename)
            save_daily_csvs(processed_df, color, month_folder)

        log("All daily CSVs generated.")

    except Exception as e:
        log(f"Error during splitting parquet files: {e}", level="ERROR")
