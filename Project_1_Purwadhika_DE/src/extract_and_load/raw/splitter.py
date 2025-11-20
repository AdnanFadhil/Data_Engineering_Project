import os
import glob
import pandas as pd
from .logger import log
from . import config

def split_parquet_files(download_dir=None):
    try:
        if not download_dir:
            download_dir = config.DOWNLOAD_DIR

        log(f"Loading parquet files from {download_dir}...")

        # Cari semua file parquet di folder
        parquet_files = glob.glob(os.path.join(download_dir, "*.parquet"))
        if not parquet_files:
            log(f"No parquet files found in {download_dir}", level="ERROR")
            return

        dfs = {}
        for file in parquet_files:
            try:
                log(f"Reading {file} ...")
                df = pd.read_parquet(file)
                dfs[os.path.basename(file)] = df
                log(f"Loaded {len(df):,} rows from {os.path.basename(file)}")
            except Exception as e:
                log(f"Failed read parquet file {file} — {e}", level="ERROR")
                continue

        os.makedirs(config.YELLOW_DIR, exist_ok=True)
        os.makedirs(config.GREEN_DIR, exist_ok=True)

        def split_and_save(df, color, datetime_col, month_folder, basename):
            if df is None or df.empty:
                log(f"No data for {color} — skipping", level="ERROR")
                return

            try:
                df[datetime_col] = pd.to_datetime(df[datetime_col], errors="coerce")
                df = df.dropna(subset=[datetime_col])
                df["date"] = df[datetime_col].dt.date.astype(str)
            except Exception as e:
                log(f"Failed processing datetime for {color} — {e}", level="ERROR")
                return

            target_dir = os.path.join(config.YELLOW_DIR if color=="yellow" else config.GREEN_DIR, month_folder)
            os.makedirs(target_dir, exist_ok=True)
            log(f"Ensured CSV folder exists: {target_dir}")


            month_year_part = basename.split("_")[-1].replace(".parquet", "") 
            year_part, month_part = month_year_part.split("-")                  

            df["source_file"] = "parquet" + month_part + year_part + "_" + df[datetime_col].dt.strftime("%d_%m_%Y")

            for date_str, group in df.groupby("date"):
                path = os.path.join(target_dir, f"{date_str}.csv")
                try:
                    group.to_csv(path, index=False)
                    log(f"Saved {path} ({len(group):,} rows)")
                except Exception as e:
                    log(f"Failed writing CSV {path} — {e}", level="ERROR")

        for parquet_file in parquet_files:
            basename = os.path.basename(parquet_file)
            color = "yellow" if "yellow" in basename.lower() else "green"
            datetime_col = "tpep_pickup_datetime" if color=="yellow" else "lpep_pickup_datetime"
            split_and_save(dfs.get(basename), color, datetime_col, month_folder=os.path.basename(download_dir), basename=basename)

        log("All daily CSVs generated.")

    except Exception as e:
        log(f"Error during splitting parquet files: {e}", level="ERROR")
