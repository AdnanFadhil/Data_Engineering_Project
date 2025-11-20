from src.extract_and_load.raw import downloader, splitter, uploader, config as raw_config, logger as raw_logger
from src.extract_and_load.clean import cleaner
from src.extract_and_load.raw.db_utils import mark_parquet_done, migrate_parquet_tracking
import os

log = raw_logger.log

class Extractor:
    def run_pipeline(self):
        result = downloader.download_parquet_files()
        if not result:
            log("No parquet to process, exiting")
            return

        month_to_process, downloaded_files = result
        log(f"=== START NYC Taxi ETL: RAW → CLEAN ({month_to_process}) ===")

        for taxi_type, cfg in raw_config.folders.items():
            csv_month_folder = os.path.join(cfg["input"], month_to_process)
            os.makedirs(csv_month_folder, exist_ok=True)
            log(f"Ensured CSV folder exists: {csv_month_folder}")

        log("Splitting parquet files into daily CSVs...")
        splitter.split_parquet_files(download_dir=os.path.join(raw_config.DOWNLOAD_DIR, month_to_process))

        log("Uploading CSVs into RAW tables...")
        for taxi_type, cfg in raw_config.folders.items():
            uploader.upload_and_archive(cfg, raw_config.engine, month=month_to_process)

        log("✓ RAW ETL complete.")

        log("Starting CLEANING process (RAW → CLEAN)...")
        cleaner.clean_tripdata(
            src_table="yellow_tripdata",
            pickup_col="tpep_pickup_datetime",
            dropoff_col="tpep_dropoff_datetime"
        )
        cleaner.clean_tripdata(
            src_table="green_tripdata",
            pickup_col="lpep_pickup_datetime",
            dropoff_col="lpep_dropoff_datetime"
        )
        log("✓ CLEAN ETL complete.")
        log(f"=== RAW → CLEAN PIPELINE FINISHED ({month_to_process}) ===")

        migrate_parquet_tracking()

        for file in downloaded_files:
            mark_parquet_done(file)
            log(f"Marked parquet done: {file}")
