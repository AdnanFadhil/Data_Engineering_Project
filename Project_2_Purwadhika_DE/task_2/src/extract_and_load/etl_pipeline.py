import os
from src.extract_and_load.raw import downloader, splitter, uploader, config as raw_config, logger as raw_logger
from src.extract_and_load.clean import cleaner
from src.extract_and_load.raw.db_utils import mark_parquet_done, migrate_parquet_tracking

log = raw_logger.log

class Extractor:
    """
    Class untuk menjalankan pipeline ETL: RAW → CLEAN.
    """

    def run_pipeline(self):
        """
        Jalankan seluruh pipeline ETL: download parquet → split CSV → upload → clean → update tracking.

        Returns: None
        """
        result = self.download_parquet()
        if not result:
            log("No parquet to process, exiting")
            return

        month_to_process, downloaded_files = result
        self.ensure_csv_folders(month_to_process)

        self.split_parquet(month_to_process)
        self.upload_csvs(month_to_process)
        self.clean_data()
        self.update_parquet_tracking(downloaded_files, month_to_process)

    def download_parquet(self):
        """
        Download file parquet dari TLC website.

        Returns:
        - tuple: (month_to_process (str), list of downloaded file paths) jika ada file
        - None: jika tidak ada file untuk diproses
        """
        return downloader.download_parquet_files()

    def ensure_csv_folders(self, month):
        """
        Pastikan folder CSV bulanan untuk setiap taxi_type ada.
        """
        for taxi_type, cfg in raw_config.folders.items():
            csv_month_folder = os.path.join(cfg["input"], month)
            os.makedirs(csv_month_folder, exist_ok=True)
            log(f"Ensured CSV folder exists: {csv_month_folder}")

    def split_parquet(self, month):
        """
        Split semua parquet yang sudah didownload menjadi daily CSV.
        """
        download_dir = os.path.join(raw_config.DOWNLOAD_DIR, month)
        log("Splitting parquet files into daily CSVs...")
        splitter.split_parquet_files(download_dir=download_dir)

    def upload_csvs(self, month):
        """
        Upload semua CSV bulanan ke RAW table.
        """
        log("Uploading CSVs into RAW tables...")
        for taxi_type, cfg in raw_config.folders.items():
            uploader.upload_and_archive(cfg, raw_config.engine, month=month)

        log("✓ RAW ETL complete.")

    def clean_data(self):
        """
        Jalankan proses cleaning: RAW → CLEAN.
        """
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

    def update_parquet_tracking(self, downloaded_files, month):
        """
        Update status file parquet di tracking table.

        Parameters:
        - downloaded_files (list): List file parquet yang sudah didownload.
        - month (str): Bulan yang sedang diproses.

        Returns: None
        """
        migrate_parquet_tracking()
        for file in downloaded_files:
            mark_parquet_done(file)
            log(f"Marked parquet done: {file}")
