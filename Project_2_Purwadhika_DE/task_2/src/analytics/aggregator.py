import os
import glob
import zipfile
import pandas as pd
from datetime import datetime
from sqlalchemy import text
import traceback

from .config import Settings
from .logger import Logger
from .db_utils import DBUtils
from .discord_notifier import DiscordNotifier
from .emailer import Emailer


class Aggregator:
    """
    Kelas untuk melakukan ETL agregasi daily NYC Taxi data:
    - Mengambil data clean
    - Menghitung agregasi harian (total trips, total revenue, avg fare)
    - Menyimpan hasil ke table aggregate & CSV
    - Membuat zip file hasil agregasi
    - Mengirim notifikasi via Discord/email
    """

    def __init__(self):
        os.makedirs(Settings.AGG_YELLOW_DIR, exist_ok=True)
        os.makedirs(Settings.AGG_GREEN_DIR, exist_ok=True)

    @staticmethod
    def format_value(key, val):
        """Format angka untuk report/Discord."""
        if val is None or val != val:  # NaN
            return "N/A"
        if key in ["total_trips"]:
            return f"{int(val):,}"
        if key in ["total_revenue", "mean_daily_income"]:
            return f"${val:,.2f}"
        return f"{val:.2f}"

    def export_table_to_csv(self, table_name, color):
        """Export hasil aggregate table ke CSV sesuai warna taxi."""
        dir_path = Settings.AGG_YELLOW_DIR if color == "yellow" else Settings.AGG_GREEN_DIR
        file_path = os.path.join(dir_path, f"{table_name}.csv")
        df = pd.read_sql(f"SELECT * FROM {Settings.SCHEMA_AGGREGATE}.{table_name}", DBUtils.engine)
        if not df.empty:
            df.to_csv(file_path, index=False)
            Logger.log(f"✓ Exported {table_name} to CSV at {file_path}")
        else:
            Logger.log(f"No data in {table_name} to export.", "WARNING")

    def zip_aggregate_files(self):
        """Zip semua CSV aggregate per color taxi."""
        today_str = datetime.now().strftime("%Y%m%d")
        zip_filename = f"update_aggregate_{today_str}.zip"
        with zipfile.ZipFile(zip_filename, "w", zipfile.ZIP_DEFLATED) as zf:
            for color, dir_path in [("yellow", Settings.AGG_YELLOW_DIR), ("green", Settings.AGG_GREEN_DIR)]:
                for file in glob.glob(os.path.join(dir_path, "*.csv")):
                    zf.write(file, arcname=os.path.join(color, os.path.basename(file)))
        Logger.log(f"✓ Aggregate CSV files zipped into {zip_filename}")
        return zip_filename

    def create_partition_table(self, clean_table, pickup_col, color):
        """Membuat table partitioned jika belum ada."""
        partition_table = f"{Settings.SCHEMA_AGGREGATE}.{color}_partitioned"
        create_sql = f"""
            CREATE TABLE IF NOT EXISTS {partition_table} (
                LIKE {Settings.SCHEMA_CLEAN}.{clean_table} INCLUDING ALL
            ) PARTITION BY RANGE ({pickup_col});
        """
        try:
            with DBUtils.engine.begin() as conn:
                conn.execute(text(create_sql))
            Logger.log(f"Partitioned table {partition_table} created")
        except Exception as e:
            Logger.log(f"Error creating partitioned table {partition_table}: {e}", "ERROR")


    def aggregate_color_daily(self, color_cfg):
        """Agregasi daily untuk satu warna taxi."""
        color = color_cfg["color"]
        clean_table = color_cfg["table"]
        pickup_col = color_cfg["pickup_col"]

        Logger.log(f"Processing {color} taxi data...")
        next_date = DBUtils.get_next_date(f"{color}_total_trips", clean_table, pickup_col)
        if not next_date:
            Logger.log(f"No more data available for {color}. Already reached the last date.", "INFO")
            return None, {}

        date_str = next_date.strftime("%Y-%m-%d")
        date_fmt = next_date.strftime("%d/%m/%Y")
        Logger.log(f"Aggregating for date: {date_str}")

        self.create_partition_table(clean_table, pickup_col, color)

        filter_sql = f"WHERE {pickup_col}::date = '{date_str}'"
        tables = {
            f"{color}_total_trips": f"""
                WITH cte AS (SELECT 1 AS dummy FROM {Settings.SCHEMA_CLEAN}.{clean_table} {filter_sql})
                SELECT '{date_str}'::date AS date, COUNT(*) AS total_trips FROM cte
            """,
            f"{color}_total_revenue": f"""
                WITH cte AS (SELECT fare_amount, total_amount FROM {Settings.SCHEMA_CLEAN}.{clean_table} {filter_sql})
                SELECT '{date_str}'::date AS date, SUM(fare_amount + total_amount) AS total_revenue FROM cte
            """,
            f"{color}_avg_fare": f"""
                WITH cte AS (SELECT fare_amount FROM {Settings.SCHEMA_CLEAN}.{clean_table} {filter_sql})
                SELECT '{date_str}'::date AS date, AVG(fare_amount) AS avg_fare FROM cte
            """
        }

        aggregates = {}
        for table_name, sql in tables.items():
            try:
                df = pd.read_sql(sql, DBUtils.engine)
                if not df.empty:
                    DBUtils.insert_or_update_table(table_name, sql, date_str)
                    self.export_table_to_csv(table_name, color)
                    col_name = df.columns[1]
                    aggregates[col_name] = df.iloc[0, 1]
            except Exception as e:
                Logger.log(f"Error aggregating {table_name} with CTE: {e}", "ERROR")

        return date_fmt, aggregates

    def aggregate_daily_partitioned(self):
        """Agregasi daily untuk semua warna taxi (yellow & green)."""
        taxi_configs = [
            {"color": "yellow", "table": "yellow_tripdata_clean",
             "pickup_col": "tpep_pickup_datetime", "dropoff_col": "tpep_dropoff_datetime"},
            {"color": "green", "table": "green_tripdata_clean",
             "pickup_col": "lpep_pickup_datetime", "dropoff_col": "lpep_dropoff_datetime"}
        ]
        success_dates = {}
        aggregates_all = {}

        with DBUtils.engine.begin() as conn:
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {Settings.SCHEMA_AGGREGATE}"))
            Logger.log(f"✓ Schema {Settings.SCHEMA_AGGREGATE} ensured")

        for cfg in taxi_configs:
            date_fmt, aggregates = self.aggregate_color_daily(cfg)
            if date_fmt:
                success_dates[cfg["color"]] = date_fmt
                aggregates_all[cfg["color"]] = aggregates

        return success_dates, aggregates_all

    def run_pipeline(self):
        """Menjalankan full ETL agregasi harian, notifikasi, dan zip file."""
        Logger.log("Starting full pipeline...")
        try:
            success_dates, aggregates = self.aggregate_daily_partitioned()
            Logger.log("Aggregation done.")

            msg_success = (
                f"Pada ETL daily aggregate data taxi US yellow tanggal {success_dates.get('yellow', 'N/A')} "
                f"dan taxi US green {success_dates.get('green', 'N/A')} berhasil dijalankan."
            )
            DiscordNotifier.send_message(msg_success)

            for color in ["yellow", "green"]:
                if color in success_dates:
                    msg = (
                        f"Hasil agregasi daily taxi US {color} ({success_dates[color]}):\n"
                        f"Total trips: {self.format_value('total_trips', aggregates[color].get('total_trips'))}\n"
                        f"Total revenue: {self.format_value('total_revenue', aggregates[color].get('total_revenue'))}\n"
                        f"Avg fare: {self.format_value('avg_fare', aggregates[color].get('avg_fare'))}\n"
                    )
                    DiscordNotifier.send_message(msg)

            zip_file = self.zip_aggregate_files()
            subject = "UPDATE DAILY Project Purwadhika Capstone 1"
            body = (
                f"NYC Taxi Aggregate CSV {datetime.now().strftime('%d/%m/%Y')}\n\n"
                "Berikut terlampir file zip berisi hasil aggregate daily taxi US (yellow & green)."
            )
            # Emailer.send_email(subject, body, zip_file)

            if os.path.exists(zip_file):
                os.remove(zip_file)
                Logger.log(f"✓ Zip file {zip_file} deleted after sending email")

        except Exception:
            err_msg = traceback.format_exc()
            Logger.log(f"ERROR pipeline: {err_msg}", "ERROR")
            DiscordNotifier.send_message(f"Terdapat error:\n{err_msg[:2000]}")
