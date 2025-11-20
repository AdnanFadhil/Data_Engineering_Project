import pandas as pd
import zipfile
import glob
import os
from datetime import datetime
from .config import Settings
from .logger import Logger
from .db_utils import DBUtils
from .discord_notifier import DiscordNotifier
from .emailer import Emailer
from sqlalchemy import text
import traceback

class Aggregator:
    def __init__(self):
        os.makedirs(Settings.AGG_YELLOW_DIR, exist_ok=True)
        os.makedirs(Settings.AGG_GREEN_DIR, exist_ok=True)

    @staticmethod
    def format_value(key, val):
        if val is None or val != val:
            return "N/A"
        if key in ["total_trips"]:
            return f"{int(val):,}"
        if key in ["total_revenue", "mean_daily_income"]:
            return f"${val:,.2f}"
        return f"{val:.2f}"

    def export_table_to_csv(self, table_name, color):
        dir_path = Settings.AGG_YELLOW_DIR if color == "yellow" else Settings.AGG_GREEN_DIR
        file_path = os.path.join(dir_path, f"{table_name}.csv")
        df = pd.read_sql(f"SELECT * FROM {Settings.SCHEMA_AGGREGATE}.{table_name}", DBUtils.engine)
        if not df.empty:
            df.to_csv(file_path, index=False)
            Logger.log(f"✓ Exported {table_name} to CSV at {file_path}")
        else:
            Logger.log(f"No data in {table_name} to export.", "WARNING")

    def zip_aggregate_files(self):
        today_str = datetime.now().strftime("%Y%m%d")
        zip_filename = f"update_aggregate_{today_str}.zip"
        with zipfile.ZipFile(zip_filename, "w", zipfile.ZIP_DEFLATED) as zf:
            for color, dir_path in [("yellow", Settings.AGG_YELLOW_DIR), ("green", Settings.AGG_GREEN_DIR)]:
                for file in glob.glob(os.path.join(dir_path, "*.csv")):
                    arcname = os.path.join(color, os.path.basename(file))
                    zf.write(file, arcname=arcname)
        Logger.log(f"✓ Aggregate CSV files zipped into {zip_filename}")
        return zip_filename

    def aggregate_daily(self):
        success_dates = {}
        aggregates = {"yellow": {}, "green": {}}

        with DBUtils.engine.begin() as conn:
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {Settings.SCHEMA_AGGREGATE}"))
            Logger.log(f"✓ Schema {Settings.SCHEMA_AGGREGATE} ensured")

        taxi_configs = [
            {"color": "yellow", "table": "yellow_tripdata_clean",
             "pickup_col": "tpep_pickup_datetime", "dropoff_col": "tpep_dropoff_datetime"},
            {"color": "green", "table": "green_tripdata_clean",
             "pickup_col": "lpep_pickup_datetime", "dropoff_col": "lpep_dropoff_datetime"}
        ]

        for cfg in taxi_configs:
            color = cfg["color"]
            clean_table = cfg["table"]
            pickup_col = cfg["pickup_col"]
            dropoff_col = cfg["dropoff_col"]

            Logger.log(f"Processing {color} taxi data...")
            aggregate_table = f"{color}_total_trips"
            next_date = DBUtils.get_next_date(aggregate_table, clean_table, pickup_col)

            if not next_date:
                Logger.log(f"No more data available for {color}. Already reached the last date.", "INFO")
                continue

            date_str = next_date.strftime("%Y-%m-%d")
            date_fmt = next_date.strftime("%d/%m/%Y")
            Logger.log(f"Aggregating for date: {date_str}")
            filter_sql = f"WHERE {pickup_col}::date = '{date_str}'"

            tables = {
                f"{color}_total_trips": f"SELECT '{date_str}'::date AS date, COUNT(*) AS total_trips FROM {Settings.SCHEMA_CLEAN}.{clean_table} {filter_sql}",
                f"{color}_total_revenue": f"SELECT '{date_str}'::date AS date, SUM(fare_amount + total_amount) AS total_revenue FROM {Settings.SCHEMA_CLEAN}.{clean_table} {filter_sql}",
                f"{color}_avg_fare": f"SELECT '{date_str}'::date AS date, AVG(fare_amount) AS avg_fare FROM {Settings.SCHEMA_CLEAN}.{clean_table} {filter_sql}",
                f"{color}_avg_distance": f"SELECT '{date_str}'::date AS date, AVG(trip_distance) AS avg_distance FROM {Settings.SCHEMA_CLEAN}.{clean_table} {filter_sql}",
                f"{color}_avg_duration": f"SELECT '{date_str}'::date AS date, AVG(EXTRACT(EPOCH FROM ({dropoff_col} - {pickup_col}))/60) AS avg_duration_minutes FROM {Settings.SCHEMA_CLEAN}.{clean_table} {filter_sql}",
                f"{color}_mean_daily_income": f"SELECT '{date_str}'::date AS date, SUM(fare_amount + total_amount)/COUNT(DISTINCT \"VendorID\") AS mean_daily_income FROM {Settings.SCHEMA_CLEAN}.{clean_table} {filter_sql}"
            }

            for table_name, sql in tables.items():
                df = pd.read_sql(sql, DBUtils.engine)
                if len(df) > 0:
                    DBUtils.insert_or_update_table(table_name, sql, date_str)
                    self.export_table_to_csv(table_name, color)
                    col_name = df.columns[1]
                    aggregates[color][col_name] = df.iloc[0, 1]

            success_dates[color] = date_fmt

        return success_dates, aggregates

    def run_pipeline(self):
        Logger.log("Starting full pipeline...")
        try:
            success_dates, aggregates = self.aggregate_daily()
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
                        f"Avg distance: {self.format_value('avg_distance', aggregates[color].get('avg_distance'))}\n"
                        f"Avg duration (min): {self.format_value('avg_duration_minutes', aggregates[color].get('avg_duration_minutes'))}\n"
                        f"Mean daily income: {self.format_value('mean_daily_income', aggregates[color].get('mean_daily_income'))}"
                    )
                    DiscordNotifier.send_message(msg)

            zip_file = self.zip_aggregate_files()
            subject = "UPDATE DAILY Project Purwadhika Capstone 1"
            body = (
                f"NYC Taxi Aggregate CSV {datetime.now().strftime('%d/%m/%Y')}\n\n"
                "Berikut terlampir file zip berisi hasil aggregate daily taxi US (yellow & green)."
            )
            Emailer.send_email(subject, body, zip_file)

            if os.path.exists(zip_file):
                os.remove(zip_file)
                Logger.log(f"✓ Zip file {zip_file} deleted after sending email")

        except Exception:
            err_msg = traceback.format_exc()
            Logger.log(f"ERROR pipeline: {err_msg}", "ERROR")
            DiscordNotifier.send_message(f"Terdapat error:\n{err_msg[:2000]}")
