import os
import glob
import zipfile
import pandas as pd
from datetime import datetime
from calendar import monthrange
from sqlalchemy import text
import traceback

from .config import Settings
from .logger import Logger
from .db_utils import DBUtils
from .discord_notifier import DiscordNotifier
# from .emailer import Emailer


class Aggregator:
    """ETL agregasi daily NYC Taxi data dengan nested partition bulanan & harian."""

    def __init__(self):
        os.makedirs(Settings.AGG_YELLOW_DIR, exist_ok=True)
        os.makedirs(Settings.AGG_GREEN_DIR, exist_ok=True)

    @staticmethod
    def format_value(key, val):
        if val is None or val != val:  # NaN
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
                    zf.write(file, arcname=os.path.join(color, os.path.basename(file)))
        Logger.log(f"✓ Aggregate CSV files zipped into {zip_filename}")
        return zip_filename

    def create_nested_partition(self, clean_table, pickup_col, color, next_date):
        """
        Buat parent partition bulanan & child harian dengan nested partitioning aman.
        - Parent: partition by month
        - Child monthly: partition by day
        - Child daily: actual table
        """
        partition_table = f"{Settings.SCHEMA_AGGREGATE}.{color}_partitioned"
        
        # --- Parent monthly partitioned ---
        try:
            with DBUtils.engine.begin() as conn:
                create_parent_sql = f"""
                    CREATE TABLE IF NOT EXISTS {partition_table} (
                        LIKE {Settings.SCHEMA_CLEAN}.{clean_table} INCLUDING ALL
                    ) PARTITION BY RANGE (DATE_TRUNC('month', {pickup_col}));
                """
                conn.execute(text(create_parent_sql))
                Logger.log(f"Parent partition table {partition_table} ensured")
        except Exception as e:
            Logger.log(f"Error creating parent partition: {e}", "ERROR")
            return

        # --- Child monthly (partitioned by day) ---
        year, month = next_date.year, next_date.month
        month_start = datetime(year, month, 1).date()
        month_end = datetime(year, month, monthrange(year, month)[1]).date()
        month_child = f"{partition_table}_{year}{month:02d}"

        try:
            with DBUtils.engine.begin() as conn:
                # Create monthly child table with partition
                create_month_sql = f"""
                    CREATE TABLE IF NOT EXISTS {month_child} (
                        LIKE {Settings.SCHEMA_CLEAN}.{clean_table} INCLUDING ALL
                    ) PARTITION BY RANGE ({pickup_col});
                """
                conn.execute(text(create_month_sql))
                Logger.log(f"✓ Child monthly partitioned table {month_child} ensured")

                # Cek apakah monthly child sudah attach
                check_attach_sql = f"""
                    SELECT 1
                    FROM pg_inherits i
                    JOIN pg_class c ON i.inhrelid = c.oid
                    WHERE i.inhparent = '{partition_table}'::regclass
                    AND c.relname = '{month_child.split('.')[-1]}';
                """
                result = conn.execute(text(check_attach_sql)).fetchone()
                if not result:
                    attach_month_sql = f"""
                        ALTER TABLE {partition_table}
                        ATTACH PARTITION {month_child}
                        FOR VALUES FROM ('{month_start}') TO ('{month_end + pd.Timedelta(days=1)}');
                    """
                    conn.execute(text(attach_month_sql))
                    Logger.log(f"✓ Monthly child {month_child} attached to {partition_table}")
                else:
                    Logger.log(f"Monthly child {month_child} already attached, skipped.", "WARNING")

        except Exception as e:
            Logger.log(f"Error creating/attaching monthly child: {e}", "WARNING")

        # --- Child harian ---
        day_start = next_date
        day_end = day_start + pd.Timedelta(days=1)
        day_child = f"{month_child}_{day_start.strftime('%d')}"

        try:
            with DBUtils.engine.begin() as conn:
                create_day_sql = f"""
                    CREATE TABLE IF NOT EXISTS {day_child} PARTITION OF {month_child}
                    FOR VALUES FROM ('{day_start}') TO ('{day_end}');
                """
                conn.execute(text(create_day_sql))
                Logger.log(f"✓ Child daily partition {day_child} ensured")
        except Exception as e:
            Logger.log(f"Error creating daily child partition {day_child}: {e}", "WARNING")


    def insert_into_partition(self, clean_table, pickup_col, color, date_str):
        """Insert data harian ke parent partition table sehingga child terisi otomatis."""
        partition_table = f"{Settings.SCHEMA_AGGREGATE}.{color}_partitioned"
        insert_sql = f"""
            INSERT INTO {partition_table} 
            SELECT *
            FROM {Settings.SCHEMA_CLEAN}.{clean_table}
            WHERE {pickup_col}::date = '{date_str}'
        """
        try:
            with DBUtils.engine.begin() as conn:
                conn.execute(text(insert_sql))
            Logger.log(f"✓ Data inserted into partition table {partition_table} for {date_str}")
        except Exception as e:
            Logger.log(f"Error inserting data into partition table {partition_table}: {e}", "ERROR")

    def aggregate_color_daily(self, color_cfg):
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

        # Pastikan parent + child partition sudah ada
        self.create_nested_partition(clean_table, pickup_col, color, next_date)

        # Insert data ke parent partition table sehingga child terisi
        self.insert_into_partition(clean_table, pickup_col, color, date_str)

        # Hitung agregasi
        tables = {
            f"{color}_total_trips": f"""
                SELECT '{date_str}'::date AS date, COUNT(*) AS total_trips
                FROM {Settings.SCHEMA_CLEAN}.{clean_table}
                WHERE {pickup_col}::date = '{date_str}'
            """,
            f"{color}_total_revenue": f"""
                SELECT '{date_str}'::date AS date, SUM(fare_amount + total_amount) AS total_revenue
                FROM {Settings.SCHEMA_CLEAN}.{clean_table}
                WHERE {pickup_col}::date = '{date_str}'
            """,
            f"{color}_avg_fare": f"""
                SELECT '{date_str}'::date AS date, AVG(fare_amount) AS avg_fare
                FROM {Settings.SCHEMA_CLEAN}.{clean_table}
                WHERE {pickup_col}::date = '{date_str}'
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
                Logger.log(f"Error aggregating {table_name}: {e}", "ERROR")

        return date_fmt, aggregates

    def aggregate_daily_partitioned(self):
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
