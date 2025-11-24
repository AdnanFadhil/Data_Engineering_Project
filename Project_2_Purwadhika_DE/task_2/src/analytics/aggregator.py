import traceback
from datetime import datetime
from .partition_manager import PartitionManager
from .partition_inserter import PartitionInserter
from .csv_exporter import CSVExporter
from .zipper import Zipper
from .logger import Logger
from .discord_notifier import DiscordNotifier
from .db_utils import DBUtils
from .config import Settings
from sqlalchemy import text
import pandas as pd

class Aggregator:
    """
    Class untuk melakukan agregasi harian taxi data (yellow & green)
    ke dalam schema aggregate, termasuk pembuatan nested partition
    dan export CSV.
    """
    @staticmethod
    def format_value(key, val):
        """
        Format value untuk reporting di Discord.

        Parameters:
        - key (str): Nama field, misal 'total_trips', 'total_revenue', 'avg_fare'
        - val (numeric | None): Nilai yang akan diformat

        Returns:
        - str: Nilai dalam format string yang sesuai, atau "N/A" jika None/NaN
        """
        if val is None or val != val:  # NaN
            return "N/A"
        if key in ["total_trips"]:
            return f"{int(val):,}"
        if key in ["total_revenue", "mean_daily_income"]:
            return f"${val:,.2f}"
        return f"{val:.2f}"

    def aggregate_color_daily(self, color_cfg):
        """
        Lakukan agregasi untuk satu warna taxi (yellow/green) per hari.

        Parameters:
        - color_cfg (dict): Konfigurasi warna taxi, berisi:
            - color: 'yellow' atau 'green'
            - table: nama tabel clean
            - pickup_col: nama kolom pickup datetime

        Behavior:
        - Tentukan tanggal berikutnya yang perlu diproses
        - Buat nested partition (parent/monthly/daily)
        - Insert data ke tabel partitioned
        - Hitung agregasi total trips, total revenue, avg fare
        - Insert/update hasil ke aggregate table
        - Export hasil ke CSV

        Returns:
        - date_fmt (str | None): Tanggal yang diproses dalam format '%d/%m/%Y'
        - aggregates (dict): Dictionary hasil agregasi
        """
        color = color_cfg["color"]
        clean_table = color_cfg["table"]
        pickup_col = color_cfg["pickup_col"]

        Logger.log(f"Processing {color} taxi data...")
        next_date = DBUtils.get_next_date(f"{color}_total_trips", clean_table, pickup_col)
        if not next_date:
            Logger.log(f"No more data available for {color}.", "INFO")
            return None, {}

        date_str = next_date.strftime("%Y-%m-%d")
        date_fmt = next_date.strftime("%d/%m/%Y")

        PartitionManager.create_nested_partition(clean_table, pickup_col, color, next_date)
        PartitionInserter.insert_daily(clean_table, pickup_col, color, date_str)

        # Aggregate tables
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
                df = DBUtils.read_sql(sql)
                if not df.empty:
                    DBUtils.insert_or_update_table(table_name, sql, date_str)
                    CSVExporter.export_table(table_name, color)
                    col_name = df.columns[1]
                    aggregates[col_name] = df.iloc[0, 1]
            except Exception as e:
                Logger.log(f"Error aggregating {table_name}: {e}", "ERROR")

        return date_fmt, aggregates

    def aggregate_daily_partitioned(self):
        """
        Lakukan agregasi harian untuk semua warna taxi (yellow & green).

        Behavior:
        - Pastikan schema aggregate ada
        - Looping setiap konfigurasi taxi (yellow/green)
        - Panggil aggregate_color_daily
        - Kumpulkan semua hasil aggregates

        Returns:
        - success_dates (dict): {'yellow': 'dd/mm/yyyy', 'green': 'dd/mm/yyyy'}
        - aggregates_all (dict): {'yellow': {...}, 'green': {...}}
        """
        taxi_configs = [
            {"color": "yellow", "table": "yellow_tripdata_clean",
             "pickup_col": "tpep_pickup_datetime", "dropoff_col": "tpep_dropoff_datetime"},
            {"color": "green", "table": "green_tripdata_clean",
             "pickup_col": "lpep_pickup_datetime", "dropoff_col": "lpep_dropoff_datetime"}
        ]
        success_dates = {}
        aggregates_all = {}

        # Pastikan schema aggregate ada
        with DBUtils.engine.begin() as conn:
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {Settings.SCHEMA_AGGREGATE}"))
            Logger.log(f"Schema {Settings.SCHEMA_AGGREGATE} ensured")

        for cfg in taxi_configs:
            date_fmt, aggregates = self.aggregate_color_daily(cfg)
            if date_fmt:
                success_dates[cfg["color"]] = date_fmt
                aggregates_all[cfg["color"]] = aggregates

        return success_dates, aggregates_all

    def run_pipeline(self):
        """
        Jalankan seluruh pipeline agregasi harian.

        Behavior:
        - Memanggil aggregate_daily_partitioned
        - Mengirimkan notifikasi Discord hasil agregasi
        - Menyimpan hasil CSV ke zip
        - Menangani error dan mengirim notifikasi jika terjadi exception
        """
        Logger.log("Starting full pipeline...")
        try:
            success_dates, aggregates = self.aggregate_daily_partitioned()
            Logger.log("Aggregation done.")

            msg_success = (
                f"ETL daily aggregate yellow: {success_dates.get('yellow','N/A')}, "
                f"green: {success_dates.get('green','N/A')} berhasil."
            )
            DiscordNotifier.send_message(msg_success)

            for color in ["yellow", "green"]:
                if color in success_dates:
                    msg = (
                        f"Hasil agregasi {color} ({success_dates[color]}):\n"
                        f"Total trips: {self.format_value('total_trips', aggregates[color].get('total_trips'))}\n"
                        f"Total revenue: {self.format_value('total_revenue', aggregates[color].get('total_revenue'))}\n"
                        f"Avg fare: {self.format_value('avg_fare', aggregates[color].get('avg_fare'))}\n"
                    )
                    DiscordNotifier.send_message(msg)
            zip_file = Zipper.zip_aggregate_files()

        except Exception:
            err_msg = traceback.format_exc()
            Logger.log(f"ERROR pipeline: {err_msg}", "ERROR")
            DiscordNotifier.send_message(f"Terdapat error:\n{err_msg[:2000]}")
