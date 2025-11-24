from sqlalchemy import text
from .logger import Logger
from .db_utils import DBUtils
from .config import Settings

class PartitionInserter:
    """Handle data insertion into daily partition tables in aggregate schema."""
    @staticmethod
    def insert_daily(clean_table, pickup_col, color, date_str):
        """
        Masukkan data dari tabel clean ke parent partition aggregate untuk tanggal tertentu.

        Parameters:
        - clean_table (str): Nama tabel sumber di schema clean.
        - pickup_col (str): Nama kolom datetime untuk filter tanggal.
        - color (str): Warna taxi ('yellow' atau 'green'), digunakan untuk penamaan tabel aggregate.
        - date_str (str): Tanggal yang diproses, format 'YYYY-MM-DD'.

        Behavior:
        - Insert semua baris dari tabel clean yang memiliki pickup_col = date_str
          ke parent partition table {color}_partitioned di schema aggregate.
        - Commit otomatis via SQLAlchemy engine.
        - Log status sukses atau error.

        Returns:
        - None
        """
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
            Logger.log(f"Data inserted into {partition_table} for {date_str}")
        except Exception as e:
            Logger.log(f"Error inserting data into partition table {partition_table}: {e}", "ERROR")
