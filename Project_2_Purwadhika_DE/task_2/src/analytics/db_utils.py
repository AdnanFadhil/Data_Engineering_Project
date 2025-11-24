from sqlalchemy import create_engine, text
import pandas as pd
from .config import Settings
from .logger import Logger

# Membuat engine SQLAlchemy
DB_URL = (
    f"postgresql://{Settings.DB_USER}:{Settings.DB_PASSWORD}"
    f"@{Settings.DB_HOST}:{Settings.DB_PORT}/{Settings.DB_NAME}"
)
engine = create_engine(DB_URL)

class DBUtils:
    """
    Utility class untuk operasi database di schema aggregate menggunakan SQLAlchemy.

    Behavior:
    - Menyediakan method untuk eksekusi query SELECT dan mengembalikan DataFrame.
    - Mendapatkan tanggal berikutnya yang perlu diproses secara incremental.
    - Insert atau update data ke aggregate table dengan mekanisme incremental.
    """
    engine = engine

    @staticmethod
    def read_sql(sql: str) -> pd.DataFrame:
        """
        Eksekusi query SELECT dan kembalikan hasil sebagai DataFrame.

        Parameters:
        - sql (str): Query SQL SELECT yang akan dieksekusi.

        Behavior:
        - Menggunakan engine SQLAlchemy untuk membaca data.
        - Jika terjadi error, akan log error dan mengembalikan DataFrame kosong.

        Returns:
        - pd.DataFrame: Hasil query, kosong jika error.
        """
        try:
            df = pd.read_sql(sql, DBUtils.engine)
            return df
        except Exception as e:
            Logger.log(f"Error executing read_sql: {e}", "ERROR")
            return pd.DataFrame()  # return empty DataFrame jika error

    @staticmethod
    def get_next_date(table_name: str, clean_table: str, date_col: str):
        """
        Mengambil tanggal berikutnya yang perlu diproses dari tabel clean ke tabel aggregate.

        Parameters:
        - table_name (str): Nama tabel di schema aggregate.
        - clean_table (str): Nama tabel di schema clean.
        - date_col (str): Nama kolom tanggal di tabel clean.

        Behavior:
        - Mengecek apakah tabel aggregate sudah ada.
        - Mengambil tanggal maksimum di tabel aggregate.
        - Mengambil tanggal maksimum dan minimum di tabel clean.
        - Menentukan tanggal berikutnya yang belum diproses secara incremental.

        Returns:
        - datetime.date | None: Tanggal berikutnya, None jika tidak ada tanggal baru.
        """
        with DBUtils.engine.connect() as conn:
            # Cek apakah aggregate table ada
            exists = conn.execute(
                text("SELECT to_regclass(:tbl)"),
                {"tbl": f"{Settings.SCHEMA_AGGREGATE}.{table_name}"}
            ).scalar()

            if exists:
                last_date = conn.execute(
                    text(f"SELECT MAX(date) FROM {Settings.SCHEMA_AGGREGATE}.{table_name}")
                ).scalar()
            else:
                last_date = None

            max_date_clean = conn.execute(
                text(f"SELECT MAX({date_col}::date) FROM {Settings.SCHEMA_CLEAN}.{clean_table}")
            ).scalar()

            if not last_date:
                next_date = conn.execute(
                    text(f"SELECT MIN({date_col}::date) FROM {Settings.SCHEMA_CLEAN}.{clean_table}")
                ).scalar()
            else:
                next_date = conn.execute(
                    text(f"""
                        SELECT MIN({date_col}::date)
                        FROM {Settings.SCHEMA_CLEAN}.{clean_table}
                        WHERE {date_col}::date > :last
                    """),
                    {"last": last_date}
                ).scalar()
            if not next_date or next_date > max_date_clean:
                return None
            return next_date

    @staticmethod
    def insert_or_update_table(table_name: str, sql: str, date_val):
        """
        Insert atau update data ke aggregate table berdasarkan tanggal.

        Parameters:
        - table_name (str): Nama tabel di schema aggregate.
        - sql (str): Query SQL SELECT yang hasilnya akan di-insert.
        - date_val (datetime.date | str): Tanggal data yang diproses.

        Behavior:
        - Jika tabel aggregate belum ada, buat tabel baru dengan hasil SQL.
        - Jika tabel sudah ada:
            - Hapus data untuk date_val.
            - Insert data baru dari query SQL.
        - Log status operasi (created / updated).

        Returns:
        - None
        """
        with DBUtils.engine.begin() as conn:
            exists = conn.execute(
                text("SELECT to_regclass(:tbl)"),
                {"tbl": f"{Settings.SCHEMA_AGGREGATE}.{table_name}"}
            ).scalar()

            if not exists:
                conn.execute(text(f"CREATE TABLE {Settings.SCHEMA_AGGREGATE}.{table_name} AS {sql}"))
                Logger.log(f"✓ Table {table_name} created (new) for date {date_val}")
            else:
                conn.execute(
                    text(f"DELETE FROM {Settings.SCHEMA_AGGREGATE}.{table_name} WHERE date = :d"),
                    {"d": date_val}
                )
                conn.execute(text(f"INSERT INTO {Settings.SCHEMA_AGGREGATE}.{table_name} {sql}"))
                Logger.log(f"✓ Table {table_name} updated (incremental) for date {date_val}")
