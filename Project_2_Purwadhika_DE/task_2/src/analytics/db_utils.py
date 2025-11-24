from sqlalchemy import create_engine, text
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
    Utility class untuk operasi database di schema aggregate.
    Menggunakan SQLAlchemy engine untuk koneksi.
    """

    engine = engine

    @staticmethod
    def get_next_date(table_name: str, clean_table: str, date_col: str):
        """
        Mengambil tanggal berikutnya yang perlu diproses dari tabel clean ke tabel aggregate.

        Parameters:
            table_name (str): Nama tabel di schema aggregate.
            clean_table (str): Nama tabel di schema clean.
            date_col (str): Nama kolom tanggal di tabel clean.

        Returns:
            next_date (datetime.date | None): Tanggal berikutnya yang perlu diproses.
                Jika tidak ada tanggal baru, mengembalikan None.

        Behavior:
            - Mengecek apakah tabel aggregate sudah ada.
            - Mengambil tanggal maksimum di tabel aggregate.
            - Mengambil tanggal maksimum dan minimum di tabel clean.
            - Menentukan tanggal berikutnya yang belum diproses secara incremental.
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
            table_name (str): Nama tabel di schema aggregate.
            sql (str): Query SQL SELECT yang akan di-insert.
            date_val (datetime.date | str): Tanggal data yang sedang diproses.

        Behavior:
            - Jika tabel aggregate belum ada, buat tabel baru dengan hasil SQL.
            - Jika tabel sudah ada:
                - Hapus data untuk date_val.
                - Insert data baru dari query SQL.
            - Log status operasi (created / updated).

        Returns:
            None
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
