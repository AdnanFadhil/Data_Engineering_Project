# ./src/analysis/exporter.py
from pathlib import Path
import os
import pandas as pd
from sqlalchemy import create_engine

from .db_config import DBConfig
from .reader import get_sql_files, read_sql_file
from .writer import save_csv, save_table
from .utils import ensure_folder, ensure_schema
from ..database.log import info, error

class AnalysisExporter:
    """
    Menyediakan mekanisme untuk mengeksekusi SQL dari folder tertentu,
    lalu mengekspor hasilnya ke CSV dan/atau menyimpan ke database.

    Attributes:
    - sql_folder (Path): Folder tempat file SQL berada.
    - output_folder (Path): Folder tujuan penyimpanan CSV.
    - schema (str): Schema database untuk menyimpan tabel hasil export.
    - conn (psycopg2.connection): Koneksi ke PostgreSQL.
    - engine (sqlalchemy.Engine): SQLAlchemy engine untuk operasi DB.

    Methods:
    - run_all():
        Jalankan semua file SQL dalam urutan tertentu:
        1. Eksekusi DDL (CREATE/DROP) untuk menyiapkan tabel.
        2. Eksekusi SELECT → hasil disimpan ke CSV dan/atau DB.
        
        Behavior:
        - Pastikan folder SQL dan output ada.
        - Pastikan schema DB ada, buat jika belum ada.
        - Baca semua file SQL di folder sql_folder.
        - Eksekusi file yang diawali CREATE/DROP sebagai DDL.
        - Eksekusi file lainnya sebagai SELECT, hasil disimpan ke CSV & tabel.
        - Logging info/error untuk setiap file SQL.
        - Tutup koneksi database di akhir proses.

        Returns:
        - None
    """
    def __init__(self, sql_folder=None, output_folder=None, schema=None):
        # Folder SQL
        self.sql_folder = Path(sql_folder) if sql_folder else Path(__file__).parent / "sql"
        ensure_folder(self.sql_folder)

        # Folder output
        default_output = Path(os.getenv("OUTPUT_FOLDER", Path(__file__).parent.parent.parent / "output"))
        self.output_folder = Path(output_folder) if output_folder else default_output
        ensure_folder(self.output_folder)

        # Schema DB
        self.schema = schema if schema else os.getenv("SCHEMA_NAME", "result")

        # Koneksi DB & SQLAlchemy engine
        self.conn = DBConfig.get_connection()
        ensure_schema(self.conn, self.schema)

        conn_str = f"postgresql+psycopg2://{DBConfig.USER}:{DBConfig.PASSWORD}@{DBConfig.HOST}:{DBConfig.PORT}/{DBConfig.DBNAME}"
        self.engine = create_engine(conn_str)

    def run_all(self):
        """Jalankan SQL: CREATE/DROP dulu, SELECT terakhir untuk CSV/DB"""
        try:
            sql_files = get_sql_files(self.sql_folder)

            # 1️⃣ Eksekusi DDL (CREATE/DROP)
            for sql_file in sql_files:
                sql_text = read_sql_file(sql_file)
                question_name = sql_file.stem
                if sql_text.lower().startswith(("create", "drop")):
                    try:
                        with self.conn.cursor() as cur:
                            cur.execute(sql_text)
                            self.conn.commit()
                        info(f"{question_name} executed successfully (DDL).")
                    except Exception as e:
                        error(f"Failed to execute {question_name} (DDL): {e}")

            # 2️⃣ Eksekusi SELECT → CSV & DB
            for sql_file in sql_files:
                sql_text = read_sql_file(sql_file)
                question_name = sql_file.stem
                if not sql_text.lower().startswith(("create", "drop")):
                    try:
                        df = pd.read_sql(sql_text, self.conn)
                        save_csv(df, self.output_folder, question_name)
                        save_table(df, self.engine, self.schema, question_name)
                        info(f"{question_name} exported to CSV & saved to table {self.schema}.{question_name}")
                    except Exception as e:
                        error(f"Failed to execute {question_name} (SELECT): {e}")

        finally:
            self.conn.close()
            info("Database connection closed.")
