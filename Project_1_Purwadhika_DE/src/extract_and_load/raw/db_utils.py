from sqlalchemy import text
from datetime import datetime
from .logger import log
from . import config
import pandas as pd


def ensure_table_exists(df_sample, table_name, engine):
    """
    Pastikan tabel ada di database; jika belum, buat dengan kolom metadata.
    Tambahkan juga entry_time dan source_file jika kolom belum ada.
    """
    schema = config.SCHEMA_RAW
    full_table = f'"{schema}"."{table_name}"'

    try:
        with engine.connect() as conn:
            # Cek apakah tabel ada
            exists = conn.execute(
                text("SELECT to_regclass(:full_table)"),
                {"full_table": full_table}
            ).scalar()

            if not exists:
                log(f"Creating new table {full_table} ...")
                df_sample.head(0).to_sql(
                    table_name,
                    engine,
                    schema=schema,
                    if_exists="replace",
                    index=False
                )
                log(f"Table {full_table} created.")

            col_check = conn.execute(text("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = :schema
                AND table_name = :table
                AND column_name IN ('entry_time', 'source_file')
            """), {"schema": schema, "table": table_name}).fetchall()
            existing_cols = [c[0] for c in col_check]

            if "entry_time" not in existing_cols:
                conn.execute(text(f'ALTER TABLE {full_table} ADD COLUMN entry_time TIMESTAMP'))
                log(f"Added column entry_time to {full_table}")

            if "source_file" not in existing_cols:
                conn.execute(text(f'ALTER TABLE {full_table} ADD COLUMN source_file TEXT'))
                log(f"Added column source_file to {full_table}")

            conn.commit()

    except Exception as e:
        log(f"Error creating or updating table {table_name}: {e}", "ERROR")


def is_file_already_inserted(table_name, filename, engine):
    """
    Cek apakah file sudah pernah dimasukkan ke tabel.
    """
    schema = config.SCHEMA_RAW
    full_table = f'"{schema}"."{table_name}"'

    try:
        with engine.connect() as conn:
            q = text(f'SELECT 1 FROM {full_table} WHERE source_file = :fname LIMIT 1')
            return conn.execute(q, {"fname": filename}).fetchone() is not None
    except Exception as e:
        log(f"Error checking duplicate file {filename}: {e}", "ERROR")
        return False


def create_parquet_tracking_table():
    """
    Pastikan tabel parquet_tracking ada dengan struktur baru.
    """
    with config.engine.connect() as conn:
        conn.execute(text(f"""
        CREATE TABLE IF NOT EXISTS "{config.SCHEMA_RAW}".parquet_tracking (
            id SERIAL PRIMARY KEY,
            file_name TEXT NOT NULL UNIQUE,
            month VARCHAR(7) NOT NULL,
            status VARCHAR(10) NOT NULL DEFAULT 'DONE',
            processed_time TIMESTAMP DEFAULT NOW()
        )
        """))
        conn.commit()


def get_last_processed_month():
    """
    Ambil bulan terakhir yang sudah diproses dari parquet_tracking.
    """
    create_parquet_tracking_table()
    with config.engine.connect() as conn:
        q = text(f"""
        SELECT month FROM "{config.SCHEMA_RAW}".parquet_tracking
        WHERE status='DONE'
        ORDER BY month DESC LIMIT 1
        """)
        row = conn.execute(q).fetchone()
        if row:
            return row[0]
    return None


def mark_parquet_done(file_name):
    """
    Tandai file parquet sudah selesai diproses.
    """
    create_parquet_tracking_table()
    month = file_name.split("_")[-1].replace(".parquet", "")

    with config.engine.connect() as conn:
        q = text(f"""
        INSERT INTO "{config.SCHEMA_RAW}".parquet_tracking (file_name, month, status)
        VALUES (:fname, :month, 'DONE')
        ON CONFLICT (file_name) DO UPDATE 
        SET status='DONE', processed_time=NOW()
        """)
        conn.execute(q, {"fname": file_name, "month": month})
        conn.commit()


def migrate_parquet_tracking():
    """
    Migrate tabel raw.parquet_tracking lama ke struktur baru:
    - Tambahkan kolom month dan processed_time
    - Isi month dari file_name
    - Tambahkan UNIQUE constraint pada file_name
    """
    with config.engine.connect() as conn:
        log("=== ADD COLUMN month IF NOT EXISTS ===")
        conn.execute(text("""
        ALTER TABLE raw.parquet_tracking
        ADD COLUMN IF NOT EXISTS month VARCHAR(7)
        """))
        conn.commit()

        log("=== ADD COLUMN processed_time IF NOT EXISTS ===")
        conn.execute(text("""
        ALTER TABLE raw.parquet_tracking
        ADD COLUMN IF NOT EXISTS processed_time TIMESTAMP DEFAULT NOW()
        """))
        conn.commit()

        log("=== POPULATE month COLUMN FROM file_name ===")
        conn.execute(text("""
        UPDATE raw.parquet_tracking
        SET month = replace(split_part(file_name, '_', 3), '.parquet', '')
        WHERE month IS NULL
        """))
        conn.commit()

        log("=== ADD UNIQUE CONSTRAINT ON file_name ===")
        result = conn.execute(text("""
        SELECT 1 
        FROM information_schema.table_constraints
        WHERE table_schema='raw' AND table_name='parquet_tracking' AND constraint_type='UNIQUE' AND constraint_name='parquet_tracking_file_name_unique'
        """)).fetchone()

        if not result:
            conn.execute(text("""
            ALTER TABLE raw.parquet_tracking
            ADD CONSTRAINT parquet_tracking_file_name_unique UNIQUE (file_name)
            """))
            conn.commit()
            log("UNIQUE constraint added to file_name")
        else:
            log("UNIQUE constraint on file_name already exists")

        log("=== Migration complete ===")
