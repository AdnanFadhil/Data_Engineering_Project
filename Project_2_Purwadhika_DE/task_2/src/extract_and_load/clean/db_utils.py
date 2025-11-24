import psycopg2
from .config import DB_CONFIG
from .logger import log

def get_connection():
    """
    Membuat koneksi ke database PostgreSQL dan kursor.

    Parameters:
        None

    Returns:
        tuple: (conn, cur)
            - conn: psycopg2 connection object dengan autocommit=True
            - cur: psycopg2 cursor object untuk eksekusi query

    Raises:
        Exception: Jika gagal melakukan koneksi ke database, error akan dicatat di log dan diteruskan.

    Behavior:
        - Mencatat ke log jika koneksi berhasil atau gagal.
        - Autocommit diaktifkan pada koneksi.
    """
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = True
        cur = conn.cursor()
        log("Clean DB connected successfully.")
        return conn, cur
    except Exception as e:
        log(f"Clean DB connection error: {e}", "ERROR")
        raise e
