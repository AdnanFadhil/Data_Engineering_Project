import psycopg2
from .config import DB_CONFIG
from .logger import log

def get_connection():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = True
        log("Clean DB connected successfully.")
        return conn, conn.cursor()
    except Exception as e:
        log(f"Clean DB connection error: {e}", "ERROR")
        raise e
