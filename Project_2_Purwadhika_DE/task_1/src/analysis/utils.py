import os
from ..database.log import info, error

def ensure_folder(path):
    if not path.exists():
        path.mkdir(parents=True, exist_ok=True)
        info(f"Folder dibuat: {path}")
    else:
        info(f"Folder sudah ada: {path}")

def ensure_schema(conn, schema):
    try:
        with conn.cursor() as cur:
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")
            conn.commit()
            info(f"Schema siap: {schema}")
    except Exception as e:
        error(f"Gagal membuat schema {schema}: {e}")
        raise
