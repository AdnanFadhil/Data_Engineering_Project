import os
from ..database.log import info, error

def ensure_folder(path):
    """
    Pastikan folder tersedia, buat jika belum ada.

    Parameters:
    - path (str | Path): Path folder yang ingin dicek atau dibuat.

    Behavior:
    - Cek apakah folder sudah ada.
    - Jika belum ada, buat folder beserta parent-nya.
    - Logging info: beri tahu folder dibuat atau sudah ada.
    
    Returns:
    - None
    """
    if not path.exists():
        path.mkdir(parents=True, exist_ok=True)
        info(f"Folder dibuat: {path}")
    else:
        info(f"Folder sudah ada: {path}")

def ensure_schema(conn, schema):
    """
    Pastikan schema database tersedia, buat jika belum ada.

    Parameters:
    - conn (psycopg2.connection): Koneksi ke PostgreSQL.
    - schema (str): Nama schema yang ingin dicek atau dibuat.

    Behavior:
    - Cek apakah schema sudah ada di database.
    - Jika belum ada, buat schema baru.
    - Commit perubahan ke database.
    - Logging info: beri tahu schema siap.
    - Jika gagal membuat schema, logging error dan raise exception.

    Returns:
    - None
    """
    try:
        with conn.cursor() as cur:
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")
            conn.commit()
            info(f"Schema siap: {schema}")
    except Exception as e:
        error(f"Gagal membuat schema {schema}: {e}")
        raise
