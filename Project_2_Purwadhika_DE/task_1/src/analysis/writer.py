import pandas as pd

from ..database.log import info, error

def save_csv(df, folder, name):
    """
    Simpan DataFrame ke file CSV di folder yang ditentukan.

    Parameters:
    - df (pd.DataFrame): DataFrame yang akan disimpan.
    - folder (Path | str): Folder tujuan penyimpanan CSV.
    - name (str): Nama file CSV tanpa ekstensi.

    Behavior:
    - Buat path lengkap untuk file CSV dengan ekstensi `.csv`.
    - Simpan DataFrame ke CSV tanpa menyertakan index.
    - Logging info saat CSV berhasil disimpan.

    Returns:
    - None
    """
    path = folder / f"{name}.csv"
    df.to_csv(path, index=False)
    info(f"CSV berhasil disimpan: {path}")

def save_table(df, engine, schema, name):
    """
    Simpan DataFrame ke tabel database menggunakan SQLAlchemy engine.

    Parameters:
    - df (pd.DataFrame): DataFrame yang akan disimpan.
    - engine (sqlalchemy.Engine): SQLAlchemy engine untuk koneksi database.
    - schema (str): Nama schema tempat tabel akan dibuat.
    - name (str): Nama tabel tujuan.

    Behavior:
    - Simpan DataFrame ke tabel database.
    - Jika tabel sudah ada, replace tabel tersebut.
    - Gunakan batch insert method `multi` untuk performa lebih baik.
    - Logging info saat tabel berhasil disimpan.

    Returns:
    - None
    """
    df.to_sql(name, engine, schema=schema, if_exists="replace", index=False, method="multi")
    info(f"Table berhasil disimpan: {schema}.{name}")
