import os
import pandas as pd
from .db_utils import DBUtils
from .config import Settings

class CSVExporter:
    """
    Utility class untuk mengekspor tabel aggregate menjadi CSV.
    """
    @staticmethod
    def export_table(table_name, color):
        """
        Mengekspor tabel aggregate ke file CSV berdasarkan warna taxi.

        Parameters:
        - table_name (str): Nama tabel di schema aggregate.
        - color (str): 'yellow' atau 'green', digunakan untuk membuat folder output.

        Behavior:
        - Membuat direktori output jika belum ada.
        - Membaca seluruh isi tabel dari schema aggregate.
        - Menyimpan hasil ke CSV dengan nama {table_name}.csv.

        Returns:
        - None
        """
        out_dir = f"resources/result/aggregate/{color}"
        os.makedirs(out_dir, exist_ok=True)
        df = DBUtils.read_sql(f"SELECT * FROM {Settings.SCHEMA_AGGREGATE}.{table_name}")
        file_path = os.path.join(out_dir, f"{table_name}.csv")
        df.to_csv(file_path, index=False)
