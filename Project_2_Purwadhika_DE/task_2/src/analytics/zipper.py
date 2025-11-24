import zipfile
import os
import pandas as pd

class Zipper:
    @staticmethod
    def zip_aggregate_files():
        """
        Mengompres semua file CSV hasil agregasi harian menjadi satu file ZIP.

        Behavior:
        - Menentukan nama file ZIP berdasarkan tanggal sekarang(tanggal process).
        - Mencari semua file di folder `resources/result/aggregate`.
        - Menambahkan setiap file ke dalam ZIP.
        - Menampilkan notifikasi bahwa ZIP telah dibuat.

        Returns:
        - str: Nama file ZIP yang telah dibuat.
        """
        zip_file = f"update_aggregate_{pd.Timestamp.now().strftime('%Y%m%d')}.zip"
        with zipfile.ZipFile(zip_file, "w") as zf:
            base_dir = "resources/result/aggregate"
            for root, dirs, files in os.walk(base_dir):
                for file in files:
                    zf.write(os.path.join(root, file))
        print(f"✓ Aggregate CSV files zipped: {zip_file}")
        return zip_file
