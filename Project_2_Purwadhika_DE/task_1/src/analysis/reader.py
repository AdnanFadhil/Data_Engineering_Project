from pathlib import Path

def get_sql_files(folder):
    """
    Ambil semua file SQL dari folder dan urutkan secara alfabet.

    Parameters:
    - folder (str | Path): Path folder tempat file SQL berada.

    Behavior:
    - Pastikan folder ada.
    - Cari semua file dengan ekstensi `.sql`.
    - Urutkan nama file secara alfabet untuk memastikan eksekusi terprediksi.

    Returns:
    - list[Path]: Daftar Path objek untuk setiap file SQL.
    """
    return sorted(Path(folder).glob("*.sql"))

def read_sql_file(path):
    """
    Baca isi file SQL dan kembalikan sebagai string.

    Parameters:
    - path (str | Path): Path file SQL yang akan dibaca.

    Behavior:
    - Baca seluruh konten file.
    - Hilangkan spasi kosong di awal/akhir.
    - Tidak mengeksekusi SQL, hanya mengembalikan teksnya.

    Returns:
    - str: Konten SQL sebagai string.
    """
    return Path(path).read_text().strip()
