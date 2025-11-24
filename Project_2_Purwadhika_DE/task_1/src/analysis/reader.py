from pathlib import Path

def get_sql_files(folder):
    """Ambil semua file SQL dari folder, urutkan alphabet"""
    return sorted(Path(folder).glob("*.sql"))

def read_sql_file(path):
    """Baca isi SQL file"""
    return Path(path).read_text().strip()
