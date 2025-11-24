import os
from datetime import datetime

# Buat folder log jika belum ada
LOG_DIR = "resources/logs"
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIR, "clean_process.log")

def log(message, level="INFO"):
    """
    Menyimpan pesan log ke file log dan mencetak ke console.

    Parameters:
        message (str): Pesan yang ingin dicatat.
        level (str): Level log, misal "INFO", "ERROR", "WARNING". Default "INFO".

    Behavior:
        - Membuat timestamp saat log dicatat.
        - Format pesan: [YYYY-MM-DD HH:MM:SS] [LEVEL] message
        - Mencetak pesan ke console.
        - Menulis pesan ke file LOG_FILE (clean_process.log) dengan encoding UTF-8.

    Returns:
        None
    """
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] [{level}] {message}"
    print(line)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")
