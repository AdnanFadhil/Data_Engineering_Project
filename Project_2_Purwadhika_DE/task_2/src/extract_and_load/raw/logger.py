import os
from datetime import datetime

LOG_DIR = "resources/logs"
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIR, "extract_load.log")

def log(message, level="INFO"):
    """
    Menyimpan pesan log ke file log dengan timestamp dan level.

    Parameters:
    - message (str): Pesan yang ingin dicatat ke log.
    - level (str): Level log, misal "INFO", "ERROR", "WARNING". Default "INFO".

    Behavior:
    - Membuat timestamp saat log dicatat.
    - Menulis pesan ke file LOG_FILE.
    - Format pesan: [YYYY-MM-DD HH:MM:SS] [LEVEL] message
    - Tidak mencetak ke console (print dikomentari untuk menghindari masalah Unicode).

    Returns: None
    """
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] [{level}] {message}"
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")

