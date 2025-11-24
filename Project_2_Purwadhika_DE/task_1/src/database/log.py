# ./src/database/log.py
import logging
from pathlib import Path
import os
from dotenv import load_dotenv

"""
Modul logging untuk aplikasi.

Behavior:
- Membaca folder log dari environment variable LOG_FOLDER (default: 'log').
- Pastikan folder log ada, buat jika belum ada.
- Semua log ditulis ke file 'app.log' di folder log dan ditampilkan ke console.
- Format log: timestamp [LEVEL] pesan log.
- Menyediakan shortcut functions: info, warning, error, debug.

Attributes:
- LOG_FOLDER (str): Folder untuk menyimpan file log.
- LOG_FILE (Path): Path file log utama ('app.log').
"""

# Load .env
load_dotenv()

# Ambil folder log dari .env
LOG_FOLDER = os.getenv("LOG_FOLDER", "log")
log_path = Path(LOG_FOLDER)
log_path.mkdir(parents=True, exist_ok=True)  # pastikan folder ada

# File log
LOG_FILE = log_path / "app.log"

# Konfigurasi logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)

# Shortcut functions
def info(msg):
    logging.info(msg)

def warning(msg):
    logging.warning(msg)

def error(msg):
    logging.error(msg)

def debug(msg):
    logging.debug(msg)
