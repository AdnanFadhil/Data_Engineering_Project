# ./src/database/log.py
import logging
from pathlib import Path
import os
from dotenv import load_dotenv

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
