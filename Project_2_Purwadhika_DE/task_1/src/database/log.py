# ./src/database/log.py
import logging
from pathlib import Path

# Path log file
BASE_PATH = Path(__file__).resolve().parent
LOG_FILE = BASE_PATH / "app.log"

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
