import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.pool import QueuePool

load_dotenv()
# DB Environment
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
SCHEMA_RAW = os.getenv("SCHEMA_RAW", "raw")
SCHEMA_CLEAN = os.getenv("SCHEMA_CLEAN", "clean")

DB_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# DB engine
engine = create_engine(
    DB_URL,
    poolclass=QueuePool,
    pool_size=5,
    max_overflow=10
)

# Checking schema exist
with engine.connect() as conn:
    conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{SCHEMA_RAW}"'))
    conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{SCHEMA_CLEAN}"'))
    conn.commit()

# Folder environment
TLC_URL = os.getenv("TLC_URL")
PARQUET_NAMES = [x.strip() for x in os.getenv("PARQUET_NAMES").split(",")]
YELLOW_URL = os.getenv("YELLOW_URL")
GREEN_URL = os.getenv("GREEN_URL")
RAW_DIR = os.getenv("RAW_DIR")
DOWNLOAD_DIR = os.getenv("DOWNLOAD_DIR")
YELLOW_DIR = os.getenv("YELLOW_DIR")
GREEN_DIR = os.getenv("GREEN_DIR")
OLD_DIR = os.getenv("OLD_DIR")
FAILED_DIR = os.getenv("FAILED_DIR")
YELLOW_TABLE = os.getenv("YELLOW_TABLE")
GREEN_TABLE = os.getenv("GREEN_TABLE")

folders = {
    "yellow": {
        "input": YELLOW_DIR,
        "old": os.path.join(OLD_DIR, "yellow"),
        "failed": os.path.join(FAILED_DIR, "yellow"),
        "table": YELLOW_TABLE
    },
    "green": {
        "input": GREEN_DIR,
        "old": os.path.join(OLD_DIR, "green"),
        "failed": os.path.join(FAILED_DIR, "green"),
        "table": GREEN_TABLE
    }
}

for cfg in folders.values():
    os.makedirs(cfg["input"], exist_ok=True)
    os.makedirs(cfg["old"], exist_ok=True)
    os.makedirs(cfg["failed"], exist_ok=True)

os.makedirs(DOWNLOAD_DIR, exist_ok=True)
