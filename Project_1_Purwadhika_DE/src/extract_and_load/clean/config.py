import os
from dotenv import load_dotenv

load_dotenv()

SCHEMA_RAW = os.getenv("SCHEMA_RAW")
SCHEMA_CLEAN = os.getenv("SCHEMA_CLEAN")

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "dbname": os.getenv("DB_NAME"),
}
