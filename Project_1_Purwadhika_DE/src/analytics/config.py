import os
from dotenv import load_dotenv

load_dotenv()

class Settings:
    DB_USER = os.getenv("DB_USER")
    DB_PASSWORD = os.getenv("DB_PASSWORD")
    DB_HOST = os.getenv("DB_HOST")
    DB_PORT = int(os.getenv("DB_PORT"))
    DB_NAME = os.getenv("DB_NAME")
    SCHEMA_CLEAN = os.getenv("SCHEMA_CLEAN")
    SCHEMA_AGGREGATE = os.getenv("SCHEMA_AGGREGATE")
    
    AGG_YELLOW_DIR = os.getenv("AGGREGATE_YELLOW_DIR")
    AGG_GREEN_DIR = os.getenv("AGGREGATE_GREEN_DIR")
    
    DISCORD_HOOK = os.getenv("DISCORD_HOOK")
    
    EMAIL_TO = os.getenv("EMAIL_TO")
    EMAIL_FROM = os.getenv("EMAIL_FROM")
    EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")
    SMTP_SERVER = os.getenv("SMTP_SERVER", "smtp.gmail.com")
    SMTP_PORT = int(os.getenv("SMTP_PORT", 587))
