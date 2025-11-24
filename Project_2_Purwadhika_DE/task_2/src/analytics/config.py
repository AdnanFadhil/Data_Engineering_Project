import os
from dotenv import load_dotenv

# Load environment variables dari file .env
load_dotenv()

class Settings:
    """
    Configuration settings yang diambil dari environment variables.

    Attributes:
        DB_USER (str): Username database.
        DB_PASSWORD (str): Password database.
        DB_HOST (str): Host database.
        DB_PORT (int): Port database.
        DB_NAME (str): Nama database.
        SCHEMA_CLEAN (str): Schema untuk clean data.
        SCHEMA_AGGREGATE (str): Schema untuk aggregate data.
        AGG_YELLOW_DIR (str): Folder output aggregate yellow taxi.
        AGG_GREEN_DIR (str): Folder output aggregate green taxi.
        DISCORD_HOOK (str): Webhook URL Discord untuk notifikasi.
        EMAIL_TO (str): Email penerima notifikasi.
        EMAIL_FROM (str): Email pengirim notifikasi.
        EMAIL_PASSWORD (str): Password/Token email pengirim.
        SMTP_SERVER (str): Server SMTP. Default "smtp.gmail.com".
        SMTP_PORT (int): Port SMTP. Default 587.
    """

    # Database settings
    DB_USER = os.getenv("DB_USER")
    DB_PASSWORD = os.getenv("DB_PASSWORD")
    DB_HOST = os.getenv("DB_HOST")
    DB_PORT = int(os.getenv("DB_PORT",5432)) 
    DB_NAME = os.getenv("DB_NAME")
    SCHEMA_CLEAN = os.getenv("SCHEMA_CLEAN")
    SCHEMA_AGGREGATE = os.getenv("SCHEMA_AGGREGATE")
    
    # Aggregate folders
    AGG_YELLOW_DIR = os.getenv("AGGREGATE_YELLOW_DIR")
    AGG_GREEN_DIR = os.getenv("AGGREGATE_GREEN_DIR")
    
    # Discord notification
    DISCORD_HOOK = os.getenv("DISCORD_HOOK")
    
    # Email notification
    EMAIL_TO = os.getenv("EMAIL_TO")
    EMAIL_FROM = os.getenv("EMAIL_FROM")
    EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")
    SMTP_SERVER = os.getenv("SMTP_SERVER", "smtp.gmail.com")
    SMTP_PORT = int(os.getenv("SMTP_PORT", 587))
