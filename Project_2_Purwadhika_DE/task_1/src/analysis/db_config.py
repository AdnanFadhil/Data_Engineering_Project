import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()  

class DBConfig:
    HOST = os.getenv("DB_HOST", "localhost")
    PORT = os.getenv("DB_PORT", "5432")
    DBNAME = os.getenv("DB_NAME", "project_capstone")
    USER = os.getenv("DB_USER", "postgres")
    PASSWORD = os.getenv("DB_PASSWORD", "postgres")

    @staticmethod
    def get_connection():
        return psycopg2.connect(
            host=DBConfig.HOST,
            port=DBConfig.PORT,
            database=DBConfig.DBNAME,
            user=DBConfig.USER,
            password=DBConfig.PASSWORD
        )
