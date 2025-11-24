import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()  

class DBConfig:
    HOST = os.getenv("DB_HOST")
    PORT = os.getenv("DB_PORT")
    DBNAME = os.getenv("DB_NAME")
    USER = os.getenv("DB_USER")
    PASSWORD = os.getenv("DB_PASSWORD")

    @staticmethod
    def get_connection():
        return psycopg2.connect(
            host=DBConfig.HOST,
            port=DBConfig.PORT,
            database=DBConfig.DBNAME,
            user=DBConfig.USER,
            password=DBConfig.PASSWORD
        )
