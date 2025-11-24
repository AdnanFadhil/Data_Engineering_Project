import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()  

class DBConfig:
    """
    Konfigurasi koneksi ke database PostgreSQL menggunakan environment variables.

    Attributes:
    - HOST (str): Hostname atau IP server database.
    - PORT (str | int): Port database (default PostgreSQL biasanya 5432).
    - DBNAME (str): Nama database yang akan diakses.
    - USER (str): Username untuk autentikasi.
    - PASSWORD (str): Password untuk autentikasi.

    Methods:
    - get_connection():
        Membuat koneksi psycopg2 ke database PostgreSQL.
        
        Behavior:
        - Membaca host, port, database, user, dan password dari environment variables.
        - Membuat koneksi psycopg2 tanpa mengeksekusi query.
        - Pastikan koneksi ditutup setelah digunakan untuk menghindari resource leak.

        Returns:
        - conn (psycopg2.connection): Objek koneksi ke database.
    """
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
