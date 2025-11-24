# ./src/database/db_manager.py
import psycopg2
from pathlib import Path
from .db_config import DBConfig

BASE_PATH = Path(__file__).resolve().parent

class DBManager:
    """
    Manajemen koneksi dan eksekusi SQL ke database PostgreSQL.

    Attributes:
    - conn (psycopg2.connection): Koneksi ke database.
    - cur (psycopg2.cursor): Cursor untuk eksekusi SQL.

    Methods:
    - run_sql_file(filepath):
        Jalankan semua statement SQL dari file.
    - create_schema():
        Buat schema database jika belum ada.
    - insert_products():
        Insert data products jika tabel masih kosong.
    - insert_users():
        Insert data users jika tabel masih kosong.
    - insert_orders():
        Insert data orders jika tabel masih kosong.
    - insert_reviews():
        Insert data reviews jika tabel masih kosong.
    - truncate_all():
        Hapus semua data tabel dan reset ID.
    - close():
        Tutup cursor dan koneksi database.
    """
    def __init__(self):
        """
        Inisialisasi DBManager.
        
        Behavior:
        - Membuat koneksi database menggunakan DBConfig.
        - Membuat cursor untuk eksekusi SQL.
        """
        self.conn = DBConfig.get_connection()
        self.cur = self.conn.cursor()

    def run_sql_file(self, filepath):
        """
        Jalankan semua statement SQL dari file.

        Parameters:
        - filepath (str | Path): Path file SQL relatif terhadap BASE_PATH.

        Behavior:
        - Baca konten file SQL.
        - Pisahkan statement berdasarkan `;`.
        - Eksekusi tiap statement secara berurutan.
        - Commit perubahan ke database.

        Returns:
        - None
        """
        file_path = BASE_PATH / filepath

        with open(file_path, "r") as file:
            sql_text = file.read()
            
        statements = [s.strip() for s in sql_text.split(";") if s.strip()]

        for stmt in statements:
            self.cur.execute(stmt + ";")

        self.conn.commit()

    def create_schema(self):
        """
        Buat schema database jika belum ada.

        Behavior:
        - Jalankan file `schema.sql`.
        - Commit schema ke database.
        
        Returns:
        - None
        """
        print("Creating schema...")
        self.run_sql_file("schema.sql")
        print("Schema created.\n")

    def insert_products(self):
        """
        Insert data products jika tabel masih kosong.

        Behavior:
        - Cek jumlah baris pada tabel products.
        - Jika kosong, jalankan `inserts/products.sql`.
        - Commit data ke database.
        
        Returns:
        - None
        """
        self.cur.execute("SELECT COUNT(*) FROM products;")
        if self.cur.fetchone()[0] > 0:
            print("Products table already has data, skipping insert.")
            return
        self.run_sql_file("inserts/products.sql")
        print("Products inserted.\n")

    def insert_users(self):
        """
        Insert data users jika tabel masih kosong.

        Behavior:
        - Cek jumlah baris pada tabel users.
        - Jika kosong, jalankan `inserts/users.sql`.
        - Commit data ke database.
        
        Returns:
        - None
        """
        self.cur.execute("SELECT COUNT(*) FROM users;")
        if self.cur.fetchone()[0] > 0:
            print("Users table already has data, skipping insert.")
            return
        self.run_sql_file("inserts/users.sql")
        print("Users inserted.\n")

    def insert_orders(self):
        """
        Insert data orders jika tabel masih kosong.

        Behavior:
        - Cek jumlah baris pada tabel orders.
        - Jika kosong, jalankan `inserts/orders.sql`.
        - Commit data ke database.
        
        Returns:
        - None
        """
        self.cur.execute("SELECT COUNT(*) FROM orders;")
        if self.cur.fetchone()[0] > 0:
            print("Orders table already has data, skipping insert.")
            return
        self.run_sql_file("inserts/orders.sql")
        print("Orders inserted.\n")

    def insert_reviews(self):
        """
        Insert data reviews jika tabel masih kosong.

        Behavior:
        - Cek jumlah baris pada tabel reviews.
        - Jika kosong, jalankan `inserts/reviews.sql`.
        - Commit data ke database.
        
        Returns:
        - None
        """
        self.cur.execute("SELECT COUNT(*) FROM reviews;")
        if self.cur.fetchone()[0] > 0:
            print("Reviews table already has data, skipping insert.")
            return
        self.run_sql_file("inserts/reviews.sql")
        print("Reviews inserted.\n")

    def truncate_all(self):
        """
        Hapus semua data tabel dan reset ID.

        Behavior:
        - Truncate semua tabel: orders, products, users, reviews.
        - Restart identity (auto-increment) dan cascade ke tabel terkait.
        - Commit perubahan ke database.
        
        Returns:
        - None
        """
        print("Truncating all tables...")
        self.cur.execute("""
            TRUNCATE TABLE 
                orders, products, users, reviews 
            RESTART IDENTITY CASCADE;
        """)
        self.conn.commit()
        print("Tables truncated.\n")

    def close(self):
        """
        Tutup koneksi database.

        Behavior:
        - Tutup cursor.
        - Tutup koneksi database.
        
        Returns:
        - None
        """
        self.cur.close()
        self.conn.close()
