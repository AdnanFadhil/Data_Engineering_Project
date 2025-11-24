# ./src/database/db_manager.py
import psycopg2
from pathlib import Path
from .db_config import DBConfig

BASE_PATH = Path(__file__).resolve().parent

class DBManager:
    def __init__(self):
        self.conn = DBConfig.get_connection()
        self.cur = self.conn.cursor()

    def run_sql_file(self, filepath):
        """Jalankan SQL dari file"""
        file_path = BASE_PATH / filepath

        with open(file_path, "r") as file:
            sql_text = file.read()
            
        statements = [s.strip() for s in sql_text.split(";") if s.strip()]

        for stmt in statements:
            self.cur.execute(stmt + ";")

        self.conn.commit()

    def create_schema(self):
        """Buat schema jika belum ada"""
        print("Creating schema...")
        self.run_sql_file("schema.sql")
        print("Schema created.\n")

    def insert_products(self):
        """Insert products jika tabel masih kosong"""
        self.cur.execute("SELECT COUNT(*) FROM products;")
        if self.cur.fetchone()[0] > 0:
            print("Products table already has data, skipping insert.")
            return
        self.run_sql_file("inserts/products.sql")
        print("Products inserted.\n")

    def insert_users(self):
        self.cur.execute("SELECT COUNT(*) FROM users;")
        if self.cur.fetchone()[0] > 0:
            print("Users table already has data, skipping insert.")
            return
        self.run_sql_file("inserts/users.sql")
        print("Users inserted.\n")

    def insert_orders(self):
        self.cur.execute("SELECT COUNT(*) FROM orders;")
        if self.cur.fetchone()[0] > 0:
            print("Orders table already has data, skipping insert.")
            return
        self.run_sql_file("inserts/orders.sql")
        print("Orders inserted.\n")

    def insert_reviews(self):
        self.cur.execute("SELECT COUNT(*) FROM reviews;")
        if self.cur.fetchone()[0] > 0:
            print("Reviews table already has data, skipping insert.")
            return
        self.run_sql_file("inserts/reviews.sql")
        print("Reviews inserted.\n")

    def truncate_all(self):
        """Hapus semua data dan reset ID"""
        print("Truncating all tables...")
        self.cur.execute("""
            TRUNCATE TABLE 
                orders, products, users, reviews 
            RESTART IDENTITY CASCADE;
        """)
        self.conn.commit()
        print("Tables truncated.\n")

    def close(self):
        """Tutup koneksi"""
        self.cur.close()
        self.conn.close()
