import psycopg2
from pathlib import Path
from .db_config import DBConfig

BASE_PATH = Path(__file__).resolve().parent

class DBManager:
    def __init__(self):
        self.conn = DBConfig.get_connection()
        self.cur = self.conn.cursor()

    def run_sql_file(self, filepath):
        file_path = BASE_PATH / filepath

        with open(file_path, "r") as file:
            sql_text = file.read()

        statements = [s.strip() for s in sql_text.split(";") if s.strip()]

        for stmt in statements:
            self.cur.execute(stmt + ";")

        self.conn.commit()

    def create_schema(self):
        print("Creating schema...")
        self.run_sql_file("schema.sql")
        print("Schema created.\n")

    def insert_products(self):
        self.run_sql_file("inserts/products.sql")

    def insert_users(self):
        self.run_sql_file("inserts/users.sql")

    def insert_orders(self):
        self.run_sql_file("inserts/orders.sql")

    def insert_reviews(self):
        self.run_sql_file("inserts/reviews.sql")

    def truncate_all(self):
        print("Truncating all tables...")
        self.cur.execute("""
            TRUNCATE TABLE 
                orders, products, users, reviews 
            RESTART IDENTITY CASCADE;
        """)
        self.conn.commit()
        print("Tables truncated.\n")

    def close(self):
        self.cur.close()
        self.conn.close()
