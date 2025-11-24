# ./src/insert.py
from database import DBManager, info, error  # import dari package database

def run_insert_all():
    """Fungsi untuk membuat schema dan insert semua data"""
    db = DBManager()
    try:
        # 1. Create table schema
        info("Creating schema...")
        db.create_schema()

        # 2. Insert all data
        info("Inserting products...")
        db.insert_products()
        info("Inserting users...")
        db.insert_users()
        info("Inserting orders...")
        db.insert_orders()
        info("Inserting reviews...")
        db.insert_reviews()

        info("All data inserted successfully.")
    except Exception as e:
        error(f"Error occurred: {e}")
    finally:
        db.close()
        info("Database connection closed.")


if __name__ == "__main__":
    run_insert_all()
