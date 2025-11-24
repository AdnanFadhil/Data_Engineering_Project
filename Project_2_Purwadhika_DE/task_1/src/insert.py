# ./src/insert.py
from .database import DBManager, info, error 

def run_insert_all():
    """
    Jalankan proses pembuatan schema dan insert semua data awal.

    Behavior:
    1. Buat schema database (jika belum ada).
    2. Insert data tabel products (jika masih kosong).
    3. Insert data tabel users (jika masih kosong).
    4. Insert data tabel orders (jika masih kosong).
    5. Insert data tabel reviews (jika masih kosong).
    6. Commit setiap insert secara otomatis.
    7. Logging info/error setiap langkah.
    8. Tutup koneksi database di akhir.

    Returns:
    - None
    """
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
