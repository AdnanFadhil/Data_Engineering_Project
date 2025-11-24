from .logger import log
from .config import SCHEMA_RAW, SCHEMA_CLEAN
from .db_utils import get_connection

def clean_tripdata(src_table, pickup_col, dropoff_col):
    """
    Jalankan proses cleaning dari RAW → CLEAN untuk tripdata NYC Taxi.

    Steps:
    1. Drop old clean table dan materialized view.
    2. Insert data ke clean table (hanya 2024-2025) dengan kolom tracking.
    3. Hapus duplicate.
    4. Buat index.
    5. Buat materialized view.

    Parameters:
    - src_table (str): Nama tabel raw, misal "yellow_tripdata".
    - pickup_col (str): Nama kolom pickup datetime.
    - dropoff_col (str): Nama kolom dropoff datetime.

    Returns: None
    """
    conn, cur = get_connection()
    raw_table = f'"{SCHEMA_RAW}"."{src_table}"'
    clean_table = f'"{SCHEMA_CLEAN}"."{src_table}_clean"'
    mview = f'"{SCHEMA_CLEAN}"."{src_table}_mv"'

    log(f"=== START CLEANING FROM RAW: {SCHEMA_RAW}.{src_table} ===")

    drop_clean_table_and_mview(cur, clean_table, mview)
    insert_clean_table(cur, raw_table, clean_table, pickup_col, dropoff_col)
    deduplicate_table(cur, clean_table, pickup_col, dropoff_col)
    create_indexes(cur, clean_table, src_table, pickup_col)
    create_materialized_view(cur, clean_table, mview, pickup_col, dropoff_col)

    log(f"=== CLEANING DONE → {SCHEMA_CLEAN}.{src_table}_clean ===")


def drop_clean_table_and_mview(cur, clean_table, mview):
    """Drop table clean dan materialized view jika ada."""
    try:
        log("Dropping existing clean table & mview...")
        cur.execute(f"DROP MATERIALIZED VIEW IF EXISTS {mview}")
        cur.execute(f"DROP TABLE IF EXISTS {clean_table} CASCADE")
        log("✓ Old clean table and materialized view removed.")
    except Exception as e:
        log(f"Error dropping clean table/mview: {e}", "ERROR")


def insert_clean_table(cur, raw_table, clean_table, pickup_col, dropoff_col):
    """Insert data ke clean table dengan kolom tracking (entry_time, load_date)."""
    try:
        log("Processing & inserting into clean table...")
        clean_sql = f"""
            CREATE TABLE {clean_table} AS
            SELECT
                "VendorID",
                "{pickup_col}"::timestamp,
                "{dropoff_col}"::timestamp,
                COALESCE(passenger_count, 1) AS passenger_count,
                trip_distance,
                COALESCE(fare_amount, 0) AS fare_amount,
                COALESCE(tip_amount, 0) AS tip_amount,
                COALESCE(tolls_amount, 0) AS tolls_amount,
                COALESCE(total_amount, fare_amount + COALESCE(tip_amount,0) 
                         + COALESCE(mta_tax,0) + COALESCE(extra,0)
                         + COALESCE(tolls_amount,0) + COALESCE(improvement_surcharge,0)) AS total_amount,
                payment_type,
                extra,
                mta_tax,
                improvement_surcharge,
                NOW() AS entry_time,
                CURRENT_DATE AS load_date
            FROM {raw_table}
            WHERE "{pickup_col}" IS NOT NULL
              AND "{dropoff_col}" IS NOT NULL
              AND EXTRACT(YEAR FROM "{pickup_col}"::timestamp) IN (2024, 2025);
        """
        cur.execute(clean_sql)
        log("✓ Clean table created with entry_time and load_date (only 2024-2025).")
    except Exception as e:
        log(f"Error creating clean table: {e}", "ERROR")


def deduplicate_table(cur, clean_table, pickup_col, dropoff_col):
    """Hapus duplicate rows dari clean table."""
    try:
        log("Removing duplicates...")
        dedup_sql = f"""
            DELETE FROM {clean_table} a
            USING {clean_table} b
            WHERE a.ctid < b.ctid
              AND a."{pickup_col}" = b."{pickup_col}"
              AND a."{dropoff_col}" = b."{dropoff_col}"
              AND a.trip_distance = b.trip_distance
              AND a."VendorID" = b."VendorID";
        """
        cur.execute(dedup_sql)
        log("✓ Dedup done.")
    except Exception as e:
        log(f"Error in deduplication: {e}", "ERROR")


def create_indexes(cur, clean_table, src_table, pickup_col):
    """Buat index untuk pickup_col dan VendorID."""
    try:
        log("Creating indexes...")
        idx_sql = f"""
            CREATE INDEX IF NOT EXISTS idx_{src_table}_{pickup_col}
            ON {clean_table} ("{pickup_col}");

            CREATE INDEX IF NOT EXISTS idx_{src_table}_vendor
            ON {clean_table} ("VendorID");
        """
        cur.execute(idx_sql)
        log("✓ Indexes created.")
    except Exception as e:
        log(f"Error creating indexes: {e}", "ERROR")


def create_materialized_view(cur, clean_table, mview, pickup_col, dropoff_col):
    """Buat materialized view untuk aggregasi trip data per tanggal."""
    try:
        log("Creating materialized view...")
        mview_sql = f"""
            CREATE MATERIALIZED VIEW {mview} AS
            SELECT
                "{pickup_col}"::date AS date,
                "VendorID",
                payment_type,
                COUNT(*) AS total_trips,
                SUM(trip_distance) AS total_distance,
                SUM(total_amount) AS total_revenue,
                AVG(trip_distance) AS avg_distance,
                AVG(total_amount) AS avg_fare,
                EXTRACT(EPOCH FROM AVG("{dropoff_col}" - "{pickup_col}")) AS avg_duration
            FROM {clean_table}
            GROUP BY date, "VendorID", payment_type;
        """
        cur.execute(mview_sql)
        log("✓ Materialized view created.")
    except Exception as e:
        log(f"Error creating materialized view: {e}", "ERROR")
