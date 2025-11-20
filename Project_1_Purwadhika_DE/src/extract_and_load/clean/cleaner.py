from .logger import log
from .config import SCHEMA_RAW, SCHEMA_CLEAN
from .db_utils import get_connection

def clean_tripdata(src_table, pickup_col, dropoff_col):

    conn, cur = get_connection()

    log(f"=== START CLEANING FROM RAW: {SCHEMA_RAW}.{src_table} ===")

    raw_table = f'"{SCHEMA_RAW}"."{src_table}"'
    clean_table = f'"{SCHEMA_CLEAN}"."{src_table}_clean"'
    mview = f'"{SCHEMA_CLEAN}"."{src_table}_mv"'

    try:
        log("Dropping existing clean table & mview...")
        cur.execute(f"DROP MATERIALIZED VIEW IF EXISTS {mview}")
        cur.execute(f"DROP TABLE IF EXISTS {clean_table} CASCADE")
        log("✓ Old clean table and materialized view removed.")
    except Exception as e:
        log(f"Error dropping clean table/mview: {e}", "ERROR")

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
                COALESCE(total_amount, fare_amount + COALESCE(tip_amount,0) + COALESCE(mta_tax,0)
                         + COALESCE(extra,0) + COALESCE(tolls_amount,0) + COALESCE(improvement_surcharge,0)) AS total_amount,
                payment_type,
                extra,
                mta_tax,
                improvement_surcharge,
                -- Tambahan kolom tracking
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
        return  

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

    log(f"=== CLEANING DONE → {SCHEMA_CLEAN}.{src_table}_clean ===")
