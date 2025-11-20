from sqlalchemy import create_engine, text
from .config import Settings
from .logger import Logger

DB_URL = f"postgresql://{Settings.DB_USER}:{Settings.DB_PASSWORD}@{Settings.DB_HOST}:{Settings.DB_PORT}/{Settings.DB_NAME}"
engine = create_engine(DB_URL)

class DBUtils:
    engine = engine

    @staticmethod
    def get_next_date(table_name, clean_table, date_col):
        with DBUtils.engine.connect() as conn:
            exists = conn.execute(
                text("SELECT to_regclass(:tbl)"), {"tbl": f"{Settings.SCHEMA_AGGREGATE}.{table_name}"}
            ).scalar()
            if exists:
                last_date = conn.execute(
                    text(f"SELECT MAX(date) FROM {Settings.SCHEMA_AGGREGATE}.{table_name}")
                ).scalar()
            else:
                last_date = None

            max_date_clean = conn.execute(
                text(f"SELECT MAX({date_col}::date) FROM {Settings.SCHEMA_CLEAN}.{clean_table}")
            ).scalar()

            if not last_date:
                next_date = conn.execute(
                    text(f"SELECT MIN({date_col}::date) FROM {Settings.SCHEMA_CLEAN}.{clean_table}")
                ).scalar()
            else:
                next_date = conn.execute(
                    text(f"""
                        SELECT MIN({date_col}::date)
                        FROM {Settings.SCHEMA_CLEAN}.{clean_table}
                        WHERE {date_col}::date > :last
                    """), {"last": last_date}
                ).scalar()

            if not next_date or next_date > max_date_clean:
                return None
            return next_date

    @staticmethod
    def insert_or_update_table(table_name, sql, date_val):
        with DBUtils.engine.begin() as conn:
            exists = conn.execute(
                text("SELECT to_regclass(:tbl)"), {"tbl": f"{Settings.SCHEMA_AGGREGATE}.{table_name}"}
            ).scalar()
            if not exists:
                conn.execute(text(f"CREATE TABLE {Settings.SCHEMA_AGGREGATE}.{table_name} AS {sql}"))
                Logger.log(f"✓ Table {table_name} created (new) for date {date_val}")
            else:
                conn.execute(text(f"DELETE FROM {Settings.SCHEMA_AGGREGATE}.{table_name} WHERE date = :d"), {"d": date_val})
                conn.execute(text(f"INSERT INTO {Settings.SCHEMA_AGGREGATE}.{table_name} {sql}"))
                Logger.log(f"✓ Table {table_name} updated (incremental) for date {date_val}")
