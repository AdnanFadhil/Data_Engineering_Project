from sqlalchemy import text
from .logger import Logger
from .db_utils import DBUtils
from .config import Settings

class PartitionInserter:
    @staticmethod
    def insert_daily(clean_table, pickup_col, color, date_str):
        partition_table = f"{Settings.SCHEMA_AGGREGATE}.{color}_partitioned"
        insert_sql = f"""
            INSERT INTO {partition_table} 
            SELECT *
            FROM {Settings.SCHEMA_CLEAN}.{clean_table}
            WHERE {pickup_col}::date = '{date_str}'
        """
        try:
            with DBUtils.engine.begin() as conn:
                conn.execute(text(insert_sql))
            Logger.log(f"Data inserted into {partition_table} for {date_str}")
        except Exception as e:
            Logger.log(f"Error inserting data into partition table {partition_table}: {e}", "ERROR")
