from datetime import datetime
from calendar import monthrange
import pandas as pd
from sqlalchemy import text
from .logger import Logger
from .db_utils import DBUtils
from .config import Settings

class PartitionManager:
    """Handle parent/monthly/daily partitions creation."""

    @staticmethod
    def create_nested_partition(clean_table, pickup_col, color, next_date):
        partition_table = f"{Settings.SCHEMA_AGGREGATE}.{color}_partitioned"

        # Parent
        try:
            with DBUtils.engine.begin() as conn:
                create_parent_sql = f"""
                    CREATE TABLE IF NOT EXISTS {partition_table} (
                        LIKE {Settings.SCHEMA_CLEAN}.{clean_table} INCLUDING ALL
                    ) PARTITION BY RANGE (DATE_TRUNC('month', {pickup_col}));
                """
                conn.execute(text(create_parent_sql))
                Logger.log(f"Parent partition table {partition_table} ensured")
        except Exception as e:
            Logger.log(f"Error creating parent partition: {e}", "ERROR")
            return

        # Monthly child
        year, month = next_date.year, next_date.month
        month_start = datetime(year, month, 1).date()
        month_end = datetime(year, month, monthrange(year, month)[1]).date()
        month_child = f"{partition_table}_{year}{month:02d}"

        try:
            with DBUtils.engine.begin() as conn:
                create_month_sql = f"""
                    CREATE TABLE IF NOT EXISTS {month_child} (
                        LIKE {Settings.SCHEMA_CLEAN}.{clean_table} INCLUDING ALL
                    ) PARTITION BY RANGE ({pickup_col});
                """
                conn.execute(text(create_month_sql))
                Logger.log(f"Child monthly partitioned table {month_child} ensured")

                # attach monthly if not exists
                check_attach_sql = f"""
                    SELECT 1
                    FROM pg_inherits i
                    JOIN pg_class c ON i.inhrelid = c.oid
                    WHERE i.inhparent = '{partition_table}'::regclass
                    AND c.relname = '{month_child.split('.')[-1]}';
                """
                result = conn.execute(text(check_attach_sql)).fetchone()
                if not result:
                    attach_month_sql = f"""
                        ALTER TABLE {partition_table}
                        ATTACH PARTITION {month_child}
                        FOR VALUES FROM ('{month_start}') TO ('{month_end + pd.Timedelta(days=1)}');
                    """
                    conn.execute(text(attach_month_sql))
                    Logger.log(f"Monthly child {month_child} attached")
        except Exception as e:
            Logger.log(f"Error creating/attaching monthly child: {e}", "WARNING")

        # Daily child
        day_start = next_date
        day_end = day_start + pd.Timedelta(days=1)
        day_child = f"{month_child}_{day_start.strftime('%d')}"
        try:
            with DBUtils.engine.begin() as conn:
                create_day_sql = f"""
                    CREATE TABLE IF NOT EXISTS {day_child} PARTITION OF {month_child}
                    FOR VALUES FROM ('{day_start}') TO ('{day_end}');
                """
                conn.execute(text(create_day_sql))
                Logger.log(f"Child daily partition {day_child} ensured")
        except Exception as e:
            Logger.log(f"Error creating daily child partition {day_child}: {e}", "WARNING")
