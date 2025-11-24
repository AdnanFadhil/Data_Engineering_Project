import os
import pandas as pd
from .config import Settings
from .logger import Logger
from .db_utils import DBUtils

class CSVExporter:
    @staticmethod
    def export_table(table_name, color):
        dir_path = Settings.AGG_YELLOW_DIR if color == "yellow" else Settings.AGG_GREEN_DIR
        os.makedirs(dir_path, exist_ok=True)
        file_path = os.path.join(dir_path, f"{table_name}.csv")
        df = pd.read_sql(f"SELECT * FROM {Settings.SCHEMA_AGGREGATE}.{table_name}", DBUtils.engine)
        if not df.empty:
            df.to_csv(file_path, index=False)
            Logger.log(f"Exported {table_name} to CSV: {file_path}")
        else:
            Logger.log(f"No data in {table_name} to export.", "WARNING")
