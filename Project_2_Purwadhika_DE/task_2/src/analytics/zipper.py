import os
import glob
import zipfile
from datetime import datetime
from .logger import Logger
from .config import Settings

class Zipper:
    @staticmethod
    def zip_aggregate_files():
        today_str = datetime.now().strftime("%Y%m%d")
        zip_filename = f"update_aggregate_{today_str}.zip"
        with zipfile.ZipFile(zip_filename, "w", zipfile.ZIP_DEFLATED) as zf:
            for color, dir_path in [("yellow", Settings.AGG_YELLOW_DIR), ("green", Settings.AGG_GREEN_DIR)]:
                for file in glob.glob(os.path.join(dir_path, "*.csv")):
                    zf.write(file, arcname=os.path.join(color, os.path.basename(file)))
        Logger.log(f"Aggregate CSV files zipped: {zip_filename}")
        return zip_filename
