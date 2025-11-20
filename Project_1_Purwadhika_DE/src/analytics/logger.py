import os
from datetime import datetime
import sys
import io

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

LOG_FILE = "resources/logs/analytics.log"
os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)

class Logger:
    @staticmethod
    def log(msg, level="INFO"):
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        line = f"[{ts}] [{level}] {msg}"
        try:
            print(line)
        except UnicodeEncodeError:
            print(line.encode(sys.stdout.encoding, errors='replace').decode(sys.stdout.encoding))
        
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(line + "\n")
