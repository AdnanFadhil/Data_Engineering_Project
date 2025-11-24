import os
from datetime import datetime
import sys
import io

# Pastikan stdout menggunakan UTF-8
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# Lokasi file log
LOG_FILE = "resources/logs/analytics.log"
os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)

class Logger:
    """
    Logger untuk mencatat pesan ke console dan file log.

    Attributes:
        LOG_FILE (str): Path file log.
    """

    @staticmethod
    def log(msg, level="INFO"):
        """
        Mencatat pesan log dengan timestamp dan level.

        Parameters:
            msg (str): Pesan yang ingin dicatat.
            level (str): Level log, misal "INFO", "ERROR", "WARNING". Default "INFO".

        Behavior:
            - Membuat timestamp saat log dicatat.
            - Format pesan: [YYYY-MM-DD HH:MM:SS] [LEVEL] message
            - Mencetak pesan ke console, menangani UnicodeEncodeError.
            - Menulis pesan ke file LOG_FILE (analytics.log) dengan encoding UTF-8.

        Returns:
            None
        """
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        line = f"[{ts}] [{level}] {msg}"

        try:
            print(line)
        except UnicodeEncodeError:
            print(line.encode(sys.stdout.encoding, errors='replace').decode(sys.stdout.encoding))

        # Simpan ke file log
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(line + "\n")
