import requests
from .config import Settings
from .logger import Logger

class DiscordNotifier:
    """
    Class untuk mengirim notifikasi ke Discord menggunakan webhook URL.
    """

    @staticmethod
    def send_message(message: str):
        """
        Mengirim pesan ke Discord melalui webhook yang sudah dikonfigurasi.

        Parameters:
            message (str): Pesan yang ingin dikirim ke channel Discord.

        Behavior:
            - Mengecek apakah Settings.DISCORD_HOOK sudah di-set.
              Jika belum, log warning dan skip.
            - Mengirim POST request ke Discord webhook dengan payload JSON {"content": message}.
            - Mengecek status code response:
                - 200 / 204: log sukses
                - lainnya: log error dengan status code dan response text
            - Menangani exception request dan mencatatnya di log.

        Returns:
            None
        """
        if not Settings.DISCORD_HOOK:
            Logger.log("DISCORD_HOOK belum di-set, skip sending message.", "WARNING")
            return

        payload = {"content": message}

        try:
            resp = requests.post(Settings.DISCORD_HOOK, json=payload)
            if resp.status_code in [200, 204]:
                Logger.log("✓ Discord message sent successfully")
            else:
                Logger.log(
                    f"ERROR sending Discord message: {resp.status_code} - {resp.text}", 
                    "ERROR"
                )
        except Exception as e:
            Logger.log(f"ERROR sending Discord message: {e}", "ERROR")
