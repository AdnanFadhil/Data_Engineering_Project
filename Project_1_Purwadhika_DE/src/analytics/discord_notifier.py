import requests
from .config import Settings
from .logger import Logger

class DiscordNotifier:
    @staticmethod
    def send_message(message):
        if not Settings.DISCORD_HOOK:
            Logger.log("DISCORD_HOOK belum di-set, skip sending message.", "WARNING")
            return
        payload = {"content": message}
        try:
            resp = requests.post(Settings.DISCORD_HOOK, json=payload)
            if resp.status_code in [200, 204]:
                Logger.log("✓ Discord message sent successfully")
            else:
                Logger.log(f"ERROR sending Discord message: {resp.status_code} - {resp.text}", "ERROR")
        except Exception as e:
            Logger.log(f"ERROR sending Discord message: {e}", "ERROR")
