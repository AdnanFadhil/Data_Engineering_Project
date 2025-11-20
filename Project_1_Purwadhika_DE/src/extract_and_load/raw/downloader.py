import os, requests
from bs4 import BeautifulSoup
from .logger import log
from . import config
from .db_utils import mark_parquet_done, get_last_processed_month

def download_parquet_files():
    last_month = get_last_processed_month()
    if last_month:
        year, month = map(int, last_month.split("-"))
        if month == 12:
            year += 1
            month = 1
        else:
            month += 1
        month_to_process = f"{year:04d}-{month:02d}"
    else:
        month_to_process = "2025-01"

    log(f"Processing month: {month_to_process}")

    log("Fetching TLC data page...")
    try:
        response = requests.get(config.TLC_URL, timeout=30)
        response.raise_for_status()
    except Exception as e:
        log(f"Failed to fetch TLC page — {e}", level="ERROR")
        return None

    soup = BeautifulSoup(response.text, "html.parser")
    all_links = [a["href"] for a in soup.find_all("a", href=True) if ".parquet" in a["href"]]

    yellow_link = next((l for l in all_links if f"yellow_tripdata_{month_to_process}" in l), None)
    green_link = next((l for l in all_links if f"green_tripdata_{month_to_process}" in l), None)

    if not yellow_link and not green_link:
        log(f"No parquet links found for {month_to_process}", "ERROR")
        return None

    month_dir = os.path.join(config.DOWNLOAD_DIR, month_to_process)
    os.makedirs(month_dir, exist_ok=True)

    downloaded_files = []

    for link, color in [(yellow_link, "yellow"), (green_link, "green")]:
        if not link:
            continue
        filename = os.path.join(month_dir, os.path.basename(link))
        if os.path.exists(filename):
            log(f"Skipping download (exists): {filename}")
            downloaded_files.append(filename)
            continue

        log(f"Downloading {link} -> {filename}")
        try:
            with requests.get(link, stream=True, timeout=60) as r:
                r.raise_for_status()
                with open(filename, "wb") as f:
                    for chunk in r.iter_content(8192):
                        f.write(chunk)
            log(f"Saved {filename}")
            downloaded_files.append(filename)
        except Exception as e:
            log(f"Failed downloading {link} — {e}", "ERROR")

    return month_to_process, downloaded_files
