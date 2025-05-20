#!/usr/bin/env python3
"""
ge_logger.py – append the most-recent 5-minute GE snapshot to a JSONL file
              and keep only the last 7 daily logs.

Run every 5 minutes via cron/Task Scheduler.

Requires: requests 2.x
"""

import json, time, requests, datetime as dt
from pathlib import Path

# --- Config ---------------------------------------------------------------
BASE_URL   = "https://prices.runescape.wiki/api/v1/osrs/5m"
REPO_DIR = Path(__file__).resolve().parent
DATA_DIR = REPO_DIR / "data"
KEEP_DAYS  = 7
USER_AGENT = "ernesto-ge-log/1.0"
# --------------------------------------------------------------------------

DATA_DIR.mkdir(parents=True, exist_ok=True)

def fetch_snapshot() -> dict:
    resp = requests.get(BASE_URL, headers={"User-Agent": USER_AGENT}, timeout=10)
    resp.raise_for_status()
    return resp.json()

def fetch_latest() -> None:
    url = "https://prices.runescape.wiki/api/v1/osrs/latest"
    out_file = DATA_DIR / "latest.json"
    resp = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=10)
    resp.raise_for_status()
    out_file.write_text(json.dumps(resp.json(), separators=(",", ":")))
    print(f"[+] refreshed instant snapshot → {out_file.name}")

def fetch_1h_if_due() -> None:
    # Round current time down to the hour (e.g., 14:23 → 14:00 → 1716127200)
    ts_hour = int(time.time() // 3600 * 3600)
    out_file = DATA_DIR / f"1h-{ts_hour}.json"

    if out_file.exists():
        return

    url = "https://prices.runescape.wiki/api/v1/osrs/1h"
    resp = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=10)
    resp.raise_for_status()

    out_file.write_text(json.dumps(resp.json(), separators=(",", ":")))
    print(f"[+] saved hourly snapshot → {out_file.name}")

def fetch_mapping() -> None:
    out_file = DATA_DIR / "mapping.json"
    # refresh if file doesn’t exist OR is older than 24 h
    if (not out_file.exists()) or (time.time() - out_file.stat().st_mtime > 86_400):
        url = "https://prices.runescape.wiki/api/v1/osrs/mapping"
        resp = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=10)
        resp.raise_for_status()
        out_file.write_text(json.dumps(resp.json(), separators=(",", ":")))
        print(f"[+] refreshed item mapping → {out_file.name}")

def append_log(payload: dict) -> None:
    today_fname = DATA_DIR / f"5m-{dt.date.today()}.jsonl"
    with today_fname.open("a") as f:
        f.write(json.dumps(payload, separators=(",", ":")) + "\n")
    print(f"[+] appended snapshot to {today_fname}")

def purge_old_logs() -> None:
    cutoff = dt.date.today() - dt.timedelta(days=KEEP_DAYS - 1)
    for path in DATA_DIR.glob("*.jsonl"):
        try:
            file_date = dt.date.fromisoformat(path.stem)
        except ValueError:
            continue  # skip unexpected filenames
        if file_date < cutoff:
            path.unlink()
            print(f"[–] deleted {path} (older than {KEEP_DAYS} days)")

def main() -> None:
    data = fetch_snapshot()        # { "data": {item_id: {...}, ...}, "timestamp": ... }
    append_log(data)
    fetch_1h_if_due()
    fetch_mapping()
    fetch_latest()
    purge_old_logs()

if __name__ == "__main__":
    main()
