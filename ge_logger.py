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
DATA_DIR   = Path("/home/ernesto/ge_logs")   # change to wherever you like
KEEP_DAYS  = 7                               # retain this many daily files
USER_AGENT = "ernesto-ge-log/1.0"
# --------------------------------------------------------------------------

DATA_DIR.mkdir(parents=True, exist_ok=True)

def fetch_snapshot() -> dict:
    resp = requests.get(BASE_URL, headers={"User-Agent": USER_AGENT}, timeout=10)
    resp.raise_for_status()
    return resp.json()

def append_log(payload: dict) -> None:
    today_fname = DATA_DIR / f"{dt.date.today()}.jsonl"
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
    purge_old_logs()

if __name__ == "__main__":
    main()
