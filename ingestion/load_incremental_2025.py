# Dataset: MTA Subway Hourly Ridership: Beginning 2025 (5wq4-mkjj)
"""
MTA Analytics Pipeline — Incremental Ingestion (2025)
-------------------------------------------------------
Loads 2025 MTA ridership data week by week, simulating
a production incremental pipeline. Each run advances a
date cursor by 7 days and uploads the new chunk to GCS.

Run manually or schedule via cron / GitHub Actions.

Author: Jesus M. De Leon
Project: MTA Analytics Pipeline
"""

import os
import json
import requests
from datetime import datetime, timedelta
from google.cloud import storage

# ── Config ────────────────────────────────────────────────────────────────────
PROJECT_ID      = "jdl-mta-project"
BUCKET_NAME     = "jdl-mta-project-mta-raw"
GCS_FOLDER      = "mta_turnstile_incremental"
CURSOR_FILE     = "ingestion/.incremental_cursor.json"  # tracks last loaded date

BASE_URL        = "https://data.ny.gov/resource/5wq4-mkjj.csv"
ROWS_PER_PULL   = 100000
CHUNK_DAYS      = 7       # how many days to load per run — change to 1 for daily

# ── Cursor Management ─────────────────────────────────────────────────────────
def load_cursor() -> datetime:
    """Load the last successfully loaded date from cursor file."""
    if os.path.exists(CURSOR_FILE):
        with open(CURSOR_FILE, "r") as f:
            data = json.load(f)
            cursor = datetime.fromisoformat(data["last_loaded_date"])
            print(f"📍 Cursor found — resuming from {cursor.date()}")
            return cursor
    else:
        # First run — start from Jan 1 2025
        start = datetime(2025, 1, 1)
        print(f"📍 No cursor found — starting fresh from {start.date()}")
        return start

def save_cursor(date: datetime):
    """Save the current cursor position after a successful load."""
    with open(CURSOR_FILE, "w") as f:
        json.dump({
            "last_loaded_date"  : date.isoformat(),
            "last_run_at"       : datetime.now().isoformat()
        }, f, indent=2)
    print(f"💾 Cursor saved → {date.date()}")

# ── GCS Client ────────────────────────────────────────────────────────────────
def get_gcs_client():
    return storage.Client(project=PROJECT_ID)

def upload_to_gcs(client, blob_name: str, data: str):
    bucket = client.bucket(BUCKET_NAME)
    blob   = bucket.blob(blob_name)
    blob.upload_from_string(data.encode("utf-8"), content_type="text/csv")
    print(f"  ✓ Uploaded → gs://{BUCKET_NAME}/{blob_name}")

# ── Incremental Fetcher ───────────────────────────────────────────────────────
def fetch_window(start: datetime, end: datetime, client) -> int:
    """
    Fetches all MTA rows between start and end dates
    using pagination and uploads chunks to GCS.
    """
    print(f"\n📦 Fetching {start.date()} → {end.date()}...")

    offset      = 0
    chunk_num   = 0
    total_rows  = 0

    while True:
        params = {
            "$where"  : f"transit_timestamp >= '{start.isoformat()}' AND transit_timestamp < '{end.isoformat()}'",
            "$limit"  : ROWS_PER_PULL,
            "$offset" : offset,
            "$order"  : "transit_timestamp ASC"
        }

        response = requests.get(BASE_URL, params=params, timeout=60)

        if response.status_code != 200:
            print(f"  ✗ API error {response.status_code}")
            break

        lines       = response.text.strip().split("\n")
        row_count   = len(lines) - 1

        if row_count <= 0:
            print(f"  ✓ No more rows in window. Total: {total_rows:,}")
            break

        # Partition by year/month/week for efficient BigQuery scanning
        blob_name = (
            f"{GCS_FOLDER}/"
            f"year={start.year}/"
            f"month={start.month:02d}/"
            f"week_starting={start.date()}/"
            f"chunk_{chunk_num:04d}.csv"
        )

        upload_to_gcs(client, blob_name, response.text)

        total_rows  += row_count
        offset      += ROWS_PER_PULL
        chunk_num   += 1

        print(f"  → Chunk {chunk_num} | Rows so far: {total_rows:,}")

        if row_count < ROWS_PER_PULL:
            print(f"  ✓ Window complete. Rows loaded: {total_rows:,}")
            break

    return total_rows

# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    print("=" * 60)
    print("  MTA Analytics Pipeline — Incremental Load (2025)")
    print("=" * 60)

    # Load cursor — where did we last stop?
    start   = load_cursor()
    end     = start + timedelta(days=CHUNK_DAYS)

    # Safety guard — don't load future dates
    today = datetime.now()
    if start >= today:
        print(f"\n✅ Pipeline is fully caught up to {start.date()}. Nothing to load.")
        return

    # Cap end date at today if window overshoots
    if end > today:
        end = today
        print(f"⚠️  End date capped at today: {end.date()}")

    # Fetch and upload the window
    client      = get_gcs_client()
    total_rows  = fetch_window(start, end, client)

    if total_rows > 0:
        save_cursor(end)
        print(f"\n✅ Incremental load complete.")
        print(f"   Rows loaded : {total_rows:,}")
        print(f"   Window      : {start.date()} → {end.date()}")
        print(f"   Next run    : will load from {end.date()}")
    else:
        print(f"\n⚠️  No rows found for window {start.date()} → {end.date()}")
        print(f"   Cursor not advanced — will retry same window next run.")

if __name__ == "__main__":
    main()