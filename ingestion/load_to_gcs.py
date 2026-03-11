# Dataset: MTA Subway Hourly Ridership 2020-2024 (wujg-7c2s)
# NOTE: 2025+ data moved to new dataset 5wq4-mkjj
# See load_incremental_2025.py for 2025 ingestion
"""
MTA Analytics Pipeline — Ingestion Layer
-----------------------------------------
Downloads MTA Turnstile CSV files from data.ny.gov
and loads them to GCS raw landing bucket.

Author: Jesus M. De Leon
Project: MTA Analytics Pipeline
"""

import os
import requests
import pandas as pd
from google.cloud import storage
from tqdm import tqdm
from datetime import datetime

# ── Config ────────────────────────────────────────────────────────────────────
PROJECT_ID      = "jdl-mta-project"
BUCKET_NAME     = "jdl-mta-project-mta-raw"
GCS_FOLDER      = "mta_turnstile"

# MTA Turnstile data via NYC Open Data (Socrata API)
# Dataset: MTA Subway Hourly Ridership (2020-present)
BASE_URL        = "https://data.ny.gov/resource/wujg-7c2s.csv"

# We want 2019 baseline + 2022-2024 recovery data
YEARS           = [2019, 2022, 2023, 2024]
ROWS_PER_PULL   = 100000  # Socrata API limit per request

# ── GCS Client ────────────────────────────────────────────────────────────────
def get_gcs_client():
    return storage.Client(project=PROJECT_ID)

def upload_to_gcs(client, bucket_name, blob_name, data: bytes):
    """Upload raw bytes to GCS bucket."""
    bucket = client.bucket(bucket_name)
    blob   = bucket.blob(blob_name)
    blob.upload_from_string(data, content_type="text/csv")
    print(f"  ✓ Uploaded → gs://{bucket_name}/{blob_name}")

# ── MTA Data Fetcher ───────────────────────────────────────────────────────────
def fetch_mta_year(year: int, client):
    """
    Fetches all MTA ridership rows for a given year
    using Socrata API pagination and uploads to GCS.
    """
    print(f"\n📦 Fetching MTA data for {year}...")

    offset      = 0
    chunk_num   = 0
    total_rows  = 0

    while True:
        params = {
            "$where"  : f"transit_timestamp >= '{year}-01-01T00:00:00' AND transit_timestamp < '{year+1}-01-01T00:00:00'",
            "$limit"  : ROWS_PER_PULL,
            "$offset" : offset,
            "$order"  : "transit_timestamp ASC"
        }

        response = requests.get(BASE_URL, params=params, timeout=60)

        if response.status_code != 200:
            print(f"  ✗ API error {response.status_code} for year {year}")
            break

        # Check if we got any rows back
        lines = response.text.strip().split("\n")
        row_count = len(lines) - 1  # subtract header

        if row_count <= 0:
            print(f"  ✓ No more rows for {year}. Total pulled: {total_rows:,}")
            break

        # Upload this chunk to GCS
        blob_name = f"{GCS_FOLDER}/year={year}/chunk_{chunk_num:04d}.csv"

        # Only include header on first chunk
        if chunk_num > 0:
            lines = [lines[0]] + lines[1:]  # keep header for simplicity

        upload_to_gcs(client, BUCKET_NAME, blob_name, response.text.encode("utf-8"))

        total_rows  += row_count
        offset      += ROWS_PER_PULL
        chunk_num   += 1

        print(f"  → Chunk {chunk_num} | Rows so far: {total_rows:,}")

        # Safety check — if fewer rows than limit, we're done
        if row_count < ROWS_PER_PULL:
            print(f"  ✓ Complete. Total rows for {year}: {total_rows:,}")
            break

    return total_rows

# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    print("=" * 60)
    print("  MTA Analytics Pipeline — Data Ingestion")
    print(f"  Target Bucket: gs://{BUCKET_NAME}/{GCS_FOLDER}/")
    print(f"  Years: {YEARS}")
    print("=" * 60)

    client      = get_gcs_client()
    grand_total = 0

    for year in YEARS:
        rows = fetch_mta_year(year, client)
        grand_total += rows

    print("\n" + "=" * 60)
    print(f"  ✅ Ingestion Complete")
    print(f"  Total rows loaded: {grand_total:,}")
    print(f"  Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

if __name__ == "__main__":
    main()