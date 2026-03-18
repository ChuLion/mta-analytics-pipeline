"""
MTA Analytics Pipeline — 2019 Baseline Ingestion (v1 — PRESERVED FOR DOCUMENTATION)
======================================================================================
⚠️  THIS VERSION IS DEPRECATED — See load_baseline_2019.py for current implementation
     See docs/decisions/001_2019_baseline_dataset_switch.md for migration rationale

Dataset : MTA Subway Turnstile Usage Data 2019
Source  : https://data.ny.gov/resource/xfn5-qji9.csv
Schema  : Legacy cumulative counter format (c_a, unit, scp, station, entries, exits)

Why this was replaced:
- Entries/exits are CUMULATIVE counters requiring LAG() delta calculation
- Station identifiers use free-text names incompatible with modern
  dataset's station_complex_id numeric keys
- 100% NULL join rate discovered during int_station_recovery development
- MTA publishes pre-aggregated hourly dataset (t69i-h2me) with identical
  schema to modern 2022-2024 data — migration eliminated complexity

Technical patterns developed here (preserved as reference):
- LAG() window function for cumulative delta calculation
- RECOVR AUD filtering for consistent 4-hour intervals
- Negative delta handling (counter resets, maintenance, rollover)
- Chunked pagination for large Socrata API datasets

Author  : Jesus M. De Leon
Created : 2026-03
Replaced: 2026-03 (ADR 001)
"""

import os
import io
import json
import math
import time
import logging
from datetime import datetime, timezone
from pathlib import Path

import requests
import pandas as pd
from google.cloud import storage

# ── Configuration ────────────────────────────────────────────────────────────

PROJECT_ID   = "jdl-mta-project"
BUCKET_NAME  = f"{PROJECT_ID}-mta-raw"
GCS_FOLDER   = "mta_turnstile_2019"          # legacy folder name
BASE_URL     = "https://data.ny.gov/resource/xfn5-qji9.csv"
CHUNK_SIZE   = 100_000
REQUEST_TIMEOUT = 60

# ── Logging ───────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── GCS helpers ───────────────────────────────────────────────────────────────

def get_gcs_client() -> storage.Client:
    return storage.Client(project=PROJECT_ID)


def upload_df_to_gcs(
    client: storage.Client,
    df: pd.DataFrame,
    blob_name: str,
) -> None:
    bucket = client.bucket(BUCKET_NAME)
    blob   = bucket.blob(blob_name)
    buf    = io.BytesIO()
    df.to_csv(buf, index=False)
    buf.seek(0)
    blob.upload_from_file(buf, content_type="text/csv")


# ── Quarter helper ─────────────────────────────────────────────────────────────

def get_quarter(date_str: str) -> str:
    """Return q1/q2/q3/q4 from a DATE string like '12/31/2019'."""
    try:
        month = int(date_str.split("/")[0])
        return f"q{math.ceil(month / 3)}"
    except Exception:
        return "q0"


# ── Main ingestion ─────────────────────────────────────────────────────────────

def load_turnstile_2019() -> None:
    """
    Paginate through the legacy MTA turnstile 2019 dataset (xfn5-qji9)
    and upload chunked CSVs to GCS.

    NOTE: This dataset stores CUMULATIVE entries/exits counters.
    Actual ridership must be derived via LAG() in dbt:

        ridership_delta = entries - LAG(entries)
        OVER (PARTITION BY c_a, unit, scp, station
              ORDER BY turnstile_timestamp)

    Filter to description = 'REGULAR' only to ensure consistent
    4-hour intervals before applying LAG().

    Negative deltas indicate counter resets or maintenance —
    set to NULL rather than filtering rows (preserves audit trail).
    """

    gcs_client  = get_gcs_client()
    offset      = 0
    chunk_index = 0
    total_rows  = 0

    log.info("Starting legacy turnstile 2019 ingestion (xfn5-qji9)")
    log.info("⚠️  Deprecated — see load_baseline_2019.py for current version")

    while True:
        params = {
            "$limit":  CHUNK_SIZE,
            "$offset": offset,
            "$order":  "date ASC, time ASC",
            "$where":  "date >= '2019-01-01T00:00:00' AND date < '2020-01-01T00:00:00'",
        }

        for attempt in range(1, 4):
            try:
                resp = requests.get(
                    BASE_URL,
                    params=params,
                    timeout=REQUEST_TIMEOUT,
                )
                resp.raise_for_status()
                break
            except requests.RequestException as exc:
                log.warning("Attempt %d failed: %s", attempt, exc)
                if attempt == 3:
                    raise
                time.sleep(5 * attempt)

        df = pd.read_csv(io.StringIO(resp.text))

        if df.empty:
            log.info("No more rows — pagination complete.")
            break

        # Derive quarter from date field for GCS partitioning
        quarter = get_quarter(str(df["date"].iloc[0])) if "date" in df.columns else "q0"

        blob_name = (
            f"{GCS_FOLDER}/year=2019/{quarter}/"
            f"chunk_{chunk_index:04d}.csv"
        )

        upload_df_to_gcs(gcs_client, df, blob_name)

        total_rows  += len(df)
        chunk_index += 1
        offset      += CHUNK_SIZE

        log.info(
            "→ Chunk %d | Rows so far: %10d | GCS: %s",
            chunk_index,
            total_rows,
            blob_name,
        )

        if len(df) < CHUNK_SIZE:
            log.info("Last chunk received — done.")
            break

    # ── Summary ───────────────────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("  ✅ Legacy Turnstile Load Complete (v1 — DEPRECATED)")
    print(f"  Total rows : {total_rows:,}")
    print(f"  Timestamp  : {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    print("  ⚠️  Schema notes for dbt (preserved for documentation):")
    print("  - entries/exits are CUMULATIVE counters")
    print("  - Use LAG() window function to calculate ridership delta")
    print("  - Partition by c_a + unit + scp (individual turnstile)")
    print("  - Filter description = 'REGULAR' for consistent intervals")
    print("  - Negative deltas → NULL (counter reset / maintenance)")
    print("  - station text field incompatible with station_complex_id")
    print("  - See stg_mta_turnstile_2019.sql for full delta logic")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    load_turnstile_2019()