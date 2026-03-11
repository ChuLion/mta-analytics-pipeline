"""
MTA Analytics Pipeline — 2019 Baseline Ingestion
--------------------------------------------------
Downloads 2019 MTA turnstile data from data.ny.gov
dataset xfn5-qji9 and loads raw CSVs to GCS.

Schema notes:
  - Cumulative entry/exit counters per individual turnstile
  - Ridership calculated in dbt as LAG() delta per turnstile unit
  - Date in ISO format: 2019-01-01T00:00:00.000
  - Time separate field: HH:MM:SS
  - Station name (text) mapped to station_complex_id in dbt
    intermediate layer to align with modern dataset schema

Author: Jesus M. De Leon
Project: MTA Analytics Pipeline
"""

import requests
from datetime import datetime
from google.cloud import storage

# ── Config ────────────────────────────────────────────────────────────────────
PROJECT_ID      = "jdl-mta-project"
BUCKET_NAME     = "jdl-mta-project-mta-raw"
GCS_FOLDER      = "mta_turnstile_2019"

# Confirmed live endpoint — data.ny.gov dataset xfn5-qji9
BASE_URL        = "https://data.ny.gov/resource/xfn5-qji9.csv"
ROWS_PER_PULL   = 100000

# ── GCS Client ────────────────────────────────────────────────────────────────
def get_gcs_client():
    return storage.Client(project=PROJECT_ID)

def upload_to_gcs(client, blob_name: str, data: str):
    """Upload raw CSV string to GCS bucket."""
    bucket = client.bucket(BUCKET_NAME)
    blob   = bucket.blob(blob_name)
    blob.upload_from_string(data.encode("utf-8"), content_type="text/csv")
    print(f"  ✓ Uploaded → gs://{BUCKET_NAME}/{blob_name}")

# ── Fetcher ───────────────────────────────────────────────────────────────────
def fetch_2019_baseline(client) -> int:
    """
    Fetches all 2019 turnstile readings using pagination.
    Filters strictly to 2019 calendar year.
    Uploads in chunks of 100k rows to GCS.
    """
    print(f"\n📦 Fetching 2019 baseline turnstile data...")
    print(f"   Source  : {BASE_URL}")
    print(f"   Target  : gs://{BUCKET_NAME}/{GCS_FOLDER}/\n")

    offset      = 0
    chunk_num   = 0
    total_rows  = 0

    while True:
        params = {
            "$where"  : "date >= '2019-01-01T00:00:00' AND date < '2020-01-01T00:00:00'",
            "$limit"  : ROWS_PER_PULL,
            "$offset" : offset,
            "$order"  : "date ASC, unit ASC, scp ASC"
        }

        response = requests.get(BASE_URL, params=params, timeout=60)

        if response.status_code != 200:
            print(f"  ✗ API error {response.status_code}: {response.text[:200]}")
            break

        lines       = response.text.strip().split("\n")
        row_count   = len(lines) - 1

        if row_count <= 0:
            print(f"  ✓ No more rows. Total loaded: {total_rows:,}")
            break

        # Partition path by quarter for efficient BigQuery scanning
        # We'll determine quarter from chunk progression
        quarter     = (chunk_num // 13) + 1  # ~13 chunks per quarter estimate
        quarter     = min(quarter, 4)         # cap at Q4

        blob_name = (
            f"{GCS_FOLDER}/"
            f"year=2019/"
            f"q{quarter}/"
            f"chunk_{chunk_num:04d}.csv"
        )

        upload_to_gcs(client, blob_name, response.text)

        total_rows  += row_count
        offset      += ROWS_PER_PULL
        chunk_num   += 1

        print(f"  → Chunk {chunk_num:>3} | Rows so far: {total_rows:>12,}")

        if row_count < ROWS_PER_PULL:
            print(f"  ✓ Complete. Total 2019 rows: {total_rows:,}")
            break

    return total_rows

# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    print("=" * 60)
    print("  MTA Analytics Pipeline — 2019 Baseline Load")
    print(f"  Endpoint : xfn5-qji9 (Turnstile Usage Data 2019)")
    print(f"  Bucket   : gs://{BUCKET_NAME}")
    print(f"  Folder   : {GCS_FOLDER}/")
    print("=" * 60)

    client      = get_gcs_client()
    total_rows  = fetch_2019_baseline(client)

    print("\n" + "=" * 60)
    print(f"  ✅ Baseline Load Complete")
    print(f"  Total rows : {total_rows:,}")
    print(f"  Timestamp  : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"\n  ⚠️  Schema notes for dbt:")
    print(f"  - entries/exits are CUMULATIVE counters")
    print(f"  - Use LAG() window function to calculate ridership delta")
    print(f"  - Partition by c_a + unit + scp (individual turnstile)")
    print(f"  - station text field needs mapping to station_complex_id")
    print("=" * 60)

if __name__ == "__main__":
    main()