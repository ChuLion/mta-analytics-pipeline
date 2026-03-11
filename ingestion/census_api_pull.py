"""
MTA Analytics Pipeline — Census Data Ingestion
------------------------------------------------
Pulls US Census Bureau ACS 5-Year estimates for
NYC zip codes / Neighborhood Tabulation Areas (NTAs).

Data collected:
  - Median household income (B19013_001E)
  - Total population (B01003_001E)
  - Car ownership / no vehicle households (B08201_002E)
  - Poverty rate (B17001_002E)
  - Population with disability (B18101_001E)

Used in dbt to power:
  - View 5: The Equity View (income vs service frequency)
  - View 6: ADA Accessibility Layer (disability + elevator data)

Author: Jesus M. De Leon
Project: MTA Analytics Pipeline
"""

import os
import json
import requests
import pandas as pd
from datetime import datetime
from google.cloud import storage
from dotenv import load_dotenv

# ── Load Environment Variables ────────────────────────────────────────────────
load_dotenv()

CENSUS_API_KEY  = os.getenv("CENSUS_API_KEY")
PROJECT_ID      = os.getenv("GCP_PROJECT_ID")
BUCKET_NAME     = os.getenv("GCS_BUCKET_NAME")
GCS_FOLDER      = "census_data"

# ── Census API Config ─────────────────────────────────────────────────────────
# ACS 5-Year Estimates — most recent available (2023)
CENSUS_BASE_URL = "https://api.census.gov/data/2023/acs/acs5"

# Variables we want — maps Census code to human-readable name
CENSUS_VARIABLES = {
    "B19013_001E" : "median_household_income",
    "B01003_001E" : "total_population",
    "B08201_002E" : "households_no_vehicle",
    "B08201_001E" : "total_households",        # denominator for car ownership rate
    "B17001_002E" : "population_in_poverty",
    "B18101_001E" : "population_with_disability",
    "NAME"        : "neighborhood_name"
}

# NYC County FIPS codes
# 005=Bronx, 047=Brooklyn, 061=Manhattan, 081=Queens, 085=Staten Island
NYC_COUNTIES = ["005", "047", "061", "081", "085"]

# ── GCS Client ────────────────────────────────────────────────────────────────
def get_gcs_client():
    return storage.Client(project=PROJECT_ID)

def upload_to_gcs(client, blob_name: str, data: str):
    """Upload CSV string to GCS bucket."""
    bucket = client.bucket(BUCKET_NAME)
    blob   = bucket.blob(blob_name)
    blob.upload_from_string(data.encode("utf-8"), content_type="text/csv")
    print(f"  ✓ Uploaded → gs://{BUCKET_NAME}/{blob_name}")

# ── Census Fetcher ────────────────────────────────────────────────────────────
def fetch_census_by_tract(county_fips: str) -> pd.DataFrame:
    """
    Fetches ACS variables at Census Tract level for a given NYC county.
    Census Tract is the most granular level that maps well to neighborhoods.
    """
    variables   = ",".join(CENSUS_VARIABLES.keys())
    state_fips  = "36"  # New York State

    params = {
        "get"   : variables,
        "for"   : "tract:*",
        "in"    : f"state:{state_fips} county:{county_fips}",
        "key"   : CENSUS_API_KEY
    }

    response = requests.get(CENSUS_BASE_URL, params=params, timeout=30)

    if response.status_code != 200:
        print(f"  ✗ Census API error {response.status_code} for county {county_fips}")
        print(f"    {response.text[:200]}")
        return pd.DataFrame()

    data    = response.json()
    headers = data[0]
    rows    = data[1:]

    df = pd.DataFrame(rows, columns=headers)
    return df

def clean_census_df(df: pd.DataFrame, county_fips: str) -> pd.DataFrame:
    """
    Renames columns to human-readable names,
    casts numeric fields, and adds derived metrics.
    """
    # Rename Census variable codes to readable names
    df = df.rename(columns=CENSUS_VARIABLES)

    # Cast numeric columns — Census returns everything as strings
    numeric_cols = [
        "median_household_income",
        "total_population",
        "households_no_vehicle",
        "total_households",
        "population_in_poverty",
        "population_with_disability"
    ]

    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Derived metrics — these become features in your equity analysis
    df["car_ownership_rate"] = (
        1 - (df["households_no_vehicle"] / df["total_households"])
    ).round(4)

    df["poverty_rate"] = (
        df["population_in_poverty"] / df["total_population"]
    ).round(4)

    df["disability_rate"] = (
        df["population_with_disability"] / df["total_population"]
    ).round(4)

    # Add geo identifiers
    df["county_fips"]   = county_fips
    df["state_fips"]    = "36"
    df["geoid"]         = "36" + county_fips + df["tract"]

    # Add borough name for readability
    borough_map = {
        "005" : "Bronx",
        "047" : "Brooklyn",
        "061" : "Manhattan",
        "081" : "Queens",
        "085" : "Staten Island"
    }
    df["borough"] = borough_map.get(county_fips, "Unknown")

    # Add load timestamp
    df["loaded_at"] = datetime.now().isoformat()

    return df

# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    print("=" * 60)
    print("  MTA Analytics Pipeline — Census Data Pull")
    print(f"  Source  : ACS 5-Year Estimates (2023)")
    print(f"  Target  : gs://{BUCKET_NAME}/{GCS_FOLDER}/")
    print(f"  Scope   : NYC 5 boroughs, Census Tract level")
    print("=" * 60)

    if not CENSUS_API_KEY:
        print("✗ ERROR: CENSUS_API_KEY not found in .env file")
        print("  Get your free key at: api.census.gov/data/key_signup.html")
        return

    client      = get_gcs_client()
    all_frames  = []

    for county_fips in NYC_COUNTIES:
        borough = {
            "005": "Bronx",
            "047": "Brooklyn",
            "061": "Manhattan",
            "081": "Queens",
            "085": "Staten Island"
        }[county_fips]

        print(f"\n📦 Fetching {borough} (county {county_fips})...")

        df = fetch_census_by_tract(county_fips)

        if df.empty:
            print(f"  ⚠️  No data returned for {borough}")
            continue

        df_clean = clean_census_df(df, county_fips)
        all_frames.append(df_clean)
        print(f"  ✓ {len(df_clean):,} census tracts fetched for {borough}")

    if not all_frames:
        print("\n✗ No data collected. Check API key and connection.")
        return

    # Combine all boroughs into single DataFrame
    df_combined = pd.concat(all_frames, ignore_index=True)

    print(f"\n📊 Combined dataset: {len(df_combined):,} census tracts across all boroughs")

    # Upload combined CSV to GCS
    blob_name   = f"{GCS_FOLDER}/acs_2023_nyc_census_tracts.csv"
    csv_data    = df_combined.to_csv(index=False)

    upload_to_gcs(client, blob_name, csv_data)

    # Also save locally for quick inspection
    local_path  = "ingestion/census_sample.csv"
    df_combined.head(20).to_csv(local_path, index=False)
    print(f"\n💾 Sample saved locally → {local_path}")
    print(f"   (First 20 rows for inspection — full data in GCS)")

    print("\n" + "=" * 60)
    print(f"  ✅ Census Pull Complete")
    print(f"  Total tracts : {len(df_combined):,}")
    print(f"  Boroughs     : {df_combined['borough'].nunique()}")
    print(f"  Timestamp    : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

if __name__ == "__main__":
    main()