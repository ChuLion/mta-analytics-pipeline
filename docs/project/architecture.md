# MTA Analytics Pipeline — Architecture Document

## Project Overview
End-to-end ELT pipeline transforming 88+ million rows of MTA transit
data into executive analytics insights. Built as a portfolio project
demonstrating production-grade data engineering practices.

## Author
Jesus M. De Leon | jesusm.deleon@hotmail.com

## Stack
| Layer | Technology |
|-------|-----------|
| Infrastructure | Terraform + GCP |
| Storage | Google Cloud Storage |
| Data Warehouse | BigQuery |
| Transformation | dbt (BigQuery adapter) |
| Orchestration | Cron (weekly incremental) |
| CI/CD | GitHub Actions |
| Visualization | Tableau Desktop + Tableau Public |
| Language | Python 3.13, SQL |

---

## Architecture Overview

```
Data Sources (External APIs)
        ↓
   Python Ingestion
        ↓
Google Cloud Storage (Raw CSV — Bronze)
        ↓
BigQuery Bronze Layer (mta_bronze)
        ↓
dbt Staging Layer (mta_silver) — views
        ↓
dbt Intermediate Layer (mta_silver) — views
        ↓
dbt Mart Layer (mta_gold) — tables
        ↓
Tableau Dashboard (Tableau Public)
```

Design pattern: ELT (not ETL) — raw data lands first, transforms in BigQuery.
Medallion architecture: Bronze → Silver → Gold.

---

## Data Sources

### MTA Subway Hourly Ridership 2022-2024
- Source: data.ny.gov (wujg-7c2s)
- Rows: 77,233,736
- Grain: station × hour × fare class
- GCS path: mta_ridership_historical/year=YYYY/month=MM/week=WW/

### MTA Subway Hourly Ridership 2019 (Baseline)
- Source: data.ny.gov (t69i-h2me)
- Rows: 20,980,589
- Note: Switched from legacy turnstile dataset — see ADR 001
- GCS path: mta_turnstile_2019_hourly/year=2019/qN/

### MTA Subway Hourly Ridership 2025 (Incremental)
- Source: data.ny.gov (5wq4-mkjj)
- Rows: 531,060 (January 2025, growing weekly)
- Note: MTA split to new endpoint at year boundary — discovered during build
- Incremental cursor: .incremental_cursor.json

### Census ACS 2023 (5-year estimates)
- Source: Census Bureau API
- Rows: 2,327 NYC census tracts
- Coverage: 5 boroughs (FIPS: 005, 047, 061, 081, 085)

---

## BigQuery Layer Structure

### Bronze (mta_bronze) — Raw ingested data
| Table | Rows | Description |
|-------|------|-------------|
| mta_ridership_2022_2024 | 77.2M | Historical hourly ridership |
| mta_ridership_2019 | 20.98M | 2019 baseline (hourly) |
| mta_ridership_2025_incremental | 531K | 2025 incremental |
| census_nyc_tracts | 2,327 | ACS demographics |

### Silver (mta_silver) — Staging views
| Model | Type | Description |
|-------|------|-------------|
| stg_mta_ridership | view | 2022-2024 hourly, cleaned |
| stg_mta_ridership_2019 | view | 2019 baseline, cleaned |
| stg_mta_ridership_2025 | incremental | 2025 incremental, cleaned |
| stg_census_income | view | Census demographics, sentinel handled |
| stg_mta_turnstile_2019 | view (disabled) | Legacy — preserved for docs |

### Silver (mta_silver) — Intermediate views
| Model | Type | Description |
|-------|------|-------------|
| int_station_recovery | view | Monthly recovery vs 2019 baseline |
| int_station_congestion | view | Hourly congestion patterns |
| int_station_utilization | view | Anomaly detection via hour spine |
| int_station_census_join | view | 800m catchment spatial join |
| int_station_capacity | view | 2019 p95 capacity by time period |

### Gold (mta_gold) — Mart tables (Tableau feeds)
| Model | Rows | Description |
|-------|------|-------------|
| mart_recovery_scorecard | ~1.3K | View 1: Recovery map |
| mart_congestion_trigger | ~180K | View 2: Congestion heat map |
| mart_efficiency_matrix | ~180K | View 3: Efficiency scatter |
| mart_demand_supply_gap | TBD | View 4: Demand vs utilization |
| mart_equity_view | TBD | View 5: Income + disruption |

---

## dbt Configuration

### Schema Routing
Custom generate_schema_name macro prevents default schema concatenation:
  - staging models → mta_silver
  - intermediate models → mta_silver
  - mart models → mta_gold

### Key dbt Variables
  baseline_year: 2019
  recovery_start_year: 2022
  max_turnstile_entries_per_interval: 5000

### Packages
  - dbt-labs/dbt_utils: 1.3.3
  - metaplane/dbt_expectations: 0.10.10

---

## Key Technical Decisions

### 1. ELT Architecture
Raw data lands in GCS first (bronze), transforms happen in BigQuery.
Rationale: Preserves raw data for reprocessing, leverages BigQuery
compute at scale, separates storage from transformation concerns.

### 2. Incremental Model for 2025 Data
stg_mta_ridership_2025 uses is_incremental() macro with MAX(transit_timestamp)
watermark. Weekly cron job appends only new rows.
Rationale: Avoids full reload of growing dataset weekly.

### 3. Hour Spine for Utilization
CROSS JOIN GENERATE_ARRAY(0,23) creates explicit zeros for missing hours.
MTA source data is sparse event data — see ADR 004.
Rationale: Required for meaningful consecutive zero detection.

### 4. 800m Catchment Area
ST_UNION_AGG of all entrance buffers with area-weighted demographics.
See ADR 005.
Rationale: Single point-in-polygon misrepresents multi-entrance stations.

### 5. Period-Matched Capacity Proxy
p95 calculated within time period buckets (AM/PM/Late Night).
See ADR 003.
Rationale: All-hour p95 produces misleading stress index values.

---

## CI/CD Pipeline

GitHub Actions workflow (.github/workflows/dbt_test.yml):
  Trigger: push to main, pull request to main
  Steps:
    1. Checkout code
    2. Set up Python 3.13
    3. Install dbt-bigquery
    4. Authenticate to GCP (service account via GitHub secret)
    5. Create dbt profiles.yml
    6. dbt deps
    7. dbt test

Service account: mta-pipeline-ci@jdl-mta-project.iam.gserviceaccount.com
Permissions: bigquery.dataViewer, bigquery.jobUser

---

## Known Limitations

1. Station ID Crosswalk (ADR 002)
   MTA reorganized station_complex_id 2019→2022.
   23 stations flagged as suspect. Full fix requires GTFS crosswalk.

2. Service Frequency Proxy
   No GTFS schedule data — utilization model uses ridership patterns
   as service proxy. Zero ridership ≠ guaranteed no service.

3. Throughput Capacity Proxy
   p95 of 2019 ridership used as capacity ceiling.
   Real capacity requires MTA engineering data.

4. 2025 Data Coverage
   Only January 2025 loaded at time of build.
   Weekly incremental will expand coverage.

---

## Repository Structure
```
mta-analytics-pipeline/
├── ingestion/
│   ├── load_baseline_2019.py          # Current 2019 loader
│   ├── load_baseline_2019_v1_turnstile.py  # Preserved v1
│   ├── load_to_gcs.py                 # 2022-2024 historical
│   ├── load_incremental_2025.py       # 2025 incremental
│   └── census_api_pull.py             # Census ACS loader
├── terraform/
│   └── main.tf                        # GCS + BigQuery infrastructure
├── dbt_project/
│   ├── models/
│   │   ├── staging/
│   │   ├── intermediate/
│   │   └── marts/
│   ├── macros/
│   │   └── generate_schema_name.sql
│   └── dbt_project.yml
├── docs/
│   ├── decisions/                     # ADR documents
│   └── interview/                     # Interview prep
├── notebooks/
│   └── eda_analysis.ipynb
└── .github/
    └── workflows/
        └── dbt_test.yml
```
