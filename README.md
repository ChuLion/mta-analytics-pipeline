# MTA Analytics Pipeline

**End-to-end ELT pipeline transforming 88 million rows of NYC subway data into executive analytics insights — built to demonstrate production-grade data engineering practices.**

[![dbt Tests](https://github.com/ChuLion/mta-analytics-pipeline/actions/workflows/dbt_test.yml/badge.svg)](https://github.com/ChuLion/mta-analytics-pipeline/actions/workflows/dbt_test.yml)
![BigQuery](https://img.shields.io/badge/BigQuery-4285F4?logo=google-cloud&logoColor=white)
![dbt](https://img.shields.io/badge/dbt-FF694B?logo=dbt&logoColor=white)
![Terraform](https://img.shields.io/badge/Terraform-7B42BC?logo=terraform&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?logo=python&logoColor=white)
![Tableau](https://img.shields.io/badge/Tableau-E97627?logo=tableau&logoColor=white)

---

## What This Project Does

The MTA subway system serves 3.5 million riders daily across 472 stations. This pipeline answers five questions executives and transit planners need answered:

1. **Recovery** — Which stations and boroughs have recovered from COVID, and which are still lagging?
2. **Congestion** — When and where is the system most stressed?
3. **Efficiency** — Are high-demand stations getting the capacity they need?
4. **Utilization** — Where are service gaps hiding in the data?
5. **Equity** — Which communities are being failed by both low recovery AND unreliable service?

**[→ View Live Dashboard on Tableau Public](#)** *(link coming soon)*

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Infrastructure | Terraform + GCP |
| Raw Storage | Google Cloud Storage (partitioned CSV) |
| Data Warehouse | BigQuery (Bronze / Silver / Gold) |
| Transformation | dbt (BigQuery adapter) |
| Testing | dbt tests — 74/75 passing |
| CI/CD | GitHub Actions |
| Visualization | Tableau Desktop + Tableau Public |
| Language | Python 3.13, SQL |

---

## Architecture
See [Architecture Diagram](docs/project/architecture.md) for full data lineage.

```
┌─────────────────────────────────────────────────────────────┐
│                     DATA SOURCES                            │
│  MTA Hourly Ridership API    Census ACS 2023 API            │
│  (wujg-7c2s, t69i-h2me,      (2,327 NYC census tracts)     │
│   5wq4-mkjj)                                                │
└────────────────────────┬────────────────────────────────────┘
                         │ Python ingestion scripts
                         ▼
┌─────────────────────────────────────────────────────────────┐
│              GOOGLE CLOUD STORAGE (Raw)                     │
│  mta_ridership_historical/year=YYYY/month=MM/week=WW/       │
│  mta_turnstile_2019_hourly/year=2019/qN/                    │
│  88M+ rows partitioned CSV                                  │
└────────────────────────┬────────────────────────────────────┘
                         │ bq load
                         ▼
┌─────────────────────────────────────────────────────────────┐
│              BIGQUERY BRONZE (mta_bronze)                   │
│  mta_ridership_2022_2024  │  mta_ridership_2019             │
│  mta_ridership_2025       │  census_nyc_tracts              │
└────────────────────────┬────────────────────────────────────┘
                         │ dbt staging models (views)
                         ▼
┌─────────────────────────────────────────────────────────────┐
│              BIGQUERY SILVER (mta_silver)                   │
│                                                             │
│  STAGING (views)                                            │
│  stg_mta_ridership         stg_mta_ridership_2019           │
│  stg_mta_ridership_2025    stg_census_income                │
│                                                             │
│  INTERMEDIATE (views)                                       │
│  int_station_recovery      int_station_congestion           │
│  int_station_utilization   int_station_census_join          │
│  int_station_capacity                                       │
└────────────────────────┬────────────────────────────────────┘
                         │ dbt mart models (tables)
                         ▼
┌─────────────────────────────────────────────────────────────┐
│               BIGQUERY GOLD (mta_gold)                      │
│                                                             │
│  mart_recovery_scorecard    ~1.3K rows                      │
│  mart_congestion_trigger    ~180K rows                      │
│  mart_efficiency_matrix     ~180K rows                      │
│  mart_equity_view           ~1.2K rows                      │
└────────────────────────┬────────────────────────────────────┘
                         │ Tableau Direct Query
                         ▼
┌─────────────────────────────────────────────────────────────┐
│                  TABLEAU PUBLIC                             │
│  View 1: Recovery Scorecard    View 2: Congestion Heat Map  │
│  View 3: Efficiency Matrix     View 4: Service Justice      │
└─────────────────────────────────────────────────────────────┘
```

**Design pattern:** ELT — raw data lands first, all transforms happen in BigQuery.
**Medallion architecture:** Bronze (raw) → Silver (clean/intermediate) → Gold (mart).

---

## Key Findings

### COVID Recovery (2022-2024)
- System-wide recovery reached **~70% of 2019 baseline** by 2024
- **Manhattan leads** recovery at ~78% | **Bronx lags** at ~61%
- **17 stations remain Critical** (<50% of 2019) — structural not cyclical

### Service Reliability
- Disruption intensity **increasing** despite rising ridership:
  17.7 → 18.5 → 19.2 disruption hours per million riders (2022→2024)
- **Brooklyn accounts for ~50%** of all system disruptions
- Bronx disruptions spiked **145%** from 2022→2023

### Equity — The Service Justice Finding
The system's biggest failure is concentrated:

| Metric | High Risk Stations | Low Risk Stations |
|--------|-------------------|------------------|
| Avg Income (catchment) | $46,898 | $107,000 |
| Avg Recovery Rate | 55% | 73% |
| Disruption Rate | 15.5 per 1K hrs | 3.0 per 1K hrs |
| Station Count (2024) | 19 | 302 |

**Low-income communities face a triple burden:** lower recovery, less reliable service, AND fewer transportation alternatives. This is not a COVID recovery story — it's a structural investment gap.

---

## Project Structure

```
mta-analytics-pipeline/
├── ingestion/
│   ├── load_baseline_2019.py          # 2019 baseline (20.98M rows)
│   ├── load_baseline_2019_v1_turnstile.py  # Preserved v1 — see ADR 001
│   ├── load_to_gcs.py                 # 2022-2024 historical (77.2M rows)
│   ├── load_incremental_2025.py       # 2025 weekly incremental (531K+ rows)
│   └── census_api_pull.py             # Census ACS 2023 (2,327 tracts)
├── infra/
│   └── main.tf                        # GCS + BigQuery via Terraform
├── dbt_project/
│   ├── models/
│   │   ├── staging/                   # 4 models + schema.yml
│   │   ├── intermediate/              # 5 models + schema.yml
│   │   └── marts/                     # 4 models + schema.yml
│   ├── macros/
│   │   └── generate_schema_name.sql   # Custom schema routing
│   ├── tests/                         # 5 custom singular tests
│   └── dbt_project.yml
├── docs/
│   ├── decisions/                     # 5 Architecture Decision Records
│   ├── interview/                     # Technical Q&A for interviews
│   └── project/                       # Architecture + data dictionary
├── notebooks/
│   └── eda_analysis.ipynb
└── .github/
    └── workflows/
        └── dbt_test.yml               # CI: dbt test on every push
```

---

## Data Sources

| Dataset | Source | Rows | Description |
|---------|--------|------|-------------|
| MTA Hourly Ridership 2022-2024 | [data.ny.gov](https://data.ny.gov/resource/wujg-7c2s) | 77.2M | Primary ridership data |
| MTA Hourly Ridership 2019 | [data.ny.gov](https://data.ny.gov/resource/t69i-h2me) | 20.98M | COVID baseline |
| MTA Ridership 2025 | [data.ny.gov](https://data.ny.gov/resource/5wq4-mkjj) | 531K+ | Incremental (weekly) |
| Census ACS 2023 | [Census Bureau API](https://api.census.gov) | 2,327 | NYC tract demographics |

All data is publicly available. No API keys required for MTA data.

---

## Notable Engineering Decisions

**Five Architecture Decision Records** are documented in `docs/decisions/`:

| ADR | Decision | Why It Matters |
|-----|----------|---------------|
| ADR 001 | Switched 2019 source from turnstile to hourly dataset | Eliminated station name mismatch — enabled direct join on station_complex_id |
| ADR 002 | Flagged suspect station IDs, shifted to borough aggregation | MTA reorganized IDs 2019→2022; 23 stations produce misleading recovery metrics |
| ADR 003 | Period-matched p95 capacity proxy | All-hour p95 produced 5x stress index; period-matching gives interpretable 0-1.2 scale |
| ADR 004 | Hour spine via CROSS JOIN GENERATE_ARRAY | MTA data is sparse event data — missing hours absent not zeroed; spine enables anomaly detection |
| ADR 005 | 800m catchment area with ST_UNION_AGG | Point-in-polygon misrepresents multi-entrance stations; area-weighted demographics match MTA equity methodology |

---

## Running This Project

### Prerequisites
- GCP project with BigQuery and GCS enabled
- Python 3.10+
- dbt-bigquery
- Terraform

### Setup

```bash
# Clone the repo
git clone https://github.com/ChuLion/mta-analytics-pipeline.git
cd mta-analytics-pipeline

# Install Python dependencies
pip install -r requirements.txt

# Authenticate to GCP
gcloud auth application-default login

# Provision infrastructure
cd infra
terraform init
terraform apply

# Load data (one-time historical load)
cd ../ingestion
python load_to_gcs.py          # 2022-2024 (~77M rows, takes ~30 min)
python load_baseline_2019.py   # 2019 baseline (~21M rows)
python census_api_pull.py      # Census ACS 2023

# Load to BigQuery
# (see ingestion/README.md for bq load commands)

# Configure dbt
cp dbt_project/profiles_template.yml ~/.dbt/profiles.yml
# Edit profiles.yml with your GCP project ID

# Run dbt pipeline
cd dbt_project
dbt deps
dbt build   # runs all models + tests
```

### Weekly Incremental Update

```bash
python ingestion/load_incremental_2025.py
dbt run --select stg_mta_ridership_2025
```

### Run Tests

```bash
cd dbt_project
dbt test
# Expected: 74/75 PASS, 1 WARN (2 stations with no census coverage)
```

---

## dbt Lineage

```
stg_mta_ridership ──────────────────────┐
stg_mta_ridership_2019 ─────────────────┼──► int_station_recovery ──────────────────► mart_recovery_scorecard
                                        │                                              mart_efficiency_matrix
stg_mta_ridership ──────────────────────┼──► int_station_congestion ─────────────────► mart_congestion_trigger
                                        │                                              mart_efficiency_matrix
stg_mta_ridership ──────────────────────┼──► int_station_utilization ────────────────► mart_equity_view
                                        │
stg_mta_ridership ──────────────────────┼──► int_station_census_join ───────────────► mart_equity_view
stg_census_income ───────────────────── ┘
                                        
stg_mta_ridership_2019 ─────────────────────► int_station_capacity ─────────────────► mart_efficiency_matrix
```

---

## Author

**Jesus M. De Leon**
20 years of progressive technical experience — starting in RF/network engineering,
transitioning into data analytics, and spending the last 7 years as a data engineer
building production pipelines, capital portfolio analytics, and executive dashboards
at Verizon Wireless.

This project was built to demonstrate modern data engineering practices
(dbt, BigQuery, GCP, Python) and translate 7 years of production analytics
experience into a publicly visible portfolio.
Senior Analytics Engineer 
[LinkedIn](https://linkedin.com/in/jesus-m-de-leon-7a019b1b/) | jesusmdeleonmelendez@gmail.com

*Built as a portfolio project demonstrating dbt, BigQuery, GCP, Python, and Tableau.*
*All data is publicly available from MTA and US Census Bureau.*