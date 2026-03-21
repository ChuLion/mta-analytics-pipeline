# ADR 002: Station ID Crosswalk — Known Limitation

## Status
Deferred

## Date
2026-03-12

## Context
During validation of int_station_recovery, two anomalous stations
were identified:

| Station | ID | 2019 Baseline | 2024 Ridership | Recovery |
|---------|-----|--------------|----------------|----------|
| Astoria Blvd (N,W) | 2 | 723,061 | 2,483,538 | 343% |
| Canarsie-Rockaway Pkwy (L) | 138 | 3,352,801 | 554,223 | 16% |

Initial assumption was data error, but further analysis confirmed
these represent real data — MTA reorganized station_complex_id
assignments between the 2019 dataset (t69i-h2me) and the 2022+
dataset (wujg-7c2s).

## Root Cause
MTA consolidated and split station complexes during the period
2019-2022, reassigning station_complex_id values:

- Merged stations: Multiple 2019 IDs consolidated into one 2022 ID
  → 2019 baseline too low → inflated recovery percentage
- Split stations: One 2019 ID split into multiple 2022 IDs
  → 2019 baseline too high → deflated recovery percentage

The silent corruption risk: stations that merged AND split in
offsetting ways will appear clean at ~100% recovery while actually
containing incorrect data.

## Current Mitigation
data_quality_flag column in int_station_recovery and
mart_recovery_scorecard identifies suspect stations:

- suspect_merged: recovery_pct > 200 (baseline too low)
- suspect_split: recovery_pct < 25 (baseline too high)
- no_baseline: station has no 2019 data
- clean: passes boundary checks

Executive dashboards filter to data_quality_flag = 'clean'.
Borough-level aggregation masks individual station noise and
provides more reliable recovery metrics.

## Station Counts Affected (2024)
- Total stations: 428
- Clean stations: 405
- Suspect/flagged: 23

## Full Resolution Path
Load MTA GTFS stops.txt to build a station crosswalk table mapping
legacy IDs to modern complex IDs.

Steps required:
1. Download current MTA GTFS feed
2. Parse stops.txt for station complex mappings
3. Load crosswalk to BigQuery bronze layer
4. Create stg_station_crosswalk.sql staging model
5. Update int_station_recovery join logic
6. Rebuild all dependent mart models

Estimated effort: 4-6 hours
Status: Deferred pending job search timeline

## Why Borough-Level Is Preferred for Executive Reporting
Borough aggregation provides more reliable recovery metrics because:
1. Individual station ID issues cancel out at aggregate level
2. Borough is the meaningful unit for policy decisions
3. The data quality story (23 suspect stations) is itself valuable signal

## Key Finding Preserved
Despite the station ID limitation, the borough-level analysis reveals:
- Manhattan: highest recovery (~78%)
- Bronx: lowest recovery (~61%)
- 17 stations remain Critical (<50%) in 2024
- These Critical stations are concentrated in low-income areas

## Interview Talking Point
"During validation I discovered MTA reorganized station complex IDs
between the 2019 and 2022 datasets. Stations were merged and split,
creating artificial inflation and deflation in recovery metrics.
I implemented a two-sided data quality flag to identify suspect
stations, shifted executive reporting to borough-level aggregation
which is more reliable, and documented the full resolution path
using GTFS crosswalk data. The 23 flagged stations are themselves
an interesting finding — they represent stations where the physical
infrastructure changed significantly between pre and post-COVID periods."
