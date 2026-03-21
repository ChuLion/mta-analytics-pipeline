# ADR 001: 2019 Baseline Dataset Migration

## Status
Accepted

## Date
2026-03-12

## Context
Initial implementation used the MTA legacy turnstile dataset (xfn5-qji9)
for 2019 baseline ridership. This dataset stores cumulative counter
readings requiring LAG() delta calculations and RECOVR AUD filtering.
Station identifiers use free-text names incompatible with the modern
dataset's station_complex_id numeric keys.

The original ingestion script (load_baseline_2019_v1_turnstile.py)
successfully loaded 10,380,903 rows to GCS and BigQuery bronze layer.

## Discovery
During development of int_station_recovery, a station name join attempt
returned 100% NULL matches — confirming schema incompatibility between
legacy and modern datasets. Sample comparison:

Modern dataset: "103 St (6)", "125 St (2,3)", "174-175 Sts (B,D)"
Legacy dataset: completely different naming convention

Investigation revealed MTA publishes a pre-aggregated hourly ridership
dataset for 2017-2019 (t69i-h2me) with identical schema to the
2022-2024 modern dataset (wujg-7c2s).

## Decision
Migrate 2019 baseline ingestion to dataset t69i-h2me.

## Implementation
- Updated load_baseline_2019.py to use new endpoint
- Preserved original as load_baseline_2019_v1_turnstile.py
- New dataset loaded: 20,980,589 rows (vs 10.4M in legacy)
- New staging model: stg_mta_ridership_2019.sql
- Legacy model: stg_mta_turnstile_2019.sql (disabled, preserved)

## Validation
2019 baseline sanity check:
- Year: 2019 only ✅
- Stations: 428 ✅
- Total ridership: 1,701.46M ✅ (matches MTA published annual figure)

## Consequences

### Positive
- Direct join on station_complex_id eliminates name mapping complexity
- Consistent schema across all years simplifies dbt models
- Recovery rate calculation becomes a clean aggregation
- Eliminates LAG() delta complexity for primary analysis path

### Preserved
- stg_mta_turnstile_2019.sql retained with deprecation notice
- load_baseline_2019_v1_turnstile.py renamed and preserved
- LAG() delta logic, RECOVR AUD filtering documented as reusable patterns
- All technical decisions documented for interview reference

## Alternatives Considered
- Fuzzy name matching (Levenshtein distance) — rejected, brittle
- Manual station mapping table — rejected, high maintenance overhead
- Keep turnstile dataset with name bridge — rejected, unnecessary complexity

## Interview Talking Point
"During development of the recovery calculation I discovered the legacy
2019 dataset used free-text station names completely incompatible with
the modern dataset's numeric station_complex_id keys. Rather than
building a fragile fuzzy matching layer, I identified that MTA publishes
a pre-aggregated hourly dataset for 2017-2019 with identical schema to
the modern data. I migrated to that source, documented the original
approach, and preserved the turnstile delta logic as a reusable pattern.
The original work wasn't wasted — it produced LAG() window function
patterns and data quality findings that inform the anomaly detection
layer built later in the project."
