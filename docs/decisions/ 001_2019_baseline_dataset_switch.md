# ADR 001: 2019 Baseline Dataset Migration

## Status
Accepted

## Date
2026-03-12

## Context
Initial implementation used MTA legacy turnstile dataset (xfn5-qji9)
for 2019 baseline ridership. This dataset stores cumulative counter
readings requiring LAG() delta calculations and RECOVR AUD filtering.
Station identifiers use free-text names incompatible with modern
dataset's station_complex_id numeric keys.

## Discovery
During development of int_station_recovery, station name join
returned 100% NULL matches — confirming schema incompatibility
between legacy and modern datasets.

Investigation revealed MTA publishes a pre-aggregated hourly
ridership dataset for 2017-2019 (t69i-h2me) with identical
schema to the 2022-2024 modern dataset (wujg-7c2s).

## Decision
Migrate 2019 baseline ingestion to dataset t69i-h2me.

## Consequences
Positive:
- Direct join on station_complex_id eliminates name mapping complexity
- Consistent schema across all years simplifies dbt models
- Recovery rate calculation becomes a clean aggregation

Preserved:
- stg_mta_turnstile_2019.sql retained for documentation
- load_baseline_2019_v1_turnstile.py renamed and preserved
- LAG() delta logic, RECOVR AUD filtering documented as reusable patterns

## Alternatives Considered
- Fuzzy name matching (Levenshtein distance) — rejected, brittle
- Manual station mapping table — rejected, high maintenance
- Keep turnstile dataset with name bridge — rejected, unnecessary complexity