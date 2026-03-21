# Known Data Limitations

## 1. Station ID Reorganization (ADR 002)
MTA reorganized station_complex_id between 2019 and 2022.
Affects recovery calculations for merged/split stations.
Flagged via data_quality_flag column.
Mitigation: Borough-level aggregation masks individual station noise.

## 2. Clark St (2,3) — January 2022 Gap
Station 334 missing January 2022 data.
Likely caused by partial line closure during renovation.
Impact: Minimal — one station, one month.
Mitigation: NULL handled gracefully in aggregations.

## 3. Astoria Blvd / Canarsie Station ID Mismatch
Station 2: 343% recovery (suspect merge)
Station 138: 16% recovery (suspect split)
Mitigation: data_quality_flag = 'suspect_merged'/'suspect_split'