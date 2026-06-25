# ADR Summary

## ADR 001: 2019 Baseline Dataset Migration

- **Status:** Accepted — 2026-03-12
- **Context:** Original 2019 baseline used legacy turnstile dataset (xfn5-qji9) with cumulative counters and free-text station names incompatible with the modern dataset's `station_complex_id` numeric keys.
- **Discovery:** Station name join returned 100% NULL matches. MTA publishes a pre-aggregated hourly dataset (t69i-h2me) for 2017-2019 with identical schema to the modern dataset (wujg-7c2s).
- **Decision:** Migrated 2019 baseline ingestion to dataset t69i-h2me. Loaded 20.98M rows vs 10.4M in legacy. New staging model: `stg_mta_ridership_2019.sql`.
- **Positive consequences:** Direct joins on `station_complex_id` eliminate name mapping; consistent schema across all years; cleaner recovery rate calculation.
- **Preserved:** Legacy turnstile ingestion and delta logic retained as reusable patterns.

---

## ADR 002: Station ID Crosswalk — Known Limitation

- **Status:** Deferred — 2026-03-12
- **Context:** MTA reorganized `station_complex_id` assignments between the 2019 (t69i-h2me) and 2022+ (wujg-7c2s) datasets — stations were merged and split.
- **Impact:** 23 of 428 stations flagged as suspect (merged → inflated recovery; split → deflated recovery). Silent corruption risk: stations that both merged and split in offsetting ways appear clean.
- **Mitigation:** Two-sided data quality flag in `int_station_recovery`: `suspect_merged` (>200%), `suspect_split` (<25%), `no_baseline`, `clean`. Decoupled reporting grains — station map filtered to `clean`; borough KPIs use all-inclusive aggregation (merged/split errors cancel out at aggregate level).
- **Full resolution:** Load MTA GTFS stops.txt to build a station crosswalk table mapping legacy IDs to modern complex IDs (4-6 hours effort). Deferred.
- **Key finding:** Manhattan highest recovery (~78%), Bronx lowest (~61%), 17 stations remain Critical (<50%) concentrated in low-income areas.

---

## ADR 003: Throughput Stress Index — Period-Matched p95 Capacity Proxy

- **Status:** Accepted — 2026-03-20
- **Context:** Initial throughput stress index = `avg_hourly_ridership / p95_capacity_proxy` where p95 was computed across ALL hours of 2019 — including overnight hours with near-zero ridership, deflating the baseline.
- **Problem:** Stations appeared at 248% of capacity — impossible, destroys trust. Grand Central: index of 10.0.
- **Decision:** Calculate p95_capacity_proxy matched to the same time period (AM / PM / Late Night) as the comparison metric.
- **Implementation:** `int_station_capacity.sql` buckets hours into AM / PM / Late Night, calculates p95 within each bucket per station. `mart_efficiency_matrix.sql` maps hour classification to matching time_bucket for join.
- **Expected index range:** 0.50 = 50% of 2019 peak period; 1.00 = fully recovered; 1.10 = 10% busier than pre-COVID.
- **Why p95:** Represents top 5% of normal operating conditions, not extreme events.

---

## ADR 004: Hour Spine for Utilization Anomaly Detection

- **Status:** Accepted — 2026-03-19
- **Context:** `int_station_utilization` designed three-layer anomaly detection for service disruptions: consecutive zero detection, historical baseline comparison, time-of-day context.
- **Problem:** All metrics returned 0 — completely non-functional.
- **Root cause:** MTA data is sparse event data — only publishes rows when ridership > 0. No row exists for a station with zero riders at 3am. Window functions after SUM aggregation eliminated zero-fare-class rows before detection.
- **Solution:** Generate a complete 24-hour spine via `CROSS JOIN` with `GENERATE_ARRAY(0, 23)`, then `LEFT JOIN` actual ridership. Missing hours become explicit NULLs → COALESCE to 0.
- **Results after fix:** 18,111 disruption hours (2022), 21,562 (2023), 23,320 (2024). Disruption intensity increasing year over year. Brooklyn ~50% of all system disruptions. Bronx disruptions spiked 145% from 2022 to 2023.
- **Limitation:** Measures service utilization (proxy via historical pattern), not true service frequency (requires GTFS schedule data).

---

## ADR 005: Station Catchment Area Methodology — 800m Buffer

- **Status:** Accepted — 2026-03-13
- **Context:** Equity analysis required associating Census ACS demographics with MTA stations. Point-in-polygon join (single tract per station) fails for stations on boundaries or with multiple entrances.
- **Decision:** 800-meter catchment area (≈10-minute walk, NYC pedestrian standard). Any intersecting census tract included, weighted by proportion of buffer area covered. Methodology matches MTA's own equity reports.
- **Multiple entrance handling:** Stations have multiple coordinate pairs. `ST_UNION_AGG` of all entrance buffers creates merged polygon representing true service area. AVG(lat/long) retained only for map pin placement.
- **Implementation:** Uses BigQuery native geography functions (`ST_BUFFER`, `ST_UNION_AGG`, `ST_INTERSECTS`, `ST_INTERSECTION`, `ST_AREA`). Performance optimization: `ST_INTERSECTS` filter applied before intersection calculation.
- **Results:** Average 7-15 tracts per station catchment. Income range: $27K (Bronx) to $220K (FiDi). All 5 boroughs represented with expected income distribution.
- **Naming:** Output is `weighted_mean_income` (not median) — true weighted median would require spatial percentile calculation, not warranted.

---

## ADR 006: Data Contracts — Future Enhancement Roadmap

- **Status:** Proposed — 2026-03-21
- **Context:** Current pipeline uses dbt schema tests as reactive validation (Layer 2). Production environments with AI/LLM layers require stronger guarantees.
- **Three-layer model:**
  - **Layer 1 (Pandera):** Schema validation in Python ingestion — catches upstream API drift before data touches pipeline. Proactive.
  - **Layer 2 (dbt tests — current):** Transformation logic errors after model runs. Runtime validation. 74/75 tests passing.
  - **Layer 3 (dbt contracts):** Compile-time contract enforcement on mart output schema — catches dropped columns before models run. Critical when AI agents query marts.
- **Why all three:** Orthogonal concerns — each catches failure modes others cannot. Layer 1 inspects raw materials, Layer 2 inspects assembly, Layer 3 inspects finished product spec.
- **Implementation roadmap:** Phase 1 (current: Layer 2 complete), Phase 2 (add Pandera to ingestion), Phase 3 (dbt contracts on marts when AI layer added), Phase 4 (contract registry for data mesh readiness).
- **AI dependency:** Schema drift that a human analyst notices and works around will cause an LLM agent to silently hallucinate or return wrong results. Data contracts are the reliability foundation for AI-powered analytics.
