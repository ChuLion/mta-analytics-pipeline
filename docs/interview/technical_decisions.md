# MTA Analytics Pipeline — Interview Talking Points

## Project Elevator Pitch
"I built an end-to-end ELT pipeline on GCP that ingests 88 million rows
of MTA subway ridership data across 428 stations and 3 years. The pipeline
uses Python for ingestion, dbt for transformation across bronze/silver/gold
layers in BigQuery, and Tableau for executive dashboards. The analytical
layer answers five key questions: which stations recovered from COVID,
when and where is the system most congested, which stations are operating
efficiently, where does demand outpace service, and which communities face
both low recovery AND service disruptions — the equity story."

---

## Q1: Walk me through the architecture

"The pipeline follows ELT — not ETL. Raw data lands in Google Cloud Storage
first as CSV files, partitioned by year/month/week. From there Python loads
it into BigQuery's bronze layer. dbt handles all transformations through
three layers: staging views clean and standardize column names and types;
intermediate views join datasets and calculate derived metrics; gold mart
tables are pre-aggregated and Tableau-optimized — reducing 77 million source
rows to under 200,000 rows per mart.

I used Terraform for infrastructure as code, GitHub Actions for CI/CD running
dbt tests on every push, and Tableau Public for the live dashboard."

---

## Q2: How did you calculate ridership from the 2019 data?

Initial answer (turnstile exploration):
"The legacy 2019 dataset stored cumulative turnstile counter readings —
not ridership directly. I built a LAG() window function pipeline partitioned
by individual turnstile unit (c_a, unit, scp composite key), ordered by
timestamp, filtering to REGULAR readings only for consistent 4-hour intervals.
I handled counter resets and maintenance events by setting negative deltas to
NULL rather than filtering rows — preserving audit trail while excluding bad values.

However, during development of the recovery calculation I discovered MTA
publishes a pre-aggregated hourly dataset for 2019 with identical schema to
the modern data. I migrated to that source — the turnstile work wasn't wasted,
it produced reusable patterns and the decision is documented as ADR 001."

---

## Q3: How did you handle the station ID mismatch between 2019 and 2022?

"When I first joined the 2019 baseline to 2022 ridership data I got 100%
NULL matches. Investigation revealed two problems: the legacy dataset used
free-text station names incompatible with the modern numeric station_complex_id,
AND MTA had reorganized station complex IDs between datasets — merging some
stations and splitting others.

I took a two-part approach: switched to the hourly 2019 dataset that uses the
same station_complex_id format (ADR 001), and implemented a data_quality_flag
on recovery calculations to identify stations where the ID reorganization
creates misleading metrics. Astoria Blvd shows 343% recovery — not real growth,
but a station merge. Canarsie shows 16% — a station split. I flagged both,
filtered them from executive views, and shifted primary reporting to borough
level where individual station issues cancel out. The full fix requires an
MTA GTFS crosswalk table — documented as ADR 002."

---

## Q4: Explain the catchment area methodology

"Rather than a simple point-in-polygon join that assigns a station to a single
census tract, I implemented an 800-meter catchment area using BigQuery's native
geospatial functions. 800 meters is the NYC pedestrian planning standard for
a 10-minute walk.

Area-Weighted Interpolation was used. Any census tract intersecting the station's
walkshed is included, weighted by the proportion of the buffer area it covers. If
35% of a tract falls within the buffer, it contributes 35% weight to the station's
demographic profile. This is standard urban planning methodology — what MTA uses 
in their own equity reports.

I also discovered stations have multiple physical entrances with different
coordinates. I used ST_UNION_AGG to merge all entrance buffers into a single
catchment polygon — this captures the true service area of the entire complex.

The result is a weighted_mean_income per station that reflects who actually
uses each station, not just who lives in the one tract where the entrance sits.
The income gap between the highest and lowest income station catchment areas
is $192,000 — $27K for South Bronx stations vs $220K for FiDi stations."

---

## Q5: What is the three-layer anomaly detection?

"The utilization model needed to detect service disruptions — hours when a
station should be active but went dark. The challenge: MTA's data is sparse
event data. They only publish rows when ridership occurs. A station at 3am
with no riders has NO ROW — not a row with zero ridership.

My first implementation returned zero disruption hours across three years —
clearly wrong. The root cause was assuming zeros would appear explicitly.

The fix was generating a complete 24-hour spine via CROSS JOIN with
GENERATE_ARRAY(0,23), then LEFT JOINing actual ridership onto it. This
converts implicit gaps into explicit zeros. Without the spine, the
AVG() and SUM() calculations were biased because they were only averaging
"active" hours. By "zero-filling" the gaps, a true temporal baseline was created.

Then I applied three-layer classification:
  Layer 1: Consecutive zero detection — rolling 3-hour window. Two or
           more consecutive zero hours flags a potential disruption.
  Layer 2: Historical baseline — compare against that station's typical
           ridership for that specific hour. If historical average > 50
           riders but current = 0, it's anomalous.
  Layer 3: Time-of-day context — overnight zeros (12am-4am) are expected,
           not disruptions.

After the fix: 18,000-23,000 disruption hours annually, with disruption
intensity INCREASING despite rising ridership — 17.7 per million riders in
2022 to 19.2 in 2024. Brooklyn accounts for ~50% of all disruptions."

---

## Q6: Why did you choose dbt for transformations?

"dbt gave me four things I couldn't get from raw SQL scripts:
  1. Dependency management — dbt builds models in the correct order
     automatically based on ref() relationships
  2. Testability — I can write tests that assert recovery_pct is between
     0 and 200, that station_complex_id is never null, that no negative
     ridership exists
  3. Documentation — schema.yml generates a data catalog with column
     descriptions that any engineer can reference
  4. Incremental models — the 2025 dataset uses is_incremental() to append
     only new rows weekly without full reloads

The bronze/silver/gold medallion architecture in BigQuery, with dbt managing
silver and gold, is the standard pattern at companies using modern data stacks."

---

## Q7: How did you optimize for Tableau performance?

"The source data is 77 million rows — Tableau can't render that directly.
The solution is pre-aggregation in the gold mart layer:

  int_station_congestion: 13M rows (daily grain)
  mart_congestion_trigger: 180K rows (station × time_period × dow × year × season)

That's a 98.6% row reduction. Tableau queries scan 180K rows instead of 13M.
With BigQuery clustering on borough and time_period, query response is
sub-second for filtered views.

I made a deliberate decision to keep intermediate models at the lowest grain
needed for analytical flexibility. The mart layer aggregates UP for Tableau —
you can always aggregate up, you can never disaggregate down."

---

## Q8: What would you do differently in production?

"Several things:
  1. GTFS crosswalk for station ID reconciliation — critical for data accuracy
  2. Airflow or Dagster for orchestration instead of cron — better visibility,
     retry logic, alerting
  3. dbt tests on every model — I have the framework but need comprehensive
     coverage: not_null, unique, accepted_values, custom assertions
  4. Elementary or re_data for data observability — anomaly detection on the
     pipeline itself, not just the data
  5. Per-station throughput thresholds instead of global — a data-driven
     approach using the actual distribution I built would refine outlier detection
  6. Streaming for 2025 data — Pub/Sub + Dataflow instead of weekly batch
     would give near-real-time ridership monitoring"

---

## Q9: What was the most interesting finding in the data?

"Two findings surprised me:

First — disruption intensity is INCREASING as ridership recovers. You'd expect
the system to get more reliable as it carries fewer passengers than 2019.
Instead, disruption hours per million riders went from 17.7 in 2022 to 19.2
in 2024. Brooklyn accounts for half of all system disruptions. The system is
under growing stress, not recovering to its former reliability.

Second — the equity analysis shows two dimensions of disadvantage for
low-income communities. South Bronx stations have both the lowest recovery
rates (some still below 50% of 2019) AND the highest disruption rates.
These communities face reduced service frequency AND unreliable service.
That's not a COVID recovery story — that's a structural investment gap."

---

## Q10: How did you handle the throughput stress calculation?

"Initial implementation produced stress index values of 2.48 — meaning stations
appeared to operate at 248% of capacity. That would destroy executive trust
immediately.

Root cause: I calculated the p95 capacity proxy across ALL hours of 2019,
including overnight hours with near-zero ridership. This pulled the p95 way
down. When I compared peak-hour ridership against that deflated baseline,
the index exploded.

Fix: Period-matched capacity. Calculate 2019 p95 separately for AM hours,
PM hours, and overnight. Compare each time period only against its own
historical baseline. Now a stress index of 1.0 means 'operating at pre-COVID
levels for this time of day' — immediately interpretable without explanation.

This is a subtle but important methodological decision. The kind that separates
analysis that builds trust from analysis that raises questions in board meetings."

---

## Key Metrics to Know Cold

| Metric | Value |
|--------|-------|
| Total rows ingested | 88+ million |
| 2019 annual ridership | 1.701 billion |
| Stations in dataset | 428 |
| 2024 system recovery rate | ~70% of 2019 |
| Highest recovery borough | Manhattan ~78% |
| Lowest recovery borough | Bronx ~61% |
| Critical stations (2024) | 17 (<50% recovery) |
| Max income gap (station) | $192,000 |
| Annual disruption hours | 18K-23K |
| Disruption trend | Increasing +8.6% 2022→2024 |
| Brooklyn disruption share | ~50% of system total |
