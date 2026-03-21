# MTA Analytics Pipeline — Data Dictionary

## Staging Layer (mta_silver)

### stg_mta_ridership
Source: mta_bronze.mta_ridership_2022_2024
Grain: station × hour × fare_class

| Column | Type | Description |
|--------|------|-------------|
| transit_timestamp | TIMESTAMP | Hour of ridership reading |
| transit_date | DATE | Date of ridership reading |
| transit_hour | INT64 | Hour of day (0-23) |
| day_of_week | INT64 | Day of week (1=Sunday, 7=Saturday) |
| transit_year | INT64 | Year extracted from timestamp |
| transit_month | INT64 | Month extracted from timestamp |
| station_complex_id | STRING | Unique station complex identifier |
| station_name | STRING | Station name (initcap cleaned) |
| borough | STRING | NYC borough (initcap cleaned) |
| latitude | FLOAT64 | Station latitude |
| longitude | FLOAT64 | Station longitude |
| transit_mode | STRING | Mode of transit (lowercase) |
| payment_method | STRING | Payment method (lowercase) |
| fare_class | STRING | Fare class category (lowercase) |
| ridership | FLOAT64 | Estimated hourly ridership — filtered to >= 0 |
| transfers | FLOAT64 | Estimated hourly transfers |
| dbt_loaded_at | TIMESTAMP | dbt load timestamp |

### stg_mta_ridership_2019
Source: mta_bronze.mta_ridership_2019
Grain: station × hour × fare_class
Note: Same schema as stg_mta_ridership — see ADR 001 for dataset migration details.

### stg_mta_ridership_2025
Source: mta_bronze.mta_ridership_2025_incremental
Grain: station × hour × fare_class
Materialization: INCREMENTAL (unique_key: transit_timestamp + station_complex_id + fare_class)
Note: MTA split 2025 data to new endpoint (5wq4-mkjj) — discovered during build.

### stg_census_income
Source: mta_bronze.census_nyc_tracts
Grain: census tract

| Column | Type | Description |
|--------|------|-------------|
| geoid | STRING | Census tract GEOID (e.g., 36061004700) |
| borough | STRING | NYC borough |
| county_fips | STRING | County FIPS code |
| total_population | INT64 | Total tract population |
| total_households | INT64 | Total households |
| households_no_vehicle | INT64 | Households without vehicle |
| median_household_income | INT64 | Median HH income. NULL where Census sentinel -666666666 indicates suppressed data |
| population_in_poverty | INT64 | Population below poverty line |
| population_with_disability | INT64 | Population with disability |
| car_ownership_rate | FLOAT64 | Households with car / total households |
| poverty_rate | FLOAT64 | Population in poverty / total population |
| disability_rate | FLOAT64 | Population with disability / total population |
| loaded_at | TIMESTAMP | Original load timestamp |
| dbt_loaded_at | TIMESTAMP | dbt load timestamp |

---

## Intermediate Layer (mta_silver)

### int_station_recovery
Source: stg_mta_ridership, stg_mta_ridership_2019
Grain: station × year-month

| Column | Type | Description |
|--------|------|-------------|
| station_complex_id | STRING | Station identifier |
| station_name | STRING | Station name |
| borough | STRING | NYC borough |
| latitude | FLOAT64 | Station latitude (avg across readings) |
| longitude | FLOAT64 | Station longitude |
| ridership_year | INT64 | Year (2022, 2023, 2024) |
| ridership_month | INT64 | Month (1-12) |
| ridership_month_date | DATE | First day of month (for Tableau date axis) |
| monthly_ridership | FLOAT64 | Total ridership for month |
| baseline_monthly_2019 | FLOAT64 | 2019 ridership for same month |
| monthly_recovery_pct | FLOAT64 | monthly_ridership / baseline × 100 |
| season | STRING | Winter/Spring/Summer/Fall |
| recovery_tier | STRING | Recovered/Recovering/Lagging/Critical |
| prev_month_ridership | FLOAT64 | Prior month ridership (LAG) |
| month_over_month_pct | FLOAT64 | MoM % change |
| data_quality_flag | STRING | clean/suspect_merged/suspect_split/no_baseline |
| dbt_loaded_at | TIMESTAMP | dbt load timestamp |

Recovery Tier Thresholds:
- Recovered: recovery_pct >= 90%
- Recovering: recovery_pct 70-89%
- Lagging: recovery_pct 50-69%
- Critical: recovery_pct < 50%

Data Quality Flags (see ADR 002):
- clean: passes boundary checks
- suspect_merged: recovery_pct > 200 (2019 baseline too low, likely station merge)
- suspect_split: recovery_pct < 25 (2019 baseline too high, likely station split)
- no_baseline: no 2019 data for this station

### int_station_congestion
Source: stg_mta_ridership
Grain: station × date × hour

| Column | Type | Description |
|--------|------|-------------|
| station_complex_id | STRING | Station identifier |
| station_name | STRING | Station name |
| borough | STRING | NYC borough |
| latitude | FLOAT64 | Station latitude |
| longitude | FLOAT64 | Station longitude |
| transit_date | DATE | Date of reading |
| transit_hour | INT64 | Hour of day (0-23) |
| day_of_week | INT64 | Day of week |
| ridership_year | INT64 | Year |
| season | STRING | Winter/Spring/Summer/Fall |
| hourly_ridership | FLOAT64 | Total ridership this hour (fare classes consolidated) |
| daily_ridership | FLOAT64 | Total station ridership that day |
| hours_reported | INT64 | Number of hours with data that day |
| historical_avg_this_hour | FLOAT64 | Historical avg ridership for this station-hour |
| station_congestion_index | FLOAT64 | hourly_ridership / (daily_ridership / hours_reported). Measures how much busier this hour is vs the station's own daily average |
| system_contribution_index | FLOAT64 | hourly_ridership / system_avg_per_station_this_hour. Measures how this station compares to the system at the same hour |
| hour_classification | STRING | Peak Hour/Near Peak/AM Off Peak/PM Off Peak/Overnight |
| station_peak_hour | INT64 | Actual peak hour number (NULL for non-peak rows) |
| data_quality_flag | STRING | clean/suspect_high |
| daily_peak_rank | INT64 | Rank of this hour within the station's day |
| dbt_loaded_at | TIMESTAMP | dbt load timestamp |

Hour Classification:
- Peak Hour: highest ridership hour for that station that day (rank=1)
- Near Peak: 2nd-3rd highest hours (rank 2-3)
- AM Off Peak: hours 5-12, not peak
- PM Off Peak: hours 13-20, not peak
- Overnight: hours 21-23 and 0-4

### int_station_utilization
Source: stg_mta_ridership (via 24-hour spine)
Grain: station × date (daily summary)

Note: Uses CROSS JOIN GENERATE_ARRAY(0,23) to create a complete 24-hour spine.
MTA source data is sparse event data — only rows where ridership > 0 are published.
Missing hours are absent entirely, not zeroed. The spine converts implicit gaps
into explicit zeros enabling meaningful consecutive zero detection. See ADR 004.

| Column | Type | Description |
|--------|------|-------------|
| station_complex_id | STRING | Station identifier |
| station_name | STRING | Station name |
| borough | STRING | NYC borough |
| latitude | FLOAT64 | Station latitude |
| longitude | FLOAT64 | Station longitude |
| transit_date | DATE | Date |
| ridership_year | INT64 | Year |
| season | STRING | Winter/Spring/Summer/Fall |
| active_hours | INT64 | Hours with ridership > 0 |
| gap_hours | INT64 | Hours with no ridership (explicit zeros from spine) |
| utilization_rate | FLOAT64 | active_hours / 24 |
| demand_intensity | FLOAT64 | daily_ridership / active_hours — riders per active hour |
| am_active_hours | INT64 | Active hours between 5-12 |
| pm_active_hours | INT64 | Active hours between 13-20 |
| disruption_hours | INT64 | Hours classified as Possible Disruption |
| anomalous_zero_hours | INT64 | Hours classified as Anomalous Zero |
| expected_low_hours | INT64 | Hours classified as Expected Low |
| max_consecutive_zeros | INT64 | Longest zero streak in rolling 3-hr window |
| daily_ridership | FLOAT64 | Total daily ridership |
| dbt_loaded_at | TIMESTAMP | dbt load timestamp |

Utilization Status Classification (three-layer anomaly detection):
- Active: ridership > 0
- Possible Disruption: ridership = 0, historical_avg > 50, consecutive_zeros >= 2
- Anomalous Zero: ridership = 0, historical_avg > 50, isolated single hour
- Expected Low: ridership = 0, historical_avg <= 50 OR overnight hours (0-4)

### int_station_census_join
Source: stg_mta_ridership, stg_census_income, BigQuery public geo_census_tracts
Grain: station (one row per station complex)

Note: Uses 800-meter catchment area with area-weighted demographics.
ST_UNION_AGG merges all entrance buffers for multi-entrance stations.
See ADR 005 for full methodology.

| Column | Type | Description |
|--------|------|-------------|
| station_complex_id | STRING | Station identifier |
| station_name | STRING | Station name |
| borough | STRING | NYC borough |
| latitude | FLOAT64 | Station centroid latitude (avg of all entrances) |
| longitude | FLOAT64 | Station centroid longitude |
| tracts_in_catchment | INT64 | Census tracts intersecting 800m buffer |
| tracts_with_demographic_data | INT64 | Tracts with non-null income data |
| catchment_population | FLOAT64 | Area-weighted population within catchment |
| weighted_mean_income | FLOAT64 | Area-weighted mean household income. NOTE: This is a weighted MEAN not weighted median — sum(income × weight) / sum(weight) |
| weighted_poverty_rate | FLOAT64 | Area-weighted poverty rate |
| weighted_car_ownership_rate | FLOAT64 | Area-weighted car ownership rate |
| weighted_disability_rate | FLOAT64 | Area-weighted disability rate |
| income_tier | STRING | Low/Middle/Upper Middle/High Income |
| dbt_loaded_at | TIMESTAMP | dbt load timestamp |

Income Tier Thresholds (NYC-specific):
- Low Income: weighted_mean_income < $40,000
- Middle Income: $40,000-$74,999
- Upper Middle Income: $75,000-$119,999
- High Income: $120,000+

### int_station_capacity
Source: stg_mta_ridership_2019
Grain: station × time_bucket

Note: p95 calculated within time_bucket to enable period-matched throughput
stress comparison. All-hour p95 produces misleading results — see ADR 003.

| Column | Type | Description |
|--------|------|-------------|
| station_complex_id | STRING | Station identifier |
| time_bucket | STRING | AM (hours 5-12) / PM (hours 13-20) / Late Night (hours 21-23, 0-4) |
| max_2019_ridership | FLOAT64 | Maximum hourly ridership in 2019 for this time period |
| p95_capacity_proxy | FLOAT64 | 95th percentile hourly ridership in 2019 for this time period. Used as practical capacity ceiling — excludes outlier events (NYE, concerts) |
| dbt_loaded_at | TIMESTAMP | dbt load timestamp |

---

## Gold Layer (mta_gold)

### mart_recovery_scorecard
Source: int_station_recovery
Grain: station × year UNION borough × year
Tableau View: View 1 — Recovery Scorecard

| Column | Type | Description |
|--------|------|-------------|
| record_type | STRING | 'station' or 'borough' |
| transit_year | INT64 | Year (2022, 2023, 2024) |
| borough | STRING | NYC borough |
| station_complex_id | STRING | Station ID (NULL for borough rows) |
| station_name | STRING | Station name or 'Borough Average' |
| latitude | FLOAT64 | Station lat / borough centroid lat |
| longitude | FLOAT64 | Station lon / borough centroid lon |
| annual_ridership | FLOAT64 | Total annual ridership |
| annual_baseline_2019 | FLOAT64 | Total 2019 baseline ridership |
| recovery_pct | FLOAT64 | annual_ridership / baseline × 100. Calculated as sum(monthly)/sum(baseline) — NOT avg of monthly percentages |
| recovery_tier | STRING | Recovered (>=90%) / Recovering (70-89%) / Lagging (50-69%) / Critical (<50%) / N/A |
| data_quality_flag | STRING | clean/suspect_merged/suspect_split/no_baseline. If ANY month is suspect, the whole year is flagged |
| borough_avg_recovery_pct | FLOAT64 | Average recovery for the borough (window function over clean stations only) |
| pct_vs_borough_avg | FLOAT64 | Station recovery minus borough average — positive = above average |
| dbt_loaded_at | TIMESTAMP | dbt load timestamp |

Notes:
- Borough rows aggregate clean stations only (data_quality_flag = 'clean')
- ~1,305 rows total (430 stations + 5 boroughs × 3 years)
- See ADR 002 for station ID reorganization context

### mart_congestion_trigger
Source: int_station_congestion
Grain: station × time_period × day_of_week × ridership_year × season
Tableau View: View 2 — Congestion Heat Map

| Column | Type | Description |
|--------|------|-------------|
| station_complex_id | STRING | Station identifier |
| station_name | STRING | Station name |
| borough | STRING | NYC borough |
| ridership_year | INT64 | Year |
| season | STRING | Winter/Spring/Summer/Fall |
| day_of_week | INT64 | Day of week (1=Sunday) |
| time_period | STRING | Hour classification: Peak Hour/Near Peak/AM Off Peak/PM Off Peak/Overnight |
| latitude | FLOAT64 | Station latitude |
| longitude | FLOAT64 | Station longitude |
| observation_count | INT64 | Number of daily observations aggregated |
| avg_hourly_ridership | FLOAT64 | Average hourly ridership for this bucket |
| median_hourly_ridership | FLOAT64 | Median via approx_quantiles(x,2)[offset(1)] — efficient approximation |
| avg_congestion_index | FLOAT64 | Avg station congestion index for this bucket |
| most_common_peak_hour | INT64 | Most frequent peak hour in this bucket (safe_offset handles nulls) |
| congestion_intensity_tier | STRING | High Congestion/Moderate/Baseline/Off Peak |
| dbt_loaded_at | TIMESTAMP | dbt load timestamp |

Congestion Intensity Tiers (derived from actual data distribution):
- High Congestion: avg_congestion_index >= 15.0
- Moderate: avg_congestion_index >= 8.0
- Baseline: avg_congestion_index >= 2.0
- Off Peak: avg_congestion_index < 2.0

### mart_efficiency_matrix
Source: mart_congestion_trigger, mart_recovery_scorecard
Grain: station × time_period × day_of_week × transit_year × season
Tableau View: View 3 — Efficiency Matrix

IMPORTANT NOTE: This mart uses avg_congestion_index (not a capacity-based
throughput stress index) to drive the efficiency quadrant classification.
A capacity proxy approach was evaluated but produced misleading values (>5x)
due to grain mismatch between aggregated time periods and hourly capacity
baselines. See ADR 003. The congestion index is internally consistent and
requires no external capacity data.

| Column | Type | Description |
|--------|------|-------------|
| station_complex_id | STRING | Station identifier |
| station_name | STRING | Station name |
| borough | STRING | NYC borough |
| transit_year | INT64 | Year |
| season | STRING | Winter/Spring/Summer/Fall |
| day_of_week | INT64 | Day of week |
| time_period | STRING | Hour classification bucket |
| latitude | FLOAT64 | Station latitude |
| longitude | FLOAT64 | Station longitude |
| avg_hourly_ridership | FLOAT64 | Average hourly ridership |
| median_hourly_ridership | FLOAT64 | Median hourly ridership |
| observation_count | INT64 | Number of daily observations aggregated |
| avg_congestion_index | FLOAT64 | Avg station congestion index — primary axis for efficiency quadrant |
| congestion_intensity_tier | STRING | High Congestion/Moderate/Baseline/Off Peak |
| most_common_peak_hour | INT64 | Most frequent peak hour |
| recovery_pct | FLOAT64 | Annual recovery vs 2019 (from mart_recovery_scorecard) |
| recovery_dq_flag | STRING | Data quality flag from recovery scorecard |
| efficiency_quadrant | STRING | Four-quadrant classification (see below) |
| congestion_tier | STRING | High Congestion/Moderate/Baseline/Off Peak (display alias) |
| dbt_loaded_at | TIMESTAMP | dbt load timestamp |

Efficiency Quadrant Logic (congestion index × recovery):
- Thriving but Strained: avg_congestion_index >= 8.0 AND recovery_pct >= 70%
  → High demand + recovered ridership — needs capacity investment
- Structural Bottleneck: avg_congestion_index >= 8.0 AND recovery_pct < 70%
  → High congestion despite incomplete recovery — infrastructure problem
- Healthy Spare Capacity: avg_congestion_index < 8.0 AND recovery_pct >= 70%
  → Recovered ridership with room to grow — efficient
- Underutilized: avg_congestion_index < 8.0 AND recovery_pct < 70%
  → Low demand, low recovery — equity concern

Threshold rationale: avg_congestion_index = 8.0 corresponds to the
Moderate congestion boundary derived from the actual data distribution
(sanity check: Peak Hour avg = 20.4, Near Peak = 15.8, PM Off Peak = 8.1).

### mart_equity_view
Source: mart_recovery_scorecard, int_station_utilization, int_station_census_join
Grain: station × year
Tableau View: View 4 — Service Justice Dashboard

The "Triple Threat" model answering three questions:
1. WHO is affected? — Census demographics (income, poverty, disability)
2. HOW MUCH do they need it? — Demand intensity (riders per active hour)
3. ARE WE FAILING THEM? — Disruption hours, service gaps

| Column | Type | Description |
|--------|------|-------------|
| station_complex_id | STRING | Station identifier |
| station_name | STRING | Station name |
| borough | STRING | NYC borough |
| latitude | FLOAT64 | Station latitude |
| longitude | FLOAT64 | Station longitude |
| transit_year | INT64 | Year (2022, 2023, 2024) |
| annual_ridership | FLOAT64 | Total annual ridership |
| recovery_pct | FLOAT64 | Annual recovery vs 2019 baseline |
| recovery_tier | STRING | Recovered/Recovering/Lagging/Critical |
| borough_avg_recovery_pct | FLOAT64 | Borough average recovery for context |
| pct_vs_borough_avg | FLOAT64 | Gap between station and borough average |
| data_quality_flag | STRING | Station data quality (clean/suspect_merged/suspect_split) |
| avg_utilization_rate | FLOAT64 | Average daily utilization rate (active_hours / 24) |
| avg_demand_intensity | FLOAT64 | Average riders per active hour — measures how intensely the station is used when it IS active |
| disruption_rate_per_1k_hours | FLOAT64 | Disruption hours per 1,000 active hours. Normalized metric — prevents large stations from dominating raw disruption counts |
| anomalous_zero_hours | INT64 | Annual count of isolated anomalous zero-ridership hours |
| avg_am_active_hours | FLOAT64 | Average AM peak hours active per day |
| avg_pm_active_hours | FLOAT64 | Average PM peak hours active per day |
| weighted_mean_income | FLOAT64 | Area-weighted mean household income for 800m catchment |
| weighted_poverty_rate | FLOAT64 | Area-weighted poverty rate for catchment |
| weighted_car_ownership_rate | FLOAT64 | Area-weighted car ownership rate for catchment |
| weighted_disability_rate | FLOAT64 | Area-weighted disability rate for catchment |
| catchment_population | FLOAT64 | Total weighted population within 800m catchment |
| tracts_in_catchment | INT64 | Number of census tracts intersecting catchment buffer |
| income_tier | STRING | Low/Middle/Upper Middle/High Income |
| equity_risk_score | FLOAT64 | Composite score 0-100 measuring equity risk (see methodology below) |
| equity_risk_tier | STRING | High Risk (>=60) / Moderate Risk (35-59) / Low Risk (<35) |
| recovery_dq_flag | STRING | Aliased from data_quality_flag to avoid join collision. Reflects recovery data quality for this station-year |
| dbt_loaded_at | TIMESTAMP | dbt load timestamp |

Note: Only includes stations where data_quality_flag = 'clean'.
Suspect stations (merged/split IDs) excluded to ensure score integrity.

#### Equity Risk Score Methodology

Composite score (0-100) with three equally weighted components:

**Component 1 — Recovery Gap (max 33 points)**
```
Points = min(max(borough_avg - recovery_pct, 0) / 20.0, 1.0) × 33
```
Measures how far behind the borough average this station is.
20+ percentage point lag = full 33 points.
Example: Borough avg 61%, station at 41% = 20pt lag = 33 points.

**Component 2 — Disruption Burden (max 33 points)**
```
Points = min(disruption_rate_per_1k_hours / 10.0, 1.0) × 33
```
Measures service reliability normalized by station activity level.
10+ disruptions per 1,000 active hours = full 33 points.
Normalization prevents large busy stations from always scoring high.

**Component 3 — Income Vulnerability (max 34 points)**
```
Points = min(max(40000 - weighted_mean_income, 0) / 20000.0, 1.0) × 34
```
Measures economic dependence on transit.
Below $40K median income begins scoring. Below $20K = full 34 points.
Higher income communities have more transportation alternatives.

**Risk Tier Thresholds:**
- High Risk: equity_risk_score >= 60 — all three factors present
- Moderate Risk: equity_risk_score 35-59 — one or two factors elevated
- Low Risk: equity_risk_score < 35 — recovering well with reliable service

**Key Findings (2024):**
- High Risk stations: 19 total (9 Bronx, 8 Brooklyn, 2 Manhattan)
- High Risk avg income: $46,898
- High Risk avg recovery: 55%
- High Risk disruption rate: 15.5 per 1,000 hours
- Low Risk avg income: $107,000
- Low Risk disruption rate: 3.0 per 1,000 hours
- Income gap High vs Low Risk: $60,000+
- Disruption rate gap: 5x higher in High Risk communities