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
| ridership | FLOAT64 | Estimated hourly ridership |
| transfers | FLOAT64 | Estimated hourly transfers |
| dbt_loaded_at | TIMESTAMP | dbt load timestamp |

### stg_mta_ridership_2019
Source: mta_bronze.mta_ridership_2019
Grain: station × hour × fare_class
Note: Same schema as stg_mta_ridership — see ADR 001

### stg_mta_ridership_2025
Source: mta_bronze.mta_ridership_2025_incremental
Grain: station × hour × fare_class
Materialization: INCREMENTAL (unique_key: transit_timestamp + station_complex_id + fare_class)
Note: MTA split 2025 data to new endpoint (5wq4-mkjj) — discovered during build

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
| median_household_income | INT64 | Median HH income (NULL where sentinel -666666666) |
| population_in_poverty | INT64 | Population below poverty line |
| population_with_disability | INT64 | Population with disability |
| car_ownership_rate | FLOAT64 | Households with car / total households |
| poverty_rate | FLOAT64 | Population in poverty / total population |
| disability_rate | FLOAT64 | Population with disability / total population |
| loaded_at | TIMESTAMP | Original load timestamp |
| dbt_loaded_at | TIMESTAMP | dbt load timestamp |

Note: median_household_income sentinel value -666666666 replaced with NULL via NULLIF()

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

Data Quality Flags:
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
| station_congestion_index | FLOAT64 | hourly_ridership / (daily_ridership / hours_reported) |
| system_contribution_index | FLOAT64 | hourly_ridership / system_avg_per_station_this_hour |
| hour_classification | STRING | Peak Hour/Near Peak/AM Off Peak/PM Off Peak/Overnight |
| station_peak_hour | INT64 | Actual peak hour (NULL for non-peak rows) |
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
Source: stg_mta_ridership (via hour spine)
Grain: station × date (daily summary)

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
| demand_intensity | FLOAT64 | daily_ridership / active_hours (riders per active hour) |
| am_active_hours | INT64 | Active hours between 5-12 |
| pm_active_hours | INT64 | Active hours between 13-20 |
| disruption_hours | INT64 | Hours classified as Possible Disruption |
| anomalous_zero_hours | INT64 | Hours classified as Anomalous Zero |
| expected_low_hours | INT64 | Hours classified as Expected Low |
| max_consecutive_zeros | INT64 | Longest zero streak in rolling 3-hr window |
| daily_ridership | FLOAT64 | Total daily ridership |
| dbt_loaded_at | TIMESTAMP | dbt load timestamp |

Utilization Status Classification (hourly grain, aggregated to daily counts):
- Active: ridership > 0
- Possible Disruption: ridership = 0, historical_avg > 50, consecutive_zeros >= 2
- Anomalous Zero: ridership = 0, historical_avg > 50, isolated (not consecutive)
- Expected Low: ridership = 0, historical_avg <= 50 OR overnight hours

Note: Hour spine (CROSS JOIN GENERATE_ARRAY) required because MTA source
data is sparse — only rows with ridership > 0 are published. See ADR 004.

### int_station_census_join
Source: stg_mta_ridership, stg_census_income, BigQuery public geo_census_tracts
Grain: station (one row per station complex)

| Column | Type | Description |
|--------|------|-------------|
| station_complex_id | STRING | Station identifier |
| station_name | STRING | Station name |
| borough | STRING | NYC borough |
| latitude | FLOAT64 | Station centroid latitude (avg of entrances) |
| longitude | FLOAT64 | Station centroid longitude |
| tracts_in_catchment | INT64 | Census tracts intersecting 800m buffer |
| tracts_with_demographic_data | INT64 | Tracts with non-null income data |
| catchment_population | FLOAT64 | Area-weighted population within catchment |
| weighted_mean_income | FLOAT64 | Area-weighted mean household income |
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

Methodology: 800m catchment area with area-weighted demographics.
ST_UNION_AGG used for multi-entrance stations. See ADR 005.

### int_station_capacity
Source: stg_mta_ridership_2019
Grain: station × time_bucket

| Column | Type | Description |
|--------|------|-------------|
| station_complex_id | STRING | Station identifier |
| time_bucket | STRING | AM/PM/Late Night |
| max_2019_ridership | FLOAT64 | Maximum hourly ridership in 2019 for this period |
| p95_capacity_proxy | FLOAT64 | 95th percentile hourly ridership in 2019 for this period |
| dbt_loaded_at | TIMESTAMP | dbt load timestamp |

Note: p95 calculated within time_bucket to enable period-matched
throughput stress comparison. See ADR 003.

Time Buckets:
- AM: hours 5-12
- PM: hours 13-20
- Late Night: hours 21-23 and 0-4

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
| recovery_pct | FLOAT64 | annual_ridership / baseline × 100 |
| recovery_tier | STRING | Recovered/Recovering/Lagging/Critical/N/A |
| data_quality_flag | STRING | clean/suspect_merged/suspect_split/no_baseline |
| borough_avg_recovery_pct | FLOAT64 | Average recovery for the borough (window function) |
| pct_vs_borough_avg | FLOAT64 | Station recovery minus borough average |
| dbt_loaded_at | TIMESTAMP | dbt load timestamp |

Notes:
- Math: sum(monthly_ridership) / sum(baseline_monthly_2019) — NOT avg of avgs
- data_quality_flag propagated: if ANY month is suspect, whole year is flagged
- Borough rows filter to clean stations only for accurate aggregation
- ~1,305 rows total (430 stations + 5 boroughs × 3 years)

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
| time_period | STRING | Hour classification bucket |
| latitude | FLOAT64 | Station latitude |
| longitude | FLOAT64 | Station longitude |
| observation_count | INT64 | Number of daily observations aggregated |
| avg_hourly_ridership | FLOAT64 | Average hourly ridership for this bucket |
| median_hourly_ridership | FLOAT64 | Median via approx_quantiles(x, 2)[offset(1)] |
| avg_congestion_index | FLOAT64 | Avg station congestion index |
| avg_system_index | FLOAT64 | Avg system contribution index |
| most_common_peak_hour | INT64 | Most frequent peak hour in this bucket |
| congestion_intensity_tier | STRING | High Congestion/Moderate/Baseline/Off Peak |
| dbt_loaded_at | TIMESTAMP | dbt load timestamp |

Congestion Intensity Tiers (data-driven thresholds):
- High Congestion: avg_congestion_index >= 15.0
- Moderate: avg_congestion_index >= 8.0
- Baseline: avg_congestion_index >= 2.0
- Off Peak: avg_congestion_index < 2.0

### mart_efficiency_matrix
Source: mart_congestion_trigger, mart_recovery_scorecard, int_station_capacity
Grain: station × time_period × day_of_week × transit_year × season
Tableau View: View 3 — Efficiency Matrix

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
| avg_congestion_index | FLOAT64 | Avg station congestion index |
| avg_system_index | FLOAT64 | Avg system contribution index |
| congestion_intensity_tier | STRING | Congestion classification |
| most_common_peak_hour | INT64 | Most frequent peak hour |
| recovery_pct | FLOAT64 | Annual recovery vs 2019 |
| data_quality_flag | STRING | Data quality classification |
| p95_capacity_proxy | FLOAT64 | 2019 p95 ridership for this time period |
| max_2019_ridership | FLOAT64 | 2019 maximum ridership for this time period |
| throughput_stress_index | FLOAT64 | avg_hourly_ridership / p95_capacity_proxy |
| stress_tier | STRING | At Capacity/High Stress/Moderate Stress/Low Stress |
| efficiency_quadrant | STRING | Four-quadrant classification |
| dbt_loaded_at | TIMESTAMP | dbt load timestamp |

Efficiency Quadrants:
- Thriving but Strained: stress >= 0.70 AND recovery >= 70%
  → High demand, high utilization — needs capacity investment
- Structural Bottleneck: stress >= 0.70 AND recovery < 70%
  → Still stressed despite incomplete recovery — infrastructure problem
- Healthy Spare Capacity: stress < 0.70 AND recovery >= 70%
  → Recovered demand with room to grow
- Underutilized: stress < 0.70 AND recovery < 70%
  → Low demand, low utilization — equity concern

Stress Tiers:
- At Capacity: throughput_stress_index >= 0.90
- High Stress: throughput_stress_index >= 0.70
- Moderate Stress: throughput_stress_index >= 0.50
- Low Stress: throughput_stress_index < 0.50

Note: Index is period-matched — AM stress compares to AM 2019 p95,
PM stress compares to PM 2019 p95. See ADR 003.
