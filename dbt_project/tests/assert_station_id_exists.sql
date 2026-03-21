-- dbt_project/tests/assert_station_id_exists.sql
-- Every station in the gold marts should exist in the staging layer
-- Catches orphaned records from joins or data quality issues

select
    m.station_complex_id
from {{ ref('mart_recovery_scorecard') }} m
where m.record_type = 'station'
  and m.station_complex_id not in (
      select distinct station_complex_id
      from {{ ref('stg_mta_ridership') }}
  )