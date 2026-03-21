-- tests/assert_recovery_pct_bounds.sql
-- Flags extreme recovery values for data quality review
-- Note: values outside bounds are flagged, not necessarily errors
-- See ADR 002 for station ID reorganization context
select *
from {{ ref('int_station_recovery') }}
-- Only fail if a 'clean' record is outside the bounds
where data_quality_flag = 'clean' 
  and (monthly_recovery_pct < 0 or monthly_recovery_pct > 200)