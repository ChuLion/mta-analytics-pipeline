-- mart_efficiency_matrix.sql
-- Gold Layer: Station Efficiency Matrix (View 3)
-- Grain: station × year × season × day_of_week × time_period
-- Joins congestion patterns with recovery context
-- Key output: efficiency_quadrant for scatter plot color coding
--
-- NOTE: Throughput stress index removed — capacity proxy produced
-- misleading values (>1.0) due to grain mismatch between
-- aggregated time_period buckets and hourly capacity baseline.
-- See ADR 003 for full technical explanation.
--
-- Efficiency quadrant uses avg_congestion_index >= 8.0 as the
-- "high congestion" threshold — internally consistent, no
-- external capacity data required. Threshold derived from
-- actual data distribution (see int_station_congestion sanity check).

{{ config(
    materialized='table',
    cluster_by=["borough", "transit_year", "efficiency_quadrant"]
) }}

with congestion as (
    select
        *,
        ridership_year                              as transit_year
    from {{ ref('mart_congestion_trigger') }}
),

recovery as (
    select
        station_complex_id,
        transit_year,
        recovery_pct,
        data_quality_flag
    from {{ ref('mart_recovery_scorecard') }}
    where record_type = 'station'
)

select
    c.station_complex_id,
    c.station_name,
    c.borough,
    c.transit_year,
    c.season,
    c.day_of_week,
    c.time_period,
    c.latitude,
    c.longitude,

    -- Volume metrics
    c.avg_hourly_ridership,
    c.median_hourly_ridership,
    c.observation_count,

    -- Congestion metrics (internally consistent, no external capacity needed)
    c.avg_congestion_index,
    -- REMOVED:c.avg_system_index,
    c.congestion_intensity_tier,
    c.most_common_peak_hour,

    -- Recovery context
    r.recovery_pct,
    r.data_quality_flag                             as recovery_dq_flag,

    -- Efficiency quadrant
    -- Combines congestion intensity with recovery trajectory
    -- Threshold: avg_congestion_index >= 8.0 = Moderate or higher
    -- Threshold: recovery_pct >= 70 = Recovering or better
    case
        when c.avg_congestion_index >= 8.0
         and r.recovery_pct >= 70
            then 'Thriving but Strained'
        when c.avg_congestion_index >= 8.0
         and r.recovery_pct < 70
            then 'Structural Bottleneck'
        when c.avg_congestion_index < 8.0
         and r.recovery_pct >= 70
            then 'Healthy Spare Capacity'
        else
            'Underutilized'
    end                                             as efficiency_quadrant,

    -- Congestion intensity as the stress signal
    -- Clean, internally consistent, no capacity proxy needed
    case
        when c.avg_congestion_index >= 15.0 then 'High Congestion'
        when c.avg_congestion_index >= 8.0  then 'Moderate'
        when c.avg_congestion_index >= 2.0  then 'Baseline'
        else                                     'Off Peak'
    end                                             as congestion_tier,

    current_timestamp()                             as dbt_loaded_at

from congestion c
left join recovery r
    on c.station_complex_id = r.station_complex_id
    and c.transit_year      = r.transit_year