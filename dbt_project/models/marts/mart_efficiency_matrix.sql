-- mart_efficiency_matrix.sql
-- Gold Layer: Station Efficiency & Stress Matrix (View 3)
-- Grain: station × year × season × day_of_week × time_period
-- Joins congestion patterns with recovery context and capacity proxy
-- Key output: efficiency_quadrant for scatter plot color coding
-- Grain: station × year × season × day_of_week × time_period

{{ config(
    materialized='table',
    cluster_by=["borough", "transit_year", "efficiency_quadrant"]
) }}

with congestion_mapped as (
    -- Step 1: Map granular congestion time periods to the 3-bucket capacity system
    select
        *,
        ridership_year as transit_year,
        case
            when time_period in ('Peak Hour', 'Near Peak', 'AM Off Peak') then 'AM'
            when time_period = 'PM Off Peak' then 'PM'
            else 'Late Night'
        end as capacity_time_bucket
    from {{ ref('mart_congestion_trigger') }}
),

recovery as (
    -- Step 2: Get annual recovery context per station
    select 
        station_complex_id, 
        transit_year, 
        recovery_pct, 
        data_quality_flag
    from {{ ref('mart_recovery_scorecard') }}
    where record_type = 'station'
),

capacity as (
    -- Step 3: Get 2019 period-matched capacity baselines
    select 
        station_complex_id, 
        time_bucket, 
        p95_capacity_proxy 
    from {{ ref('int_station_capacity') }}
)

select
    -- Dimensions
    c.station_complex_id,
    c.station_name,
    c.borough,
    c.transit_year,
    c.season,
    c.day_of_week,
    c.time_period,
    c.latitude,
    c.longitude,
    
    -- Base Metrics
    c.avg_hourly_ridership,
    cap.p95_capacity_proxy,
    r.recovery_pct,
    
    -- Calculated Index: Throughput Stress
    round(
        safe_divide(c.avg_hourly_ridership, cap.p95_capacity_proxy), 
    3) as throughput_stress_index,

    -- Categorical Dimension 1: Stress Tier
    case 
        when safe_divide(c.avg_hourly_ridership, cap.p95_capacity_proxy) >= 0.90 then 'At Capacity'
        when safe_divide(c.avg_hourly_ridership, cap.p95_capacity_proxy) >= 0.70 then 'High Stress'
        when safe_divide(c.avg_hourly_ridership, cap.p95_capacity_proxy) >= 0.50 then 'Moderate Stress'
        else 'Low Stress'
    end as stress_tier,

    -- Categorical Dimension 2: Efficiency Quadrant (The View 3 Hero Metric)
    case 
        when safe_divide(c.avg_hourly_ridership, cap.p95_capacity_proxy) >= 0.70 
             and r.recovery_pct >= 70 then 'Thriving but Strained'
        when safe_divide(c.avg_hourly_ridership, cap.p95_capacity_proxy) >= 0.70 
             and r.recovery_pct < 70  then 'Structural Bottleneck'
        when safe_divide(c.avg_hourly_ridership, cap.p95_capacity_proxy) < 0.70 
             and r.recovery_pct >= 70 then 'Healthy Spare Capacity'
        else                               'Underutilized'
    end as efficiency_quadrant,

    -- Metadata & Quality
    r.data_quality_flag as recovery_dq_flag,
    current_timestamp() as dbt_loaded_at

from congestion_mapped c
left join recovery r 
    on c.station_complex_id = r.station_complex_id 
    and c.transit_year = r.transit_year
left join capacity cap 
    on c.station_complex_id = cap.station_complex_id
    and c.capacity_time_bucket = cap.time_bucket