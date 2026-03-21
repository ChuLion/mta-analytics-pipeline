{{ config(
    materialized='table',
    cluster_by=["borough", "day_of_week", "time_period", "ridership_year"]
) }}

with base as (
    select
        station_complex_id,
        station_name,
        borough,
        latitude,
        longitude,
        ridership_year,
        season,
        day_of_week,
        hour_classification as time_period,
        hourly_ridership,
        station_congestion_index,
        system_contribution_index,
        station_peak_hour
    from {{ ref('int_station_congestion') }}
),

capacity as (
    select 
        station_complex_id,
        p95_capacity_proxy 
    from {{ ref('int_station_capacity') }}
),

aggregated as (
    select
        -- Dimensions
        b.station_complex_id,
        b.borough,
        b.ridership_year,
        b.season,
        b.day_of_week,
        b.time_period,
        
        -- Metadata
        any_value(b.station_name)               as station_name,
        any_value(b.latitude)                   as latitude,
        any_value(b.longitude)                  as longitude,
        any_value(cap.p95_capacity_proxy)       as p95_capacity_ceiling,
        
        -- Volume Metrics
        count(*)                                as observation_count,
        round(avg(b.hourly_ridership), 1)       as avg_hourly_ridership,
        approx_quantiles(b.hourly_ridership, 2)[offset(1)] as median_hourly_ridership,
        
        -- Congestion Indices (Temporal)
        round(avg(b.station_congestion_index), 3) as avg_congestion_index,
        
        -- Stress Index (Physical/Throughput)
        -- Comparing current demand against the station's own 2019 p95 ceiling
        round(
            safe_divide(avg(b.hourly_ridership), any_value(cap.p95_capacity_proxy)), 
        3) as throughput_stress_index,

        -- SAFE_OFFSET prevents crash if all values are NULL
        approx_top_count(b.station_peak_hour, 1)[safe_offset(0)].value as most_common_peak_hour

    from base b
    left join capacity cap on b.station_complex_id = cap.station_complex_id
    group by 
        station_complex_id,
        borough,
        ridership_year,
        season,
        day_of_week,
        time_period
)

select
    *,
    -- Classification 1: Intensity (Relative to own station baseline)
    case 
        when avg_congestion_index >= 15.0 then 'High Congestion'
        when avg_congestion_index >= 8.0  then 'Moderate'
        when avg_congestion_index >= 2.0  then 'Baseline'
        else                                  'Off Peak'
    end as congestion_intensity_tier,

    -- Classification 2: Stress (Relative to 2019 physical limit)
    case
        when throughput_stress_index >= 0.90 then 'At Capacity'
        when throughput_stress_index >= 0.70 then 'High Stress'
        when throughput_stress_index >= 0.50 then 'Moderate Stress'
        else                                      'Low Stress'
    end as throughput_stress_tier,
    
    current_timestamp() as dbt_loaded_at
from aggregated