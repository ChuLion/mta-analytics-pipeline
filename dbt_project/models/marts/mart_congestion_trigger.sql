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

aggregated as (
    select
        -- Dimensions
        station_complex_id,
        borough,
        ridership_year,
        season,
        day_of_week,
        time_period,
        
        -- Metadata
        any_value(station_name)               as station_name,
        any_value(latitude)                   as latitude,
        any_value(longitude)                  as longitude,
        
        -- Volume Metrics
        count(*)                              as observation_count,
        round(avg(hourly_ridership), 1)       as avg_hourly_ridership,
        approx_quantiles(hourly_ridership, 2)[offset(1)] as median_hourly_ridership,
        
        -- Congestion Indices
        round(avg(station_congestion_index), 3) as avg_congestion_index,
        round(avg(system_contribution_index), 3) as avg_system_index,
        
        -- FIXED: Safe Peak Hour Extraction using SAFE_OFFSET
        -- This returns NULL instead of an error if the array is empty
        approx_top_count(station_peak_hour, 1)[safe_offset(0)].value as most_common_peak_hour

    from base
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
    -- Congestion Intensity Classification
    case 
        when avg_congestion_index >= 15.0 then 'High Congestion'
        when avg_congestion_index >= 8.0  then 'Moderate'
        when avg_congestion_index >= 2.0  then 'Baseline'
        else                                  'Off-Peak'
    end                                       as congestion_intensity_tier,
    
    current_timestamp()                       as dbt_loaded_at
from aggregated