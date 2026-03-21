-- int_station_capacity.sql
-- Intermediate model: 2019 capacity benchmarks per station
-- p95 of hourly ridership used as practical capacity proxy
-- Excludes NYE/outlier events that inflate true max
-- Target: mta_silver
-- 2019 Capacity Benchmarks by Time Bucket (AM, PM, Late Night)
-- Grain: station × time_bucket

with base_2019 as (
    select
        station_complex_id,
        transit_hour,
        ridership as hourly_ridership,
        -- Three-bucket system for robust joining across all congestion slices
        case
            when transit_hour between 5  and 12 then 'AM'
            when transit_hour between 13 and 20 then 'PM'
            else                                     'Late Night'
        end as time_bucket
    from {{ ref('stg_mta_ridership_2019') }}
),

capacity_calc as (
    select
        station_complex_id,
        time_bucket,
        hourly_ridership,
        -- Window function to find the 95th percentile for each bucket
        percentile_cont(hourly_ridership, 0.95) over (
            partition by station_complex_id, time_bucket
        ) as p95_proxy_raw
    from base_2019
),

aggregated as (
    select
        station_complex_id,
        time_bucket,
        max(hourly_ridership)        as max_2019_ridership,
        round(max(p95_proxy_raw), 0) as p95_capacity_proxy
    from capacity_calc
    group by 
        station_complex_id, 
        time_bucket
)

select
    station_complex_id,
    time_bucket,
    max_2019_ridership,
    p95_capacity_proxy,
    current_timestamp()          as dbt_loaded_at
from aggregated