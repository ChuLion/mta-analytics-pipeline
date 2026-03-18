-- int_station_congestion.sql
-- Intermediate model: Station-level congestion patterns by hour
-- Target: mta_silver dataset

with base as (
    select
        station_complex_id,
        station_name,
        borough,
        latitude,
        longitude,
        transit_date,
        transit_hour,
        day_of_week,
        cast(ridership as float64) as ridership
    from {{ ref('stg_mta_ridership') }}
),

-- 1. Daily totals per station (Internal Benchmark)
station_daily as (
    select
        station_complex_id,
        transit_date,
        sum(ridership)                        as daily_ridership,
        count(*)                              as hours_reported
    from base
    group by 
        station_complex_id, 
        transit_date
),

-- 2. Hourly totals (Explicit grouping to ensure accuracy)
station_hourly as (
    select
        station_complex_id,
        station_name,
        borough,
        latitude,
        longitude,
        transit_date,
        transit_hour,
        day_of_week,
        sum(ridership)                        as hourly_ridership
    from base
    group by 
        station_complex_id,
        station_name,
        borough,
        latitude,
        longitude,
        transit_date,
        transit_hour,
        day_of_week
),

-- 3. System-wide hourly profile (External Benchmark)
system_hourly as (
    select
        transit_date,
        transit_hour,
        sum(hourly_ridership)                 as system_total_ridership,
        count(distinct station_complex_id)    as active_stations
    from station_hourly
    group by 
        transit_date, 
        transit_hour
),

-- 4. Scored Metrics and Initial Dimensional Logic
scored as (
    select
        -- Identifiers
        h.station_complex_id,
        h.station_name,
        h.borough,
        h.latitude,
        h.longitude,
        h.transit_date,
        h.transit_hour,
        h.day_of_week,
        
        -- Volume and Performance Metrics
        h.hourly_ridership,
        
        -- Time Dimensions for Tableau
        extract(year from h.transit_date)       as ridership_year,
        case extract(month from h.transit_date)
            when 12 then 'Winter' when 1 then 'Winter' when 2 then 'Winter'
            when 3  then 'Spring' when 4 then 'Spring' when 5 then 'Spring'
            when 6  then 'Summer' when 7 then 'Summer' when 8 then 'Summer'
            else 'Fall'
        end                                     as season,

        -- Benchmark Indices
        round(
            safe_divide(
                h.hourly_ridership,
                safe_divide(d.daily_ridership, nullif(d.hours_reported, 0))
            ), 3
        )                                       as station_congestion_index,

        round(
            safe_divide(
                h.hourly_ridership,
                safe_divide(sh.system_total_ridership, nullif(sh.active_stations, 0))
            ), 3
        )                                       as system_contribution_index,

        -- Data Quality Flag (Placeholder at 5000 for evaluation)
        case
            when h.hourly_ridership > 5000 then 'suspect_high'
            when h.hourly_ridership < 0    then 'suspect_negative'
            else 'clean'
        end                                     as data_quality_flag

    from station_hourly h
    join station_daily d
        on h.station_complex_id = d.station_complex_id
       and h.transit_date       = d.transit_date
    join system_hourly sh
        on h.transit_date       = sh.transit_date
       and h.transit_hour       = sh.transit_hour
),

-- 5. Peak Identification (Window Function)
final_ranked as (
    select
        s.*,
        dense_rank() over (
            partition by station_complex_id, transit_date
            order by hourly_ridership desc
        )                                      as daily_peak_rank
    from scored s
)

select
    -- Core Fields
    station_complex_id,
    station_name,
    borough,
    latitude,
    longitude,
    transit_date,
    transit_hour,
    day_of_week,
    ridership_year,
    season,
    hourly_ridership,
    station_congestion_index,
    system_contribution_index,
    data_quality_flag,

    -- Unified Hour Classification (Consolidated Logic)
    case
        when daily_peak_rank = 1                        then 'Peak Hour'
        when daily_peak_rank <= 3                       then 'Near Peak'
        when transit_hour between 5  and 12             then 'AM Off Peak'
        when transit_hour between 13 and 20             then 'PM Off Peak'
        else                                                 'Overnight'
    end                                                 as hour_classification,

    -- Preserve actual peak hour for high-value insights
    case 
        when daily_peak_rank = 1 then transit_hour 
        else null 
    end                                                 as station_peak_hour,

    current_timestamp()                                 as dbt_loaded_at
from final_ranked