-- int_station_utilization.sql
-- Intermediate model: Station utilization and anomaly detection
--
-- KEY DESIGN DECISION: Source data only publishes rows when ridership > 0.
-- Missing hours are absent entirely, not zeroed. A complete hour spine
-- is generated via CROSS JOIN + GENERATE_ARRAY(0,23) to make gaps explicit.
-- This converts implicit gaps into true zeros enabling consecutive zero
-- detection via window functions.
--
-- THREE-LAYER ANOMALY DETECTION:
--   Layer 1: Consecutive zero detection (rolling 3-hour window on spine)
--   Layer 2: Historical baseline comparison (station × hour profile)
--   Layer 3: Time-of-day context (overnight zeros expected)
--
-- Target: mta_silver dataset

with station_dates as (
    -- Distinct station × date combinations that exist in the data
    -- Used as the base for spine generation
    select
        station_complex_id,
        station_name,
        borough,
        avg(latitude)                               as latitude,
        avg(longitude)                              as longitude,
        transit_date
    from {{ ref('stg_mta_ridership') }}
    group by
        station_complex_id,
        station_name,
        borough,
        transit_date
),

hour_spine as (
    -- Generate all 24 hours for every station × date combination
    -- This ensures missing hours appear as explicit gaps, not absent rows
    -- CROSS JOIN with GENERATE_ARRAY creates one row per hour
    select
        s.station_complex_id,
        s.station_name,
        s.borough,
        s.latitude,
        s.longitude,
        s.transit_date,
        hour_of_day                                 as transit_hour
    from station_dates s
    cross join unnest(generate_array(0, 23))        as hour_of_day
),

fare_consolidated as (
    -- Consolidate fare classes into single hourly ridership per station
    -- Only rows with ridership > 0 exist here (sparse event data)
    select
        station_complex_id,
        transit_date,
        transit_hour,
        sum(ridership)                              as hourly_ridership
    from {{ ref('stg_mta_ridership') }}
    group by
        station_complex_id,
        transit_date,
        transit_hour
),

hourly_with_gaps as (
    -- LEFT JOIN actual ridership onto complete spine
    -- Missing hours become explicit NULL → coalesced to 0
    -- is_gap_hour = 1 means station existed but had zero riders that hour
    select
        s.station_complex_id,
        s.station_name,
        s.borough,
        s.latitude,
        s.longitude,
        s.transit_date,
        s.transit_hour,
        coalesce(f.hourly_ridership, 0)             as hourly_ridership,
        case
            when f.hourly_ridership is null then 1
            else 0
        end                                         as is_gap_hour
    from hour_spine s
    left join fare_consolidated f
        on s.station_complex_id = f.station_complex_id
        and s.transit_date      = f.transit_date
        and s.transit_hour      = f.transit_hour
),

historical_profile as (
    -- Historical baseline per station × hour
    -- Built on spine data so zeros are included in the average
    -- historical_zero_rate: % of days this hour had zero ridership
    -- High rate = structurally low demand (expected)
    -- Low rate + current zero = anomaly (possible disruption)
    select
        station_complex_id,
        transit_hour,
        avg(hourly_ridership)                       as historical_avg_this_hour,
        round(
            safe_divide(
                countif(hourly_ridership = 0),
                count(*)
            ), 4
        )                                           as historical_zero_rate,
        count(*)                                    as days_observed
    from hourly_with_gaps
    group by
        station_complex_id,
        transit_hour
),

hourly_stats as (
    -- Apply window functions on complete spine data
    -- Consecutive zeros now meaningful — gaps are explicit zeros
    select
        h.*,
        p.historical_avg_this_hour,
        p.historical_zero_rate,
        p.days_observed,

        -- Layer 1: Rolling 3-hour zero window
        -- Counts zero-ridership hours in current + 2 preceding hours
        countif(h.hourly_ridership = 0) over (
            partition by h.station_complex_id, h.transit_date
            order by h.transit_hour
            rows between 2 preceding and current row
        )                                           as consecutive_zero_count

    from hourly_with_gaps h
    left join historical_profile p
        on h.station_complex_id = p.station_complex_id
        and h.transit_hour      = p.transit_hour
),

utilization_logic as (
    -- Layer 2 + 3: Classify each station-hour
    -- Combines historical baseline with time-of-day context
    select
        *,
        case
            -- Active service
            when hourly_ridership > 0
                then 'Active'

            -- Layer 3: Overnight zeros are structurally expected
            when hourly_ridership = 0
             and transit_hour between 0 and 4
                then 'Expected Low'

            -- Layer 2: Historical pattern confirms zero is normal
            -- Station historically has zero >50% of the time this hour
            when hourly_ridership = 0
             and historical_zero_rate >= 0.5
                then 'Expected Low'

            -- Layer 1 + 2: Zero during typically active hours
            -- with 2+ hour streak → likely real disruption
            when hourly_ridership = 0
             and historical_avg_this_hour > 50
             and consecutive_zero_count >= 2
                then 'Possible Disruption'

            -- Isolated zero during typically active hours
            when hourly_ridership = 0
             and historical_avg_this_hour > 50
                then 'Anomalous Zero'

            -- Zero but history also shows low demand
            else 'Expected Low'
        end                                         as utilization_status

    from hourly_stats
),

final as (
    -- Daily aggregation: roll up hourly grain to station × day
    -- ~500 stations × 1,000 days = ~500K rows (Tableau friendly)
    select
        station_complex_id,
        station_name,
        borough,
        latitude,
        longitude,
        transit_date,
        extract(year from transit_date)             as ridership_year,
        case extract(month from transit_date)
            when 12 then 'Winter' when 1 then 'Winter' when 2 then 'Winter'
            when 3  then 'Spring' when 4 then 'Spring' when 5 then 'Spring'
            when 6  then 'Summer' when 7 then 'Summer' when 8 then 'Summer'
            else 'Fall'
        end                                         as season,

        -- Utilization metrics
        countif(hourly_ridership > 0)               as active_hours,
        countif(is_gap_hour = 1)                    as gap_hours,
        round(
            safe_divide(
                countif(hourly_ridership > 0),
                24
            ), 3
        )                                           as utilization_rate,

        -- Demand intensity: riders per active hour
        -- High = efficient utilization of active service hours
        round(
            safe_divide(
                sum(hourly_ridership),
                countif(hourly_ridership > 0)
            ), 2
        )                                           as demand_intensity,

        -- Peak window utilization
        countif(
            transit_hour between 5 and 12
            and hourly_ridership > 0
        )                                           as am_active_hours,
        countif(
            transit_hour between 13 and 20
            and hourly_ridership > 0
        )                                           as pm_active_hours,

        -- Anomaly detection counts
        countif(utilization_status = 'Possible Disruption')
                                                    as disruption_hours,
        countif(utilization_status = 'Anomalous Zero')
                                                    as anomalous_zero_hours,
        countif(utilization_status = 'Expected Low')
                                                    as expected_low_hours,

        -- Consecutive zero severity
        max(consecutive_zero_count)                 as max_consecutive_zeros,

        -- Total daily ridership
        sum(hourly_ridership)                       as daily_ridership,

        -- Audit
        current_timestamp()                         as dbt_loaded_at

    from utilization_logic
    group by
        station_complex_id,
        station_name,
        borough,
        latitude,
        longitude,
        transit_date,
        extract(year from transit_date),
        case extract(month from transit_date)
            when 12 then 'Winter' when 1 then 'Winter' when 2 then 'Winter'
            when 3  then 'Spring' when 4 then 'Spring' when 5 then 'Spring'
            when 6  then 'Summer' when 7 then 'Summer' when 8 then 'Summer'
            else 'Fall'
        end
)

select * from final