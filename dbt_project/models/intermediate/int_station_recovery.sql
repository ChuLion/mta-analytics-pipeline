-- int_station_recovery.sql
-- Intermediate model: monthly ridership recovery vs 2019 baseline
-- Target: mta_silver dataset (routed via dbt_project.yml)

with ridership_monthly as (
    select
        station_complex_id,
        station_name,
        borough,
        avg(latitude)                               as latitude,
        avg(longitude)                              as longitude,
        extract(year from transit_date)             as ridership_year,
        extract(month from transit_date)            as ridership_month,
        -- For sorting and Tableau date axis
        date_trunc(transit_date, month)             as ridership_month_date,
        sum(ridership)                              as monthly_ridership
    from {{ ref('stg_mta_ridership') }}
    group by
        station_complex_id,
        station_name,
        borough,
        extract(year from transit_date),
        extract(month from transit_date),
        date_trunc(transit_date, month)
),

baseline_monthly_2019 as (
    -- 2019 monthly baseline for seasonal comparison
    select
        station_complex_id,
        extract(month from transit_date)            as ridership_month,
        sum(ridership)                              as baseline_monthly_2019
    from {{ ref('stg_mta_ridership_2019') }}
    group by
        station_complex_id,
        extract(month from transit_date)
),

baseline_annual_2019 as (
    -- Annual baseline for overall recovery context
    select
        station_complex_id,
        sum(ridership)                              as baseline_annual_2019
    from {{ ref('stg_mta_ridership_2019') }}
    group by station_complex_id
),

final_calculations as (
    select
        curr.*,
        base_m.baseline_monthly_2019,
        base_a.baseline_annual_2019,
        
        -- Month-over-same-month-2019 recovery
        round(
            safe_divide(curr.monthly_ridership, base_m.baseline_monthly_2019) * 100, 2
        ) as monthly_recovery_pct,

        -- Window function for momentum (Previous Month)
        lag(curr.monthly_ridership) over (
            partition by curr.station_complex_id
            order by curr.ridership_month_date
        ) as prev_month_ridership

    from ridership_monthly curr
    left join baseline_monthly_2019 base_m
        on curr.station_complex_id = base_m.station_complex_id
        and curr.ridership_month = base_m.ridership_month
    left join baseline_annual_2019 base_a
        on curr.station_complex_id = base_a.station_complex_id
)

select
    *,
    
    -- Month over previous month (momentum) calculation
    round(
        safe_divide(
            monthly_ridership - prev_month_ridership, 
            prev_month_ridership
        ) * 100, 2
    ) as month_over_month_pct,

    -- DATA QUALITY FLAG
    case
        when monthly_recovery_pct > 200 then 'suspect_merged'
        when monthly_recovery_pct < 25  then 'suspect_split'
        when baseline_monthly_2019 is null then 'no_baseline'
        else 'clean'
    end as data_quality_flag,

    -- SEASON CLASSIFICATION
    case ridership_month
        when 12 then 'Winter'
        when 1  then 'Winter'
        when 2  then 'Winter'
        when 3  then 'Spring'
        when 4  then 'Spring'
        when 5  then 'Spring'
        when 6  then 'Summer'
        when 7  then 'Summer'
        when 8  then 'Summer'
        else         'Fall'
    end as season,

    -- RECOVERY TIER
    case
        when monthly_recovery_pct >= 90 then 'Recovered'
        when monthly_recovery_pct >= 70 then 'Recovering'
        when monthly_recovery_pct >= 50 then 'Lagging'
        else 'Critical'
    end as recovery_tier,

    -- Audit
    current_timestamp() as dbt_loaded_at

from final_calculations