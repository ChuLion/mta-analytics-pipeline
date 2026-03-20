{{ config(
    materialized='table',
    cluster_by=["borough", "recovery_tier", "transit_year"]
) }}

with station_annual as (
    select
        station_complex_id,
        any_value(station_name) as station_name,
        borough,
        ridership_year,
        avg(latitude) as latitude,
        avg(longitude) as longitude,
        sum(monthly_ridership) as annual_ridership,
        sum(baseline_monthly_2019) as annual_baseline_2019,
        max(case when data_quality_flag = 'suspect_merged' then 1 else 0 end) as has_suspect_merged,
        max(case when data_quality_flag = 'suspect_split'  then 1 else 0 end) as has_suspect_split,
        max(case when data_quality_flag = 'no_baseline'    then 1 else 0 end) as has_no_baseline
    from {{ ref('int_station_recovery') }}
    group by 1, 3, 4
),

station_final as (
    select
        'station' as record_type,
        ridership_year as transit_year,
        borough,
        station_complex_id,
        station_name,
        latitude,
        longitude,
        annual_ridership,
        annual_baseline_2019,
        round(safe_divide(annual_ridership, annual_baseline_2019) * 100, 2) as recovery_pct,
        case
            when has_suspect_merged = 1 then 'suspect_merged'
            when has_suspect_split  = 1 then 'suspect_split'
            when has_no_baseline    = 1 then 'no_baseline'
            else 'clean'
        end as data_quality_flag
    from station_annual
),

borough_final as (
    select
        'borough' as record_type,
        transit_year,
        borough,
        cast(null as string) as station_complex_id,
        concat(borough, ' Average') as station_name,
        avg(latitude) as latitude,
        avg(longitude) as longitude,
        sum(annual_ridership) as annual_ridership,
        sum(annual_baseline_2019) as annual_baseline_2019,
        round(safe_divide(sum(annual_ridership), sum(annual_baseline_2019)) * 100, 2) as recovery_pct,
        'clean' as data_quality_flag
    from station_final
    where data_quality_flag = 'clean'
    group by 1, 2, 3
),

unioned_base as (
    select * from station_final
    union all
    select * from borough_final
)

select 
    *,
    case
        when recovery_pct is null then 'N/A'
        when recovery_pct >= 90  then 'Recovered'
        when recovery_pct >= 70  then 'Recovering'
        when recovery_pct >= 50  then 'Lagging'
        else                          'Critical'
    end as recovery_tier,
    round(
        avg(case when record_type = 'station' and data_quality_flag = 'clean' 
                 then recovery_pct end) 
        over (partition by borough, transit_year), 
    2) as borough_avg_recovery_pct,
    round(
        recovery_pct - avg(case when record_type = 'station' and data_quality_flag = 'clean' 
                                then recovery_pct end) 
                       over (partition by borough, transit_year), 
    2) as pct_vs_borough_avg,
    current_timestamp() as dbt_loaded_at
from unioned_base