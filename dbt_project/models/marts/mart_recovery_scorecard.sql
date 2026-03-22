{{ config(
    materialized='table',
    cluster_by=["borough", "transit_year"]
) }}

-- Step 1: Aggregate ALL stations (including suspect ones) to get the true total
with station_annual as (
    select
        borough,
        ridership_year as transit_year,
        sum(monthly_ridership) as annual_ridership,
        sum(baseline_monthly_2019) as annual_baseline_2019
    from {{ ref('int_station_recovery') }}
    group by 1, 2
),

-- Step 2: Calculate Borough-level recovery
borough_final as (
    select
        'borough' as record_type,
        transit_year,
        borough,
        -- Use NULLs for station-specific columns to keep schema clean for Tableau
        cast(null as string) as station_complex_id,
        concat(borough, ' Total System') as display_name,
        
        annual_ridership,
        annual_baseline_2019,
        
        round(
            safe_divide(annual_ridership, annual_baseline_2019) * 100, 
        2) as recovery_pct,
        
        -- In the Mart, this represents the "Full Inclusion" truth
        'system_inclusive' as data_quality_flag
    from station_annual
)

select 
    *,
    -- Recovery tier at the Borough level
    case
        when recovery_pct is null then 'N/A'
        when recovery_pct >= 90   then 'Recovered'
        when recovery_pct >= 70   then 'Recovering'
        when recovery_pct >= 50   then 'Lagging'
        else                           'Critical'
    end as recovery_tier,
    
    -- In a borough-only mart, pct_vs_avg becomes a comparison 
    -- against the system-wide average (Optional but useful)
    round(
        recovery_pct - avg(recovery_pct) over (partition by transit_year), 
    2) as pct_vs_system_avg,

    current_timestamp() as dbt_loaded_at
from borough_final