-- mart_equity_view.sql
-- Gold Layer: Service Justice Dashboard (View 5)
-- The "Triple Threat" model combining:
--   WHO is affected    → Census demographics (income, poverty, disability)
--   HOW MUCH they need → Demand intensity (riders per active hour)
--   ARE WE FAILING     → Disruption hours, utilization gaps
--
-- Grain: station × year
-- This grain is intentional — equity analysis is annual,
-- not hourly. The story is structural, not operational.

{{ config(
    materialized='table',
    cluster_by=["borough", "transit_year", "equity_risk_tier"]
) }}

with recovery as (
    select
        station_complex_id,
        station_name,
        borough,
        latitude,
        longitude,
        transit_year,
        annual_ridership,
        recovery_pct,
        borough_avg_recovery_pct,
        data_quality_flag
    from {{ ref('mart_recovery_scorecard') }}
    where record_type = 'station'
),

utilization as (
    select
        station_complex_id,
        ridership_year                              as transit_year,
        round(avg(demand_intensity), 1)             as avg_demand_intensity,
        -- Normalizing disruption burden per 1k active hours to compare small vs large hubs
        round(
            safe_divide(
                sum(disruption_hours) * 1000,
                sum(active_hours)
            ), 2
        )                                           as disruption_rate_per_1k_hours
    from {{ ref('int_station_utilization') }}
    group by 1, 2
),

demographics as (
    select
        station_complex_id,
        weighted_mean_income,
        weighted_poverty_rate,
        income_tier
    from {{ ref('int_station_census_join') }}
),

combined as (
    select
        r.station_complex_id,
        r.station_name,
        r.borough,
        r.latitude,
        r.longitude,
        r.transit_year,
        r.annual_ridership,
        r.recovery_pct,
        r.borough_avg_recovery_pct,
        u.avg_demand_intensity,
        u.disruption_rate_per_1k_hours,
        d.weighted_mean_income,
        d.weighted_poverty_rate,
        d.income_tier,
        r.data_quality_flag
    from recovery r
    left join utilization u
        on r.station_complex_id = u.station_complex_id
        and r.transit_year      = u.transit_year
    left join demographics d
        on r.station_complex_id = d.station_complex_id
),

final_scores as (
    select
        *,
        -- EQUITY RISK SCORE (0-100)
        -- Composite score combining three dimensions of systemic vulnerability
        round(
            -- Component 1: Recovery gap (max 33 points)
            -- Stations lagging their borough by 20+ points receive full score
            (least(safe_divide(greatest(borough_avg_recovery_pct - recovery_pct, 0), 20.0), 1) * 33)
            + 
            -- Component 2: Disruption burden (max 33 points)
            -- 10+ disruptions per 1k active hours (approx 1% of service) = full score
            (least(safe_divide(disruption_rate_per_1k_hours, 10.0), 1) * 33)
            + 
            -- Component 3: Income vulnerability (max 34 points)
            -- Below $40K median income = full score; scales linearly to $60K
            (least(safe_divide(greatest(60000 - weighted_mean_income, 0), 20000.0), 1) * 34),
        1) as equity_risk_score
    from combined
    where data_quality_flag = 'clean'
)

select
    *,
    -- Risk Tiering for Dashboard Filtering and Map Color Coding
    case
        when equity_risk_score >= 60 then 'High Risk'
        when equity_risk_score >= 35 then 'Moderate Risk'
        else                              'Low Risk'
    end as equity_risk_tier,
    current_timestamp() as dbt_loaded_at
from final_scores