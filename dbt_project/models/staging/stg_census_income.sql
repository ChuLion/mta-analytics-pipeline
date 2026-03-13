-- stg_census_income.sql
-- Staging model for NYC census tract income and related metrics
-- Source: mta_bronze.census_nyc_tracts
-- Performs: column renaming, type casting, basic cleaning
--           and replaces sentinel values in income columns
-- Target: mta_silver dataset

with source as (
    select * from {{ source('mta_bronze', 'census_nyc_tracts') }}
),

renamed as (
    select
        -- Geography / identifiers
        cast(geoid as string)                                as geoid,
        initcap(borough)                                     as borough,
        cast(county_fips as string)                          as county_fips,

        -- Core population + households
        cast(total_population as int64)                      as total_population,
        cast(total_households as int64)                      as total_households,
        cast(households_no_vehicle as int64)                 as households_no_vehicle,

        -- Income (sentinel -666666666 -> NULL)
        cast(
            nullif(cast(median_household_income as int64), -666666666)
            as int64
        )                                                    as median_household_income,

        -- Population subgroups
        cast(population_in_poverty as int64)                 as population_in_poverty,
        cast(population_with_disability as int64)            as population_with_disability,

        -- Rates / derived ratios
        cast(car_ownership_rate as float64)                  as car_ownership_rate,
        cast(poverty_rate as float64)                        as poverty_rate,
        cast(disability_rate as float64)                     as disability_rate,

        -- Audit
        cast(loaded_at as timestamp)                         as loaded_at,
        current_timestamp()                                  as dbt_loaded_at

    from source
)

select * from renamed
