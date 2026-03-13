-- stg_mta_ridership_2025.sql
-- Staging model for 2025 MTA hourly ridership data
-- Source: mta_bronze.mta_ridership_2025_incremental
-- Performs: column renaming, type casting, basic cleaning
-- Target: mta_silver dataset
-- stg_mta_ridership_2025.sql
{{ config(
    materialized = 'incremental',
    unique_key = 'surrogate_key',
    on_schema_change = 'sync_all_columns'
) }}
with source as (
    select * from {{ source('mta_bronze', 'mta_ridership_2025_incremental') }}
),

renamed as (
    select
        -- Timestamps
        cast(transit_timestamp as timestamp)                as transit_timestamp,
        cast(transit_timestamp as date)                     as transit_date,
        extract(hour from cast(transit_timestamp as timestamp))
                                                            as transit_hour,
        extract(dayofweek from cast(transit_timestamp as timestamp))
                                                            as day_of_week,
        extract(year from cast(transit_timestamp as timestamp))
                                                            as transit_year,
        extract(month from cast(transit_timestamp as timestamp))
                                                            as transit_month,

        -- Station identifiers
        cast(station_complex_id as string)                  as station_complex_id,
        initcap(station_complex)                            as station_name,
        initcap(borough)                                    as borough,

        -- Geography
        cast(latitude as float64)                           as latitude,
        cast(longitude as float64)                          as longitude,

        -- Transit mode
        lower(transit_mode)                                 as transit_mode,
        lower(payment_method)                               as payment_method,
        lower(fare_class_category)                          as fare_class,

        -- Metrics
        cast(ridership as float64)                          as ridership,
        cast(transfers as float64)                          as transfers,

        -- Audit
        current_timestamp()                                 as dbt_loaded_at

    from source
    where
        -- Remove nulls in critical fields
        transit_timestamp is not null
        and station_complex_id is not null
        and ridership is not null
        -- Remove negative ridership
        and cast(ridership as float64) >= 0
)

select * from renamed
