-- stg_mta_ridership_2019.sql
-- Staging model for 2019 MTA hourly ridership baseline
-- Source: mta_bronze.mta_ridership_2019
-- Schema identical to stg_mta_ridership (2022-2024)
-- Switched from legacy turnstile dataset (xfn5-qji9) to
-- hourly ridership dataset (t69i-h2me) for schema consistency
-- Direct join on station_complex_id eliminates name mapping complexity

with source as (
    select * from {{ source('mta_bronze', 'mta_ridership_2019') }}
),

renamed as (
    select
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
        cast(station_complex_id as string)                  as station_complex_id,
        initcap(station_complex)                            as station_name,
        initcap(borough)                                    as borough,
        cast(latitude as float64)                           as latitude,
        cast(longitude as float64)                          as longitude,
        lower(transit_mode)                                 as transit_mode,
        lower(payment_method)                               as payment_method,
        lower(fare_class_category)                          as fare_class,
        cast(ridership as float64)                          as ridership,
        cast(transfers as float64)                          as transfers,
        current_timestamp()                                 as dbt_loaded_at
    from source
    where
        transit_timestamp is not null
        and station_complex_id is not null
        and ridership is not null
        and cast(ridership as float64) >= 0
)

select * from renamed