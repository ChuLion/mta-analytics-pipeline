-- stg_mta_turnstile_2019.sql
-- Staging model for 2019 MTA turnstile data
-- Source: mta_bronze.mta_turnstile_2019
-- Performs: column renaming, type casting, basic cleaning,
--           and converts cumulative entries into per-interval ridership
-- Target: mta_silver dataset

with source as (
    select * from {{ source('mta_bronze', 'mta_turnstile_2019') }}
),

lagged as (
    select
        c_a,
        unit,
        scp,
        station,
        line_name,
        division,
        date,
        time,
        description,
        cast(entries as int64) as entries_cumulative,
        cast(exits as int64)   as exits_cumulative,

        -- Previous cumulative values (per turnstile)
        lag(cast(entries as int64)) over (
            partition by c_a, unit, scp, station
            order by
                timestamp(
                    datetime(
                        cast(date as date),
                        cast(time as time)
                    )
                )
        ) as prev_entries_cumulative,
        lag(cast(exits as int64)) over (
            partition by c_a, unit, scp, station
            order by
                timestamp(
                    datetime(
                        cast(date as date),
                        cast(time as time)
                    )
                )
        ) as prev_exits_cumulative

    from source
    where upper(description) = 'REGULAR'
),

renamed as (
    select
        -- Turnstile identifiers
        cast(c_a as string)                                  as control_area,
        cast(unit as string)                                 as unit,
        cast(scp as string)                                  as scp,
        initcap(station)                                     as station_name,
        initcap(line_name)                                   as line_name,
        initcap(division)                                    as division,

        -- Timestamps
        cast(date as date)                                   as turnstile_date,
        cast(time as string)                                 as turnstile_time_raw,
        timestamp(
            datetime(
                cast(date as date),
                cast(time as time)
            )
        )                                                    as turnstile_timestamp,

        -- Raw cumulative counters
        entries_cumulative,
        exits_cumulative,

        -- Previous cumulative values (per turnstile)
        prev_entries_cumulative,
        prev_exits_cumulative,

        -- Per-interval deltas (handle resets, outliers, and bad data)
        case
            when prev_entries_cumulative is null then null
            when entries_cumulative < prev_entries_cumulative then null  -- reset or counter rollover
            when (entries_cumulative - prev_entries_cumulative) > {{ var('max_turnstile_entries_per_interval') }}  then null  -- physical impossibility
            else entries_cumulative - prev_entries_cumulative
        end                                                  as entries_delta,
        case
            when prev_exits_cumulative is null then null
            when exits_cumulative < prev_exits_cumulative then null      -- reset or counter rollover
            else exits_cumulative - prev_exits_cumulative
        end                                                  as exits_delta,

        -- Derived ridership (entries-only, to match modern dataset definition)
        case
            when prev_entries_cumulative is null then null
            when entries_cumulative < prev_entries_cumulative then null
            when (entries_cumulative - prev_entries_cumulative) > 50000 then null  -- physical impossibility
            else entries_cumulative - prev_entries_cumulative
        end                                                  as ridership,

        -- Audit
        current_timestamp()                                  as dbt_loaded_at

    from lagged
)

select * from renamed
