-- int_station_census_join.sql
-- Intermediate model: Station-level weighted demographic profile

-- Methodology: 800-meter catchment area (approx 10-min walk)
-- Each station gets a weighted average of census tract demographics
-- where weight = (intersection area) / (buffer area)
-- This ensures stations near tract boundaries capture demographics
-- from ALL surrounding tracts proportionally.

-- Reference: Standard urban planning catchment methodology
-- Buffer: 800m ≈ 0.5 mile ≈ 10-min walk (NYC pedestrian standard)

with stations as (
    -- Aggregate multiple entrances per station into unified buffer
    -- ST_UNION_AGG merges overlapping 800m buffers from all entrances
    -- into a single catchment polygon representing the true service area
    select
        station_complex_id,
        -- Take any non-null station name (same for all entrances)
        MAX(station_name)                           as station_name,
        MAX(borough)                                as borough,
        -- Centroid of all entrances for mapping purposes
        AVG(latitude)                               as latitude,
        AVG(longitude)                              as longitude,
        -- Union all entrance buffers into single catchment area
        ST_UNION_AGG(
            ST_BUFFER(
                ST_GEOGPOINT(longitude, latitude),
                800
            )
        )                                           as station_buffer
    from {{ ref('stg_mta_ridership') }}
    where latitude is not null
      and longitude is not null
    group by station_complex_id
),

census_tracts as (
    -- Get NYC census tract geometries from BigQuery public data
    select
        geo_id,
        tract_name,
        tract_geom,
        -- Pre-calculate tract area for proportional weighting
        ST_AREA(tract_geom)                         as tract_area_sqm
    from `bigquery-public-data.geo_census_tracts.us_census_tracts_national`
    where state_fips_code = '36'
      and county_fips_code in ('005', '047', '061', '081', '085')
      and tract_geom is not null
),

census_demographics as (
    select * from {{ ref('stg_census_income') }}
),

-- Spatial join: find all tracts that intersect each station buffer
station_tract_intersections as (
    select
        s.station_complex_id,
        s.station_name,
        s.borough,
        s.latitude,
        s.longitude,
        t.geo_id,
        t.tract_area_sqm,

        -- Buffer area from unioned entrance buffers
        ST_AREA(s.station_buffer)                   as buffer_area_sqm,

        -- Intersection with unioned buffer
        ST_INTERSECTION(
            s.station_buffer,
            t.tract_geom
        )                                           as intersection_geom

    from stations s
    inner join census_tracts t
        on ST_INTERSECTS(s.station_buffer, t.tract_geom)
),

-- Calculate proportional weights (intersection area from geometry, once per row)
weighted_intersections as (
    select
        i.*,
        d.median_household_income,
        d.total_population,
        d.poverty_rate,
        d.car_ownership_rate,
        d.disability_rate,
        d.households_no_vehicle,

        -- Weight = intersection area / buffer area (area from intersection_geom, no second ST_INTERSECTION)
        safe_divide(
            ST_AREA(i.intersection_geom),
            i.buffer_area_sqm
        )                                           as catchment_weight,

        -- Alternative weight: intersection area / tract area
        safe_divide(
            ST_AREA(i.intersection_geom),
            i.tract_area_sqm
        )                                           as tract_coverage_pct

    from station_tract_intersections i
    left join census_demographics d
        on i.geo_id = d.geoid
),

-- Final aggregation: weighted average demographics per station
final as (
    select
        station_complex_id,
        station_name,
        borough,
        latitude,
        longitude,
        count(geo_id)                               as tracts_in_catchment,
        countif(median_household_income is not null) as tracts_with_demographic_data,
        sum(total_population * catchment_weight)    as catchment_population,
        safe_divide(
            sum(case when median_household_income is not null
                then median_household_income * catchment_weight end),
            sum(case when median_household_income is not null
                then catchment_weight end)
        )                                           as weighted_mean_income,
        safe_divide(
            sum(poverty_rate * catchment_weight),
            sum(catchment_weight)
        )                                           as weighted_poverty_rate,
        safe_divide(
            sum(car_ownership_rate * catchment_weight),
            sum(catchment_weight)
        )                                           as weighted_car_ownership_rate,
        safe_divide(
            sum(disability_rate * catchment_weight),
            sum(catchment_weight)
        )                                           as weighted_disability_rate,
        current_timestamp()                         as dbt_loaded_at
    from weighted_intersections
    group by 1,2,3,4,5
),

-- Separate CTE for income tier — references weighted_mean_income cleanly
final_with_tiers as (
    select
        *,
        case
            when weighted_mean_income < 40000  then 'Low Income'
            when weighted_mean_income < 75000  then 'Middle Income'
            when weighted_mean_income < 120000 then 'Upper Middle Income'
            else                                    'High Income'
        end                                         as income_tier
    from final
)

select * from final_with_tiers