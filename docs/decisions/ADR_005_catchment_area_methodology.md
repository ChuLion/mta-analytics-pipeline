# ADR 005: Station Catchment Area Methodology — 800m Buffer

## Status
Accepted

## Date
2026-03-13

## Context
The equity analysis (View 4) requires associating demographic data
from Census ACS with MTA station locations. The naive approach is a
point-in-polygon join: find which census tract contains the station
entrance point and assign that tract's demographics to the station.

## Problem With Point-in-Polygon
Standard approach assigns ALL demographics from ONE tract to a station.
This fails when:
  - Station entrance sits on a tract boundary
  - Multiple tracts surround a station within walking distance
  - Station has multiple entrances in different tracts

A station on the border of a wealthy tract and a low-income tract
would be assigned to only one, giving a misleading demographic profile.

## Decision: 800-Meter Catchment Area with Area Weighting

Any census tract intersecting an 800-meter circle around a station
is considered part of that station's service area, weighted by the
proportion of the buffer it covers.

800m ≈ 0.5 miles ≈ 10-minute walk (NYC pedestrian planning standard)

This is the methodology used by MTA's own equity reports and standard
urban planning practice.

## Mathematical Approach

  catchment_weight = intersection_area / buffer_area

  weighted_mean_income = Σ(income_i × weight_i) / Σ(weight_i)

Where the sum is over all census tracts intersecting the buffer.

Example:
  Tract A: buffer covers 80% of tract → weight = 0.80
  Tract B: buffer covers 35% of tract → weight = 0.35
  Tract C: buffer covers 12% of tract → weight = 0.12

  weighted_mean_income =
    (income_A × 0.80 + income_B × 0.35 + income_C × 0.12)
    ÷ (0.80 + 0.35 + 0.12)

## Multiple Entrance Handling
Discovery: Stations have multiple coordinate pairs (different physical
entrances). Station 624 had 4 distinct coordinate pairs.

Initial approach: AVG(lat/long) as centroid — incorrect, picks
arbitrary midpoint.

Solution: ST_UNION_AGG of all entrance buffers creates a single merged
polygon representing the TRUE service area of the entire station complex.

The unioned buffer:
  - Extends coverage to all entrance locations
  - Creates irregular shapes for stations with spread-out entrances
  - Captures more census tracts than any single entrance buffer

AVG(lat/long) is retained only for map pin placement in Tableau —
not used for the demographic calculation.

## BigQuery Implementation
Uses native BigQuery geography functions:
  ST_GEOGPOINT(longitude, latitude) — create point from coordinates
  ST_BUFFER(point, 800) — 800-meter radius buffer
  ST_UNION_AGG(buffers) — merge multiple entrance buffers
  ST_INTERSECTS(buffer, tract) — filter to relevant tracts only
  ST_INTERSECTION(buffer, tract) — compute overlap geometry
  ST_AREA(geometry) — calculate areas for weighting
  SAFE_DIVIDE — handle zero-population tracts (parks, airports)

Performance optimization: ST_INTERSECTS filter applied before
ST_INTERSECTION calculation. Intersection geometry only computed
for relevant tracts, not all 2,327 NYC tracts.

## Naming Note
Output column is weighted_mean_income (not weighted_median_income).
sum(income × weight) / sum(weight) is a weighted MEAN.
A true weighted median would require spatial percentile calculation —
much more complex and not warranted for this use case.

## Data Source
Census tract geometries: bigquery-public-data.geo_census_tracts.us_census_tracts_national
  - Confirmed GEOGRAPHY type ✅
  - geo_id format matches our geoid format (e.g., 36061004700) ✅
  - Covers all 5 NYC boroughs ✅

Census demographics: ACS 2023 5-year estimates via Census API
  - 2,327 NYC census tracts loaded
  - Sentinel value -666666666 handled via NULLIF() in staging
  - Covers: median_household_income, poverty_rate, car_ownership_rate,
    disability_rate, households_no_vehicle

## Results
  - Average tracts per station catchment: 7-15
  - Income range: $27K (Bronx stations) to $220K (FiDi stations)
  - Income gap between highest/lowest station: ~$192,000
  - All 5 boroughs represented with expected income distribution

## Interview Talking Point
"Rather than a simple point-in-polygon join that assigns a station to
a single census tract, I implemented an 800-meter catchment area using
BigQuery's native geospatial functions. Any census tract intersecting
the station's walkshed is included, weighted by the proportion of the
buffer area it covers.

I also discovered that stations have multiple physical entrances with
different coordinates. I used ST_UNION_AGG to merge all entrance buffers
into a single catchment polygon representing the true service area —
this is how real transit planners define station catchment areas.

The result is a weighted_mean_income and weighted_poverty_rate per
station that reflects who actually uses each station, not just who lives
in the one tract where the entrance happens to sit. This methodology
matches what MTA uses in their own equity reports."
