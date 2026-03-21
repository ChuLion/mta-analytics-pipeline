# ADR 004: Hour Spine for Utilization Anomaly Detection

## Status
Accepted

## Date
2026-03-19

## Context
int_station_utilization was designed to detect service disruptions
using a three-layer anomaly detection approach:
  Layer 1: Consecutive zero hour detection (rolling 3-hour window)
  Layer 2: Historical baseline comparison (station × hour profile)
  Layer 3: Time-of-day context (overnight zeros expected)

Initial implementation applied window functions simultaneously with
GROUP BY aggregation to consolidate fare classes and detect zeros.

## Problem Discovered
Sanity check results:
  total_disruption_hours: 0
  zero_ridership_days: 0
  max_zero_window: 0

The anomaly detection was completely non-functional.

## Root Cause — Two Bugs

### Bug 1: No Zeros After SUM
Initial assumption: zero-ridership hours appear as rows with ridership = 0
Reality: The source data (wujg-7c2s) only publishes rows when ridership > 0

MTA does not publish a row for every station × hour × fare class.
They only publish rows when someone actually rode. A station at 3am
with zero riders has NO ROW AT ALL — not a row with ridership = 0.

This is the fundamental difference between:
  Dense time series: every interval has a row (zeros explicit)
  Sparse event data: only active intervals have rows (zeros implicit)

MTA ridership data is sparse event data.

### Bug 2: Window Function Order of Operations
countif(sum(ridership) = 0) inside a window function applied to a
GROUP BY query is evaluated after aggregation. The aggregation
eliminates zero-fare-class rows before the window function can detect
them. Even if zeros existed, this approach would miss them.

## Solution: Generate a Complete Hour Spine

To detect gaps, a complete hour spine is generated via:
  CROSS JOIN with GENERATE_ARRAY(0, 23)

This creates all 24 hours for every station × date combination.
Actual ridership is LEFT JOINed onto the spine.
Missing hours become explicit NULLs → COALESCE to 0.

Now hourly_ridership = 0 is meaningful:
  "This station existed this day but had no riders this hour"

The consecutive zero window function works correctly on spine data.

## Implementation
CTE progression in int_station_utilization.sql:
  1. station_dates: distinct station × date combinations
  2. hour_spine: CROSS JOIN GENERATE_ARRAY(0,23) → all 24 hours
  3. fare_consolidated: aggregate fare classes separately
  4. hourly_with_gaps: LEFT JOIN spine → ridership
  5. historical_profile: baseline built on spine (includes zeros)
  6. hourly_stats: window functions on complete data
  7. utilization_logic: three-layer classification
  8. final: daily aggregation

## Results After Fix
  total_disruption_hours (2022): 18,111
  total_disruption_hours (2023): 21,562
  total_disruption_hours (2024): 23,320
  max_zero_window: 3

Disruption intensity increasing year over year:
  2022: 17.7 disruption hours per million riders
  2023: 18.5 disruption hours per million riders
  2024: 19.2 disruption hours per million riders

Brooklyn accounts for ~50% of all system disruptions.
Bronx disruptions spiked 145% from 2022 to 2023 (known capital work).

## Key Limitation
The model measures SERVICE UTILIZATION not SERVICE FREQUENCY.
True frequency requires GTFS schedule data (planned trips per hour).
Zero ridership hours are classified using historical pattern matching
as a proxy — not ground truth.

The disruption_hours metric identifies hours that were:
  - Historically active (historical_avg > 50 riders)
  - Currently showing zero ridership
  - Part of a 2+ hour consecutive gap

This is a strong proxy but not a definitive measurement.

## Interview Talking Point
"The initial utilization model returned zero disruption hours across
three years — clearly wrong. The root cause was a fundamental assumption
about the data structure: I assumed zero-ridership hours would appear
as rows with ridership = 0. The MTA source data is sparse event data —
only rows where ridership occurred are published. Missing hours are
absent entirely, not zeroed.

The fix required generating a complete 24-hour spine using CROSS JOIN
with GENERATE_ARRAY, then LEFT JOINing actual ridership onto it. This
converts implicit gaps into explicit zeros, making consecutive zero
detection meaningful. This is a critical pattern for anyone working
with event-driven data sources — the difference between sparse and
dense time series determines whether your anomaly detection works at all.

After the fix, the model detected 18,000-23,000 disruption hours
annually with a concerning trend: disruption intensity is increasing
even as ridership recovers, suggesting the system is under growing
stress."
