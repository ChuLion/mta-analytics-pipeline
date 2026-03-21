# ADR 003: Throughput Stress Index — Period-Matched p95 Capacity Proxy

## Status
Accepted

## Date
2026-03-20

## Context
Initial implementation of mart_efficiency_matrix calculated throughput
stress as:

  throughput_stress_index = avg_hourly_ridership / p95_capacity_proxy

Where p95_capacity_proxy was computed across ALL hours of 2019 for
each station.

## Problem Discovered
Sanity check revealed avg_stress values of 2.488 — meaning stations
appeared to be operating at 248% of capacity. This is impossible and
would immediately destroy executive trust in the dashboard.

Root cause: The p95 denominator was calculated across all 24 hours
including overnight hours (1am, 2am, 3am) with near-zero ridership.
This pulled the p95 DOWN dramatically. When peak hours were compared
against this deflated benchmark, the index exploded above 1.0.

Example of the problem:
  Grand Central p95 (all hours, including 3am) = 800 riders
  Grand Central Peak Tuesday avg = 8,000 riders
  Index = 10.0 ← meaningless, destroys credibility

## Decision
Calculate p95_capacity_proxy matched to the same time period as
the comparison metric. This ensures apples-to-apples comparison.

## Implementation
int_station_capacity.sql updated to:
1. Bucket hours into AM / PM / Late Night (matching int_station_congestion)
2. Calculate p95 within each time bucket per station
3. Join capacity to mart_efficiency_matrix on station_complex_id + time_bucket

mart_efficiency_matrix.sql updated to:
1. Map hour_classification to time_bucket for join compatibility
   - Peak Hour / Near Peak / AM Off Peak → AM
   - PM Off Peak → PM
   - Overnight → Late Night
2. Join capacity on time_bucket instead of station alone

## Expected Index Range After Fix
  0.50 = station running at 50% of its typical 2019 peak period level
  1.00 = back to 2019 peak period levels (fully recovered)
  1.10 = 10% busier than 2019 peak period (exceeding pre-COVID)

## Executive Framing
"This index compares current ridership during each time period against
the same time period's performance in 2019. A score of 1.0 means fully
recovered to pre-COVID capacity utilization. Above 1.0 means operating
busier than the pre-COVID baseline for that time of day."

## Why p95 Instead of Maximum
The absolute maximum ridership for a station might represent a unique
event — New Year's Eve, a major concert, emergency rerouting. These
are not representative of operational capacity.

p95 represents the top 5% of normal operating conditions — what the
station regularly handles at its busiest. This is a more defensible
and stable capacity proxy.

## Alternatives Considered
- True capacity from MTA engineering data — not publicly available
- Turnstile count × throughput rate — requires loading deprecated data
- Fixed maximum per station type — arbitrary, not data-driven
- All-hours p95 (original) — rejected, produces misleading results

## Interview Talking Point
"The initial throughput stress index produced values above 200%,
which would immediately raise questions in an executive presentation.
The root cause was comparing peak-hour ridership against a p95 baseline
that included overnight hours — an apples-to-oranges comparison.
The fix was period-matched capacity: calculate the 2019 p95 for AM hours,
PM hours, and overnight separately, then compare each time period only
against its own historical baseline. This is a subtle but important
methodological decision — the kind that separates analysis that builds
trust from analysis that raises questions. I documented both the problem
and the fix so any engineer inheriting this project understands why the
capacity join includes a time_bucket key."
