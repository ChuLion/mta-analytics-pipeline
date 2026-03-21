-- This test ensures transit_year is within the expected range (2022-2025)
-- We use a custom test because BigQuery requires explicit type handling for INT64
select
    transit_year
from {{ ref('stg_mta_ridership') }}
where transit_year not in (2022, 2023, 2024, 2025)
  and transit_year is not null