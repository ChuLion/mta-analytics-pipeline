-- tests/assert_ridership_not_negative.sql
-- Ensures no negative ridership values slip through staging filter
select *
from {{ ref('stg_mta_ridership') }}
where ridership < 0