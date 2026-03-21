-- tests/assert_equity_score_bounds.sql
-- Equity risk score should always be between 0 and 100
select *
from {{ ref('mart_equity_view') }}
where equity_risk_score < 0
   or equity_risk_score > 100