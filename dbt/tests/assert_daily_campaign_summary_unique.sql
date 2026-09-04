-- Composite uniqueness: one row per (day, campaign_id).
select
    day,
    campaign_id,
    count(*) as row_count
from {{ ref('daily_campaign_summary') }}
group by day, campaign_id
having count(*) > 1
