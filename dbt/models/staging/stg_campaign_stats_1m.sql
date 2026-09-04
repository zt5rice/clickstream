-- Stage curated 1-minute campaign funnel stats.
select
    window_start,
    campaign_id,
    events,
    views,
    clicks,
    conversions,
    users
from {{ source('curated', 'campaign_stats_1m') }}
