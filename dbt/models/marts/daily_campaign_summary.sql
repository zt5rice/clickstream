-- Daily campaign funnel summary built from curated 1-minute windows.
select
    (window_start at time zone 'UTC')::date as day,
    campaign_id,
    sum(events)::bigint as events,
    sum(views)::bigint as views,
    sum(clicks)::bigint as clicks,
    sum(conversions)::bigint as conversions,
    count(*)::bigint as window_count
from {{ ref('stg_campaign_stats_1m') }}
group by 1, 2
