-- Daily page/device summary built from curated 1-minute windows.
-- `users` is intentionally excluded: summing per-window approximate distinct
-- counts would over-count daily uniques, so we expose exact views instead.
select
    (window_start at time zone 'UTC')::date as day,
    page,
    device,
    sum(views)::bigint as views,
    count(*)::bigint as window_count
from {{ ref('stg_page_views_1m') }}
group by 1, 2, 3
