-- Stage curated 1-minute page-view aggregates.
select
    window_start,
    page,
    device,
    views,
    users
from {{ source('curated', 'page_views_1m') }}
