-- Composite uniqueness: one row per (day, page, device).
select
    day,
    page,
    device,
    count(*) as row_count
from {{ ref('daily_page_summary') }}
group by day, page, device
having count(*) > 1
