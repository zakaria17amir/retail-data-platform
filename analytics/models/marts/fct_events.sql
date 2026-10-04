select
    event_date,
    event_type,
    count(*) as event_count,
    count(distinct session_id) as session_count
from {{ ref('stg_clickstream') }}
group by event_date, event_type
