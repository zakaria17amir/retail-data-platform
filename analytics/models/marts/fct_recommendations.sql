with clicked as (
    select distinct
        session_id,
        product_id,
        rank,
        event_ts_utc
    from {{ ref('stg_clickstream') }}
    where event_type = 'recommendation_clicked'
)

select
    shown.event_id,
    shown.session_id,
    shown.product_id,
    shown.rank,
    shown.rec_model_version as model_version,
    shown.rec_strategy as strategy,
    shown.event_date,
    shown.event_ts_utc,
    clicked.session_id is not null as is_clicked
from {{ ref('stg_clickstream') }} as shown
left join clicked
    on
        shown.session_id = clicked.session_id
        and shown.product_id = clicked.product_id
        and shown.rank = clicked.rank
        and shown.event_ts_utc = clicked.event_ts_utc
where shown.event_type = 'recommendation_shown'
