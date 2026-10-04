select
    review_pk,
    review_id,
    order_id,
    review_score,
    cast(review_creation_ts_local as date) as review_creation_date,
    review_creation_ts_utc,
    review_answer_ts_utc
from {{ ref('stg_order_reviews') }}
