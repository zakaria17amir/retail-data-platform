select
    review_pk,
    review_id,
    order_id,
    review_score,
    review_comment_title,
    review_comment_message,
    _source_lsn,
    _source_ts,
    _is_deleted,
    review_creation_ts_local,
    review_creation_ts_utc,
    review_answer_ts_local,
    review_answer_ts_utc,
    _silver_loaded_at
from {{ latest_export(source('silver', 'order_reviews')) }}
