select
    session_date,
    count(*) as sessions,
    sum(case when product_view_count > 0 then 1 else 0 end) as product_view_sessions,
    sum(case when add_to_cart_count > 0 then 1 else 0 end) as add_to_cart_sessions,
    sum(case when has_checkout then 1 else 0 end) as checkout_sessions,
    sum(case when has_checkout then 1 else 0 end) / count(*) as conversion_rate
from {{ ref('fct_sessions') }}
group by session_date
