-- Revenue is defined once: the daily mart must reconcile with revenue-order items.
select
    rpt.revenue as rpt_daily_sales_revenue,
    items.revenue as fct_order_items_revenue
from (select coalesce(sum(revenue), 0) as revenue from {{ ref('rpt_daily_sales') }}) as rpt
cross join (
    select coalesce(sum(item_revenue), 0) as revenue
    from {{ ref('fct_order_items') }}
    where is_revenue_order
) as items
where abs(rpt.revenue - items.revenue) > 0.005
