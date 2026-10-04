-- Review Focus 1: the SCD2 joins must not drop or duplicate orders.
select
    fct.n as fct_orders_rows,
    stg.n as stg_orders_rows
from (select count(*) as n from {{ ref('fct_orders') }}) as fct
cross join (select count(*) as n from {{ ref('stg_orders') }}) as stg
where fct.n != stg.n
