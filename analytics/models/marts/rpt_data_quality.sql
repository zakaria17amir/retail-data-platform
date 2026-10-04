-- _rule_metrics has one row per micro-batch, so a run's batches are summed here
select
    'silver_rule' as check_type,
    run_id,
    max(run_ts) as checked_at,
    table_name,
    rule_id as check_name,
    cast(null as varchar) as severity,
    -- duckdb sums bigint to hugeint, which external parquet stores as double
    cast(sum(rows_in) as bigint) as rows_in,
    cast(sum(rows_rejected) as bigint) as rows_rejected,
    100.0 * sum(rows_rejected) / nullif(sum(rows_in), 0) as pct_rejected,
    cast(null as boolean) as success,
    cast(null as varchar) as observed_value
from {{ ref('stg_rule_metrics') }}
group by run_id, table_name, rule_id

union all

select
    'dq_expectation' as check_type,
    run_id,
    checked_at,
    table_name,
    expectation as check_name,
    severity,
    cast(null as bigint) as rows_in,
    cast(null as bigint) as rows_rejected,
    cast(null as double) as pct_rejected,
    success,
    observed_value
from {{ ref('stg_dq_results') }}
