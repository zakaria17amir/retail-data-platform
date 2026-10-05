select
    run_id,
    -- "table" is reserved in duckdb and snowflake, so the quotes are required
    "table" as table_name,  -- noqa: RF06
    rule_id,
    rows_in,
    rows_rejected,
    pct_rejected,
    run_ts
from {{ latest_export(source('silver', 'rule_metrics')) }}
