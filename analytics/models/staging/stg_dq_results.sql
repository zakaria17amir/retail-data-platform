select
    run_id,
    -- "table" is reserved in duckdb and snowflake, so the quotes are required
    "table" as table_name,  -- noqa: RF06
    expectation,
    severity,
    success,
    observed_value,
    details,
    checked_at
from {{ latest_export(source('silver', 'dq_results')) }}
