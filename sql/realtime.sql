-- schema ml, not olist: the olist_cdc publication covers TABLES IN SCHEMA olist only
CREATE SCHEMA IF NOT EXISTS ml;

CREATE TABLE IF NOT EXISTS ml.order_risk (
    order_id text PRIMARY KEY,
    probability double precision,
    model_version text,
    scored_at timestamptz,
    source_ts timestamptz,
    latency_ms double precision
);
