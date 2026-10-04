-- schema rag, not olist: the olist_cdc publication covers TABLES IN SCHEMA olist only
CREATE EXTENSION IF NOT EXISTS vector;
CREATE SCHEMA IF NOT EXISTS rag;

-- chunk_id = hash(source, product_id, text), so re-indexing unchanged text is a no-op
CREATE TABLE IF NOT EXISTS rag.chunks (
    chunk_id text PRIMARY KEY,
    source text NOT NULL CHECK (source IN ('product_doc', 'review_summary', 'review')),
    product_id text NOT NULL,
    category text,
    text text NOT NULL,
    embedding vector(1024) NOT NULL,
    tsv tsvector GENERATED ALWAYS AS (to_tsvector('simple', text)) STORED,
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS chunks_embedding_hnsw ON rag.chunks
    USING hnsw (embedding vector_cosine_ops);
CREATE INDEX IF NOT EXISTS chunks_tsv_gin ON rag.chunks USING gin (tsv);
CREATE INDEX IF NOT EXISTS chunks_product_id ON rag.chunks (product_id);
