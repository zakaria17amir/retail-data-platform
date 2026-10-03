# ADR-0002: Silo (MinIO fork) as the local object store

## Status

Accepted

## Context

The lakehouse needs an S3-compatible store locally. The MinIO community distribution has ended:
the upstream repo was archived (Apr 2026) and the `minio/minio` and `minio/mc` Docker Hub images
were removed (Sept 2026). The quay.io copy is frozen and lacks the CVE-2025-62506 fix.

## Decision

Use Silo (`pgsty/silo`, with `pgsty/mc`), the maintained pgsty fork. The Compose service stays
named `minio`; the S3 API, `MINIO_*` variables and `server /data` command are unchanged, so the
mapping to AWS S3 is unaffected. Images are pinned by tag and digest.

## Consequences

- Swapping the store later is a one-line image change.
- Depends on a small community project; the pin keeps builds reproducible.

## Alternatives

- Garage, SeaweedFS, RustFS: new client configuration for Spark s3a and DuckDB httpfs with no gain
  for a local dev store.
- Building MinIO from source: not reproducible for reviewers.
- Frozen `pgsty/minio` or quay image: no security fixes.
