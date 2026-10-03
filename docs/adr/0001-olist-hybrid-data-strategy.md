# ADR-0001: Olist real core with replayer and derived clickstream

## Status
Accepted

## Context

The platform must show both a "clean up real, messy data" story and a CDC/streaming story. Olist
(~100k orders, 2016-2018, nine relational tables) has real defects: duplicated order items,
impossible timestamps, nulls, geolocation outliers, Portuguese categories, no product names. It is
static, has no clickstream, and has no product text for GenAI.

## Decision

Load Olist as-is into Postgres schema `olist`. A replayer pushes rows in time-compressed
chronological order (inserts and realistic updates), so Debezium CDC has live traffic. A
clickstream simulator derives events from the same orders, and an LLM enriches the catalogue with
names and descriptions.

## Consequences

- Real cleanup work in silver plus genuine CDC and streaming paths.
- Replayer and simulator are deterministic by seed, so tests are reproducible.
- Clickstream and product text are synthetic and must be labelled as such.
- Olist is CC BY-NC-SA 4.0: the project is non-commercial and carries attribution.

## Alternatives

- Fully synthetic data: no real cleanup story.
- REES46 only: real clickstream, but no relational/CDC core.
