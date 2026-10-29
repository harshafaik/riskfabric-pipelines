# 0002 — Plain Parquet, not a table format

## Context

The pipeline needs to replace a day's worth of data on rerun, and to answer "is
this partition the same as the one I loaded before?" Iceberg, Delta Lake and Hudi
all solve this class of problem, with ACID commits, schema evolution and time
travel.

## Decision

Use plain Parquet files partitioned by `event_date`, written atomically via a temp
file and rename, with a deterministic `_manifest.json` per partition. Idempotency
is proven by a content fingerprint over the committed rows.

## Why

The dataset is single-node by design: 9,096 rows in the committed fixture, and
about 1.5M in the full synthetic run. A table format exists to coordinate
concurrent writers and readers at scale; here there is one writer, no concurrent
readers, and the partition is the natural unit of replacement.

Partition overwrite plus a manifest gives the two properties that actually matter
here — atomic replacement and verifiable equality — at the cost of one directory
convention and about forty lines of code. Adopting Iceberg would add a catalog, a
table service or a DuckDB extension, and a metadata layer to keep correct, in
exchange for guarantees the workload does not exercise.

## Consequences

- Reruns are byte-identical, which makes CI assertions trivial and makes the
  "same input, same output" property obvious to a reader.
- No time travel and no schema evolution across files. A contract version bump
  means a new directory, not a metadata update.
- Writers must not run concurrently on the same partition. Nothing enforces that
  today; the orchestrator would.

## When to revisit

If this ever needs multiple concurrent writers, sub-partition pruning at
table granularity, or time-travel queries, Iceberg over the same Parquet files is
the upgrade path and the partition layout survives it. Until then, the simpler
mechanism is the honest one.
