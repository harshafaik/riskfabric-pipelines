# Orchestration

## Overview

`orchestration/definitions.py` maps the pipeline onto Dagster assets, one partition per
`(feed, event_date)`. It consumes the inbox, bronze and contract paths, calls
`pipeline.ingest_partition` for bronze and `dbt build` for marts, and produces materializations,
one asset check and a daily schedule. It contains no validation or SQL of its own.

## Schema

### PipelinePaths

| Field | Type | Description |
| :--- | :--- | :--- |
| `inbox` | `str` | Source root; defaults to `fixtures/inbox`. |
| `data_root` | `str` | Output root; defaults to `data`. |
| `contracts` | `str` | Contract directory; defaults to `contracts`. |

### Assets

| Asset | Dependencies | Group | Partition |
| :--- | :--- | :--- | :--- |
| `customers_bronze` | none | bronze | daily |
| `transactions_bronze` | `customers_bronze` | bronze | daily |
| `star_schema` | `transactions_bronze` | marts | daily |

All three use `RetryPolicy(max_retries=2)`.

### Checks

| Check | Target | Pass condition |
| :--- | :--- | :--- |
| `transactions_bronze_fresh` | `transactions_bronze` | newest bronze date equals newest inbox transaction date |

## Design Decisions

**The partition is the unit of work.** `DailyPartitionsDefinition` covers `2026-01-01` through
`2026-01-30`; the end date is exclusive and is read from `RFP_PARTITION_END`. A partition
rerun replaces only that day, which makes per-partition backfill in the UI the idempotency
demonstration rather than a separate script.

**The ingest manifest becomes materialization metadata.** Each bronze asset emits `feed`,
`partition`, `status`, `rows_in`, `rows_committed`, `rows_rejected`, `contract` and `fingerprint`.
The same numbers that appear in `_manifest.json` are queryable in the Dagster run record, so
lineage and row accounting travel with the materialization.

**Retries are bounded and uniform.** Every asset uses `max_retries=2`. A transient file or
DuckDB error is retried; a contract violation or a missing required feed is deterministic and
fails the run after the same budget.

**Freshness means caught up with the latest delivery.** The check compares the newest
`bronze/transactions/event_date=*` directory with the newest
`inbox/transactions/event_date=*`. It detects lag between delivery and ingestion. It does not
detect interior gaps, and a partition with zero rows is treated as present.

**The schedule is derived from the partitioned job.** `build_schedule_from_partitioned_job`
creates the daily schedule, so the scheduled run targets the current partition rather than
re-running the whole asset.

**Paths are a ConfigurableResource.** `PipelinePaths` is injectable, which lets the test suite
point the assets at `tmp_path` without mutating module state or the working directory.

**Position in the pipeline.** The assets wrap `pipeline.py` and the dbt project. Nothing in
`ingest/` or `models/` imports Dagster, so both remain runnable without an orchestrator.

## Current Limitations

The partition window is fixed at import time. Extending it requires setting
`RFP_PARTITION_END` before the definitions load; the asset graph does not grow with new
deliveries on its own.

The schedule exists but nothing runs it. There is no deployed daemon, no `DAGSTER_HOME`, and no
run history beyond local executions, so the claim is an orchestrator definition rather than a
running service.

`star_schema` shells out to the `dbt` executable instead of using the dbt Python API. This keeps
the asset close to the CI command, but it depends on `dbt` being on `sys.executable`'s path and
parses stdout for the summary line.

The freshness check detects lag only. It does not detect holes in the sequence of bronze
partitions, partial partitions, or a delivery that arrives with all rows quarantined.
