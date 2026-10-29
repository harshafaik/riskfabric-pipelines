# 0007 — Dagster, not Airflow

## Context

The pipeline has a known set of `(feed, event_date)` partitions. Each must be independently
re-runnable, recorded and checked for freshness. The two candidates are Dagster's asset model
and Airflow's task model.

## Decision

Use Dagster. Three assets (`customers_bronze`, `transactions_bronze`, `star_schema`) share a
daily partition definition and `retry_policy(max_retries=2)`. The ingest manifest is emitted as
materialization metadata, and one asset check compares the newest bronze partition with the
newest delivered one. The schedule is built from the partitioned job.

## Why

The subject is tables, not tasks. Asset definitions make the partition set, materialization
history and check results first-class without bolting on a catalog. Per-partition backfill in
the UI is exactly the unit of replacement the pipeline defines.

The manifest already contains the metadata Dagster wants: rows in, rows committed, rows
rejected, fingerprint and contract version. The integration is a mapping, not a second
bookkeeping system.

Airflow's task model fits a fixed sequence of operational actions. The `nsrdb` sibling uses it
for a monthly rebuild, which is the right shape there. It can express this pipeline, but
partition state and dataset lineage require additional work.

## Rejected alternatives

**Airflow.** Larger operator ecosystem and near-universal recognition in job descriptions. The
graph is tasks, so a partition-aware backfill and dataset lineage need extra plugins. The
concepts transfer, so the cost is a tool, not a skill.

**No orchestrator.** The CLI plus cron can run a range, but there is no run history, no
per-partition state and no check evaluation. Scheduling early is the cheapest credibility
available, and a shell loop does not produce it.

**Both.** Two orchestrators read as indecision and double the operational surface.

## Consequences

The definition is a Python module, and `dagster definitions validate` runs in CI.

The schedule exists but is not deployed. There is no daemon, no `DAGSTER_HOME` and no run
history beyond local executions, so this repository claims an orchestrator definition, not
weeks of green runs.

The partition window is fixed at import time through `RFP_PARTITION_END`. Extending it is an
environment change before the definitions load, not a runtime discovery.

## Related

- `orchestration/definitions.py`
- `docs/components/orchestration.md`
- `README.md` § Orchestration
