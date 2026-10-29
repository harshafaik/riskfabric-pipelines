# Pipeline

## Overview

`ingest/pipeline.py` runs one partition of one feed end to end. It consumes a `Contract`, a
partition date and the inbox/data roots, and produces a `Report` after validating, writing and
reconciling the partition. It is the only module that sequences arrival, reference loading,
validation and persistence.

## Schema

### Report

| Field | Type | Description |
| :--- | :--- | :--- |
| `feed` | `str` | Feed name. |
| `partition` | `date` | Requested partition. |
| `status` | `str` | `committed` or `skipped`. |
| `rows_in` | `int` | Rows read from the source file; zero for a skipped partition. |
| `rows_committed` | `int` | Rows written to bronze. |
| `rows_rejected` | `int` | Rows written to quarantine. |
| `fingerprint` | `str \| None` | Manifest fingerprint; `None` when skipped. |

## API

| Name | Signature | Description |
| :--- | :--- | :--- |
| `ingest_partition` | `ingest_partition(contract, partition, *, inbox, data_root) -> Report` | Ingest one feed for one day. |
| `load_reference_ids` | `load_reference_ids(data_root, feed, target) -> set[str]` | Union of ids from every bronze partition of the referenced feed. |
| `MissingFeed` | `MissingFeed(Exception)` | Raised when a required feed's source file is absent. |

## Design Decisions

**Arrival is checked before anything else.** A required feed with no source file raises
`MissingFeed`, which the CLI maps to exit code `3`. An optional feed returns a `Report` with
`status="skipped"` and writes nothing; the customer snapshot is optional because it is periodic
rather than daily.

**All validation happens before any write.** The source is read, structure is checked,
references are loaded, rows are validated and `min_rows` is asserted before the destination is
touched. A rejected delivery therefore preserves the previous committed partition rather than
replacing it with an empty one.

**Row accounting is asserted, not assumed.** Before writing, the pipeline checks
`rows_in == rows_committed + rows_rejected`. A mismatch raises an `AssertionError` rather than
producing a manifest that does not balance. This invariant is the repository's headline claim.

**Reference feeds are resolved to id sets once per partition.** `load_reference_ids` reads every
bronze partition of the referenced feed and returns a set. An empty set raises a
`ContractViolation` with a clear message instead of quarantining every fact row as an orphan.
On the fixture this scans one 500-row customer partition per transaction day.

**A clean rerun removes stale quarantine.** When a rerun rejects no rows, an existing
`rejected.parquet` for that partition is deleted, so quarantine always reflects the latest
delivery rather than accumulating historical rejects.

**Position in the pipeline.** `cli.py` calls `ingest_partition` for each requested date and
feed; `orchestration/definitions.py` calls it once per Dagster partition. Both receive the same
`Report`, which the CLI prints and Dagster records as materialization metadata.

## Current Limitations

`load_reference_ids` reads the full referenced feed for every fact partition. With one customer
snapshot this is negligible; with daily snapshots it becomes quadratic in partitions. A cached
or incrementally maintained id index would fix it.

The `Report` is returned and printed but never persisted by this module. Run history, duration
and reject-rate trends exist only if Dagster or the caller records them.

A `min_rows` failure discards accepted rows as well as rejected ones, and the quarantine file is
not written in that branch. The previous partition survives, but the operator loses the
per-row detail that would explain the failure.

The pipeline holds no lock and assumes a single writer per partition. Concurrent invocations are
unsafe; the writer module documents the same constraint.
