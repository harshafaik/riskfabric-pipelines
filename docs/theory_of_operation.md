# Theory of Operation

This document explains the model behind the pipeline: what a partition is, why validation is
staged, and which properties are guaranteed rather than intended.

## 1. A partition is a pure function of its input

`ingest_partition(contract, date, inbox, data_root)` reads exactly one source file, applies one
contract and writes one directory. There is no shared accumulator and no wall clock. The
manifest holds counts, columns and a fingerprint, but no ingestion timestamp. Two runs over the
same inbox and contract produce byte-identical output; CI proves this across all `62` files.

The corollary is a separation of concerns: provenance is not the data's job. The orchestrator
records when a partition ran; the partition records only what it contains.

## 2. Validation is staged by blast radius

Validation runs structure, then types, then semantics, because the three stages fail at
different scopes.

| Stage | Example | Scope of failure |
| :--- | :--- | :--- |
| Structure | missing `mcc`, unexpected `surprise`, empty file | whole file; nothing is written |
| Types | `amount = "not-a-number"` | one row; reason `type:amount` |
| Semantics | `currency = "USD"`, `age = 12`, duplicate key | one row; reason `invalid:<column>` or `duplicate:primary_key` |
| Partition | transaction dated for another day | one row; reason `partition_mismatch` |
| Reference | `customer_id` absent from customers bronze | one row; reason `ref:customer_id` |
| Volume | fewer accepted rows than `min_rows` | whole file; the previous partition is preserved |

The ordering matters. A row is never compared against a schema that does not match its shape,
and a structural failure cannot leave a partially validated file on disk.

## 3. Reject and continue

A bad row should not cost the partition. On the 2026-01-15 fixture, a single unparseable
`amount` sends one row to quarantine and commits the other `243`. Rejected rows retain every
original column plus `_reject_reason`; multiple reasons are joined with `; ` so a row that is
both out of range and an orphan is diagnosed in one read.

Duplicate primary keys reject every copy, not the second one. Keeping the first copy would make
the committed set depend on row order, which contradicts Section 1.

## 4. The partition is the unit of replacement

A late correction rewrites the whole day. The new file and manifest replace the old ones
atomically, the fingerprint changes, and the neighbouring day is untouched. A row whose
timestamp belongs to another day is quarantined as `partition_mismatch` rather than rerouted,
so the partition remains a function of the contract and the requested date. The replay
behaviour is demonstrated in `tests/test_late_arrival.py` and discussed in
[0003](decisions/0003-late-arriving-corrections.md) and
[0004](decisions/0004-partition-mismatch-is-an-error.md).

## 5. Dimensions before facts

References form a DAG. `dependency_order` sorts it, so `customers` is ingested before
`transactions` and a new feed joins the order by declaring a reference rather than by editing
the CLI. If the dimension is absent, the run fails before writing instead of quarantining every
fact row as an orphan.

## 6. Reconciliation closes the loop

Writing is not proof. The committed file is read back and compared with the manifest: row
count, column list and fingerprint. A partial or corrupted write fails with exit code `4`
rather than reporting success. This is the check that turns "the write returned" into "the data
landed".

## 7. Determinism has a boundary

The data is deterministic; the manifest is deterministic only while the inbox path is passed
the same way. The `source` field stores the path as supplied, so a relative and an absolute
inbox produce different manifests for identical rows. This is a known hole in the invariant and
is recorded in [0005](decisions/0005-manifest-path-independence.md) with the fix direction.

## 8. Orchestration adds no correctness

Dagster partitions map one to one onto the unit above. A backfill is a loop of pure functions,
and the freshness check reads the same directory tree the pipeline wrote. No orchestration
state is required for a partition to be correct; the orchestrator adds scheduling, run history
and retry, not semantics.

## Invariants

| Invariant | Guard |
| :--- | :--- |
| One partition, one file, one manifest | `writer.py` fixed filenames; `reconcile` |
| Writes are atomic | `_atomic` temp file plus `os.replace` |
| Manifest is deterministic | sorted JSON, no wall clock; CI byte-identity step |
| Row accounting balances | assertion in `pipeline.ingest_partition` |
| A partition is a pure function of its input | idempotency test; CI full-feed rerun |
| Row failures never abort a partition | validation quarantine path |
| File failures never write a partition | structure and `min_rows` checks before writes |
| Dimensions precede facts | `dependency_order` and a pre-write reference check |

These are restated for consumers in [architecture.md](architecture.md#invariants).
