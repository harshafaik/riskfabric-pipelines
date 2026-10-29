# Failure modes

What actually happens when something goes wrong. Every row here is exercised by a
test in `tests/` unless marked otherwise.

| Failure | Detection | Effect | Exit |
|---|---|---|---|
| Required feed absent | `arrival` path does not exist | nothing written | 3 |
| Optional snapshot absent | same, `required: false` | partition reported as skipped | 0 |
| Empty file | `file has no rows` | nothing written | 2 |
| Renamed or dropped column | `missing columns: <name>` | nothing written | 2 |
| Unexpected column | `columns not in transactions_v1: <name>` | nothing written | 2 |
| Unparseable value | `type:<column>` | row quarantined, rest committed | 0 |
| Value out of range / not allowed | `invalid:<column>` | row quarantined, rest committed | 0 |
| Duplicate primary key | `duplicate:primary_key` | **all** copies quarantined | 0 |
| Row for another day | `partition_mismatch` | row quarantined | 0 |
| Unknown reference | `ref:<column>` | row quarantined | 0 |
| Reference feed not loaded | validation error before any write | nothing written | 2 |
| Too few valid rows | `min_rows` check | nothing written, previous partition preserved | 2 |
| Written file ≠ manifest | read-back reconciliation | exception | 4 |
| Interrupted write | temp file orphaned | target untouched; rerun overwrites temp | — |

## The cases worth explaining

**A bad row does not cost you the partition.** Validation is per row for types and
semantics, and per file for structure. One unparseable amount sends one row to
quarantine and 270 rows to bronze. The manifest records the split, so "we loaded
99.6% today" is visible rather than silently true.

**A validation failure preserves the previous good partition.** All validation
happens before any write. A file that fails `min_rows` leaves yesterday's committed
partition exactly as it was, so a bad delivery cannot destroy a good load.

**Duplicate keys reject every copy, not the second one.** `is_duplicated()` marks
all occurrences. Committing the first copy and quarantining the rest would make the
result depend on row order, which breaks determinism.

**Partition mismatch is a validation error, not a routing decision.** A transaction
dated the 16th arriving in the 15th's file is quarantined. Silently re-routing it
would make the partition a function of file contents rather than of the contract,
and would hide an upstream delivery bug.

**Reconciliation catches the case where the write succeeded but the data did not
land as expected.** The partition is reopened and hashed after writing. If the row
count, column list or fingerprint disagrees with the manifest, the run fails
instead of reporting success.

## Known gaps

- **Stale temp files.** A process killed between `write` and `os.replace` leaves a
  `.part-00000.parquet.tmp` behind. It is harmless and overwritten next run, but
  nothing cleans it up proactively.
- **Cross-partition duplicates.** Only within-partition uniqueness is enforced.
  Two days may each contain the same `transaction_id`; the warehouse layer and its
  `unique` test are responsible for that.
- **No alerting.** Reject rate and missing-feed conditions are printed and reflected
  in exit codes. Nothing pages anyone. That belongs to the orchestration milestone:
  asset checks on quarantine row count and on partition freshness.
- **No run history.** The manifest is deterministic by design, so the record of
  *when* a partition was produced lives outside the data. The orchestrator will own it.
