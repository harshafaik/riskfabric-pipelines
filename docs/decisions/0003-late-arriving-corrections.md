# 0003 — Late-arriving corrections replace a partition

## Context

The source is a daily feed, but a delivery can be corrected after it lands: one row changes, a
row is added, or a row dated for another day arrives. The pipeline must absorb a replay without
duplicating rows, without touching other days, and without losing the reason a row was rejected.

## Decision

The partition is the unit of replacement. Re-ingesting `(feed, event_date)` atomically rewrites
`part-00000.parquet` and `_manifest.json`, replaces any `rejected.parquet`, and leaves every
other partition untouched. A row dated for another day is quarantined as `partition_mismatch`
rather than rerouted.

## Evidence

`tests/test_late_arrival.py` performs the replay against the committed fixture:

1. Ingest the customer snapshot, `2026-01-15` (244 committed rows) and `2026-01-16`.
2. Rewrite the `2026-01-15` source with three changes: one amount corrected by `+100`, one new
   transaction dated `2026-01-15 23:59`, one new transaction dated `2026-01-16 10:00`.
3. Assert the partition fingerprint changes, `rows_committed` becomes `245`, and `rows_rejected`
   becomes `1` with `transaction_id = txn-wrong-day` and reason `partition_mismatch`.
4. Assert the corrected amount is `original + 100` in the committed file.
5. Assert the `2026-01-16` partition is byte-identical to its pre-replay state.
6. Replay the same delivery again and assert every file for `2026-01-15` is byte-identical to the
   first replay.

## Consequences

A correction rewrites the whole day. Cost is proportional to the day, not to the size of the
diff, and the source must deliver the complete day rather than a patch.

`fact_transaction` is incremental with a three-day lookback. A correction to a day older than
three days is not picked up by a normal incremental run; it needs a targeted `dbt run` or a full
refresh. The ingest side has no such window.

Quarantine is overwrite-per-partition. A replay with no rejects deletes the previous
`rejected.parquet`, so reject history for earlier deliveries is not retained. Run history, if it
is needed, belongs to the orchestrator.

## Alternatives rejected

**Row-level upsert keyed on `transaction_id`.** This would require a merge engine in the ingest
path and would leave the manifest ambiguous about which delivery produced each row. It also
makes a replay depend on the rows already present rather than solely on the new file.

**Rerouting wrong-day rows to the correct partition.** This hides an upstream delivery bug and
makes a partition a function of file contents rather than of the requested date. Recorded
separately in [0004](0004-partition-mismatch-is-an-error.md).

## Related

- `ingest/pipeline.py` — writes the partition
- `ingest/writer.py` — atomic replacement and reconciliation
- `docs/components/pipeline.md`
- `docs/theory_of_operation.md` §4
