# 0004 — A wrong-day row is quarantined, not rerouted

## Context

A file delivered for `event_date=2026-01-15` can contain a row whose timestamp is
`2026-02-01`. The pipeline can move the row to the correct partition or reject it.

## Decision

Reject the row with `_reject_reason = partition_mismatch` and write it to the quarantine
directory of the requested day. Do not write anything to the partition implied by the
timestamp.

## Why

The partition is a function of the contract and the requested date. Rerouting makes it a
function of file contents, so replaying the same file could mutate a different partition and
invalidate that partition's manifest without a run record.

A wrongly dated row is an upstream delivery bug. Silently correcting it removes the only signal
that the producer's partitioning is wrong.

Rerouting would write to a partition that may not have been requested or validated in this run.
The target's manifest and fingerprint would change without a corresponding reconciliation.

## Evidence

`tests/test_ingest.py::test_rows_for_another_day_are_quarantined` injects a `2026-02-01`
timestamp into the `2026-01-15` file. It asserts exactly one rejected row with reason
`partition_mismatch` and no rejection of the remaining rows.
`tests/test_late_arrival.py` repeats the case inside a full replay and asserts the neighbouring
day is untouched.

## Consequences

The operator sees the row in `quarantine/transactions/event_date=2026-01-15/rejected.parquet`
with its reason and can return it to the producer. The requested partition still commits its
valid rows, so one misdated row does not cost the day.

A late row must be re-delivered in the correct day's file. The pipeline will not repair it, and
there is no dead-letter replay command.

## Alternatives rejected

**Silent rerouting.** Recorded above: it converts a delivery bug into invisible state.

**Failing the whole file.** This treats a row-level defect as a structural one and costs every
good row in the partition. Structure failures already have that scope; a timestamp defect does
not.
