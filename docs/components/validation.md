# Validation

## Overview

`ingest/validation.py` splits a source frame into accepted and rejected rows. It consumes a
`pl.DataFrame`, the feed's `Contract`, the requested partition and the referenced id sets, and
produces a `ValidationResult` whose rejected frame carries a `_reject_reason` column. Structure
failures reject the whole file; type and semantic failures reject individual rows.

## Schema

### ValidationResult

| Field | Type | Description |
| :--- | :--- | :--- |
| `accepted` | `pl.DataFrame` | Rows with contract types and a partition column, ready for bronze. |
| `rejected` | `pl.DataFrame` | Rows with contract types, the partition column and `_reject_reason`. |

## API

| Name | Signature | Description |
| :--- | :--- | :--- |
| `check_structure` | `check_structure(df, contract, partition) -> None` | Raise `ContractViolation` on missing, unexpected or too few columns or rows. |
| `validate_rows` | `validate_rows(df, contract, partition, references) -> ValidationResult` | Cast, apply semantic checks, and split accepted from rejected. |
| `ContractViolation` | `ContractViolation(feed, partition, problems)` | Carries a list of human-readable problems. |

## Design Decisions

**Structure rejects the file; types and semantics reject rows.** A missing column, an
unexpected column or an empty file raises before any write, so the previous good partition is
preserved. A bad amount or timestamp sends one row to
`quarantine/transactions/event_date=YYYY-MM-DD/rejected.parquet` and commits the rest. On the
2026-01-15 fixture this is the difference between writing nothing and committing `244` rows.

**Type checks compare the source value with the cast value.** Every column is copied to a
hidden `__src__<name>` column before casting. A failed cast is then reported once as
`type:<column>` rather than twice, once as a type error and once as a nullability violation.
String columns whose source dtype is not string are additionally rejected, because a numeric
value silently cast to text would otherwise pass.

**Semantic checks run on the cast value; nullability runs on the source value.** Allowed
values, ranges and patterns test the typed column. Nullability tests `__src__`, so a value
that failed to cast is not double-counted as missing.

**Duplicate primary keys reject every copy.** `is_duplicated()` marks all occurrences, not
just the second one. Committing the first copy and quarantining the rest would make the
committed set depend on row order, which contradicts the deterministic-output invariant.

**Partition mismatch is a rejection, not routing.** A row whose `timestamp` falls on another
day is quarantined as `partition_mismatch`. Silently moving it to the correct partition would
make the partition a function of file contents rather than of the requested contract, and would
hide an upstream delivery bug. See
[0004](../decisions/0004-partition-mismatch-is-an-error.md).

**Position in the pipeline.** `pipeline.py` calls `check_structure` after reading the source
and `validate_rows` after loading reference ids. The accepted frame goes to `writer.py`; the
rejected frame goes to quarantine.

## Current Limitations

Primary-key uniqueness is enforced within a partition only. Two days may each contain the same
`transaction_id`; the star schema's `unique` test is the only cross-partition guard. A global
uniqueness check would need a bronze-wide index or a warehouse constraint.

Reference checks compare against the union of ids in every bronze partition of the referenced
feed. There is no as-of join, so a customer added by a later snapshot is considered valid for
an earlier transaction. Fixing this requires snapshot dates in the reference lookup.

`pattern` is applied with `str.contains`, which matches a substring. A pattern such as
`[0-9]{4}` accepts `x1234y`. Authors must anchor patterns with `^` and `$`; the contract loader
does not enforce that.

A `min_rows` failure discards the entire partition even when most rows passed validation. The
rejected rows are also not written in that case, so the operator sees only the aggregate
message printed by the CLI.
