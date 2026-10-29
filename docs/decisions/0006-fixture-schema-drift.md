# 0006 — The fixture drifted from scrip schema v1 to v2

## What was wrong

The committed inbox was generated against scrip schema v1, where `transactions.timestamp` and
`customers.registration_date` were strings. scrip schema v2 emits `Datetime(us, UTC)` and
`Date`. `fixtures/build_fixtures.py` still partitioned with
`pl.col("timestamp").str.slice(0, 10)`, which raises
`SchemaError: invalid series dtype: expected String, got datetime[μs]` against v2 input.

CI therefore proved idempotency over data the source no longer emits.

The documented fixture size was also wrong. README and TASKS said "9,096 rows". The transaction
feed has 8,596 rows and the customer snapshot has 500, so 9,096 is the combined total; the
sentence attributed it to transactions.

## Quantified impact

| Quantity | Value |
| :--- | :--- |
| Transaction rows | 8,596 |
| Customer rows | 500 |
| Combined rows | 9,096 |
| `transactions.timestamp` in fixture v1 | `String` |
| `transactions.timestamp` in fixture v2 | `Datetime(us)` |
| `customers.registration_date` in v2 | `Date` |
| `build_fixtures.py` on v2 source, before fix | exit with `SchemaError` |
| `test_rows_for_another_day_are_quarantined`, before fix | `TypeError` building a Series |
| 2026-01-01 transactions | 271 |
| 2026-01-15 transactions | 244 |

The 30-day window and per-day counts are otherwise unchanged; regeneration with the same seed
reproduced 8,596 transactions.

## Root cause

The fixture builder assumed the v1 dtype and performed no schema-version check. The docs
repeated a combined row count as a transaction count. The test helper injected a timestamp as a
string because that was the v1 dtype.

## Fix

- `build_fixtures.py` partitions on `pl.col("timestamp").dt.date()`.
- It reads `scrip.schema_version` from the source Parquet metadata and exits non-zero unless the
  value is `"2"`.
- The fixture was regenerated from scrip seed 42, 500 customers, 30 days, base date
  `2026-01-31T00:00:00Z`.
- `test_rows_for_another_day_are_quarantined` uses `datetime(2026, 2, 1, 10, 0, 0)`.
- README, `fixtures/README.md` and TASKS now state 8,596 transactions, 500 customers and 9,096
  total.

## Prevention

The schema-version guard was verified by writing a source file with
`scrip.schema_version = "3"`; `build_fixtures.py` exits `1` with
`schema_version is '3', expected '2'`. CI runs `pytest` and the full-feed rerun against the
regenerated v2 fixture.

## Related

- `fixtures/build_fixtures.py`
- `fixtures/README.md`
- `tests/test_late_arrival.py`
- `tests/test_ingest.py`
