# Superseded Implementations

Historical notes for designs that were replaced. These do not describe current behaviour.

## Fixture builder v1

`fixtures/build_fixtures.py` partitioned transactions with
`pl.col("timestamp").str.slice(0, 10)`, which works only while `timestamp` is a string. scrip
schema v2 emits a Datetime, so the builder failed with `SchemaError`. It was replaced by
`pl.col("timestamp").dt.date()` plus a `scrip.schema_version == "2"` guard. Recorded in
[0006](decisions/0006-fixture-schema-drift.md).

## Fixture v1 data

The committed inbox previously carried string timestamps and registration dates. It was
regenerated against v2; the row counts and the 30-day window are unchanged. Older copies are
not retained because the fixture is reproducible from scrip with seed 42.

## Rejected, not implemented

The following were considered and not built, so they have no superseded implementation to
document:

- Iceberg or Delta table format — [0002](decisions/0002-no-table-format.md)
- ClickHouse as the batch store — [0002](decisions/0002-no-table-format.md)
- Spark — [performance.md](performance.md) § Interpretation
- Airflow — [0007](decisions/0007-dagster-over-airflow.md)
