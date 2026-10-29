# Architecture

## Flow

```
inbox/<feed>/event_date=YYYY-MM-DD/<feed>.parquet
        │
        │  1. arrival      file exists, non-empty, expected path
        │  2. structure    required columns exactly, row count
        │  3. types        cast to contract types
        │  4. semantics    nullability, allowed values, ranges, patterns
        │                  primary-key uniqueness, partition match, references
        ▼
bronze/<feed>/event_date=YYYY-MM-DD/
        part-00000.parquet      accepted rows, contract types
        _manifest.json          counts, columns, fingerprint
        │
        └── accepted rows only. Rejected rows go to
            quarantine/<feed>/event_date=YYYY-MM-DD/rejected.parquet
            with a _reject_reason column.
```

## Transform layer

A dbt project (`dbt_project.yml`, `models/`) reads `bronze/` Parquet and produces
a small star schema in DuckDB:

- `staging/stg_transactions`, `staging/stg_customers` — typed views over bronze.
- `marts/dim_customer` — SCD2 on `state` and `credit_band`; one row per customer
  per validity period, keyed by `customer_sk`.
- `marts/dim_card`, `marts/dim_merchant`, `marts/dim_date` — conformed
  dimensions derived from observed activity and a date spine.
- `marts/fact_transaction` — one row per `transaction_id`, incremental with a
  three-day lookback so a late correction replaces affected rows.

Every model declares its grain in `schema.yml`. Generic tests cover keys,
referential integrity and accepted values; two singular tests guard the
negative-interval and over-flagging bug classes. Facts join dimensions on natural
keys; consumers doing point-in-time analysis use the SCD2 validity range.

Orchestration is still absent and is the next stage.

## Invariants

1. **One partition, one file, one manifest.** No partial directory states.
2. **Writes are atomic.** Every file is written to a `.tmp` sibling and renamed.
3. **The manifest is deterministic.** It contains no wall-clock timestamp, so a
   rerun is byte-identical.
4. **Row accounting balances.** `rows_in == rows_committed + rows_rejected`.
   The pipeline asserts this before writing.
5. **A partition is a pure function of its input.** Same inbox file plus same
   contract produces the same bytes.
6. **Reject-and-continue.** Row-level failures never abort a partition; file-level
   failures never write one.
7. **Dimensions before facts.** `contracts` declare references, and the CLI derives
   the feed order from them (`customers` before `transactions`).

## Fingerprints

`fingerprint()` hashes the partition's rows as CSV, sorted by primary key. It is
independent of row order and of Parquet file metadata, so it answers the question
"is this the same data?" rather than "is this the same file?". Reconciliation
compares it after every write.

## Partitioning

`event_date` is derived from the event timestamp and is asserted to match the
requested partition. A row whose timestamp belongs to another day is quarantined as
`partition_mismatch` rather than silently landing in the wrong file. This is what
makes a late-arriving correction possible later: the partition is the unit of
replacement.

## Why the CLI derives feed order

`transactions` declares `references: customer_id -> customers.customer_id`. The CLI
topologically sorts feeds so the dimension is loaded first. If a fact partition
references a feed with no bronze data, validation fails with a clear message
instead of quarantining every row as an orphan.

## Known limits

- Primary-key uniqueness is enforced **within a partition**. Cross-partition
  uniqueness is a warehouse concern and belongs in the dbt layer.
- References are checked against all existing bronze partitions of the referenced
  feed, not against a specific snapshot version. When the customer snapshot starts
  changing daily, this needs an as-of join.
- Quarantine is append-by-overwrite per partition: a clean rerun removes the
  previous `rejected.parquet` for that day. Historical reject rates therefore
  require the orchestration layer to record run metadata.
