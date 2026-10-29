# Transforms

## Overview

The dbt project (`dbt_project.yml`, `models/`) reads bronze Parquet from the `bronze_root` var
and builds a star schema in DuckDB. It consumes `bronze/transactions` and `bronze/customers` and
produces two staging views, four dimensions and one incremental fact, with 53 data tests. It
contains no ingestion logic.

## Schema

| Model | Layer | Materialization | Grain |
| :--- | :--- | :--- | :--- |
| `stg_transactions` | staging | view | one row per `transaction_id` |
| `stg_customers` | staging | view | one row per `customer_id` per `snapshot_date` |
| `dim_customer` | marts | table | one row per `customer_id` per validity period |
| `dim_card` | marts | table | one row per `card_id` |
| `dim_merchant` | marts | table | one row per `merchant_id` |
| `dim_date` | marts | table | one row per calendar day |
| `fact_transaction` | marts | incremental | one row per `transaction_id` |

### fact_transaction (key columns)

| Field | Type | Description |
| :--- | :--- | :--- |
| `transaction_sk` | `VARCHAR` | `md5(transaction_id)`; surrogate key for the row. |
| `transaction_id` | `VARCHAR` | Natural key and incremental `unique_key`. |
| `event_date` | `DATE` | Partition date; incremental lookback predicate. |
| `transaction_ts` | `TIMESTAMP` | Event timestamp, UTC. |
| `customer_id` | `VARCHAR` | Natural key to `dim_customer`. |
| `card_id` | `VARCHAR` | Natural key to `dim_card`. |
| `merchant_id` | `VARCHAR` | Natural key to `dim_merchant`. |
| `amount` | `DOUBLE` | Transaction amount. |
| `is_fraud` | `BOOLEAN` | Noisy operational label from the source feed. |
| `previous_transaction_ts` | `TIMESTAMP` | Prior transaction on the same card, ordered by `(transaction_ts, transaction_id)`. |
| `time_since_last_transaction_seconds` | `BIGINT` | Seconds since `previous_transaction_ts`; null for a card's first row. |

## Design Decisions

**Grain is declared for every model in `schema.yml`.** `fact_transaction` is one row per
`transaction_id`; `dim_customer` is one row per customer per validity period. The same strings
appear in the model metadata and in every manifest produced upstream, so the two layers cannot
disagree silently about grain.

**`dim_customer` is slowly changing on `state` and `credit_band`.** `credit_band` is derived
from `credit_score` into four buckets. A new version starts when either attribute changes,
`valid_to` is the day before the next version, the open version ends at `9999-12-31`, and
`is_current` marks it. The surrogate key is `md5(customer_id | snapshot_date)`.

**`fact_transaction` is incremental with a three-day lookback.** The materialization is
`delete+insert` keyed on `transaction_id`, and the predicate is
`event_date >= max(event_date) - 3 days`. The `lag()` for `time_since_last_transaction_seconds`
is computed over full history before that filter, so the first rows of a rewritten window still
see their true predecessor.

**Dimensions are derived from observed activity.** `dim_card` and `dim_merchant` use
`arg_min`/`arg_max` on `event_date` to pick a stable account, customer, name and category.
`dim_date` is a spine from the minimum to the maximum transaction date, so the date dimension
covers exactly the loaded window.

**Facts join dimensions on natural keys.** The four `relationships` tests assert that every
`customer_id`, `card_id`, `merchant_id` and `event_date` exists in its dimension. Surrogate keys
are exposed on the dimensions but are not joined into the fact; the tradeoff is recorded in
[0008](../decisions/0008-natural-vs-surrogate-keys.md).

**Staging reads Parquet directly.** dbt-duckdb 1.11 provides no external-source support, so
`stg_transactions` and `stg_customers` call `read_parquet()` with `hive_partitioning = false`
rather than using `source()`. The lineage graph therefore starts at staging.

**Position in the pipeline.** The project consumes bronze written by `pipeline.py` and is run
either directly with `dbt build --profiles-dir .` or by the `star_schema` Dagster asset.

## Current Limitations

The SCD2 change-detection branch is unexercised. The committed fixture contains one customer
snapshot, so every customer produces exactly one current version. A test with two snapshots and
a changed `state` is needed before the history path can be claimed as verified.

`star_schema` runs a full `dbt build` for every partition. The models are correct and
idempotent, but a 30-day backfill executes all 53 tests 30 times. Partition-scoped model
selection would reduce this.

The fact carries natural keys rather than `customer_sk`, `card_sk` and `merchant_sk`.
Point-in-time joins against `dim_customer` require the consumer to use the validity range.
Joining the as-of surrogate key into the fact would move that logic upstream, at the cost of a
more complex incremental model.

Cross-partition `transaction_id` uniqueness is not tested. The `unique` test runs per build over
the current table, which catches duplicates after a load but not two partitions that disagree
before the incremental merge.
