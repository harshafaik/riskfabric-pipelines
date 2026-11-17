# riskfabric-pipelines

[![CI](https://github.com/harshafaik/riskfabric-pipelines/actions/workflows/ci.yml/badge.svg)](https://github.com/harshafaik/riskfabric-pipelines/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

`nsrdb` proves scale and distribution. This repository proves correctness at the
boundary: every row is validated against a versioned contract, every partition is a
deterministic function of its input, and every load is reconciled after write.

Contract-validated ingestion, idempotent partition loads and reconciliation over a
deterministic synthetic transaction feed.

This repository is the data-engineering half of a three-part system:

```
scrip  ──dataset v1──▶  riskfabric-pipelines  ──analytical tables──▶  riskfabric-ml
(simulation)            (this repo)                                  (model + serving)
```

| Repository | Responsibility |
| :--- | :--- |
| `scrip` | OpenStreetMap world building and deterministic agent simulation |
| **riskfabric-pipelines** (this repository) | Contract-validated ingestion, star schema, orchestration |
| [`riskfabric-ml`](https://github.com/harshafaik/riskfabric-ml) | Feature engineering, training, calibration, scoring, monitoring, case review |
| [`RiskFabric`](https://github.com/harshafaik/RiskFabric) (archived) | Original end-to-end monolith |

[`nsrdb`](https://github.com/harshafaik/nsrdb) carries the scale and distribution story.
`scrip` is the seeded Rust generator that produced `fixtures/inbox`; its provenance is
recorded in [`fixtures/README.md`](fixtures/README.md#provenance).

It does not train models. It ends at curated, validated tables.

## Status

Implemented: contracts, ingestion, quarantine, reconciliation, idempotency, CI,
the dbt-over-DuckDB transform layer (staging views, star schema, incremental
fact, data tests), and Dagster orchestration (partitioned assets, retry policy,
freshness check, daily schedule).

## Quick start

```bash
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev,transform,orchestrate]"

# Ingest the full 30-day fixture. Dimensions are ordered before facts:
# the customer snapshot lands first, then each daily transaction file.
python -m ingest run --feed all --from 2026-01-01 --to 2026-01-30

# Ingest a single transaction partition. The dimension must already be loaded;
# if it is not, the run fails with "reference feed 'customers' has no bronze
# partitions" rather than quarantining every row as an orphan.
python -m ingest run --feed customers --date 2026-01-01
python -m ingest run --feed transactions --date 2026-01-15

# Run the tests.
pytest

# Transform bronze into the star schema (DuckDB).
dbt build --profiles-dir .
dbt docs generate --profiles-dir . && dbt docs serve --profiles-dir .

# Orchestrate the same assets, partitioned by day.
dagster dev -f orchestration/definitions.py
```

The three extras give one path through ingest, tests, transforms and orchestration.
Ingest alone needs only the base install; `pytest` skips the dbt and Dagster tests
when their extras are absent.

Output:

```
2026-01-01  customers     committed  in=500   ok=500   rejected=0   sha256:52cec60c...
2026-01-01  transactions  committed  in=271   ok=271   rejected=0   sha256:59449b6b...
2026-01-02  customers     skipped (no snapshot)
2026-01-02  transactions  committed  in=264   ok=264   rejected=0   sha256:a234c096...
```

## Verify it yourself

```bash
rm -rf /tmp/rfp

# 1. Two runs over the same inbox produce byte-identical output.
python -m ingest run --feed all --from 2026-01-01 --to 2026-01-30 --data /tmp/rfp/data
find /tmp/rfp/data -type f | sort | xargs sha256sum > /tmp/rfp/first.sha
python -m ingest run --feed all --from 2026-01-01 --to 2026-01-30 --data /tmp/rfp/data
sha256sum -c /tmp/rfp/first.sha --quiet && echo "62 files identical"

# 2. The suite covers boundary cases, late-arrival replay and the dbt models.
pytest

# 3. The transform builds and tests the star schema.
RFP_DUCKDB=/tmp/rfp/riskfabric.duckdb \
  dbt build --profiles-dir . --vars '{bronze_root: /tmp/rfp/data/bronze}'
```

Expected: `62 files identical`, a green pytest run, and `PASS=53` from dbt
(46 data tests plus 7 models).

## What makes this more than a copy job

**Contracts, not conventions.** Every feed has a versioned YAML contract declaring
column types, nullability, allowed values, ranges, patterns, primary key, grain,
partition key and referential dependencies. Ingest validates against the contract;
it never infers a schema from the file.

```yaml
# contracts/transactions_v1.yml
columns:
  amount:    { type: float64, nullable: false, min: 0.0, max: 1000000.0 }
  currency:  { type: string,  nullable: false, allowed: ["INR"] }
  timestamp: { type: timestamp, nullable: false }
references:
  - { column: customer_id, feed: customers, target: customer_id }
```

**Three-stage validation, two rejection modes.** Structure failures reject the whole
file and write nothing. Type and semantic failures reject individual rows into
`data/quarantine/<feed>/event_date=.../rejected.parquet` with a reason
(`type:amount`, `ref:customer_id`, `duplicate:primary_key`, `partition_mismatch`),
with the pre-cast source values kept as `_src_<column>`, so one bad row does not
block 270 good ones.

**Idempotency, proven.** A partition is a pure function of its input: rerunning the
same day produces byte-identical Parquet and manifest. There is no wall-clock
timestamp in the manifest, because provenance belongs to the orchestrator and
determinism belongs to the data. The [verify block](#verify-it-yourself) replays the
full 30-day window and compares every output byte.

**Reconciliation, not hope.** After writing, the partition is read back and its
row count, column list and content fingerprint are compared against the manifest.
A partial load cannot pass as a complete one.

**A feed that never arrives is an error.** `arrival.required: true` makes a missing
partition fail loudly. The customer snapshot is `required: false` and is logged as
skipped, because it is periodic rather than daily.

## Layout

```
contracts/            versioned YAML per feed
ingest/
  contracts.py        load and validate the contract itself
  validation.py       structure → types → semantics, with per-row quarantine
  writer.py           partition paths, atomic writes, fingerprints, reconciliation
  pipeline.py         one partition of one feed
  cli.py              `python -m ingest run ...`
models/
  staging/            stg_transactions, stg_customers (views over bronze)
  marts/              dim_customer (SCD2), dim_card, dim_merchant, dim_date,
                      fact_transaction (incremental)
orchestration/        Dagster assets: bronze partitions, dbt build, freshness check
fixtures/inbox/       31 committed partitions generated by scrip (seed 42)
tests/                pytest suite plus dbt singular tests
dbt_project.yml       transform project (bronze_root var, materializations)
profiles.yml          DuckDB profile (data/riskfabric.duckdb)
docs/                 architecture, theory, failure modes, components,
                      decisions, performance
```

## Transforms

The dbt project reads `bronze/` Parquet directly and builds a small star schema
in `data/riskfabric.duckdb`:

| Model | Grain |
|---|---|
| `stg_transactions` / `stg_customers` | one row per transaction / per customer snapshot |
| `dim_customer` | one row per `customer_id` per validity period (SCD2) |
| `dim_card` / `dim_merchant` | one row per `card_id` / `merchant_id` |
| `dim_date` | one row per calendar day |
| `fact_transaction` | one row per `transaction_id`, incremental with a 3-day lookback |

Generic tests cover keys, referential integrity and accepted values. Two singular
tests encode bug classes that broke once: a negative
`time_since_last_transaction_seconds` (sort-after-join) and a daily fraud rate
outside three standard deviations of the window mean (over-flagging).

## Orchestration

Dagster, not Airflow: the unit here is a known set of `(feed, event_date)`
partitions that must each be fresh and independently re-runnable, which is the
asset model. Airflow's task/DAG model fits a fixed monthly rebuild sequence; the
`nsrdb` half of this system uses it for exactly that. Here, three assets
(`customers_bronze`, `transactions_bronze`, `star_schema`) share a daily
partition, each runs with `retry_policy(max_retries=2)`, the ingest manifest is
emitted as materialization metadata, and one asset check fails when bronze has
not caught up with the latest delivered partition. Backfilling a single day in
the UI is the idempotency demo; materialize the `2026-01-01` customer snapshot
first, because every transaction partition references the customer feed.

```bash
dagster dev -f orchestration/definitions.py
```

A partition on disk:

```
data/bronze/transactions/event_date=2026-01-15/
  part-00000.parquet
  _manifest.json      rows_in, rows_committed, rows_rejected, columns,
                      primary_key, fingerprint, contract version
```

## What this is not

- Not a production pipeline: the source is synthetic, and there are no real customers.
- Not distributed: the feed is 8,596 rows in the committed fixture. Polars on one
  machine is the correct tool at this size; Spark would be costume jewellery.
- Not a lakehouse: plain Parquet with partition overwrite, not Iceberg or Delta.
  `docs/decisions/0002-no-table-format.md` explains why.
- Not a running service. The Dagster schedule is defined and validated, but no daemon
  is deployed here, so there is no hosted run history.

## Further reading

- [`docs/index.md`](docs/index.md) — the documentation index
- [`docs/architecture.md`](docs/architecture.md) — data flow and invariants
- [`docs/theory_of_operation.md`](docs/theory_of_operation.md) — what a partition is and what is guaranteed
- [`docs/failure-modes.md`](docs/failure-modes.md) — what happens when each thing breaks
- [`docs/components/`](docs/components/index.md) — one document per module
- [`docs/decisions/`](docs/decisions_index.md) — choices, incidents and rejected alternatives
- [`docs/performance.md`](docs/performance.md) — measured timings and query latencies
- [`docs/WRITING_SPEC.md`](docs/WRITING_SPEC.md) — the rules component docs follow
- [`fixtures/README.md`](fixtures/README.md) — how the fixture was generated
