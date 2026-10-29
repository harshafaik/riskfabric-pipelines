# TASKS — finish this repo, then stop

Status (2026-10-29): ingestion, the SQL layer, the late-arrival demo and Dagster
orchestration are done; the repo is published and CI is green. What remains is item 6
(the README cross-links).

**Definition of done.** When all six items below are true, this repo is finished as a
portfolio piece. Do not keep growing it after that; the next marginal hour belongs to
`nsrdb`, not here.

Positioning, because it decides what to build: `nsrdb` proves scale and distribution.
This repo proves **correctness at the boundary and partition-level recovery**. Those are
different questions and this repo should never try to answer the first one.

---

## 1. Commit it and let CI run — P0, ~30 minutes

The repo has a `.git` directory with zero commits, zero refs and no remote. Eleven paths
untracked. The CI workflow has therefore never executed once.

```bash
git add -A && git commit -m "feat: contract-validated ingestion with quarantine, idempotency and reconciliation"
git remote add origin git@github.com:harshafaik/riskfabric-pipelines.git
git push -u origin main
```

Do `scrip` first — this repo's fixtures are provenance-linked to it, and a reviewer
following the chain should land on a real repo at both ends.

**Acceptance:** the Actions run is green on the first push, and the job log shows
`all files byte-identical on rerun`.

## 2. Regenerate the fixture from current scrip — P0, ~2 hours

`fixtures/build_fixtures.py:41` does `pl.col("timestamp").str.slice(0, 10)`, which raises
`SchemaError: invalid series dtype: expected String, got datetime[μs]` against scrip's
schema v2. The committed fixture is still v1: `timestamp` and `registration_date` are
`String`. So CI currently proves the ingester is idempotent over v1 data that the source
no longer emits.

- Partition on `pl.col("timestamp").dt.date()`, drop the temp column.
- Regenerate `fixtures/inbox` from a fresh `scrip generate --seed 42 --days 30
  --base-date 2026-01-31`.
- Assert in the fixture builder that `scrip.schema_version == "2"` and refuse otherwise,
  so a future v3 fails loudly here instead of producing silently wrong partitions.
- Record the resolved params in `fixtures/README.md`.

**Acceptance:** `python fixtures/build_fixtures.py` succeeds on current scrip output;
`pytest` is green; a v3 schema in the Parquet metadata makes it exit non-zero with a clear
message.

## 3. The late-arriving correction demo — P1, ~half a day. Highest value per hour here.

The machinery exists (`partition_mismatch` quarantine, partition as the unit of
replacement) and nothing demonstrates it. This is the single most quotable thing in the
repo and it is currently an assertion in prose.

Write `tests/test_late_arrival.py`:

1. Ingest 2026-01-15 normally; record `fingerprint` and `rows_committed`.
2. Replay the same day with three changes: one row corrected (different amount), one row
   added, and one row whose timestamp belongs to 2026-01-16.
3. Assert: partition fingerprint changed; row count is old + 1; the wrong-day row is in
   `quarantine/transactions/event_date=2026-01-15/rejected.parquet` with
   `_reject_reason == "partition_mismatch"`; the 2026-01-16 partition is untouched.
4. Assert the rerun of step 2 is byte-identical to step 2.

**Acceptance:** the test above passes, and it is the one you demo live in an interview.

## 4. The SQL layer — P1, one weekend. This is the only competency row still absent.

dbt over DuckDB, reading `bronze/` as sources. Small on purpose:

```
models/staging/     stg_transactions, stg_customers
models/marts/       dim_customer (SCD2 on state/credit_band), dim_card, dim_merchant,
                    dim_date, fact_transaction
```

- Grain written into `schema.yml` for every model: `fact_transaction` is one row per
  `transaction_id`; state it before anyone asks.
- Generic tests: `unique`, `not_null` on keys, `relationships` fact → dims, `accepted_values`
  on `transaction_channel` and `currency`.
- Two singular tests, each encoding a bug class you have actually hit:
  `assert_no_negative_time_since_last_transaction` (the Polars sort-before-join bug) and
  `assert_alert_volume_within_3_sigma` (the calibration-gap over-flagging).
  "This check exists because it broke once" is the most credible sentence in DE.
- One incremental model with a 3-day `is_incremental()` lookback, correct `unique_key`.
- `dbt build` in CI against the fixture.

**Acceptance:** `dbt build` green from a clean clone; `dbt docs generate` shows the lineage
graph; every mart has a documented grain.

## 5. Orchestration — P2, one weekend, then leave it running

Dagster here, and one paragraph in the README explaining why not Airflow.

Assets map 1:1 onto what this repo already models: `(feed, event_date)` is a Dagster
partition, the manifest is the materialisation metadata, and per-partition backfill in the
UI *is* the idempotency demo. Add three assets, a daily partition, `retry_policy(max_retries=2)`,
one freshness check on `bronze/transactions`, and a schedule. Then leave it running
against daily `scrip` output.

Do not move `nsrdb` to Dagster and do not add a second Airflow here. `nsrdb`'s Airflow DAG
stays as written — run it once there, and in the interview say plainly: Airflow's task
model fits a monthly rebuild sequence; Dagster's asset model fits a known set of
partitions that must each be fresh and re-runnable. Stating that trade-off is the point.

**Acceptance:** four weeks of green daily runs, and a screenshot of the partition matrix.

## 6. README — P2, ~1 hour

- First two lines: what question this answers that `nsrdb` does not.
- Link `scrip`, `nsrdb` and `RiskFabric` — all four directions, not just this one.
- Keep the existing "What this is not" section. It is doing real work.

---

## Do not do

- **Do not add the other three feed contracts yet.** accounts, cards and fraud_metadata
  are the same validation logic typed out three more times; it adds zero new competency.
  Do it only if you want the end-to-end `RiskFabric` claim, and then it is four hours of
  typing, listed last.
- **Do not add Spark, Iceberg, Kafka or a cloud warehouse.** 8,596 rows in the committed
  fixture. `docs/decisions/0002-no-table-format.md` already makes this argument better
  than a new tool would.
- **Do not build 40 dbt models.** Eight, with grain and lineage documented.
- **Do not add a dashboard.** It is garnish and it would blur the repo's subject.
- **Do not let this repo train or serve a model.** It ends at curated tables.

---

## The one number to keep quoting

`rows_in == rows_committed + rows_rejected`, asserted before every write, with a
reconciliation pass that reads the partition back afterwards. That, plus the byte-identical
rerun, is the whole argument of this repo. Everything else is supporting detail.
