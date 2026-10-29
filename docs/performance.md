# Performance

Measured on the committed fixture on one machine. Every number comes from this repository; none
are copied from a sibling project.

## Hardware

| Component | Detail |
| :--- | :--- |
| CPU | AMD Ryzen 7 4800H, 8 cores / 16 threads |
| RAM | 15 GiB |
| OS | Fedora Linux 44 (Workstation) |
| Python | 3.14.7 |

## Toolchain

| Tool | Version |
| :--- | :--- |
| Polars | 2.1.0 |
| DuckDB | 2.0.0 |
| dbt-core | 1.12.6 |
| dbt-duckdb | 1.11.1 |
| Dagster | 1.13.27 |

## Methodology

The fixture is the committed 30-day slice: 31 partitions, 8,596 transactions and 500 customers,
for 9,096 rows in total. Wall times are measured with `/usr/bin/time -f '%e'`, after one
discarded run that warms the page cache, with N=3 and reported as mean ± standard deviation.
Query latencies are N=20 and reported as p50/p95/p99. All runs are single-process and
single-machine; no concurrency is measured.

## Summary

| Operation | Result |
| :--- | :--- |
| Full ingest, 30 days | 0.64 ± 0.00 s |
| Replay of the same 30 days | 0.64 ± 0.01 s |
| `dbt build` (7 models, 53 checks) | 6.35 ± 0.10 s |
| Ingest throughput | ~14,200 rows/s |
| Peak RSS, ingest | ~119 MiB |
| Peak RSS, dbt | ~274 MiB |
| Parquet written | 1,133,420 bytes |
| Bytes per transaction | 126.2 |
| Bytes per customer | 97.5 |

## Ingestion

The first run on a cold page cache took `0.83 s`; warm runs settle at `0.64 s`. The rerun is the
same cost as the first run because the work is identical — idempotency is a property of the
output, not a cache. The run writes 62 files: 31 part files, 31 manifests, and the directories
that hold them.

At 9,096 rows in `0.64 s`, throughput is approximately 14,200 rows/s, or 13,400 transactions/s
for the transaction feed alone. Peak resident memory is about 122 MB, dominated by the Polars
frame and the reference id set, not by the output.

Quarantine is not exercised in this measurement: the fixture validates cleanly, so no
`rejected.parquet` is written.

## Transforms

`dbt build` measures `6.35 ± 0.10 s` wall time, of which about `1.2 s` is dbt's own model and
test execution. The remaining roughly five seconds is process startup and adapter import, which
is why the wall time is several times the reported run time. The build executes 7 models and 53
data tests and passes all of them.

DuckDB query latency over the built marts:

| Query | p50 | p95 | p99 |
| :--- | ---: | ---: | ---: |
| `count(*)` from `fact_transaction` | 0.6 ms | 1.0 ms | 1.0 ms |
| Daily fraud rate, grouped by `event_date` | 1.5 ms | 2.7 ms | 2.7 ms |
| `fact_transaction` join `dim_customer`, grouped by state | 3.0 ms | 4.3 ms | 4.3 ms |

The join is the slowest query because it reads both sides and aggregates; all three are
sub-5 ms at this scale, so query time is not the constraint. Storage size is.

## Interpretation

These numbers describe a 9,096-row fixture and should not be extrapolated. The pipeline is
single-node by design and the dominant cost at larger volumes would be Parquet write and
reference-id scanning, neither of which is exercised here. Spark or a distributed warehouse
would add operational cost without changing the correctness properties this repository exists
to demonstrate.

## How to Reproduce

```bash
rm -rf /tmp/rfp-bench
/usr/bin/time -f "%e s, %M KB" \
  python -m ingest run --feed all --from 2026-01-01 --to 2026-01-30 --data /tmp/rfp-bench/data

RFP_DUCKDB=/tmp/rfp-bench/riskfabric.duckdb /usr/bin/time -f "%e s, %M KB" \
  dbt build --profiles-dir . --vars '{bronze_root: /tmp/rfp-bench/data/bronze}'
```

Run each command three times and discard the first to reproduce the warm-cache figures.
