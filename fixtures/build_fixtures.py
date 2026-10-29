"""Build ``fixtures/inbox`` from a scrip run.

Provenance (seed 42, 30-day window ending 2026-01-31):

    scrip fixtures --out-dir /tmp/refs
    scrip generate --customers 500 --seed 42 --days 30 \
        --base-date 2026-01-31T00:00:00Z --txns-per-customer 3..20 \
        --input-dir /tmp/refs --out-dir /tmp/out
    python fixtures/build_fixtures.py --source /tmp/out --out fixtures/inbox

The committed inbox is what CI and a clean clone use; this script only exists to
show how it was produced and to regenerate it at a different scale.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import polars as pl

SNAPSHOT_DATE = "2026-01-01"
EXPECTED_SCHEMA_VERSION = "2"


def _assert_schema_version(path: Path) -> None:
    actual = pl.read_parquet_metadata(path).get("scrip.schema_version")
    if actual != EXPECTED_SCHEMA_VERSION:
        raise SystemExit(
            f"{path}: scrip schema_version is {actual!r}, expected "
            f"{EXPECTED_SCHEMA_VERSION!r}. Regenerate with a matching scrip "
            "release, or add a contract version before upgrading."
        )


def _write(frame: pl.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.write_parquet(path)


def build(source: Path, destination: Path) -> None:
    for table in ("customers", "transactions"):
        _assert_schema_version(source / f"{table}.parquet")

    customers = pl.read_parquet(source / "customers.parquet")
    _write(
        customers,
        destination
        / "customers"
        / f"event_date={SNAPSHOT_DATE}"
        / "customers.parquet",
    )

    transactions = pl.read_parquet(source / "transactions.parquet").with_columns(
        pl.col("timestamp").dt.date().alias("event_date")
    )
    partitions = transactions.partition_by("event_date", as_dict=True)
    for key, part in partitions.items():
        day = key[0] if isinstance(key, tuple) else key
        _write(
            part.drop("event_date"),
            destination
            / "transactions"
            / f"event_date={day}"
            / "transactions.parquet",
        )

    print(
        f"customers: {customers.height} rows (snapshot {SNAPSHOT_DATE})\n"
        f"transactions: {transactions.height} rows across {len(partitions)} daily files"
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True, help="scrip output directory")
    parser.add_argument("--out", type=Path, default=Path("fixtures/inbox"))
    args = parser.parse_args()
    build(args.source, args.out)


if __name__ == "__main__":
    main()
