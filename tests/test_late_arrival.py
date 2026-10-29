"""Late-arriving correction: a replayed partition replaces the old one.

The machinery already exists — partition overwrite, ``partition_mismatch``
quarantine, deterministic reruns. This test is the demonstration: a corrected
row, an added row and a row for the wrong day arrive after the first load.
"""

from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

import polars as pl

from tests.helpers import (
    SNAPSHOT,
    TX_DAY,
    ingest,
    partition,
    quarantine,
    rewrite,
)

NEXT_DAY = date(2026, 1, 16)
PART_FILE = "part-00000.parquet"


def _set_value(frame: pl.DataFrame, column: str, index: int, value) -> pl.DataFrame:
    values = frame[column].to_list()
    values[index] = value
    return frame.with_columns(pl.Series(column, values, dtype=frame.schema[column]))


def _replay(frame: pl.DataFrame) -> pl.DataFrame:
    """Correct one row and add two: one for this day, one for the next."""
    corrected = _set_value(frame, "amount", 0, float(frame["amount"][0]) + 100.0)

    added = corrected.slice(1, 1).with_columns(
        pl.lit("txn-added-late").alias("transaction_id"),
        pl.lit(datetime(2026, 1, 15, 23, 59, 0)).alias("timestamp"),
    )

    wrong_day = corrected.slice(2, 1).with_columns(
        pl.lit("txn-wrong-day").alias("transaction_id"),
        pl.lit(datetime(2026, 1, 16, 10, 0, 0)).alias("timestamp"),
    )

    return pl.concat([corrected, added, wrong_day])


def _files(data: Path, feed: str, day: date) -> dict[str, bytes]:
    """Every byte written for a feed/day, bronze and quarantine."""
    contents: dict[str, bytes] = {}
    for root in (partition(data, feed, day), quarantine(data, feed, day)):
        if root.exists():
            for path in sorted(root.iterdir()):
                contents[f"{root.name}/{path.name}"] = path.read_bytes()
    return contents


def test_late_arrival_correction_replaces_the_partition(contracts, inbox, data):
    ingest(contracts, "customers", SNAPSHOT, inbox, data)
    original = ingest(contracts, "transactions", TX_DAY, inbox, data)
    ingest(contracts, "transactions", NEXT_DAY, inbox, data)
    next_day_before = _files(data, "transactions", NEXT_DAY)

    first_load = pl.read_parquet(partition(data, "transactions", TX_DAY) / PART_FILE)
    first_transaction_id = first_load["transaction_id"][0]
    first_amount = float(first_load["amount"][0])

    rewrite(inbox, TX_DAY, _replay)
    replayed = ingest(contracts, "transactions", TX_DAY, inbox, data)

    # The partition changed, gained exactly the added row, and quarantined the
    # row that belongs to another day.
    assert replayed.fingerprint != original.fingerprint
    assert replayed.rows_committed == original.rows_committed + 1
    assert replayed.rows_rejected == 1

    rejected = pl.read_parquet(quarantine(data, "transactions", TX_DAY) / "rejected.parquet")
    assert rejected["transaction_id"].to_list() == ["txn-wrong-day"]
    assert rejected["_reject_reason"].to_list() == ["partition_mismatch"]

    # The correction itself landed, not just the added row.
    committed = pl.read_parquet(partition(data, "transactions", TX_DAY) / PART_FILE)
    corrected = committed.filter(pl.col("transaction_id") == first_transaction_id)
    assert corrected.height == 1
    assert float(corrected["amount"][0]) == first_amount + 100.0

    # The neighbouring day is untouched.
    assert _files(data, "transactions", NEXT_DAY) == next_day_before

    # Replaying the same corrected delivery is byte-identical.
    replay_snapshot = _files(data, "transactions", TX_DAY)
    ingest(contracts, "transactions", TX_DAY, inbox, data)
    assert _files(data, "transactions", TX_DAY) == replay_snapshot
