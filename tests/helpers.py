from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl

from ingest.contracts import Contract
from ingest.pipeline import Report, ingest_partition

REPO_ROOT = Path(__file__).resolve().parents[1]
FIXTURE_INBOX = REPO_ROOT / "fixtures" / "inbox"
CONTRACTS_DIR = REPO_ROOT / "contracts"

SNAPSHOT = date(2026, 1, 1)
TX_DAY = date(2026, 1, 15)
PART_FILE = "part-00000.parquet"


def ingest(
    contracts: dict[str, Contract],
    feed: str,
    day: date,
    inbox: Path,
    data: Path,
) -> Report:
    return ingest_partition(contracts[feed], day, inbox=inbox, data_root=data)


def ingest_baseline(contracts: dict[str, Contract], inbox: Path, data: Path) -> Report:
    """Load the customer snapshot, then one transaction partition."""
    ingest(contracts, "customers", SNAPSHOT, inbox, data)
    return ingest(contracts, "transactions", TX_DAY, inbox, data)


def source(inbox: Path, day: date) -> Path:
    return inbox / "transactions" / f"event_date={day.isoformat()}" / "transactions.parquet"


def rewrite(inbox: Path, day: date, transform) -> None:
    path = source(inbox, day)
    transform(pl.read_parquet(path)).write_parquet(path)


def set_value(frame: pl.DataFrame, column: str, index: int, value) -> pl.DataFrame:
    values = frame[column].to_list()
    values[index] = value
    return frame.with_columns(pl.Series(column, values, dtype=frame.schema[column]))


def partition(data: Path, feed: str, day: date) -> Path:
    return data / "bronze" / feed / f"event_date={day.isoformat()}"


def quarantine(data: Path, feed: str, day: date) -> Path:
    return data / "quarantine" / feed / f"event_date={day.isoformat()}"
