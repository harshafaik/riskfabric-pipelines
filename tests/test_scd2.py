"""SCD2 history: a second customer snapshot creates a new version on change."""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
from datetime import date
from pathlib import Path

import pytest

duckdb = pytest.importorskip("duckdb")
pl = pytest.importorskip("polars")

from tests.helpers import REPO_ROOT, SNAPSHOT, ingest

SECOND_SNAPSHOT = date(2026, 1, 2)


def _dbt_executable() -> str:
    sibling = Path(sys.executable).with_name("dbt")
    if sibling.exists():
        return str(sibling)
    found = shutil.which("dbt")
    if found is None:
        pytest.skip("dbt is not installed")
    return found


def _write_second_snapshot(inbox: Path) -> tuple[str, str]:
    source = (
        inbox
        / "customers"
        / f"event_date={SNAPSHOT.isoformat()}"
        / "customers.parquet"
    )
    frame = pl.read_parquet(source)
    customer_id = frame["customer_id"][0]
    original_state = frame["state"][0]
    new_state = "ZZ" if original_state != "ZZ" else "YY"

    mutated = (
        frame.with_row_index("_row")
        .with_columns(
            pl.when(pl.col("_row") == 0)
            .then(pl.lit(new_state))
            .otherwise(pl.col("state"))
            .alias("state"),
            pl.when(pl.col("_row") == 0)
            .then(pl.lit(599, dtype=pl.UInt32))
            .otherwise(pl.col("credit_score"))
            .alias("credit_score"),
        )
        .drop("_row")
    )
    destination = (
        inbox
        / "customers"
        / f"event_date={SECOND_SNAPSHOT.isoformat()}"
        / "customers.parquet"
    )
    destination.parent.mkdir(parents=True)
    mutated.write_parquet(destination)
    return customer_id, new_state


def test_a_changed_customer_gets_a_second_version(contracts, inbox, data):
    customer_id, new_state = _write_second_snapshot(inbox)

    ingest(contracts, "customers", SNAPSHOT, inbox, data)
    ingest(contracts, "customers", SECOND_SNAPSHOT, inbox, data)
    ingest(contracts, "transactions", SNAPSHOT, inbox, data)
    ingest(contracts, "transactions", SECOND_SNAPSHOT, inbox, data)

    warehouse = data / "riskfabric.duckdb"
    completed = subprocess.run(
        [
            _dbt_executable(),
            "build",
            "--profiles-dir",
            str(REPO_ROOT),
            "--vars",
            f"{{bronze_root: {(data / 'bronze').as_posix()}}}",
        ],
        cwd=REPO_ROOT,
        env={**os.environ, "RFP_DUCKDB": str(warehouse)},
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0, completed.stdout[-2000:] + completed.stderr[-2000:]

    connection = duckdb.connect(str(warehouse), read_only=True)
    try:
        rows = connection.execute(
            "select valid_from, valid_to, is_current, state, credit_band "
            "from dim_customer where customer_id = ? order by valid_from",
            [customer_id],
        ).fetchall()
    finally:
        connection.close()

    assert len(rows) == 2
    first, second = rows
    assert first[0] == SNAPSHOT
    assert first[1] == date(2026, 1, 1)
    assert first[2] is False
    assert second[0] == SECOND_SNAPSHOT
    assert second[1] == date(9999, 12, 31)
    assert second[2] is True
    assert second[3] == new_state
    assert second[4] == "subprime"
