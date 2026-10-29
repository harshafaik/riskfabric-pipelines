"""Orchestration: asset partitions, the freshness check and the dbt asset."""

from __future__ import annotations

from datetime import date

import duckdb
import pytest
from dagster import materialize

from orchestration.definitions import (
    PipelinePaths,
    customers_bronze,
    star_schema,
    transactions_bronze,
    transactions_bronze_fresh,
)
from tests.helpers import CONTRACTS_DIR, SNAPSHOT, TX_DAY, ingest, partition

LAST_DAY = date(2026, 1, 30)


def _paths(inbox, data) -> PipelinePaths:
    return PipelinePaths(
        inbox=str(inbox),
        data_root=str(data),
        contracts=str(CONTRACTS_DIR),
    )


def test_materializes_a_partition_and_records_metadata(contracts, inbox, data):
    materialize(
        [customers_bronze],
        partition_key=SNAPSHOT.isoformat(),
        resources={"paths": _paths(inbox, data)},
    )
    result = materialize(
        [transactions_bronze],
        partition_key=TX_DAY.isoformat(),
        resources={"paths": _paths(inbox, data)},
    )

    assert result.success
    committed = partition(data, "transactions", TX_DAY) / "part-00000.parquet"
    assert committed.exists()

    materializations = [
        event.event_specific_data.materialization
        for event in result.get_asset_materialization_events()
    ]
    transactions = [
        materialization
        for materialization in materializations
        if materialization.asset_key.path == ["transactions_bronze"]
    ]
    assert len(transactions) == 1
    assert transactions[0].metadata["rows_committed"].value > 0
    assert transactions[0].metadata["contract"].text == "transactions_v1"


def test_freshness_check_fails_when_bronze_is_behind(contracts, inbox, data):
    ingest(contracts, "customers", SNAPSHOT, inbox, data)

    result = materialize(
        [transactions_bronze, transactions_bronze_fresh],
        partition_key=TX_DAY.isoformat(),
        resources={"paths": _paths(inbox, data)},
    )

    checks = result.get_asset_check_evaluations()
    assert len(checks) == 1
    assert checks[0].passed is False
    assert checks[0].metadata["latest_inbox"].text == LAST_DAY.isoformat()
    assert checks[0].metadata["latest_bronze"].text == TX_DAY.isoformat()


def test_freshness_check_passes_at_the_latest_delivery(contracts, inbox, data):
    ingest(contracts, "customers", SNAPSHOT, inbox, data)

    result = materialize(
        [transactions_bronze, transactions_bronze_fresh],
        partition_key=LAST_DAY.isoformat(),
        resources={"paths": _paths(inbox, data)},
    )

    checks = result.get_asset_check_evaluations()
    assert len(checks) == 1
    assert checks[0].passed is True


def test_star_schema_asset_builds_the_warehouse(contracts, inbox, data):
    materialize(
        [customers_bronze],
        partition_key=SNAPSHOT.isoformat(),
        resources={"paths": _paths(inbox, data)},
    )
    result = materialize(
        [transactions_bronze, star_schema],
        partition_key=TX_DAY.isoformat(),
        resources={"paths": _paths(inbox, data)},
    )

    assert result.success
    warehouse = data / "riskfabric.duckdb"
    assert warehouse.exists()

    connection = duckdb.connect(str(warehouse), read_only=True)
    try:
        rows = connection.execute("select count(*) from fact_transaction").fetchone()[0]
    finally:
        connection.close()
    assert rows > 0
