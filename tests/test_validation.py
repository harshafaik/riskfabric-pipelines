"""Contracts are the boundary: they load, they fail loudly, they order feeds."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from ingest.contracts import ContractError, dependency_order, load_contracts

BASE_COLUMNS = {
    "id": {"type": "string", "nullable": False},
    "amount": {"type": "float64", "nullable": False, "min": 0.0},
}


def write_contract(directory: Path, **overrides) -> Path:
    document = {
        "feed": "widgets",
        "version": 1,
        "grain": "one row per id",
        "arrival": {"path": "widgets/event_date={event_date}/widgets.parquet"},
        "partition": {"key": "event_date", "from": "requested"},
        "primary_key": ["id"],
        "columns": BASE_COLUMNS,
        "references": [],
    }
    document.update(overrides)
    path = directory / "widgets_v1.yml"
    path.write_text(yaml.safe_dump(document))
    return path


def test_contracts_load_with_grain_and_primary_key(contracts):
    transactions = contracts["transactions"]

    assert transactions.version == 1
    assert transactions.primary_key == ("transaction_id",)
    assert transactions.partition_from == "column"
    assert transactions.column("amount").maximum == 1_000_000.0
    assert transactions.column("currency").allowed == ("INR",)


def test_dependency_order_places_dimensions_first(contracts):
    assert dependency_order(contracts) == ["customers", "transactions"]


def test_unknown_type_is_rejected(tmp_path):
    path = write_contract(
        tmp_path,
        columns={"id": {"type": "uuid", "nullable": False}},
    )

    with pytest.raises(ContractError, match="unknown type"):
        load_contracts(tmp_path)


def test_primary_key_must_be_a_declared_column(tmp_path):
    write_contract(tmp_path, primary_key=["missing"])

    with pytest.raises(ContractError, match="primary key"):
        load_contracts(tmp_path)


def test_partition_column_must_be_declared(tmp_path):
    write_contract(
        tmp_path,
        partition={"key": "event_date", "from": "column", "column": "event_time"},
    )

    with pytest.raises(ContractError, match="partition column"):
        load_contracts(tmp_path)


def test_reference_to_an_unknown_feed_is_rejected(tmp_path):
    reference = {"column": "id", "feed": "ghosts", "target": "id"}
    write_contract(tmp_path, references=[reference])

    with pytest.raises(ContractError, match="unknown feed"):
        dependency_order(load_contracts(tmp_path))
