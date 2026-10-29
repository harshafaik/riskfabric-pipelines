"""Feed contracts: the versioned schema, grain and semantics of each input.

A contract is a YAML file. It is the only place a column's name, type, nullability
and allowed values are written down. Ingest validates against it and nothing else.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

COLUMN_TYPES = {
    "string",
    "int32",
    "int64",
    "uint32",
    "float64",
    "boolean",
    "date",
    "timestamp",
}


class ContractError(Exception):
    """The contract file itself is malformed or inconsistent."""


@dataclass(frozen=True)
class Column:
    name: str
    type: str
    nullable: bool = True
    allowed: tuple[str, ...] | None = None
    minimum: float | None = None
    maximum: float | None = None
    pattern: str | None = None


@dataclass(frozen=True)
class Reference:
    column: str
    feed: str
    target: str


@dataclass(frozen=True)
class Contract:
    feed: str
    version: int
    grain: str
    arrival_path: str
    required: bool
    min_rows: int
    partition_key: str
    partition_from: str
    partition_column: str | None
    primary_key: tuple[str, ...]
    columns: tuple[Column, ...]
    references: tuple[Reference, ...]

    @property
    def name(self) -> str:
        return f"{self.feed}_v{self.version}"

    @property
    def column_names(self) -> tuple[str, ...]:
        return tuple(c.name for c in self.columns)

    def column(self, name: str) -> Column:
        for column in self.columns:
            if column.name == name:
                return column
        raise KeyError(f"{self.feed}: no column {name!r} in contract")


def _require(mapping: dict[str, Any], key: str, where: str) -> Any:
    if key not in mapping:
        raise ContractError(f"{where}: missing required key {key!r}")
    return mapping[key]


def _parse_column(name: str, spec: dict[str, Any], where: str) -> Column:
    if not isinstance(spec, dict):
        raise ContractError(f"{where}.{name}: expected a mapping")
    declared = _require(spec, "type", f"{where}.{name}")
    if declared not in COLUMN_TYPES:
        raise ContractError(
            f"{where}.{name}: unknown type {declared!r} (expected one of {sorted(COLUMN_TYPES)})"
        )
    allowed = spec.get("allowed")
    if allowed is not None:
        allowed = tuple(str(value) for value in allowed)
    return Column(
        name=name,
        type=declared,
        nullable=bool(spec.get("nullable", True)),
        allowed=allowed,
        minimum=spec.get("min"),
        maximum=spec.get("max"),
        pattern=spec.get("pattern"),
    )


def load_contract(path: Path) -> Contract:
    raw = yaml.safe_load(path.read_text())
    if not isinstance(raw, dict):
        raise ContractError(f"{path}: expected a YAML mapping")

    where = path.name
    feed = _require(raw, "feed", where)
    version = int(_require(raw, "version", where))
    grain = _require(raw, "grain", where)

    arrival = _require(raw, "arrival", where)
    partition = _require(raw, "partition", where)
    primary_key = tuple(str(k) for k in _require(raw, "primary_key", where))

    partition_from = partition.get("from", "column")
    if partition_from not in {"column", "requested"}:
        raise ContractError(f"{where}: partition.from must be 'column' or 'requested'")
    partition_column = partition.get("column")

    columns = tuple(
        _parse_column(name, spec, where)
        for name, spec in (_require(raw, "columns", where)).items()
    )
    if not columns:
        raise ContractError(f"{where}: contract declares no columns")

    names = [c.name for c in columns]
    duplicates = sorted({n for n in names if names.count(n) > 1})
    if duplicates:
        raise ContractError(f"{where}: duplicate column names {duplicates}")

    for key_column in primary_key:
        if key_column not in names:
            raise ContractError(f"{where}: primary key {key_column!r} is not a declared column")
    if partition_from == "column" and partition_column not in names:
        raise ContractError(
            f"{where}: partition column {partition_column!r} is not a declared column"
        )

    references = tuple(
        Reference(
            column=_require(ref, "column", f"{where}.references"),
            feed=_require(ref, "feed", f"{where}.references"),
            target=_require(ref, "target", f"{where}.references"),
        )
        for ref in raw.get("references") or []
    )
    for ref in references:
        if ref.column not in names:
            raise ContractError(f"{where}: reference column {ref.column!r} is not declared")

    return Contract(
        feed=feed,
        version=version,
        grain=grain,
        arrival_path=_require(arrival, "path", f"{where}.arrival"),
        required=bool(arrival.get("required", True)),
        min_rows=int(arrival.get("min_rows", 1)),
        partition_key=partition.get("key", "event_date"),
        partition_from=partition_from,
        partition_column=partition_column,
        primary_key=primary_key,
        columns=columns,
        references=references,
    )


def load_contracts(directory: Path) -> dict[str, Contract]:
    """Load every contract in a directory, keeping the highest version per feed."""
    contracts: dict[str, Contract] = {}
    for path in sorted(directory.glob("*.yml")):
        contract = load_contract(path)
        current = contracts.get(contract.feed)
        if current is not None:
            if current.version == contract.version:
                raise ContractError(
                    f"duplicate contract version {contract.version} for feed {contract.feed!r}"
                )
            if current.version > contract.version:
                continue
        contracts[contract.feed] = contract
    if not contracts:
        raise ContractError(f"no contracts found in {directory}")
    return contracts


def dependency_order(contracts: dict[str, Contract]) -> list[str]:
    """Topological order: a feed's references are ingested before the feed itself."""
    order: list[str] = []
    visiting: set[str] = set()

    def visit(feed: str) -> None:
        if feed in order:
            return
        if feed in visiting:
            raise ContractError(f"circular reference involving {feed!r}")
        visiting.add(feed)
        for ref in contracts[feed].references:
            if ref.feed not in contracts:
                raise ContractError(f"{feed}: references unknown feed {ref.feed!r}")
            visit(ref.feed)
        visiting.discard(feed)
        order.append(feed)

    for feed in sorted(contracts):
        visit(feed)
    return order
