"""Ingest one partition of one feed."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path

import polars as pl

from .contracts import Contract
from .validation import ContractViolation, check_structure, validate_rows
from .writer import (
    MANIFEST_FILE,
    PART_FILE,
    REJECT_FILE,
    fingerprint,
    partition_dir,
    quarantine_dir,
    reconcile,
    write_json_atomic,
    write_parquet_atomic,
)

COMMITTED = "committed"
SKIPPED = "skipped"


class MissingFeed(Exception):
    """A required feed did not arrive for a partition."""


@dataclass(frozen=True)
class Report:
    feed: str
    partition: date
    status: str
    rows_in: int = 0
    rows_committed: int = 0
    rows_rejected: int = 0
    fingerprint: str | None = None


def _source_path(inbox: Path, contract: Contract, partition: date) -> Path:
    return inbox / contract.arrival_path.format(event_date=partition.isoformat())


def load_reference_ids(data_root: Path, feed: str, target: str) -> set[str]:
    """Collect the ids of a referenced feed from every bronze partition."""
    files = sorted((data_root / "bronze" / feed).glob(f"event_date=*/{PART_FILE}"))
    if not files:
        return set()
    ids: set[str] = set()
    for path in files:
        values = pl.read_parquet(path, columns=[target])[target]
        ids.update(str(value) for value in values.drop_nulls().to_list())
    return ids


def ingest_partition(
    contract: Contract,
    partition: date,
    *,
    inbox: Path,
    data_root: Path,
) -> Report:
    source = _source_path(inbox, contract, partition)

    if not source.exists():
        if contract.required:
            raise MissingFeed(
                f"{contract.feed} {partition.isoformat()}: expected {source}"
            )
        return Report(
            feed=contract.feed,
            partition=partition,
            status=SKIPPED,
        )

    frame = pl.read_parquet(source)
    check_structure(frame, contract, partition)

    references: dict[str, set[str]] = {}
    for ref in contract.references:
        ids = load_reference_ids(data_root, ref.feed, ref.target)
        if not ids:
            raise ContractViolation(
                contract.feed,
                partition,
                [
                    f"reference feed {ref.feed!r} has no bronze partitions; "
                    f"ingest {ref.feed} before {contract.feed}"
                ],
            )
        references[ref.column] = ids

    result = validate_rows(frame, contract, partition, references)

    if result.accepted.height < contract.min_rows:
        raise ContractViolation(
            contract.feed,
            partition,
            [
                f"{result.accepted.height} of {frame.height} rows passed validation; "
                f"expected at least {contract.min_rows}"
            ],
        )

    destination = partition_dir(data_root, contract.feed, partition)
    accepted = result.accepted
    manifest = {
        "feed": contract.feed,
        "contract": contract.name,
        "contract_version": contract.version,
        "partition": partition.isoformat(),
        "grain": contract.grain,
        "source": source.relative_to(inbox).as_posix(),
        "rows_in": frame.height,
        "rows_committed": accepted.height,
        "rows_rejected": result.rejected.height,
        "columns": list(accepted.columns),
        "primary_key": list(contract.primary_key),
        "fingerprint": fingerprint(accepted, contract.primary_key),
    }
    if manifest["rows_in"] != manifest["rows_committed"] + manifest["rows_rejected"]:
        raise AssertionError("row accounting does not balance")

    rejected_path = quarantine_dir(data_root, contract.feed, partition) / REJECT_FILE
    if result.rejected.height:
        write_parquet_atomic(result.rejected, rejected_path)
    elif rejected_path.exists():
        rejected_path.unlink()

    target = destination / PART_FILE
    write_parquet_atomic(accepted, target)
    write_json_atomic(manifest, destination / MANIFEST_FILE)
    reconcile(target, manifest, contract.primary_key)

    return Report(
        feed=contract.feed,
        partition=partition,
        status=COMMITTED,
        rows_in=manifest["rows_in"],
        rows_committed=manifest["rows_committed"],
        rows_rejected=manifest["rows_rejected"],
        fingerprint=manifest["fingerprint"],
    )
