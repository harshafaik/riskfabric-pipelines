"""Partition layout, atomic writes, fingerprints and manifests.

A partition is a directory containing exactly one Parquet file and one manifest.
Writes are atomic (temp file + rename) and the manifest contains no wall-clock
timestamp, so re-running a partition produces identical output.
"""

from __future__ import annotations

import hashlib
import json
import os
from datetime import date
from pathlib import Path
from typing import Callable

import polars as pl

PART_FILE = "part-00000.parquet"
MANIFEST_FILE = "_manifest.json"
REJECT_FILE = "rejected.parquet"


class ReconciliationError(Exception):
    """A written partition does not match the manifest that describes it."""


def partition_dir(data_root: Path, feed: str, partition: date) -> Path:
    return data_root / "bronze" / feed / f"event_date={partition.isoformat()}"


def quarantine_dir(data_root: Path, feed: str, partition: date) -> Path:
    return data_root / "quarantine" / feed / f"event_date={partition.isoformat()}"


def fingerprint(df: pl.DataFrame, key: tuple[str, ...]) -> str:
    """Content hash of a frame, independent of row order and file metadata."""
    payload = df.sort(list(key)).write_csv(separator=",")
    return "sha256:" + hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _atomic(path: Path, write: Callable[[Path], None]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name("." + path.name + ".tmp")
    write(temporary)
    os.replace(temporary, path)


def write_parquet_atomic(df: pl.DataFrame, path: Path) -> None:
    _atomic(path, lambda tmp: df.write_parquet(tmp, compression="zstd"))


def write_json_atomic(payload: dict, path: Path) -> None:
    _atomic(
        path,
        lambda tmp: tmp.write_text(
            json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        ),
    )


def reconcile(path: Path, manifest: dict, key: tuple[str, ...]) -> None:
    """Read the committed partition back and prove it matches the manifest."""
    committed = pl.read_parquet(path)

    expected_rows = manifest["rows_committed"]
    if committed.height != expected_rows:
        raise ReconciliationError(
            f"{path}: manifest says {expected_rows} rows, file has {committed.height}"
        )
    if list(committed.columns) != manifest["columns"]:
        raise ReconciliationError(f"{path}: columns differ from manifest")

    actual = fingerprint(committed, key)
    if actual != manifest["fingerprint"]:
        raise ReconciliationError(
            f"{path}: fingerprint {actual} does not match manifest {manifest['fingerprint']}"
        )
