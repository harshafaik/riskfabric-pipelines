"""Idempotency: a rerun of the same partition is byte-identical."""

from __future__ import annotations

import json
import os
from pathlib import Path

import polars as pl

from ingest.writer import fingerprint
from tests.helpers import (
    FIXTURE_INBOX,
    SNAPSHOT,
    TX_DAY,
    ingest,
    ingest_baseline,
    partition,
)


def _snapshot(data) -> tuple[bytes, bytes]:
    directory = partition(data, "transactions", TX_DAY)
    return (
        (directory / "part-00000.parquet").read_bytes(),
        (directory / "_manifest.json").read_bytes(),
    )


def test_rerun_produces_byte_identical_output(contracts, inbox, data):
    ingest_baseline(contracts, inbox, data)
    first = _snapshot(data)

    ingest_baseline(contracts, inbox, data)
    second = _snapshot(data)

    assert first == second


def test_fingerprint_does_not_depend_on_row_order(contracts, inbox, data):
    ingest_baseline(contracts, inbox, data)
    directory = partition(data, "transactions", TX_DAY)
    committed = pl.read_parquet(directory / "part-00000.parquet")
    shuffled = committed.sample(fraction=1.0, shuffle=True, seed=7)

    assert fingerprint(committed, ("transaction_id",)) == fingerprint(
        shuffled, ("transaction_id",)
    )


def test_manifest_contains_no_wall_clock_time(contracts, inbox, data):
    ingest_baseline(contracts, inbox, data)
    _, manifest = _snapshot(data)

    # The partition must be a pure function of its input, so no timestamp may
    # appear in it. Provenance belongs to the orchestrator, not the data.
    assert b"ingested_at" not in manifest
    assert b"2026-10" not in manifest


def test_manifest_source_is_relative_to_the_inbox(contracts, data, tmp_path):
    relative_inbox = Path(os.path.relpath(FIXTURE_INBOX, Path.cwd()))
    absolute_data = tmp_path / "absolute-data"

    ingest(contracts, "customers", SNAPSHOT, relative_inbox, data)
    ingest(contracts, "customers", SNAPSHOT, FIXTURE_INBOX.resolve(), absolute_data)

    relative_manifest = (
        partition(data, "customers", SNAPSHOT) / "_manifest.json"
    ).read_text()
    absolute_manifest = (
        partition(absolute_data, "customers", SNAPSHOT) / "_manifest.json"
    ).read_text()

    assert relative_manifest == absolute_manifest
    assert json.loads(relative_manifest)["source"] == (
        "customers/event_date=2026-01-01/customers.parquet"
    )
