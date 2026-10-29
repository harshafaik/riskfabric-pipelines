"""End-to-end behaviour of one partition: commit, quarantine, reject, reconcile."""

from __future__ import annotations

import json
import shutil
from datetime import date, datetime

import polars as pl
import pytest

from ingest.pipeline import MissingFeed
from ingest.validation import ContractViolation
from tests.helpers import (
    FIXTURE_INBOX,
    SNAPSHOT,
    TX_DAY,
    ingest,
    ingest_baseline,
    partition,
    quarantine,
    rewrite,
    set_value,
    source,
)


def test_customer_snapshot_lands(contracts, inbox, data):
    report = ingest(contracts, "customers", SNAPSHOT, inbox, data)

    assert report.status == "committed"
    assert report.rows_in == report.rows_committed == 500
    assert report.rows_rejected == 0
    assert (partition(data, "customers", SNAPSHOT) / "part-00000.parquet").exists()


def test_transaction_partition_commits_every_valid_row(contracts, inbox, data):
    report = ingest_baseline(contracts, inbox, data)

    assert report.status == "committed"
    assert report.rows_in > 0
    assert report.rows_committed == report.rows_in
    assert report.rows_rejected == 0


def test_manifest_matches_the_committed_partition(contracts, inbox, data):
    report = ingest_baseline(contracts, inbox, data)

    directory = partition(data, "transactions", TX_DAY)
    manifest = json.loads((directory / "_manifest.json").read_text())
    committed = pl.read_parquet(directory / "part-00000.parquet")

    assert manifest["feed"] == "transactions"
    assert manifest["contract"] == "transactions_v1"
    assert manifest["rows_committed"] == committed.height == report.rows_committed
    assert manifest["rows_in"] == manifest["rows_committed"] + manifest["rows_rejected"]
    assert manifest["columns"] == list(committed.columns)
    assert manifest["fingerprint"] == report.fingerprint


def test_absent_optional_feed_is_skipped(contracts, inbox, data):
    report = ingest(contracts, "customers", date(2026, 1, 10), inbox, data)

    assert report.status == "skipped"
    assert not partition(data, "customers", date(2026, 1, 10)).exists()


def test_missing_required_feed_raises(contracts, inbox, data):
    with pytest.raises(MissingFeed, match="transactions"):
        ingest(contracts, "transactions", date(2026, 2, 15), inbox, data)


def test_facts_require_their_dimension_first(contracts, inbox, data):
    with pytest.raises(ContractViolation, match="reference feed 'customers'"):
        ingest(contracts, "transactions", TX_DAY, inbox, data)

    assert not partition(data, "transactions", TX_DAY).exists()


@pytest.mark.parametrize(
    ("transform", "problem"),
    [
        (lambda frame: frame.drop("mcc"), "missing columns: mcc"),
        (
            lambda frame: frame.with_columns(pl.lit(1).alias("surprise")),
            "columns not in transactions_v1: surprise",
        ),
        (lambda frame: frame.head(0), "file has no rows"),
    ],
    ids=["missing-column", "unexpected-column", "empty-file"],
)
def test_structural_problems_reject_the_whole_file(
    contracts, inbox, data, transform, problem
):
    rewrite(inbox, TX_DAY, transform)

    with pytest.raises(ContractViolation) as caught:
        ingest(contracts, "transactions", TX_DAY, inbox, data)

    assert any(problem in issue for issue in caught.value.problems)
    assert not partition(data, "transactions", TX_DAY).exists()


def test_type_errors_are_quarantined_with_reasons(contracts, inbox, data):
    ingest(contracts, "customers", SNAPSHOT, inbox, data)

    def corrupt(frame: pl.DataFrame) -> pl.DataFrame:
        frame = frame.with_columns(
            pl.col("amount").cast(pl.String),
            pl.col("timestamp").cast(pl.String),
        )
        frame = set_value(frame, "amount", 0, "not-a-number")
        return set_value(frame, "timestamp", 1, "not-a-time")

    rewrite(inbox, TX_DAY, corrupt)
    report = ingest(contracts, "transactions", TX_DAY, inbox, data)

    assert report.rows_rejected == 2
    rejected = pl.read_parquet(quarantine(data, "transactions", TX_DAY) / "rejected.parquet")
    reasons = rejected["_reject_reason"].to_list()
    assert any("type:amount" in reason for reason in reasons)
    assert any("type:timestamp" in reason for reason in reasons)


def test_orphan_reference_is_quarantined(contracts, inbox, data):
    ingest(contracts, "customers", SNAPSHOT, inbox, data)
    rewrite(inbox, TX_DAY, lambda frame: set_value(frame, "customer_id", 0, "ghost"))

    report = ingest(contracts, "transactions", TX_DAY, inbox, data)

    assert report.rows_rejected == 1
    rejected = pl.read_parquet(quarantine(data, "transactions", TX_DAY) / "rejected.parquet")
    assert rejected["_reject_reason"].to_list() == ["ref:customer_id"]


def test_duplicate_primary_keys_are_quarantined(contracts, inbox, data):
    ingest(contracts, "customers", SNAPSHOT, inbox, data)
    rewrite(
        inbox,
        TX_DAY,
        lambda frame: frame.vstack(frame.head(1)),
    )

    report = ingest(contracts, "transactions", TX_DAY, inbox, data)

    # Both copies of the duplicated key are rejected, not just the second one.
    assert report.rows_rejected == 2
    rejected = pl.read_parquet(quarantine(data, "transactions", TX_DAY) / "rejected.parquet")
    assert set(rejected["_reject_reason"].to_list()) == {"duplicate:primary_key"}


def test_rows_for_another_day_are_quarantined(contracts, inbox, data):
    ingest(contracts, "customers", SNAPSHOT, inbox, data)
    rewrite(
        inbox,
        TX_DAY,
        lambda frame: set_value(frame, "timestamp", 0, datetime(2026, 2, 1, 10, 0, 0)),
    )

    report = ingest(contracts, "transactions", TX_DAY, inbox, data)

    assert report.rows_rejected == 1
    rejected = pl.read_parquet(quarantine(data, "transactions", TX_DAY) / "rejected.parquet")
    assert rejected["_reject_reason"].to_list() == ["partition_mismatch"]


def test_clean_rerun_clears_a_stale_quarantine_file(contracts, inbox, data):
    ingest(contracts, "customers", SNAPSHOT, inbox, data)
    rewrite(inbox, TX_DAY, lambda frame: set_value(frame, "customer_id", 0, "ghost"))
    ingest(contracts, "transactions", TX_DAY, inbox, data)
    assert (quarantine(data, "transactions", TX_DAY) / "rejected.parquet").exists()

    shutil.copyfile(source(FIXTURE_INBOX, TX_DAY), source(inbox, TX_DAY))
    report = ingest(contracts, "transactions", TX_DAY, inbox, data)

    assert report.rows_rejected == 0
    assert not (quarantine(data, "transactions", TX_DAY) / "rejected.parquet").exists()
