"""Dagster orchestration: one partition per (feed, event_date).

The bronze assets are thin wrappers around ``ingest.ingest_partition``; the
manifest it writes becomes materialization metadata. ``star_schema`` runs the
dbt project against the same bronze root. The daily schedule is built from the
partitioned job, so a backfill of one day in the UI is exactly the idempotency
demonstration.

Run the UI with::

    dagster dev -f orchestration/definitions.py
"""

import os
import shutil
import subprocess
import sys
from datetime import date
from pathlib import Path

from dagster import (
    AssetCheckResult,
    AssetExecutionContext,
    ConfigurableResource,
    DailyPartitionsDefinition,
    Definitions,
    MaterializeResult,
    RetryPolicy,
    asset,
    asset_check,
    build_schedule_from_partitioned_job,
    define_asset_job,
)

from ingest.contracts import load_contracts
from ingest.pipeline import ingest_partition

PART_FILE = "part-00000.parquet"

# End is exclusive: this window covers 2026-01-01 .. 2026-01-30, the committed
# fixture. Point RFP_PARTITION_END at tomorrow's date for live scrip output.
PARTITION_END = os.environ.get("RFP_PARTITION_END", "2026-01-31")
partitions = DailyPartitionsDefinition(start_date="2026-01-01", end_date=PARTITION_END)

RETRY = RetryPolicy(max_retries=2)


class PipelinePaths(ConfigurableResource):
    inbox: str = "fixtures/inbox"
    data_root: str = "data"
    contracts: str = "contracts"


def _ingest(context: AssetExecutionContext, paths: PipelinePaths, feed: str) -> MaterializeResult:
    partition = date.fromisoformat(context.partition_key)
    contract = load_contracts(Path(paths.contracts))[feed]
    report = ingest_partition(
        contract,
        partition,
        inbox=Path(paths.inbox),
        data_root=Path(paths.data_root),
    )

    metadata = {
        "feed": feed,
        "partition": context.partition_key,
        "status": report.status,
        "rows_in": report.rows_in,
        "rows_committed": report.rows_committed,
        "rows_rejected": report.rows_rejected,
        "contract": contract.name,
    }
    if report.fingerprint is not None:
        metadata["fingerprint"] = report.fingerprint

    context.log.info(
        "%s %s %s in=%s ok=%s rejected=%s",
        context.partition_key,
        feed,
        report.status,
        report.rows_in,
        report.rows_committed,
        report.rows_rejected,
    )
    return MaterializeResult(metadata=metadata)


@asset(partitions_def=partitions, retry_policy=RETRY, group_name="bronze")
def customers_bronze(context: AssetExecutionContext, paths: PipelinePaths) -> MaterializeResult:
    """Customer dimension snapshot. Periodic, so most partitions are a no-op."""
    return _ingest(context, paths, "customers")


@asset(
    partitions_def=partitions,
    retry_policy=RETRY,
    deps=[customers_bronze],
    group_name="bronze",
)
def transactions_bronze(context: AssetExecutionContext, paths: PipelinePaths) -> MaterializeResult:
    """Daily transaction partition, validated against transactions_v1."""
    return _ingest(context, paths, "transactions")


def _date_partitions(root: Path) -> list[str]:
    return sorted(
        path.name.split("=", 1)[1]
        for path in root.glob("event_date=*")
        if path.is_dir()
    )


def bronze_freshness(paths: PipelinePaths) -> tuple[bool, dict[str, str]]:
    """Bronze is fresh when it has caught up with the latest delivered partition."""
    inbox = _date_partitions(Path(paths.inbox) / "transactions")
    bronze = _date_partitions(Path(paths.data_root) / "bronze" / "transactions")

    latest_inbox = max(inbox) if inbox else "none"
    latest_bronze = max(bronze) if bronze else "none"
    return (
        latest_inbox != "none" and latest_bronze == latest_inbox,
        {"latest_inbox": latest_inbox, "latest_bronze": latest_bronze},
    )


@asset_check(asset=transactions_bronze)
def transactions_bronze_fresh(paths: PipelinePaths) -> AssetCheckResult:
    passed, metadata = bronze_freshness(paths)
    return AssetCheckResult(passed=passed, metadata=metadata)


def _dbt_executable() -> str:
    sibling = Path(sys.executable).with_name("dbt")
    if sibling.exists():
        return str(sibling)
    found = shutil.which("dbt")
    if found is None:
        raise RuntimeError("dbt not found; install the 'transform' extra")
    return found


@asset(
    partitions_def=partitions,
    retry_policy=RETRY,
    deps=[transactions_bronze],
    group_name="marts",
)
def star_schema(context: AssetExecutionContext, paths: PipelinePaths) -> MaterializeResult:
    """Build and test the dbt star schema over the current bronze root."""
    repo_root = Path(__file__).resolve().parents[1]
    data_root = Path(paths.data_root).resolve()
    data_root.mkdir(parents=True, exist_ok=True)

    warehouse = data_root / "riskfabric.duckdb"
    environment = {**os.environ, "RFP_DUCKDB": str(warehouse)}
    command = [
        _dbt_executable(),
        "build",
        "--profiles-dir",
        str(repo_root),
        "--vars",
        f"{{bronze_root: {(data_root / 'bronze').as_posix()}}}",
    ]

    completed = subprocess.run(
        command, cwd=repo_root, env=environment, capture_output=True, text=True
    )
    if completed.returncode != 0:
        raise RuntimeError(
            f"dbt build failed ({completed.returncode}):\n"
            f"{completed.stdout[-2000:]}\n{completed.stderr[-2000:]}"
        )

    summary = [line for line in completed.stdout.splitlines() if line.strip().startswith("Done.")]
    context.log.info(summary[-1] if summary else "dbt build succeeded")

    return MaterializeResult(
        metadata={"partition": context.partition_key, "warehouse": str(warehouse)}
    )


ingest_and_transform = define_asset_job(
    "ingest_and_transform",
    selection=[customers_bronze, transactions_bronze, star_schema],
)


def build_definitions(paths: PipelinePaths | None = None) -> Definitions:
    return Definitions(
        assets=[customers_bronze, transactions_bronze, star_schema],
        asset_checks=[transactions_bronze_fresh],
        jobs=[ingest_and_transform],
        schedules=[build_schedule_from_partitioned_job(ingest_and_transform)],
        resources={"paths": paths or PipelinePaths()},
    )


defs = build_definitions()
