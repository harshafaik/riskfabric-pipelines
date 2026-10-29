"""Command line entry point: ``python -m ingest run ...``."""

from __future__ import annotations

import argparse
import sys
from datetime import date, timedelta
from pathlib import Path

from .contracts import ContractError, dependency_order, load_contracts
from .pipeline import MissingFeed, ingest_partition
from .validation import ContractViolation
from .writer import ReconciliationError

EXIT_OK = 0
EXIT_CONTRACT = 2
EXIT_MISSING_FEED = 3
EXIT_RECONCILIATION = 4


def _parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError(f"expected YYYY-MM-DD, got {value!r}") from error


def _date_range(start: date, end: date) -> list[date]:
    if end < start:
        raise SystemExit("error: --to is before --from")
    return [start + timedelta(days=offset) for offset in range((end - start).days + 1)]


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="rfp",
        description="Contract-validated ingestion for the RiskFabric feed.",
    )
    subcommands = parser.add_subparsers(dest="command", required=True)

    run = subcommands.add_parser("run", help="ingest a feed for one partition or a range")
    run.add_argument("--feed", required=True, help="feed name, or 'all'")
    run.add_argument("--date", type=_parse_date, help="single partition (YYYY-MM-DD)")
    run.add_argument("--from", dest="start", type=_parse_date, help="first partition")
    run.add_argument("--to", dest="end", type=_parse_date, help="last partition")
    run.add_argument("--inbox", type=Path, default=Path("fixtures/inbox"))
    run.add_argument("--data", type=Path, default=Path("data"))
    run.add_argument("--contracts", type=Path, default=Path("contracts"))
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)

    if args.date and (args.start or args.end):
        raise SystemExit("error: use either --date or --from/--to, not both")
    if not args.date and not (args.start and args.end):
        raise SystemExit("error: provide --date, or both --from and --to")

    partitions = [args.date] if args.date else _date_range(args.start, args.end)

    try:
        contracts = load_contracts(args.contracts)
    except ContractError as error:
        print(f"error: {error}", file=sys.stderr)
        return EXIT_CONTRACT

    order = dependency_order(contracts)
    if args.feed == "all":
        feeds = order
    elif args.feed in contracts:
        feeds = [args.feed]
    else:
        print(
            f"error: unknown feed {args.feed!r}; contracts define {', '.join(order)}",
            file=sys.stderr,
        )
        return EXIT_CONTRACT

    committed = skipped = rejected = 0
    for partition in partitions:
        for feed in feeds:
            try:
                report = ingest_partition(
                    contracts[feed],
                    partition,
                    inbox=args.inbox,
                    data_root=args.data,
                )
            except MissingFeed as error:
                print(f"error: {error}", file=sys.stderr)
                return EXIT_MISSING_FEED
            except ContractViolation as error:
                print(f"contract violation: {error}", file=sys.stderr)
                for problem in error.problems:
                    print(f"  - {problem}", file=sys.stderr)
                return EXIT_CONTRACT
            except ReconciliationError as error:
                print(f"reconciliation failed: {error}", file=sys.stderr)
                return EXIT_RECONCILIATION

            if report.status == "skipped":
                skipped += 1
                print(f"{partition.isoformat()}  {report.feed:<12}  skipped (no snapshot)")
                continue

            committed += 1
            rejected += report.rows_rejected
            print(
                f"{partition.isoformat()}  {report.feed:<12}  committed  "
                f"in={report.rows_in:<5} ok={report.rows_committed:<5} "
                f"rejected={report.rows_rejected:<3} {report.fingerprint}"
            )

    print(
        f"\n{committed} partition(s) committed, {skipped} skipped, "
        f"{rejected} row(s) quarantined"
    )
    return EXIT_OK


if __name__ == "__main__":
    raise SystemExit(main())
