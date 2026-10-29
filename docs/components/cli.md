# CLI

## Overview

`ingest/cli.py` is the `python -m ingest` entry point and the `rfp` console script. It parses
arguments, loads contracts, derives feed order and calls `pipeline.ingest_partition` for each
requested date. It consumes paths and a date range and produces a line per partition plus a
summary, with failures mapped to distinct exit codes.

## API

| Name | Signature | Description |
| :--- | :--- | :--- |
| `main` | `main(argv: list[str] \| None = None) -> int` | Entry point; returns the process exit code. |
| `_build_parser` | `_build_parser() -> argparse.ArgumentParser` | Defines the `run` subcommand and its options. |
| `_parse_date` | `_parse_date(value: str) -> date` | Parses `YYYY-MM-DD`; raises an argument error otherwise. |
| `_date_range` | `_date_range(start, end) -> list[date]` | Inclusive date range; rejects an inverted range. |

| Option | Type | Description |
| :--- | :--- | :--- |
| `--feed` | `str` | Feed name or `all`. `all` uses `dependency_order`. |
| `--date` | `YYYY-MM-DD` | Single partition. Mutually exclusive with `--from`/`--to`. |
| `--from` | `YYYY-MM-DD` | First partition of a range. |
| `--to` | `YYYY-MM-DD` | Last partition of a range. |
| `--inbox` | `Path` | Source root; defaults to `fixtures/inbox`. |
| `--data` | `Path` | Output root; defaults to `data`. |
| `--contracts` | `Path` | Contract directory; defaults to `contracts`. |

| Exit code | Meaning |
| :--- | :--- |
| `0` | Every requested partition committed or was skipped. |
| `2` | Contract error, structural violation or too few valid rows. |
| `3` | A required feed's source file is absent. |
| `4` | Read-back reconciliation failed. |

## Design Decisions

**Exit codes are part of the interface.** `0`, `2`, `3` and `4` distinguish success, a bad
contract or file, a missing feed and a failed reconciliation. CI and an orchestrator can branch
on the code without parsing stdout.

**Single-date and range modes are exclusive.** Supplying both `--date` and `--from`/`--to`
exits immediately, and supplying neither does the same. Ambiguous invocations fail before any
file is read.

**`--feed all` follows the contract graph.** The CLI does not hardcode `customers` before
`transactions`; it calls `dependency_order`. Adding a new reference changes the order without a
CLI edit.

**Partitions are processed in date-major, feed-minor order.** For each date the CLI iterates
the ordered feeds, which matches the dependency requirement for a backfill starting at the
first snapshot.

**Position in the pipeline.** The CLI is the local entry point to `pipeline.py`. The Dagster
assets call `ingest_partition` directly and reproduce the same per-partition semantics.

## Current Limitations

There are no CLI-level tests. The pytest suite calls `ingest_partition` directly, so exit codes
`2`, `3` and `4` and the argument exclusivity rules are verified by reading the code rather
than by a test. A subprocess test per exit code would close the gap.

Output is printed, not logged. There is no structured event stream, log level or run identifier,
so a failed range is diagnosed from the terminal transcript.

A failure stops the run at the first failing partition. Earlier partitions in the range remain
committed, which is the intended per-partition behaviour, but there is no checkpoint or resume
command; the operator reruns the range and relies on idempotency.
