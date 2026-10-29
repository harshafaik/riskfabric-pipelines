# Writer

## Overview

`ingest/writer.py` owns the on-disk contract: partition paths, atomic writes, content
fingerprints, deterministic manifests and read-back reconciliation. It consumes an accepted
`pl.DataFrame` and a manifest dictionary, and produces one Parquet file plus one JSON manifest
per partition. It contains no validation or feed-specific logic.

## API

| Name | Signature | Description |
| :--- | :--- | :--- |
| `partition_dir` | `partition_dir(data_root, feed, partition) -> Path` | `bronze/<feed>/event_date=YYYY-MM-DD/`. |
| `quarantine_dir` | `quarantine_dir(data_root, feed, partition) -> Path` | `quarantine/<feed>/event_date=YYYY-MM-DD/`. |
| `fingerprint` | `fingerprint(df, key) -> str` | `sha256:` hash of the frame serialized as CSV, sorted by `key`. |
| `write_parquet_atomic` | `write_parquet_atomic(df, path) -> None` | Write to a temp sibling, then `os.replace`. |
| `write_json_atomic` | `write_json_atomic(payload, path) -> None` | Sorted-key JSON with a trailing newline, written atomically. |
| `reconcile` | `reconcile(path, manifest, key) -> None` | Read the file back and compare rows, columns and fingerprint. |
| `ReconciliationError` | `ReconciliationError(Exception)` | Raised when the committed file disagrees with the manifest. |

The module also defines `PART_FILE` (`part-00000.parquet`), `MANIFEST_FILE` (`_manifest.json`)
and `REJECT_FILE` (`rejected.parquet`).

## Design Decisions

**One partition is one file and one manifest.** A directory never contains a half-written part
file or a manifest describing a different file. The fixed names make reconciliation and the
quarantine path predictable without a catalog.

**Every write is atomic.** `_atomic` writes to `.<name>.tmp` and calls `os.replace`, so a
reader sees either the previous file or the new one. The temp file is created in the target
directory, which keeps the rename on the same filesystem.

**The fingerprint is order-independent.** `fingerprint` sorts by the primary key and hashes the
CSV serialization, so a shuffled frame with the same rows produces the same `sha256:`. This is
what makes the idempotency test meaningful: it compares data, not Parquet metadata.

**The manifest contains no wall clock.** Keys are sorted, indentation is fixed and the file ends
with a newline. A rerun of the same partition over the same input therefore produces identical
bytes; the CI step asserts this over all 62 output files. Run-time provenance belongs to the
orchestrator; determinism belongs to the data.

**Provenance is recorded relative to the inbox.** The manifest's `source` field stores the
source path relative to the inbox root, not as supplied. The same partition ingested through a
relative or an absolute `--inbox` therefore produces identical manifest bytes. The incident that
prompted this is recorded in
[0005](../decisions/0005-manifest-path-independence.md).

**Reconciliation reads the file back.** After writing, `reconcile` compares the committed row
count, the column list and the fingerprint against the manifest. A partial or corrupted write
fails the run with exit code `4` instead of being reported as success.

**Position in the pipeline.** `pipeline.py` calls these functions after validation. The
manifest it builds is also what Dagster emits as materialization metadata.

## Current Limitations

There is no lock. Two processes writing the same partition can interleave their temp files and
replacements; the last `os.replace` wins and the manifest may describe the other writer's rows.
A per-partition lock file or an orchestrator-level concurrency limit would prevent this.

A process killed between `write` and `os.replace` leaves a `.<name>.tmp` file. It is harmless
and overwritten on the next run, but nothing removes orphaned temp files proactively.

Fingerprints are computed over CSV text. `write_csv` escapes separators and quotes, so collisions
are not known to occur, but the hash is not a cryptographic commitment to the Parquet bytes.
