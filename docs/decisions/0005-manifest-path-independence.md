# 0005 — The manifest's `source` field leaked the inbox path

**Status:** fixed. `source` is now stored relative to the inbox root.

## What was wrong

`pipeline.ingest_partition` wrote the inbox path exactly as supplied into `_manifest.json`
under `source`. The same rows, contract and requested date could therefore produce different
manifests depending on how the path was spelled.

## Quantified impact

Ingesting `customers/event_date=2026-01-01` twice, once with `--inbox fixtures/inbox` and once
with `--inbox /home/harshafaik/Projects/riskfabric-pipelines/fixtures/inbox`, produced manifests
that differ on exactly one line, line 37:

```
"source": "fixtures/inbox/customers/event_date=2026-01-01/customers.parquet"
"source": "/home/harshafaik/Projects/riskfabric-pipelines/fixtures/inbox/customers/event_date=2026-01-01/customers.parquet"
```

The part file is identical. The contradiction is with the invariant "a partition is a pure
function of its input": the input here includes a path spelling that does not change the data.

The CI byte-identity check does not catch this because both runs use the same relative paths.

## Root cause

`source` was stored as `source.as_posix()` on the `Path` handed to `ingest_partition`. Nothing
normalized it against a configured root, and the manifest had no notion of a root.

## Fix

Store the path relative to the inbox root. `ingest/pipeline.py` now writes
`source.relative_to(inbox).as_posix()`, so
`customers/event_date=2026-01-01/customers.parquet` is recorded regardless of how `--inbox` was
spelled. The field stays useful for debugging, and the root itself no longer appears in the
output.

## Prevention

`tests/test_idempotency.py::test_manifest_source_is_relative_to_the_inbox` ingests the customer
snapshot through a relative and an absolute inbox into two data roots and asserts the manifests
are byte-identical. It fails against the old behaviour.

## Related

- `ingest/pipeline.py` — manifest construction
- `ingest/writer.py` — `write_json_atomic`
- `tests/test_idempotency.py` — regression test
- `docs/components/writer.md` — provenance decision
- `docs/theory_of_operation.md` §7
