# 0005 — The manifest's `source` field leaks the inbox path

## What was wrong

`pipeline.ingest_partition` writes the inbox path exactly as supplied into `_manifest.json`
under `source`. The same rows, contract and requested date can therefore produce different
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

`source` is stored as `source.as_posix()` on the `Path` handed to `ingest_partition`. Nothing
normalizes it against a configured root, and the manifest has no notion of a root.

## Fix

Store the path relative to a configured inbox root, or drop `source` entirely and let the
orchestrator record provenance. The first keeps the field useful for debugging; the second is
consistent with the stated separation between determinism and provenance.

## Prevention

Add a test that ingests the same partition through a relative and an absolute inbox into two
data roots and compares the manifests byte for byte. The test must fail before the fix and pass
after. Until then, the limitation is recorded in `docs/components/writer.md` and
`docs/theory_of_operation.md` §7 rather than left implicit.

## Related

- `ingest/pipeline.py` — manifest construction
- `ingest/writer.py` — `write_json_atomic`
- `docs/components/writer.md` — Current Limitations
- `docs/theory_of_operation.md` §7
