# Decisions

Two record shapes are used. Decision records state a choice, its rationale and the rejected
alternatives. Incident records state what was wrong, the measured impact, the root cause, the
fix and the prevention.

## Decision records

- [0001 — Contracts at the boundary](decisions/0001-contracts-at-the-boundary.md) — why the
  schema is declared rather than inferred.
- [0002 — Plain Parquet, not a table format](decisions/0002-no-table-format.md) — why Iceberg
  and Delta were rejected at this scale.
- [0003 — Late-arriving corrections replace a partition](decisions/0003-late-arriving-corrections.md)
  — the unit of replacement, with the replay evidence.
- [0004 — A wrong-day row is quarantined, not rerouted](decisions/0004-partition-mismatch-is-an-error.md)
  — why a row is never silently moved.
- [0007 — Dagster, not Airflow](decisions/0007-dagster-over-airflow.md) — the asset-model
  tradeoff and the rejected orchestrators.
- [0008 — Facts join dimensions on natural keys](decisions/0008-natural-vs-surrogate-keys.md)
  — the deferred as-of join.

## Incident records

- [0005 — The manifest's `source` field leaks the inbox path](decisions/0005-manifest-path-independence.md)
  — a hole in the determinism invariant.
- [0006 — The fixture drifted from scrip schema v1 to v2](decisions/0006-fixture-schema-drift.md)
  — the stale fixture, the wrong row count and the schema-version guard.
