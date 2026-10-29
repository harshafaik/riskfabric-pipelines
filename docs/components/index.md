# Components

One document per module. Every file follows [WRITING_SPEC.md](../WRITING_SPEC.md).

| Component | Source | Responsibility |
| :--- | :--- | :--- |
| [Contract Loader](contracts.md) | `ingest/contracts.py` | Load versioned YAML contracts and derive feed order |
| [Validation](validation.md) | `ingest/validation.py` | Three-stage validation with per-row quarantine |
| [Writer](writer.md) | `ingest/writer.py` | Partition layout, atomic writes, fingerprints, reconciliation |
| [Pipeline](pipeline.md) | `ingest/pipeline.py` | Ingest one partition of one feed |
| [CLI](cli.md) | `ingest/cli.py` | Argument parsing and exit codes |
| [Transforms](transforms.md) | `models/`, `dbt_project.yml` | Staging views and the star schema in DuckDB |
| [Orchestration](orchestration.md) | `orchestration/definitions.py` | Dagster assets, partitions, freshness check and schedule |

## Flow

```
cli.py
  └─ pipeline.py
       ├─ contracts.py   resolve schema, grain and dependencies
       ├─ validation.py  structure → types → semantics
       └─ writer.py      partition, manifest, reconciliation
            └─ data/bronze/

models/                read data/bronze/ and build marts in data/riskfabric.duckdb
orchestration/         call pipeline.py per partition, then dbt via models/
```

The invariants shared by these components are listed in
[architecture.md](../architecture.md#invariants) and explained in
[theory_of_operation.md](../theory_of_operation.md).
