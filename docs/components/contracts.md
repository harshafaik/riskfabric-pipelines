# Contract Loader

## Overview

`ingest/contracts.py` loads and validates the versioned YAML contracts in `contracts/`.
It consumes one YAML file per feed and produces frozen `Contract` objects plus a
topological feed order. It is the only place a column's name, type, nullability, range,
pattern and referential dependency are written down.

## Schema

### Contract

| Field | Type | Description |
| :--- | :--- | :--- |
| `feed` | `str` | Feed name; must match the directory used under `bronze/`. |
| `version` | `int` | Contract version. `load_contracts` keeps the highest version per feed. |
| `grain` | `str` | Declared grain, copied into every manifest. |
| `arrival_path` | `str` | Path template relative to the inbox; `{event_date}` is substituted. |
| `required` | `bool` | When true, a missing source file raises `MissingFeed`; when false the partition is skipped. |
| `min_rows` | `int` | Minimum accepted rows before the partition is rejected as a whole. |
| `partition_key` | `str` | Name of the partition column written to bronze; defaults to `event_date`. |
| `partition_from` | `str` | `column` derives the partition from a timestamp column; `requested` uses the CLI date. |
| `partition_column` | `str \| None` | Timestamp column used when `partition_from` is `column`. |
| `primary_key` | `tuple[str, ...]` | Columns that must be unique within a partition. |
| `columns` | `tuple[Column, ...]` | Ordered column contracts. |
| `references` | `tuple[Reference, ...]` | Foreign-key dependencies on other feeds. |

### Column

| Field | Type | Description |
| :--- | :--- | :--- |
| `name` | `str` | Column name as it appears in the source file. |
| `type` | `str` | One of `string`, `int32`, `int64`, `uint32`, `float64`, `boolean`, `date`, `timestamp`. |
| `nullable` | `bool` | Whether a null source value is allowed. Defaults to true. |
| `allowed` | `tuple[str, ...] \| None` | Closed set of accepted values. |
| `minimum` | `float \| None` | Inclusive lower bound for numeric columns. |
| `maximum` | `float \| None` | Inclusive upper bound for numeric columns. |
| `pattern` | `str \| None` | Regular expression applied with `str.contains`. |

### Reference

| Field | Type | Description |
| :--- | :--- | :--- |
| `column` | `str` | Referencing column in this feed. |
| `feed` | `str` | Referenced feed name. |
| `target` | `str` | Referenced column, read from every bronze partition of that feed. |

## API

| Name | Signature | Description |
| :--- | :--- | :--- |
| `load_contract` | `load_contract(path: Path) -> Contract` | Parse and validate one contract file. |
| `load_contracts` | `load_contracts(directory: Path) -> dict[str, Contract]` | Load every `*.yml`, keeping the highest version per feed. |
| `dependency_order` | `dependency_order(contracts: dict[str, Contract]) -> list[str]` | Topological order with referenced feeds first. |
| `ContractError` | `ContractError(Exception)` | Raised when a contract file is malformed or inconsistent. |

## Design Decisions

**Contracts are the only schema authority.** Nothing infers a schema from the source file.
`check_structure` compares the file's columns against the contract in both directions, so a
renamed or dropped column and an unannounced new column both fail the load. The rationale and
the rejected alternative are in [0001](../decisions/0001-contracts-at-the-boundary.md).

**The contract is validated before it is used.** `load_contract` rejects an unknown column
type, a primary key that is not a declared column, a partition column that is not declared,
and duplicate column names. `load_contracts` rejects two files declaring the same feed and
version, and `dependency_order` rejects references to an unknown feed and circular references.
A malformed contract stops the run with exit code `2` before any data is read.

**Grain is declared, not inferred.** `customers` is one row per `customer_id` per snapshot;
`transactions` is one row per `transaction_id`. The string is copied verbatim into
`_manifest.json`, so a consumer can verify the expected grain without reading the contract.

**Feed order is derived from references.** `dependency_order` walks `references` and emits
referenced feeds first, so `--feed all` loads `customers` before `transactions`. The ordering
is the same contract that `validation.py` uses to check foreign keys, which prevents the two
notions of dependency from drifting apart.

**Position in the pipeline.** `cli.py` and `orchestration/definitions.py` both call
`load_contracts` and pass the resulting `Contract` to `pipeline.py`. Nothing downstream reads
YAML directly.

## Current Limitations

The evolution policy in the `evolution:` block is prose. The loader does not compare a new
version against the previous one, so the additive-only rule is a convention rather than an
enforced constraint. Enforcement would require retaining historical contracts at load time.

Only the highest version per feed is kept in memory. A run cannot validate the same feed
against two contract versions, which blocks a staged rollout where old and new partitions
coexist.

Regular expressions are not compiled at load time. An invalid `pattern` fails during row
validation rather than during contract loading, so the error path is later and less obvious
than it should be.

Only `customers` and `transactions` are contracted. `accounts`, `cards` and `fraud_metadata`
exist in the scrip output but are intentionally out of scope for this repository.
