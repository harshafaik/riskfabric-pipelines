# 0001 — Contracts at the boundary

## Context

Ingesting a feed from another system means trusting a schema you do not control.
The usual failure is silent: a column is renamed or a type changes, the loader
infers a new dtype, and a downstream join quietly drops rows six hours later.

RiskFabric's own history has an example of the same class of bug — a feature that
was hardcoded to zero because the producer and consumer disagreed about a column,
with nothing failing at the boundary.

## Decision

Every feed has a versioned YAML contract committed to this repository. It declares
the grain, primary key, partition key, column types, nullability, allowed values,
ranges, patterns, and referential dependencies. Ingest validates against the
contract and rejects anything that does not match, including **unexpected**
columns.

Contracts evolve additively: new columns must be nullable, and any rename, removal
or type widening requires a new `_vN` file.

## Consequences

- A schema change upstream fails at ingestion, loudly, instead of surfacing as a
  downstream data-quality incident.
- The contract is the only place a type is written down. Consumers pin a contract
  version; the manifest records which one produced each partition.
- Strictness on unexpected columns costs flexibility. A producer adding a column
  before the consumer updates will fail the load. That is deliberate: an unannounced
  schema change is a bug, and the fix is a contract change, not a permissive reader.

## Rejected alternatives

**Infer the schema from the file.** Polars and pandas will both do this happily.
It cannot detect a renamed column, and it accepts a type drift that breaks
downstream models without an error. Inference describes what arrived; a contract
describes what is allowed to arrive.

**Validate in the warehouse.** dbt tests run after the data is already in the
warehouse, which is later and more expensive. Warehouse tests remain valuable for
business rules; the contract covers the boundary.
