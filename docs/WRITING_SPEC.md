# Component Documentation Specification

> Canonical structure, tone and content rules for every file in `docs/components/`.
> Adapted from RiskFabric's documentation spec for Python, Polars and dbt.
> Apply it exactly when authoring or rewriting a component document.

## 1. File Structure

Every component document contains these sections in this order:

```
# {Component Name}
## Overview
## Schema        (only when the component defines public dataclasses)
## API           (only when the surface is functions or a CLI)
{body paragraphs}
## Current Limitations
```

At least one of `## Schema` or `## API` is required. Body paragraphs carry no heading.
Do not add `## References`, `## See Also` or `## Examples`.

## 2. Section Rules

### Title

- Human-readable name in title case, no backticks.
- Use the component, not the filename: `# Contract Loader`, not `# contracts.py`.

### Overview

- One paragraph, at most five sentences.
- Answer four questions: which module (source path in backticks), what it does, what it consumes, what it produces.
- No bullet lists.

### Schema

- Required when the component defines public dataclasses.
- One subsection per class: `### {ClassName}`.
- One table with exactly these columns: `| Field | Type | Description |`.
- Field names and Python types in backticks.
- A description states what the field means, how it is derived, and any allowed values.
- Mark deliberate placeholders explicitly.

### API

- Required when the component's surface is functions or a command line.
- One table with exactly these columns: `| Name | Signature | Description |`.
- Signatures and parameter names in backticks.

### Body Paragraphs (Design Rationale)

- Two to five paragraphs. Each covers one distinct decision.
- Bold the decision name at the start of the paragraph.
- Prefer concrete quantities and paths: row counts, thresholds, exit codes, filenames.
- The final paragraph states the component's position in the pipeline: what it consumes and what consumes it.

### Current Limitations

- One to four paragraphs, one per limitation.
- Each states the problem, the consequence, and the fix direction.
- No bullet lists. Do not hedge with "might" or "could potentially".

## 3. Tone and Voice

- Terse, technical, third person. Do not use `we`, `you` or `I`.
- Active voice. "The loader rejects the file", not "the file is rejected".
- No marketing adjectives.
- Use Python and dbt terminology: dataclass, `pl.DataFrame`, `LazyFrame`, model, materialization, grain.

## 4. Formatting

| Element | Rule |
| :--- | :--- |
| File paths | Backticks: `ingest/validation.py` |
| Classes and functions | Backticks: `validate_rows` |
| Contract fields and columns | Backticks: `customer_id` |
| Numeric values | Inline: `244`, `0.83 s`, `sha256:` |
| Tables | Left-align every column with `:---` |
| Emphasis | Bold only for decision names |
| HTML | Not permitted, including `<details>` and `<div>` |

## 5. Length

| Complexity | Target lines |
| :--- | :--- |
| Single function or dataclass | 30–45 |
| Multi-function module | 45–65 |
| Multi-stage subsystem | 65–90 |

A document under 30 lines is a stub and is not acceptable for a non-trivial component.

## 6. Anti-Patterns

- A `## Schema`-less document for a module that defines dataclasses.
- A limitation with no consequence or fix direction.
- Vague rationale ("efficient", "flexible") without a supporting quantity.
- Placeholder sections such as `## Schema` followed by `TBD`.
- Numbers copied from another repository instead of measured here.

## 7. Reference Examples

- `components/writer.md` — schema completeness and atomic-write rationale.
- `components/validation.md` — named decisions and per-row semantics.
- `components/transforms.md` — grain and model-level specificity.
