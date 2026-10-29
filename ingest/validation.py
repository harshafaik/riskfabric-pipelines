"""Three-stage validation: structure, types, semantics.

Structure failures reject the whole file - nothing is written. Type and semantic
failures reject individual rows, which are written to quarantine with a reason so
the rest of the partition can still land.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from functools import reduce
from operator import or_
from typing import Mapping

import polars as pl

from .contracts import Column, Contract

_SOURCE_PREFIX = "__src__"


class ContractViolation(Exception):
    """The file cannot be ingested at all."""

    def __init__(self, feed: str, partition: date, problems: list[str]):
        self.feed = feed
        self.partition = partition
        self.problems = problems
        super().__init__(f"{feed} {partition.isoformat()}: " + "; ".join(problems))


@dataclass
class ValidationResult:
    accepted: pl.DataFrame
    rejected: pl.DataFrame


_CAST_TARGET = {
    "string": pl.String,
    "int32": pl.Int32,
    "int64": pl.Int64,
    "uint32": pl.UInt32,
    "float64": pl.Float64,
    "boolean": pl.Boolean,
}


def _source(name: str) -> str:
    """Name of the hidden column holding the pre-cast value."""
    return f"{_SOURCE_PREFIX}{name}"


def _cast_expr(column: Column) -> pl.Expr:
    """Cast a column to its contract type without raising on bad values."""
    expr = pl.col(column.name)
    if column.type == "timestamp":
        return expr.cast(pl.String, strict=False).str.to_datetime(
            time_zone="UTC", strict=False
        )
    if column.type == "date":
        return expr.cast(pl.String, strict=False).str.to_date("%Y-%m-%d", strict=False)
    if column.type == "string":
        return expr.cast(pl.String, strict=False)
    return expr.cast(_CAST_TARGET[column.type], strict=False)


def check_structure(df: pl.DataFrame, contract: Contract, partition: date) -> None:
    """Raise if the file's shape does not match the contract."""
    expected = contract.column_names
    missing = [name for name in expected if name not in df.columns]
    unexpected = [name for name in df.columns if name not in expected]

    problems: list[str] = []
    if missing:
        problems.append(f"missing columns: {', '.join(missing)}")
    if unexpected:
        problems.append(f"columns not in {contract.name}: {', '.join(unexpected)}")
    if df.height == 0:
        problems.append("file has no rows")
    elif df.height < contract.min_rows:
        problems.append(f"expected at least {contract.min_rows} rows, got {df.height}")

    if problems:
        raise ContractViolation(contract.feed, partition, problems)


def _type_condition(column: Column) -> pl.Expr | None:
    """True where a non-null source value cannot become the contract type."""
    if column.type == "string":
        return None
    return pl.col(_source(column.name)).is_not_null() & pl.col(column.name).is_null()


def _semantic_condition(column: Column) -> pl.Expr | None:
    value = pl.col(column.name)
    original = pl.col(_source(column.name))
    conditions: list[pl.Expr] = []

    # Nullability is judged on the source value, so a cast failure is reported
    # once as a type error rather than twice.
    if not column.nullable:
        conditions.append(original.is_null())
    if column.allowed is not None:
        conditions.append(value.is_not_null() & ~value.is_in(list(column.allowed)))
    if column.minimum is not None:
        conditions.append(value.is_not_null() & (value < column.minimum))
    if column.maximum is not None:
        conditions.append(value.is_not_null() & (value > column.maximum))
    if column.pattern is not None:
        conditions.append(value.is_not_null() & ~value.str.contains(column.pattern))

    if not conditions:
        return None
    return reduce(or_, conditions)


def _duplicate_condition(contract: Contract) -> pl.Expr:
    key = list(contract.primary_key)
    if len(key) == 1:
        return pl.col(key[0]).is_duplicated()
    return pl.struct(key).is_duplicated()


def _partition_condition(contract: Contract, partition: date) -> pl.Expr | None:
    if contract.partition_from == "requested":
        return None
    return pl.col(contract.partition_column).dt.date().ne(pl.lit(partition))


def _with_partition(
    frame: pl.DataFrame, contract: Contract, partition: date
) -> pl.DataFrame:
    if contract.partition_from == "requested":
        column = pl.lit(partition, dtype=pl.Date)
    else:
        column = pl.col(contract.partition_column).dt.date()
    return frame.with_columns(column.alias(contract.partition_key))


def validate_rows(
    df: pl.DataFrame,
    contract: Contract,
    partition: date,
    references: Mapping[str, set[str]] | None = None,
) -> ValidationResult:
    """Split rows into accepted and rejected, with a reason per rejected row.

    ``references`` maps a referenced column to the ids present in the referenced
    feed's bronze tables.
    """
    # Keep the pre-cast values under a hidden name: type checks must compare the
    # source value with the cast result, and expressions are evaluated later.
    tagged = df.with_columns(
        [pl.col(column.name).alias(_source(column.name)) for column in contract.columns]
    )
    casted = tagged.with_columns(
        [_cast_expr(column).alias(column.name) for column in contract.columns]
    )

    checks: list[tuple[pl.Expr, str]] = []

    for column in contract.columns:
        if column.type == "string" and df.schema[column.name] not in (
            pl.String,
            pl.Null,
        ):
            # A string column holding numbers would silently cast; treat it as
            # a type error rather than accepting it.
            checks.append((pl.col(_source(column.name)).is_not_null(), f"type:{column.name}"))
        else:
            type_condition = _type_condition(column)
            if type_condition is not None:
                checks.append((type_condition, f"type:{column.name}"))

        semantic = _semantic_condition(column)
        if semantic is not None:
            checks.append((semantic, f"invalid:{column.name}"))

    checks.append((_duplicate_condition(contract), "duplicate:primary_key"))

    partition_condition = _partition_condition(contract, partition)
    if partition_condition is not None:
        checks.append((partition_condition, "partition_mismatch"))

    for ref in contract.references:
        allowed = (references or {}).get(ref.column)
        if allowed is None:
            raise ContractViolation(
                contract.feed,
                partition,
                [f"reference feed {ref.feed!r} is not loaded; ingest it before {contract.feed}"],
            )
        checks.append(
            (
                pl.col(ref.column).is_not_null()
                & ~pl.col(ref.column).is_in(sorted(allowed)),
                f"ref:{ref.column}",
            )
        )

    reasons = [
        pl.when(condition.fill_null(False))
        .then(pl.lit(label))
        .otherwise(None)
        for condition, label in checks
    ]
    reason_column = pl.concat_str(reasons, separator="; ", ignore_nulls=True)

    frame = _with_partition(casted, contract, partition)
    frame = frame.with_columns(
        pl.when(reason_column == "")
        .then(pl.lit(None, dtype=pl.String))
        .otherwise(reason_column)
        .alias("_reject_reason")
    ).drop([_source(column.name) for column in contract.columns])

    rejected = frame.filter(pl.col("_reject_reason").is_not_null())
    accepted = frame.filter(pl.col("_reject_reason").is_null()).drop("_reject_reason")
    return ValidationResult(accepted=accepted, rejected=rejected)
