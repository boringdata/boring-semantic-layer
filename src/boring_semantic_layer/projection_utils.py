"""Lightweight projection pushdown for semantic layer."""

from __future__ import annotations

import logging
from collections.abc import Callable, Mapping
from typing import Any

import ibis.expr.types as ir
from attrs import frozen

from .graph_utils import walk_nodes
from .utils import Result, failure, success

logger = logging.getLogger(__name__)


@frozen
class TableRequirements:
    """Column requirements for each table in the query."""

    requirements: Mapping[str, frozenset[str]]

    @classmethod
    def empty(cls) -> TableRequirements:
        """Create empty requirements."""
        return cls(requirements={})

    @classmethod
    def from_dict(cls, requirements: Mapping[str, set[str] | frozenset[str]]) -> TableRequirements:
        """Create from dict, converting sets to frozensets."""
        return cls(requirements={k: frozenset(v) for k, v in requirements.items()})

    def to_dict(self) -> dict[str, frozenset[str]]:
        """Convert to dict for compatibility."""
        return dict(self.requirements)

    def get_columns(self, table: str) -> frozenset[str]:
        """Get required columns for a table."""
        return self.requirements.get(table, frozenset())

    def add_columns(self, table: str, columns: frozenset[str] | set[str]) -> TableRequirements:
        """Add columns for a table (returns new instance)."""
        new_reqs = dict(self.requirements)
        existing = new_reqs.get(table, frozenset())
        new_reqs[table] = existing | frozenset(columns)
        return TableRequirements(requirements=new_reqs)

    def merge(self, other: TableRequirements) -> TableRequirements:
        """Merge two requirements (union of columns)."""
        new_reqs = dict(self.requirements)
        for table, cols in other.requirements.items():
            new_reqs[table] = new_reqs.get(table, frozenset()) | cols
        return TableRequirements(requirements=new_reqs)

    def __contains__(self, table: str) -> bool:
        return table in self.requirements

    def __repr__(self) -> str:
        items = [f"{t}:{len(c)}" for t, c in self.requirements.items()]
        return f"TableRequirements({', '.join(items)})"


def extract_column_names(expr: ir.Expr) -> Result[frozenset[str], Exception]:
    """Extract column names referenced in an Ibis expression.

    Uses graph traversal to find all Field operations.

    Returns:
        Result with frozenset of column names or exception
    """
    try:
        from ibis.expr import operations as ops

        field_nodes = walk_nodes(ops.Field, expr)
        return success(frozenset(field.name for field in field_nodes))
    except Exception as e:
        logger.debug(f"Failed to extract column names: {e}")
        return failure(e)


def extract_columns_from_callable(
    fn: Callable[[ir.Table], Any],
    table: ir.Table,
) -> Result[frozenset[str], Exception]:
    """Extract column names that a callable (dimension/measure) uses.

    Calls the function with the table and inspects the resulting expression
    to see what columns it references.

    Args:
        fn: Callable that takes a table and returns an expression
        table: Table to call the function with

    Returns:
        Result with frozenset of column names or exception
    """
    try:
        result = fn(table)
        if isinstance(result, ir.Expr):
            return extract_column_names(result)
        return success(frozenset())
    except Exception as e:
        logger.debug(f"Failed to extract columns from callable: {e}")
        return failure(e)


def extract_columns_from_callable_safe(
    fn: Callable[[ir.Table], Any],
    table: ir.Table,
) -> frozenset[str]:
    """Safe version that returns empty set on any error.

    Unwraps Result to frozenset for backward compatibility.
    """
    return extract_columns_from_callable(fn, table).unwrap_or(frozenset())


def include_all_columns_for_table(
    requirements: TableRequirements,
    table: ir.Table,
    table_name: str,
) -> TableRequirements:
    """Conservative fallback: include all columns from a table.

    Used when we can't determine specific columns needed.
    """
    all_cols = frozenset(table.columns)
    return requirements.add_columns(table_name, all_cols)


def _apply_requirements_to_tables(
    requirements: TableRequirements,
    table_names: list[str],
    cols: frozenset[str],
) -> TableRequirements:
    """Apply column requirements across all named tables.

    Conservative approach for joins where column origin is ambiguous.
    """
    result = requirements
    for table_name in table_names:
        result = result.add_columns(table_name, cols)
    return result


def _parse_prefixed_field(key: str) -> tuple[str | None, str]:
    """Parse prefixed field like 'customers.name' into (table_name, col_name).

    Returns:
        (table_name, col_name) if prefixed, (None, key) otherwise
    """
    if "." in key:
        parts = key.split(".", 1)
        return (parts[0], parts[1])
    return (None, key)


def extract_requirements_from_measures(
    measures: Mapping[str, Callable[[ir.Table], Any]],
    table: ir.Table,
    table_names: list[str],
) -> TableRequirements:
    """Extract column requirements from measures.

    For each measure, extract what columns it needs and associate with
    the appropriate table.

    Args:
        measures: Dict of measure_name -> callable
        table: Base table (may be joined)
        table_names: List of table names in the query

    Returns:
        Requirements for all measures
    """

    def process_measure(reqs: TableRequirements, measure_fn: Callable) -> TableRequirements:
        """Process a single measure, accumulating requirements."""
        return (
            extract_columns_from_callable(measure_fn, table)
            .map(
                lambda cols: _apply_requirements_to_tables(reqs, table_names, cols)
                if cols
                else reqs
            )
            .unwrap_or(reqs)
        )

    from functools import reduce

    return reduce(process_measure, measures.values(), TableRequirements.empty())


def _extract_requirement_for_key(
    key: str,
    dimensions: Mapping[str, Callable[[ir.Table], Any]],
    table: ir.Table,
    table_names: list[str],
    available_cols: frozenset[str],
    current_reqs: TableRequirements,
) -> TableRequirements:
    """Extract column requirement for a single key."""
    table_name, col_name = _parse_prefixed_field(key)

    if table_name and table_name in table_names:
        return current_reqs.add_columns(table_name, frozenset([col_name]))

    dim_fn = dimensions.get(key)
    if dim_fn:
        return (
            extract_columns_from_callable(dim_fn, table)
            .map(lambda cols: cols & available_cols)
            .map(
                lambda cols: _apply_requirements_to_tables(current_reqs, table_names, cols)
                if cols
                else current_reqs
            )
            .unwrap_or(current_reqs)
        )

    if key in available_cols:
        return _apply_requirements_to_tables(current_reqs, table_names, frozenset([key]))

    return current_reqs


def extract_requirements_from_keys(
    keys: list[str],
    dimensions: Mapping[str, Callable[[ir.Table], Any]],
    table: ir.Table,
    table_names: list[str],
) -> TableRequirements:
    """Extract column requirements from group-by keys.

    Args:
        keys: List of dimension names to group by
        dimensions: Dict of dimension_name -> callable
        table: Base table
        table_names: List of table names

    Returns:
        Requirements for all keys
    """
    available_cols = frozenset(table.columns) if hasattr(table, "columns") else frozenset()

    def process_key(reqs: TableRequirements, key: str) -> TableRequirements:
        return _extract_requirement_for_key(
            key, dimensions, table, table_names, available_cols, reqs
        )

    from functools import reduce

    return reduce(process_key, keys, TableRequirements.empty())
