"""Column tracking & per-table column-requirement extraction.

Used by projection pushdown to figure out which columns each leaf table
must produce. Split from ``_core.py`` to keep the relational ops module
focused on Op definitions.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Iterable, Mapping, Sequence
from typing import Any

from attrs import field, frozen
from ibis.expr import operations as ibis_ops
from ibis.expr import types as ir

from .._xorq import FrozenDict
from ..graph_utils import walk_nodes

logger = logging.getLogger(__name__)


# ==============================================================================
# Column Tracking for Projection Pushdown
# ==============================================================================


@frozen
class ColumnTracker:
    """Immutable tracker for column references during expression evaluation.

    Uses frozenset for tracked columns. New columns are added by creating
    new tracker instances with updated sets.
    """

    columns: frozenset[str] = field(factory=frozenset, converter=frozenset)

    def with_column(self, col_name: str) -> ColumnTracker:
        """Return new tracker with additional column."""
        return ColumnTracker(columns=self.columns | {col_name})

    def merge(self, other: ColumnTracker) -> ColumnTracker:
        """Return new tracker with merged columns."""
        return ColumnTracker(columns=self.columns | other.columns)


@frozen
class ColumnExtractionResult:
    """Result of column extraction with error handling.

    Separates successful extraction from error cases.
    """

    columns: frozenset[str] = field(factory=frozenset, converter=frozenset)
    extraction_failed: bool = False
    error_type: type[Exception] | None = None

    @classmethod
    def success(cls, columns: set[str] | frozenset[str]) -> ColumnExtractionResult:
        """Create successful result."""
        return cls(columns=frozenset(columns), extraction_failed=False)

    @classmethod
    def failure(cls, error: Exception) -> ColumnExtractionResult:
        """Create failure result with error information."""
        return cls(
            columns=frozenset(),
            extraction_failed=True,
            error_type=type(error),
        )

    def is_success(self) -> bool:
        """Check if extraction succeeded."""
        return not self.extraction_failed


@frozen
class JoinColumnExtractionResult:
    """Result of join column extraction for both tables."""

    left_columns: frozenset[str] = field(factory=frozenset, converter=frozenset)
    right_columns: frozenset[str] = field(factory=frozenset, converter=frozenset)
    extraction_failed: bool = False
    error_type: type[Exception] | None = None

    @classmethod
    def success(
        cls,
        left: set[str] | frozenset[str],
        right: set[str] | frozenset[str],
    ) -> JoinColumnExtractionResult:
        """Create successful result."""
        return cls(
            left_columns=frozenset(left),
            right_columns=frozenset(right),
            extraction_failed=False,
        )

    @classmethod
    def failure(cls, error: Exception) -> JoinColumnExtractionResult:
        """Create failure result with error information."""
        return cls(
            left_columns=frozenset(),
            right_columns=frozenset(),
            extraction_failed=True,
            error_type=type(error),
        )

    def is_success(self) -> bool:
        """Check if extraction succeeded."""
        return not self.extraction_failed


def _make_tracking_proxy(
    table: ir.Table,
    on_access: Callable[[str], None],
) -> Any:
    """Create tracking proxy with custom access handler.

    Composable factory that enables different tracking strategies
    via the on_access callback.
    """

    class _TrackingProxy:
        """Proxy that tracks attribute and item access."""

        def __init__(self, inner_table: ir.Table, access_handler: Callable[[str], None]):
            object.__setattr__(self, "_table", inner_table)
            object.__setattr__(self, "_on_access", access_handler)

        def __getattr__(self, name: str):
            if name.startswith("_"):
                return getattr(self._table, name)
            self._on_access(name)
            return getattr(self._table, name)

        def __getitem__(self, name: str):
            self._on_access(name)
            return self._table[name]

    return _TrackingProxy(table, on_access)


def _extract_columns_from_callable(
    fn: Any,
    table: ir.Table,
) -> ColumnExtractionResult:
    """Extract column names referenced by a callable.

    Uses immutable tracking and returns structured result.
    """
    if not callable(fn):
        return ColumnExtractionResult.success(frozenset())

    tracker_ref = [ColumnTracker()]

    def on_column_access(col_name: str) -> None:
        tracker_ref[0] = tracker_ref[0].with_column(col_name)

    try:
        tracking_proxy = _make_tracking_proxy(table, on_column_access)
        fn(tracking_proxy)
        return ColumnExtractionResult.success(tracker_ref[0].columns)

    except Exception as e:
        return ColumnExtractionResult.failure(e)


def _extract_join_key_columns(
    on: Callable[[Any, Any], ir.BooleanValue],
    left_table: ir.Table,
    right_table: ir.Table,
) -> JoinColumnExtractionResult:
    left_tracker_ref = [ColumnTracker()]
    right_tracker_ref = [ColumnTracker()]

    def on_left_access(col_name: str) -> None:
        left_tracker_ref[0] = left_tracker_ref[0].with_column(col_name)

    def on_right_access(col_name: str) -> None:
        right_tracker_ref[0] = right_tracker_ref[0].with_column(col_name)

    try:
        left_proxy = _make_tracking_proxy(left_table, on_left_access)
        right_proxy = _make_tracking_proxy(right_table, on_right_access)
        on(left_proxy, right_proxy)

        return JoinColumnExtractionResult.success(
            left_tracker_ref[0].columns,
            right_tracker_ref[0].columns,
        )

    except Exception as e:
        return JoinColumnExtractionResult.failure(e)


# ==============================================================================
# Table Column Requirements
# ==============================================================================


@frozen
class TableColumnRequirements:
    """Immutable representation of column requirements per table.

    Maps table names to sets of required column names.
    """

    requirements: FrozenDict[str, frozenset[str]] = field(
        factory=lambda: FrozenDict({}),
        converter=lambda d: FrozenDict(
            {k: frozenset(v) if not isinstance(v, frozenset) else v for k, v in d.items()},
        ),
    )

    def with_column(self, table_name: str, col_name: str) -> TableColumnRequirements:
        """Return new requirements with additional column for table."""
        current_cols = self.requirements.get(table_name, frozenset())
        updated_cols = current_cols | {col_name}

        return TableColumnRequirements(
            requirements=dict(self.requirements) | {table_name: updated_cols},
        )

    def with_columns(
        self,
        table_name: str,
        col_names: Iterable[str],
    ) -> TableColumnRequirements:
        """Return new requirements with multiple columns for table."""
        current_cols = self.requirements.get(table_name, frozenset())
        updated_cols = current_cols | frozenset(col_names)

        return TableColumnRequirements(
            requirements=dict(self.requirements) | {table_name: updated_cols},
        )

    def merge(self, other: TableColumnRequirements) -> TableColumnRequirements:
        """Merge requirements from another instance."""
        merged_dict = dict(self.requirements)

        for table, cols in other.requirements.items():
            if table in merged_dict:
                merged_dict[table] = merged_dict[table] | cols
            else:
                merged_dict[table] = cols

        return TableColumnRequirements(requirements=merged_dict)

    def to_dict(self) -> dict[str, set[str]]:
        """Convert to mutable dict for API compatibility."""
        return {table: set(cols) for table, cols in self.requirements.items()}


def _parse_prefixed_field(field_name: str) -> tuple[str | None, str]:
    """Parse potentially prefixed field name.

    Args:
        field_name: Field name, possibly prefixed (e.g., "table.column")

    Returns:
        Tuple of (table_name or None, column_name)
    """
    if "." in field_name:
        table, col = field_name.split(".", 1)
        return (table, col)
    return (None, field_name)


def _extract_requirements_from_keys(
    keys: Iterable[str],
    merged_dimensions: Mapping[str, Any],
    all_roots: Sequence[Any],
    table: ir.Table,
) -> TableColumnRequirements:
    """Extract column requirements from group-by keys using graph traversal."""
    requirements = TableColumnRequirements()

    for key in keys:
        table_name, col_name = _parse_prefixed_field(key)

        if table_name:
            # Prefixed: we know the table
            requirements = requirements.with_column(table_name, col_name)
        else:
            # Unprefixed: resolve dimension or use conservative fallback
            if key in merged_dimensions:
                dim_fn = merged_dimensions[key]

                try:
                    # Evaluate the dimension to get an Ibis expression
                    dim_expr = dim_fn(table)

                    # Walk the expression graph to find all Field nodes (column references)
                    field_names = {node.name for node in walk_nodes(ibis_ops.Field, dim_expr)}

                    # Filter to only actual columns in the table schema
                    actual_cols = {col for col in field_names if col in table.columns}

                    if actual_cols:
                        for root in all_roots:
                            if root.name:
                                requirements = requirements.with_columns(root.name, actual_cols)
                    else:
                        # Fallback: assume key name is column name
                        for root in all_roots:
                            if root.name:
                                requirements = requirements.with_column(root.name, key)
                except Exception:
                    logger.debug(
                        "dimension graph traversal failed for %r; "
                        "treating key name as column name",
                        key,
                        exc_info=True,
                    )
                    for root in all_roots:
                        if root.name:
                            requirements = requirements.with_column(root.name, key)
            else:
                # Raw column
                for root in all_roots:
                    if root.name:
                        requirements = requirements.with_column(root.name, key)

    return requirements


def _extract_requirements_from_measures(
    aggs: Mapping[str, Callable],
    all_roots: Sequence[Any],
    table: ir.Table,
) -> TableColumnRequirements:
    """Extract column requirements from measure aggregations using graph traversal."""
    from ._core import _unwrap

    requirements = TableColumnRequirements()

    for measure_name, measure_fn in aggs.items():
        fn = _unwrap(measure_fn)

        try:
            # Evaluate the measure to get an Ibis expression
            measure_expr = fn(table)

            # Walk the expression graph to find all Field nodes (column references)
            field_names = {node.name for node in walk_nodes(ibis_ops.Field, measure_expr)}

            # Filter to only actual columns in the table schema
            actual_cols = {col for col in field_names if col in table.columns}

            if actual_cols:
                for root in all_roots:
                    if root.name:
                        requirements = requirements.with_columns(root.name, actual_cols)
        except Exception:
            logger.debug(
                "measure graph traversal failed for %r; "
                "falling back to name-based column inference",
                measure_name,
                exc_info=True,
            )
            # Conservative fallback: if measure name looks like a column, include it
            if measure_name.isidentifier():
                for root in all_roots:
                    if root.name:
                        requirements = requirements.with_column(root.name, measure_name)

    return requirements
