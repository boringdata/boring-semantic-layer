"""Column-reference tracking for projection pushdown and join-key extraction.

Result-style types (ColumnTracker / ColumnExtractionResult /
JoinColumnExtractionResult) plus the tracking proxy that records which
columns a dimension/measure callable touches.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from attrs import field, frozen
from ibis.expr import types as ir

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
            # Relation methods such as ``count`` are not columns. Tracking
            # them makes CountStar appear to reference a fictitious field and
            # corrupts source-ownership inference.
            if name in self._table.columns:
                self._on_access(name)
            return getattr(self._table, name)

        def __getitem__(self, name: str):
            if name in self._table.columns:
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
