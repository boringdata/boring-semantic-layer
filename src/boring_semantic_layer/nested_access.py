"""Experimental: Malloy-style automatic nested array access.

This module provides automatic unnesting for nested array columns, allowing
Malloy-style syntax like `t.hits.count()` instead of requiring explicit
`t.unnest("hits").count()` calls.

⚠️  EXPERIMENTAL: This feature uses proxy objects to intercept array column
access. The API may change in future versions.

Example:
    >>> # Without nested access (explicit unnest):
    >>> sessions.unnest("hits").with_measures(hits_count=lambda t: t.count())

    >>> # With nested access (Malloy-style, experimental):
    >>> sessions.with_measures(hits_count=lambda t: t.hits.count())
"""

from __future__ import annotations

from typing import Any

from attrs import frozen
from ibis.expr import types as ir
from toolz import curry


@frozen
class NestedAccessMarker:
    """Marker indicating a nested array access that requires unnesting.

    This is returned by NestedArrayProxy when aggregation methods are called.
    The converter will detect these markers and automatically apply table-level
    unnesting.

    Attributes:
        operation: The aggregation operation (e.g., "count", "sum", "mean")
        array_path: List of nested array columns to unnest (e.g., ["hits"])
        field_path: List of struct fields to access after unnesting (e.g., ["pageTitle"])
        base_expr: Optional Ibis expression to use after unnesting (for complex operations)
    """

    operation: str
    array_path: tuple[str, ...]
    field_path: tuple[str, ...] = ()
    base_expr: Any = None

    def __repr__(self):
        path = ".".join(self.array_path + self.field_path)
        return f"NestedAccess({path}.{self.operation}())"


@frozen
class NestedArrayProxy:
    """Proxy that captures nested array access patterns for automatic unnesting.

    This proxy intercepts attribute access on array columns and builds up
    the access path. When an aggregation method is called, it returns a
    NestedAccessMarker that the converter can use to generate the appropriate
    table-level unnest operations.

    Example:
        >>> # t.hits returns NestedArrayProxy
        >>> # t.hits.pageTitle returns NestedArrayProxy with field path
        >>> # t.hits.count() returns NestedAccessMarker("count", ["hits"])
        >>> # t.hits.product.count() returns NestedAccessMarker("count", ["hits", "product"])
    """

    table: ir.Table
    array_path: tuple[str, ...]
    field_path: tuple[str, ...] = ()

    def with_field(self, name: str) -> NestedArrayProxy:
        """Create new proxy with additional field in the path.

        Strategy for handling nested arrays:
        - If we have exactly one field in field_path, check if it's actually an array
        - If it's an array, move it to array_path
        - Otherwise, append to field_path
        """
        if len(self.field_path) == 1:
            # Check if the field is actually an array (nested array case like t.hits.product)
            field_name = self.field_path[0]

            # Try to determine if this field is an array by checking the schema
            # After unnesting array_path, we get a struct column with nested fields
            try:
                # Get the column after unnesting
                unnested_tbl = self.table
                for arr in self.array_path:
                    if arr in unnested_tbl.columns:
                        unnested_tbl = unnested_tbl.unnest(arr)

                # Check if the field is an array
                if field_name in unnested_tbl.columns:
                    col = getattr(unnested_tbl, field_name)
                    col_type = str(col.type())

                    if col_type.startswith("array"):
                        # It's an array - promote to array_path
                        return type(self)(
                            table=self.table,
                            array_path=self.array_path + self.field_path,
                            field_path=(name,),
                        )
            except Exception:
                # If we can't determine, fall through to default behavior
                pass

            # Not an array or can't determine - keep in field_path
            return type(self)(
                table=self.table,
                array_path=self.array_path,
                field_path=self.field_path + (name,),
            )
        else:
            # Continue building field path
            return type(self)(
                table=self.table,
                array_path=self.array_path,
                field_path=self.field_path + (name,),
            )

    def __getattr__(self, name: str):
        """Capture field access within nested structures.

        The challenge: we don't know if a field is an array or struct until we inspect it.
        Strategy: Assume that if we're accessing a second field, the first might be an array.

        Examples:
        - t.hits.count() → array_path=("hits",), field_path=()
        - t.hits.product.count() → array_path=("hits", "product"), field_path=()
        - t.hits.page.pageTitle → array_path=("hits",), field_path=("page", "pageTitle")
        """
        return self.with_field(name)

    def __getitem__(self, key: str):
        """Support bracket notation for field access."""
        return self.with_field(key)

    def _make_marker(self, operation: str, use_field_path: bool = True) -> NestedAccessMarker:
        """Create aggregation marker with appropriate array/field path handling.

        Args:
            operation: The aggregation operation name
            use_field_path: If False, promote all fields to array_path (for count)

        Returns:
            NestedAccessMarker configured for the operation
        """
        if use_field_path:
            return NestedAccessMarker(operation, self.array_path, self.field_path)
        else:
            # For count: treat remaining field_path as arrays to unnest
            return NestedAccessMarker(
                operation,
                self.array_path + self.field_path,
                (),
            )

    # Aggregation methods that return markers
    def count(self) -> NestedAccessMarker:
        """Count rows after unnesting.

        If we have a field_path, it represents arrays that need to be unnested.
        Move them to array_path for proper unnesting.
        """
        return self._make_marker("count", use_field_path=False)

    def sum(self) -> NestedAccessMarker:
        """Sum values after unnesting."""
        return self._make_marker("sum")

    def mean(self) -> NestedAccessMarker:
        """Average values after unnesting."""
        return self._make_marker("mean")

    def avg(self) -> NestedAccessMarker:
        """Average values after unnesting (alias for mean)."""
        return self._make_marker("mean")

    def min(self) -> NestedAccessMarker:
        """Minimum value after unnesting."""
        return self._make_marker("min")

    def max(self) -> NestedAccessMarker:
        """Maximum value after unnesting."""
        return self._make_marker("max")

    def nunique(self) -> NestedAccessMarker:
        """Count distinct values after unnesting."""
        return self._make_marker("nunique")

    def __repr__(self):
        path = ".".join(self.array_path + self.field_path)
        return f"NestedArrayProxy({path})"


def is_array_column(table: ir.Table, column_name: str) -> bool:
    """Check if a column is an array type."""
    try:
        col = getattr(table, column_name)
        col_type = col.type()
        return str(col_type).startswith("array")
    except (AttributeError, KeyError):
        return False


@curry
def make_array_proxy(table: ir.Table, column_name: str) -> NestedArrayProxy:
    """Factory for creating NestedArrayProxy instances.

    Args:
        table: The Ibis table
        column_name: The array column name

    Returns:
        NestedArrayProxy initialized with the column
    """
    return NestedArrayProxy(table=table, array_path=(column_name,))


def create_table_proxy(table: ir.Table) -> NestedTableProxy:
    """Create a proxy that intercepts array column access.

    Args:
        table: The Ibis table to wrap

    Returns:
        NestedTableProxy that returns NestedArrayProxy for array columns
    """
    return NestedTableProxy(table)


@frozen
class NestedTableProxy:
    """Proxy table that returns NestedArrayProxy for array columns.

    This is used in measure lambdas to enable Malloy-style nested access:
        lambda t: t.hits.count()  # t is a NestedTableProxy
    """

    table: ir.Table

    def __getattr__(self, name: str):
        """Return proxy for array columns, regular columns otherwise."""
        # Check if this is an array column
        if is_array_column(self.table, name):
            # Return proxy that captures nested access
            return make_array_proxy(self.table, name)
        else:
            # Regular column access - pass through to underlying table
            return getattr(self.table, name)

    def __getitem__(self, name: str):
        """Support bracket notation for column access."""
        return self.__getattr__(name)

    # Pass through common table methods
    def count(self):
        """Count table rows."""
        return self.table.count()

    def nunique(self, *args, **kwargs):
        """Count distinct values."""
        return self.table.nunique(*args, **kwargs)

    def __repr__(self):
        return f"NestedTableProxy({self.table})"
