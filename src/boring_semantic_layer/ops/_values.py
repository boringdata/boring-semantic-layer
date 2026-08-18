"""Value types of the semantic model: Dimension, Measure, CalcMeasure.

Plus the hashability wrapper for user callables and the dimension proxies
their evaluation uses.
"""

from __future__ import annotations

import re
from collections.abc import Callable, Mapping
from difflib import get_close_matches
from typing import Any

from attrs import field, frozen
from ibis.common.deferred import Deferred
from ibis.expr import types as ir


def _is_deferred(expr) -> bool:
    # Duck-type check: works for both ibis and xorq Deferred objects
    return hasattr(expr, "_resolver") and hasattr(expr, "resolve")


@frozen
class _CallableWrapper:
    """Hashable wrapper for Callable and Deferred.

    Both raw callables (lambda) and user Deferred (_.foo) are not hashable
    and cannot be stored in FrozenDict. This wrapper provides hashability
    using identity-based hashing.
    """

    _fn: Any

    def __call__(self, *args, **kwargs):
        return self._fn(*args, **kwargs)

    def __hash__(self):
        # should this be dask.base.tokenize()?
        return hash(id(self._fn))

    @property
    def unwrap(self):
        return self._fn


def _ensure_wrapped(fn: Any) -> _CallableWrapper:
    """Wrap Callable or Deferred for hashability."""
    return fn if isinstance(fn, _CallableWrapper) else _CallableWrapper(fn)


def _reject_bool_resolution(result: Any, source: Any) -> None:
    """Reject expressions that resolved to a Python bool.

    A bool here almost always means a comparison mixed plain-ibis and
    xorq-vendored objects (e.g. ``t.col == ibis.literal(...)`` where ``t``
    is xorq-backed): both ``__eq__`` implementations return
    ``NotImplemented`` for the foreign type, so Python falls back to
    identity comparison and yields a plain ``False``.  Left unchecked, that
    compiles into a constant predicate and silently returns wrong results.
    """
    if isinstance(result, bool):
        raise TypeError(
            f"Expression {source!r} resolved to the Python bool {result!r} "
            "instead of an ibis expression. This usually means a comparison "
            "mixed plain-ibis and xorq-vendored objects (e.g. "
            "`t.col == ibis.literal(...)` against a xorq-backed table). "
            "Compare against plain Python values instead (`t.col == 'AA'`) "
            "or build the literal with the table's own ibis flavor: "
            "`from boring_semantic_layer.nested_compile import get_ibis_module; "
            "get_ibis_module(t).literal(...)`. For a deliberately constant "
            "predicate, return `get_ibis_module(t).literal(True)` rather "
            "than a Python bool."
        )


def _format_column_error(e: AttributeError, table: ir.Table) -> str:
    """Format a helpful error message for missing column errors."""
    # Extract the column name from the error
    match = re.search(r"has no attribute ['\"]([^'\"]+)['\"]", str(e))
    missing_col = match.group(1) if match else "unknown"

    # Get available columns
    available_cols = list(table.columns) if hasattr(table, "columns") else []

    # Build error message
    parts = [f"Dimension expression references non-existent column '{missing_col}'."]

    if len(available_cols) > 20:
        parts.append(f"Table has {len(available_cols)} columns. First 15: {available_cols[:15]}")
    elif available_cols:
        parts.append(f"Available columns: {available_cols}")
    else:
        parts.append(f"No columns available in {type(table).__name__} object")

    # Suggest similar column names
    suggestions = get_close_matches(missing_col, available_cols, n=3, cutoff=0.6)
    if suggestions:
        parts[-1] += f". Did you mean: {suggestions}?"

    # Add helpful tip
    example = suggestions[0] if suggestions else "column_name"
    parts.append(
        f"\n\nTip: Check that your dimension expression uses the correct column name. "
        f"For example: lambda t: t.{example}"
    )

    return " ".join(parts)


class _DimPrefixProxy:
    """Resolves ``proxy.column`` to ``dims["prefix.column"](table)``."""

    __slots__ = ("_tbl", "_dims", "_prefix")

    def __init__(self, tbl, dims: dict, prefix: str):
        object.__setattr__(self, "_tbl", tbl)
        object.__setattr__(self, "_dims", dims)
        object.__setattr__(self, "_prefix", prefix)

    def __getattr__(self, name: str):
        full_name = f"{self._prefix}.{name}"
        if full_name in self._dims:
            return self._dims[full_name](self._tbl)
        raise AttributeError(
            f"No dimension '{full_name}' found. "
            f"Available dimensions with prefix '{self._prefix}.': "
            f"{[k for k in self._dims if k.startswith(self._prefix + '.')]}"
        )


class _DimensionTableProxy:
    """Proxy that wraps an ibis table to support model-prefix navigation.

    Allows dimension lambdas like ``lambda t: t.flights.carrier`` to work on
    joined tables by resolving ``t.flights.carrier`` through the merged
    dimension map (``dims["flights.carrier"](table)``).
    """

    __slots__ = ("_tbl", "_dims")

    def __init__(self, tbl, dims: dict):
        object.__setattr__(self, "_tbl", tbl)
        object.__setattr__(self, "_dims", dims)

    def __getattr__(self, name: str):
        prefix = f"{name}."
        if any(k.startswith(prefix) for k in self._dims):
            return _DimPrefixProxy(self._tbl, self._dims, name)
        return getattr(self._tbl, name)

    def __getitem__(self, name: str):
        if name in self._dims:
            return self._dims[name](self._tbl)
        return self._tbl[name]

    @property
    def columns(self):
        return self._tbl.columns


@frozen(kw_only=True, slots=True)
class Dimension:
    expr: Callable[[ir.Table], ir.Value] | Deferred
    description: str | None = None
    is_entity: bool = False
    is_time_dimension: bool = False
    is_event_timestamp: bool = False
    smallest_time_grain: str | None = None
    derived_dimensions: tuple[str, ...] = ()
    metadata: Mapping[str, Any] = field(factory=dict, eq=False, hash=False)

    def __call__(self, table: ir.Table, _dims: dict | None = None) -> ir.Value:
        try:
            result = self.expr.resolve(table) if _is_deferred(self.expr) else self.expr(table)
        except AttributeError as e:
            # Retry with a prefix-aware proxy for joined tables where
            # model prefixes are used (e.g., lambda t: t.flights.carrier)
            if _dims and not _is_deferred(self.expr) and callable(self.expr):
                try:
                    proxy = _DimensionTableProxy(table, _dims)
                    proxy_result = self.expr(proxy)
                except AttributeError as proxy_err:
                    # Preserve explicit prefix-proxy errors (e.g. missing
                    # "model.field") to avoid silent fallback to unprefixed
                    # columns, but keep normal missing-column errors on the
                    # original table so they get the helpful formatter below.
                    if str(proxy_err).startswith("No dimension '"):
                        raise
                except Exception:
                    pass
                else:
                    _reject_bool_resolution(proxy_result, self.expr)
                    return proxy_result
            # Provide helpful error for missing columns
            if "'Table' object has no attribute" in str(
                e
            ) or "'Join' object has no attribute" in str(e):
                raise AttributeError(_format_column_error(e, table)) from e
            raise
        else:
            _reject_bool_resolution(result, self.expr)
            return result

    def to_json(self) -> Mapping[str, Any]:
        base = {"description": self.description}
        if self.is_entity:
            base["is_entity"] = True
        if self.is_event_timestamp:
            base["is_event_timestamp"] = True
        if self.is_time_dimension:
            base["smallest_time_grain"] = self.smallest_time_grain
        if self.derived_dimensions:
            base["derived_dimensions"] = list(self.derived_dimensions)
        if self.metadata:
            base.update(self.metadata)
        return base

    def __hash__(self) -> int:
        return hash(
            (
                self.description,
                self.is_entity,
                self.is_event_timestamp,
                self.is_time_dimension,
                self.smallest_time_grain,
                self.derived_dimensions,
            ),
        )


@frozen(kw_only=True, slots=True)
class Measure:
    expr: Callable[[ir.Table], ir.Value] | Deferred
    description: str | None = None
    requires_unnest: tuple[str, ...] = ()  # Internal: Arrays that must be unnested
    original_expr: Any = field(default=None, eq=False, hash=False)
    metadata: Mapping[str, Any] = field(factory=dict, eq=False, hash=False)

    def __call__(self, table: ir.Table) -> ir.Value:
        result = self.expr.resolve(table) if _is_deferred(self.expr) else self.expr(table)
        _reject_bool_resolution(result, self.expr)
        return result

    @property
    def locality(self) -> str | None:
        """Derive locality from requires_unnest (most nested level)."""
        return self.requires_unnest[-1] if self.requires_unnest else None

    def to_json(self) -> Mapping[str, Any]:
        base = {"description": self.description}
        if self.locality:
            base["locality"] = self.locality
        if self.requires_unnest:
            base["requires_unnest"] = list(self.requires_unnest)
        if self.metadata:
            base.update(self.metadata)
        return base

    def __hash__(self) -> int:
        return hash((self.description, self.requires_unnest))


@frozen(kw_only=True, slots=True)
class CalcMeasure:
    """Stored representation of a calc (post-aggregation) measure.

    Holds the user's original lambda — the analyzer-classified ibis
    expression is recomputed from the lambda at query time against the
    actual base table. ``depends_on`` is captured at classification time
    so the planner can auto-include base-measure dependencies in
    aggregations even when the user did not request them explicitly.
    """

    expr: Any  # callable | Deferred
    description: str | None = None
    requires_unnest: tuple[str, ...] = ()
    depends_on: frozenset[str] = field(factory=frozenset, converter=frozenset)
    prefer_known: frozenset[str] = field(factory=frozenset, converter=frozenset)
    metadata: Mapping[str, Any] = field(factory=dict, eq=False, hash=False)

    def to_json(self) -> Mapping[str, Any]:
        base = {"description": self.description}
        if self.requires_unnest:
            base["requires_unnest"] = list(self.requires_unnest)
        if self.metadata:
            base.update(self.metadata)
        return base

    def __hash__(self) -> int:
        return hash((self.description, self.requires_unnest, self.depends_on, self.prefer_known))
