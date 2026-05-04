"""Public value objects: ``Dimension`` and ``Measure``.

Plus the prefix proxies that let dimension lambdas use model-prefix
navigation (``lambda t: t.flights.carrier``) on joined tables.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any

from attrs import field, frozen
from ibis.common.deferred import Deferred
from ibis.expr import types as ir


def _is_deferred(expr) -> bool:
    # Duck-type check: works for both ibis and xorq Deferred objects
    return hasattr(expr, "_resolver") and hasattr(expr, "resolve")


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
            return self.expr.resolve(table) if _is_deferred(self.expr) else self.expr(table)
        except AttributeError as e:
            # Retry with a prefix-aware proxy for joined tables where
            # model prefixes are used (e.g., lambda t: t.flights.carrier)
            if _dims and not _is_deferred(self.expr) and callable(self.expr):
                try:
                    proxy = _DimensionTableProxy(table, _dims)
                    return self.expr(proxy)
                except AttributeError as proxy_err:
                    # Preserve explicit prefix-proxy errors (e.g. missing
                    # "model.field") to avoid silent fallback to unprefixed
                    # columns, but keep normal missing-column errors on the
                    # original table so they get the helpful formatter below.
                    if str(proxy_err).startswith("No dimension '"):
                        raise
                except Exception:
                    pass
            # Provide helpful error for missing columns
            if "'Table' object has no attribute" in str(
                e
            ) or "'Join' object has no attribute" in str(e):
                from ._measure_helpers import _format_column_error

                raise AttributeError(_format_column_error(e, table)) from e
            raise

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
        return self.expr.resolve(table) if _is_deferred(self.expr) else self.expr(table)

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
