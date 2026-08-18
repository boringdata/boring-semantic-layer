"""Dimension-resolving table proxies used by filter/join/aggregate lowering.

Historically this module also registered ``ibis.expr.sql.convert`` handlers
for every semantic op — a parallel, drifted copy of the real ``to_untagged``
lowering in ``ops.py`` with no remaining caller. Those handlers are gone;
only the resolver proxies survive.
"""

from __future__ import annotations

from collections.abc import Callable

from attrs import field, frozen
from ibis.expr import types as ir


class _PrefixProxy:
    """Proxy for chained attribute access like t.airports.state.

    When accessing t.airports on a joined model, returns this proxy
    which tracks the prefix and resolves t.airports.state to the
    dimension named "airports.state".

    Only supports single-depth: prefix.column (e.g., "airports.state").
    """

    __slots__ = ("_resolver", "_prefix")

    def __init__(self, resolver: _Resolver, prefix: str):
        object.__setattr__(self, "_resolver", resolver)
        object.__setattr__(self, "_prefix", prefix)

    def __getattr__(self, name: str):
        full_name = f"{self._prefix}.{name}"
        # Try to resolve the full prefixed name as a dimension
        if full_name in self._resolver._dims:
            return self._resolver._dims[full_name](self._resolver._t).name(full_name)

        # Fallback to raw table column access
        return getattr(self._resolver._t, name)


@frozen
class _Resolver:
    """Resolver for dimensions in filter/join predicates.

    Provides attribute access to dimensions and raw table columns,
    resolving dimension functions to named expressions.

    Supports chained access for joined models:
    - t.state -> resolves "state" dimension
    - t.airports.state -> resolves "airports.state" dimension
    """

    _t: ir.Table
    _dims: dict[str, Callable] = field(factory=dict)

    def __getattr__(self, name: str):
        # Direct match in dims
        if name in self._dims:
            return self._dims[name](self._t).name(name)

        # Check if name is a table prefix (e.g., "airports" in "airports.state")
        prefix_pattern = f"{name}."
        has_prefixed_dims = any(k.startswith(prefix_pattern) for k in self._dims)
        if has_prefixed_dims:
            return _PrefixProxy(resolver=self, prefix=name)

        # Fallback to raw table column
        return getattr(self._t, name)

    def __getitem__(self, name: str):
        # Materialized columns win (e.g. prefixed columns already present
        # on a post-aggregation result, where the dimension lambda would
        # be stale). Fall back to declared dimensions so bracket access
        # also resolves prefixed names like t["orders.status"] on tables
        # that don't carry them as literal columns yet.
        try:
            return getattr(self._t, name)
        except AttributeError:
            if name in self._dims:
                return self._dims[name](self._t).name(name)
            raise


@frozen
class _AggResolver:
    """Resolver for dimensions and measures in aggregate operations.

    Provides attribute access to both dimensions and measures,
    handling prefixed names from joins (e.g., "table__column").
    """

    _t: ir.Table
    _dims: dict[str, Callable]
    _meas: dict[str, Callable]

    def __getattr__(self, key: str):
        if key in self._dims:
            return self._dims[key](self._t)
        if key in self._meas:
            return self._meas[key](self._t)
        return getattr(self._t, key)

    def __getitem__(self, key: str):
        # Materialized columns win (a stale dim/measure lambda must not
        # shadow a column that already exists on the result); fall back
        # to declared dims/measures for prefixed names like
        # t["orders.total"] that aren't literal columns yet.
        try:
            return getattr(self._t, key)
        except AttributeError:
            if key in self._dims:
                return self._dims[key](self._t)
            if key in self._meas:
                return self._meas[key](self._t)
            raise
