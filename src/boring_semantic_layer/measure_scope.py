"""Scopes for evaluating user-supplied measure / dimension lambdas.

This module is the legacy compatibility surface for ``MeasureScope`` and
``ColumnScope`` — the lookup proxies passed into measure callables. The
curated calc-measure AST (``MeasureRef``, ``AllOf``, ``BinOp`` …) used to
live here too; it has been removed in favor of the analyzer-based path
in :mod:`boring_semantic_layer.calc_compiler`. ``MeasureScope`` is now a
thin pass-through that returns ibis values directly, kept around for the
post-aggregation ``SemanticMutateOp`` path which still constructs a
scope to evaluate ad-hoc mutate lambdas.
"""

from __future__ import annotations

import difflib
from typing import Any

from attrs import field, frozen


class UnknownMeasureRefError(AttributeError):
    """Raised when a lambda references an unknown measure or column.

    Subclasses :class:`AttributeError` so existing code that catches
    attribute errors continues to work, but the analyzer-based classifier
    re-raises this specific subclass instead of swallowing it. Surfaces
    typos at construction time with a "did you mean?" suggestion built
    from the surrounding measure / column names.
    """


def _has_prefixed_columns(tbl, name: str) -> bool:
    """Check if table has columns with the given prefix (e.g., 'flights.' prefix)."""
    if not hasattr(tbl, "columns"):
        return False
    prefix = f"{name}."
    return any(c.startswith(prefix) for c in tbl.columns)


class _ColumnPrefixProxy:
    """Proxy for navigating prefixed column names on joined ibis tables.

    Supports chained attribute access like ``t.flights.carrier`` which
    resolves to ``table["flights.carrier"]`` when the table has columns
    with the ``"flights."`` prefix (typical after joins).
    """

    __slots__ = ("_tbl", "_prefix")

    def __init__(self, tbl, prefix: str):
        object.__setattr__(self, "_tbl", tbl)
        object.__setattr__(self, "_prefix", prefix)

    def __getattr__(self, name: str):
        full_name = f"{self._prefix}.{name}"
        if hasattr(self._tbl, "columns") and full_name in self._tbl.columns:
            return self._tbl[full_name]
        raise AttributeError(
            f"No column '{full_name}' found on the table. "
            f"Available columns with prefix '{self._prefix}.': "
            f"{[c for c in (self._tbl.columns if hasattr(self._tbl, 'columns') else []) if c.startswith(self._prefix + '.')]}"
        )

    def __getitem__(self, name: str):
        full_name = f"{self._prefix}.{name}"
        if hasattr(self._tbl, "columns") and full_name in self._tbl.columns:
            return self._tbl[full_name]
        raise KeyError(
            f"No column '{full_name}' found on the table."
        )


def _resolve_column_short_name(tbl, name):
    """Resolve a column name against a table.

    Tries direct column access first; falls back to ``getattr(tbl, name)``
    for ibis methods. Raises ``AttributeError`` with a helpful message
    suggesting fully qualified names when the short name matches prefixed
    columns.
    """
    if hasattr(tbl, "columns") and name in tbl.columns:
        return tbl[name]

    if hasattr(tbl, "columns"):
        suffix = f".{name}"
        matches = [c for c in tbl.columns if c.endswith(suffix)]
        if matches:
            raise AttributeError(
                f"Column '{name}' not found. Did you mean one of the fully qualified names: "
                f"{matches}? Use bracket notation, e.g. t[\"{matches[0]}\"]."
            )

    return getattr(tbl, name)


def _resolve_column_item(tbl, name):
    return tbl[name]


@frozen(kw_only=True, slots=True)
class MeasureScope:
    """Lookup proxy passed to measure / mutate lambdas.

    Compared with :class:`~boring_semantic_layer.calc_compiler.IbisCalcScope`,
    this scope is a thin pass-through to the underlying ibis table. It is
    still used by the post-aggregation ``SemanticMutateOp`` path (where
    ``post_agg=True``) and by callers that want suffix-resolution of
    measure names without virtual aggregated tables.
    """

    tbl: Any = field(alias="_tbl")
    known: tuple[str, ...] = field(converter=tuple, alias="_known")
    known_set: frozenset[str] = field(init=False, alias="_known_set")
    post_agg: bool = field(default=False, alias="_post_agg")

    def __attrs_post_init__(self):
        object.__setattr__(self, "known_set", frozenset(self.known))

    def _typo_suggestion(self, name: str) -> str | None:
        cutoff = 0.80
        candidates: list[tuple[str, str]] = []
        if self.known:
            for match in difflib.get_close_matches(name, self.known, n=3, cutoff=cutoff):
                candidates.append(("measure", match))
        if hasattr(self.tbl, "columns"):
            for match in difflib.get_close_matches(
                name, list(self.tbl.columns), n=3, cutoff=cutoff
            ):
                candidates.append(("column", match))
        if not candidates:
            return None
        formatted = ", ".join(f"{kind} {match!r}" for kind, match in candidates)
        return f"Did you mean: {formatted}?"

    def __getattr__(self, name: str):
        if name.startswith("_"):
            raise AttributeError(
                f"'{type(self).__name__}' object has no attribute '{name}'",
            )

        if hasattr(self.tbl, "columns") and name in self.tbl.columns:
            return self.tbl[name]

        if _has_prefixed_columns(self.tbl, name):
            return _ColumnPrefixProxy(self.tbl, name)

        try:
            return _resolve_column_short_name(self.tbl, name)
        except AttributeError:
            suggestion = self._typo_suggestion(name)
            if suggestion:
                raise UnknownMeasureRefError(
                    f"{name!r} is not a known measure or column. {suggestion}"
                ) from None
            raise

    def __getitem__(self, name: str):
        return _resolve_column_item(self.tbl, name)

    def all(self, ref):
        from ._xorq import ibis as ibis_mod

        if isinstance(ref, str):
            return self.tbl[ref].sum().over(ibis_mod.window())

        if hasattr(ref, "__class__") and "ibis" in str(type(ref).__module__):
            if "Scalar" in type(ref).__name__:
                return ref.over(ibis_mod.window())
            return ref.sum().over(ibis_mod.window())

        raise TypeError(
            "t.all(...) expects a string column name or an ibis expression",
        )


@frozen(kw_only=True, slots=True)
class ColumnScope:
    tbl: Any = field(alias="_tbl")

    def __getattr__(self, name: str):
        if name.startswith("_"):
            raise AttributeError(
                f"'{type(self).__name__}' object has no attribute '{name}'",
            )

        from .nested_access import create_table_proxy, is_array_column

        if is_array_column(self.tbl, name):
            proxy = create_table_proxy(self.tbl)
            return getattr(proxy, name)

        if _has_prefixed_columns(self.tbl, name):
            return _ColumnPrefixProxy(self.tbl, name)

        return getattr(self.tbl, name)

    def __getitem__(self, name: str):
        return self.tbl[name]

    def all(self, ref):
        from ._xorq import ibis as ibis_mod

        if isinstance(ref, str):
            return self.tbl[ref].sum().over(ibis_mod.window())

        if hasattr(ref, "__class__") and "ibis" in str(type(ref).__module__):
            if "Scalar" in type(ref).__name__:
                return ref.over(ibis_mod.window())
            return ref.sum().over(ibis_mod.window())

        raise TypeError(
            "t.all(...) expects a string column name or an ibis expression",
        )
