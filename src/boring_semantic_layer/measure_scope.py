from __future__ import annotations

import difflib
from collections.abc import Iterable
from typing import Any

from attrs import field, frozen
from returns.maybe import Maybe, Some
from toolz import curry


class UnknownMeasureRefError(AttributeError):
    """Raised when a calc-measure lambda references an unknown name.

    Subclasses :class:`AttributeError` so existing code that ``except``\\ s
    on attribute errors continues to work, but ``_classify_measure``
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

    Supports chained attribute access like ``t.flights.carrier`` which resolves
    to ``table["flights.carrier"]`` when the table has columns with the
    ``"flights."`` prefix (typical after joins).
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


class _PendingMethodCall:
    """Captures a method access on a calc-measure AST node, waiting for ``()``."""

    __slots__ = ("_receiver", "_method")

    def __init__(self, receiver, method):
        object.__setattr__(self, "_receiver", receiver)
        object.__setattr__(self, "_method", method)

    def __call__(self, *args, **kwargs):
        if args and hasattr(args[0], "columns"):
            return self._receiver  # table-call passthrough
        return MethodCall(self._receiver, self._method, args, tuple(sorted(kwargs.items())))

    def __getattr__(self, name):
        if name.startswith("_"):
            raise AttributeError(name)
        # Zero-arg call of current method, then chain next method
        zero_call = MethodCall(self._receiver, self._method, (), ())
        return _PendingMethodCall(zero_call, name)


class _Node:
    def _bin(self, op: str, other: Any) -> BinOp:
        return BinOp(op, self, other)

    def __add__(self, o: Any):
        return self._bin("add", o)

    def __sub__(self, o: Any):
        return self._bin("sub", o)

    def __mul__(self, o: Any):
        return self._bin("mul", o)

    def __truediv__(self, o: Any):
        return self._bin("div", o)

    def __radd__(self, o: Any):
        return BinOp("add", o, self)

    def __rsub__(self, o: Any):
        return BinOp("sub", o, self)

    def __rmul__(self, o: Any):
        return BinOp("mul", o, self)

    def __rtruediv__(self, o: Any):
        return BinOp("div", o, self)

    # Method-style arithmetic parity with ibis value expressions, e.g. t.x.add(1)
    def add(self, other: Any) -> BinOp:
        return self + other

    def sub(self, other: Any) -> BinOp:
        return self - other

    def mul(self, other: Any) -> BinOp:
        return self * other

    def div(self, other: Any) -> BinOp:
        return self / other

    def __getattr__(self, name):
        if name.startswith("_"):
            raise AttributeError(f"'{type(self).__name__}' has no attribute {name!r}")
        return _PendingMethodCall(self, name)


@frozen
class MeasureRef(_Node):
    name: str


@frozen
class AllOf(_Node):
    ref: MeasureRef


@frozen
class BinOp(_Node):
    op: str
    left: Any
    right: Any


@frozen
class MethodCall(_Node):
    receiver: Any
    method: str
    args: tuple = ()
    kwargs: tuple = ()  # tuple of (key, value) pairs


@frozen
class AggregationExpr(_Node):
    column: str
    operation: str
    post_ops: tuple = field(default=(), converter=tuple)

    def __getattr__(self, name: str):
        if name.startswith("_"):
            raise AttributeError(f"AggregationExpr has no attribute {name!r}")
        return AggregationExpr(
            column=self.column, operation=self.operation, post_ops=self.post_ops + ((name, (), ()),)
        )

    def __call__(self, *args, **kwargs):
        if args and hasattr(args[0], "columns"):
            return self

        if not self.post_ops:
            raise TypeError("Cannot call AggregationExpr with arguments when no post_ops exist")

        *rest, (method_name, _, _) = self.post_ops
        return AggregationExpr(
            column=self.column,
            operation=self.operation,
            post_ops=tuple(rest) + ((method_name, args, tuple(sorted(kwargs.items()))),),
        )


MeasureExpr = MeasureRef | AllOf | BinOp | MethodCall | AggregationExpr | float | int


def validate_calc_ast(expr: Any, measure_name: str | None = None) -> None:
    """Walk a calc-measure AST and raise ``ValueError`` on illegal shapes.

    The AST nodes are unconstrained at construction (Any-typed fields), so
    invalid compositions like ``AllOf(BinOp(...))`` parse but later fail
    deep inside the compiler with confusing messages. Run this after
    classification to surface the structural problem early, naming the
    offending calc measure when known.

    ``AllOf.ref`` must be a ``MeasureRef`` or ``AggregationExpr``. Other
    refs (BinOp, MethodCall, nested AllOf) are not supported by either
    the direct compile path or the rewrite-then-compile pipeline in
    ``compile_grouped_with_all``.
    """
    where = f" in calc measure {measure_name!r}" if measure_name else ""

    def walk(node):
        if isinstance(node, AllOf):
            if not isinstance(node.ref, (MeasureRef, AggregationExpr)):
                raise ValueError(
                    f"Invalid AllOf{where}: ref must be a measure reference or "
                    f"inline aggregation, got {type(node.ref).__name__}. "
                    f"Wrap it in a named measure first, e.g. "
                    f".with_measures(my_measure=...) then use t.all(t.my_measure)."
                )
            walk(node.ref)
        elif isinstance(node, BinOp):
            walk(node.left)
            walk(node.right)
        elif isinstance(node, MethodCall):
            walk(node.receiver)
            for arg in node.args:
                walk(arg)

    walk(expr)


class DeferredColumn:
    _AGGREGATIONS = {
        "sum": "sum",
        "mean": "mean",
        "avg": "mean",
        "count": "count",
        "min": "min",
        "max": "max",
    }

    def __init__(self, column_name: str, tbl: Any):
        self._column_name = column_name
        self._tbl = tbl
        self._column = tbl[column_name]

        for method_name, operation in self._AGGREGATIONS.items():
            setattr(
                self,
                method_name,
                lambda op=operation: AggregationExpr(column=self._column_name, operation=op),
            )

    def __getattr__(self, name):
        return getattr(self._column, name)

    def __add__(self, other):
        return self._column + other

    def __radd__(self, other):
        return other + self._column

    def __sub__(self, other):
        return self._column - other

    def __rsub__(self, other):
        return other - self._column

    def __mul__(self, other):
        return self._column * other

    def __rmul__(self, other):
        return other * self._column

    def __truediv__(self, other):
        return self._column / other

    def __rtruediv__(self, other):
        return other / self._column

    def __eq__(self, other):
        return self._column.__eq__(other)

    def __ne__(self, other):
        return self._column.__ne__(other)

    def __lt__(self, other):
        return self._column.__lt__(other)

    def __le__(self, other):
        return self._column.__le__(other)

    def __gt__(self, other):
        return self._column.__gt__(other)

    def __ge__(self, other):
        return self._column.__ge__(other)


@curry
def _resolve_measure_name(
    name: str,
    known: tuple[str, ...],
    known_set: frozenset[str],
) -> Maybe[str]:
    if name in known_set:
        return Some(name)
    # Suffix matching: resolve unprefixed name to prefixed equivalent
    suffix = f".{name}"
    matches = tuple(k for k in known if k.endswith(suffix))
    if len(matches) == 1:
        return Some(matches[0])
    return Maybe.from_optional(None)


def _make_known_measures(
    measures: Iterable[str],
) -> tuple[tuple[str, ...], frozenset[str]]:
    known_tuple = tuple(measures) if not isinstance(measures, tuple) else measures
    return (known_tuple, frozenset(known_tuple))


def _resolve_column_short_name(tbl, name):
    """Resolve a column name against a table, requiring fully qualified names after joins.

    Tries direct column access first; falls back to ``getattr(tbl, name)``
    for ibis methods.  Raises ``AttributeError`` with a helpful message
    suggesting FQDN when the short name matches prefixed columns.
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
    """Resolve a column name via bracket access, requiring fully qualified names after joins."""
    return tbl[name]


@frozen(kw_only=True, slots=True)
class MeasureScope:
    tbl: Any = field(alias="_tbl")
    known: tuple[str, ...] = field(converter=tuple, alias="_known")
    known_set: frozenset[str] = field(init=False, alias="_known_set")
    post_agg: bool = field(default=False, alias="_post_agg")

    def __attrs_post_init__(self):
        object.__setattr__(self, "known_set", frozenset(self.known))

    def __getattr__(self, name: str):
        if name.startswith("_"):
            raise AttributeError(
                f"'{type(self).__name__}' object has no attribute '{name}'",
            )

        if self.post_agg:
            if _has_prefixed_columns(self.tbl, name):
                return _ColumnPrefixProxy(self.tbl, name)
            return _resolve_column_short_name(self.tbl, name)

        maybe_measure = _resolve_measure_name(name, self.known, self.known_set).map(MeasureRef)
        if isinstance(maybe_measure, Some):
            return maybe_measure.unwrap()

        if hasattr(self.tbl, "columns") and name in self.tbl.columns:
            return DeferredColumn(name, self.tbl)

        # Support prefix navigation for joined tables (e.g., t.flights.carrier)
        if _has_prefixed_columns(self.tbl, name):
            return _ColumnPrefixProxy(self.tbl, name)

        # Fall through to ibis (covers Table methods like ``count``, ``filter``).
        # If ibis rejects too, surface a typo suggestion rather than the opaque
        # ibis AttributeError so the user can see a "did you mean?" hint.
        try:
            return _resolve_column_short_name(self.tbl, name)
        except AttributeError:
            suggestion = self._typo_suggestion(name)
            if suggestion:
                raise UnknownMeasureRefError(
                    f"{name!r} is not a known measure or column. {suggestion}"
                ) from None
            raise

    def _typo_suggestion(self, name: str) -> str | None:
        # 0.80 catches single-character typos and case mistakes
        # (``flight_konut`` vs ``flight_count`` ≈ 0.83) without flagging
        # legitimate substring overlaps (``net_revenue`` vs
        # ``total_net_revenue`` ≈ 0.79). Calibrated against real-world
        # confusable measure names.
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

    def __getitem__(self, name: str):
        if self.post_agg:
            return _resolve_column_item(self.tbl, name)

        maybe_measure = _resolve_measure_name(name, self.known, self.known_set).map(MeasureRef)
        if isinstance(maybe_measure, Some):
            return maybe_measure.unwrap()
        return _resolve_column_item(self.tbl, name)

    def all(self, ref):
        from xorq.vendor import ibis as ibis_mod

        if isinstance(ref, str):
            if self.post_agg:
                return self.tbl[ref].sum().over(ibis_mod.window())

            maybe_measure = _resolve_measure_name(ref, self.known, self.known_set).map(
                lambda name: AllOf(MeasureRef(name))
            )
            if isinstance(maybe_measure, Some):
                return maybe_measure.unwrap()
            return self.tbl[ref].sum().over(ibis_mod.window())

        if isinstance(ref, MeasureRef):
            return AllOf(ref)

        if isinstance(ref, AggregationExpr):
            return AllOf(ref)

        if hasattr(ref, "__class__") and "ibis" in str(type(ref).__module__):
            if "Scalar" in type(ref).__name__:
                return ref.over(ibis_mod.window())
            else:
                return ref.sum().over(ibis_mod.window())

        raise TypeError(
            "t.all(...) expects either a measure reference (e.g., t.flight_count), "
            "a string measure name (e.g., 'flight_count'), an AggregationExpr, "
            "or an ibis expression (e.g., t.distance.sum())",
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

        # Support prefix navigation for joined tables (e.g., t.flights.carrier)
        if _has_prefixed_columns(self.tbl, name):
            return _ColumnPrefixProxy(self.tbl, name)

        return getattr(self.tbl, name)

    def __getitem__(self, name: str):
        return self.tbl[name]

    def all(self, ref):
        from xorq.vendor import ibis as ibis_mod

        if isinstance(ref, str):
            return self.tbl[ref].sum().over(ibis_mod.window())

        if isinstance(ref, AggregationExpr):
            return AllOf(ref)

        if hasattr(ref, "__class__") and "ibis" in str(type(ref).__module__):
            if "Scalar" in type(ref).__name__:
                return ref.over(ibis_mod.window())
            else:
                return ref.sum().over(ibis_mod.window())

        raise TypeError(
            "t.all(...) expects either a string column name (e.g., 'flight_count'), "
            "an AggregationExpr, or an ibis expression (e.g., t.distance.sum())",
        )
