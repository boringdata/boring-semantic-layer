from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from attrs import field, frozen
from returns.maybe import Maybe, Some
from toolz import curry


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
class AggregationExpr(_Node):
    column: str
    operation: str
    post_ops: tuple = field(default=(), converter=tuple)

    def __getattr__(self, name: str):
        if name.startswith("_"):
            raise AttributeError(f"AggregationExpr has no attribute {name!r}")
        return AggregationExpr(
            column=self.column, operation=self.operation, post_ops=self.post_ops + ((name, (), {}),)
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
            post_ops=tuple(rest) + ((method_name, args, kwargs),),
        )


MeasureExpr = MeasureRef | AllOf | BinOp | AggregationExpr | float | int


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
    result = next((k for k in known if k.endswith(f".{name}")), None)
    return Maybe.from_optional(result)


def _make_known_measures(
    measures: Iterable[str],
) -> tuple[tuple[str, ...], frozenset[str]]:
    known_tuple = tuple(measures) if not isinstance(measures, tuple) else measures
    return (known_tuple, frozenset(known_tuple))


def _resolve_column_short_name(tbl, name):
    """Resolve a short column name against a table that may have dot-prefixed columns.

    Tries direct column access first; on failure, searches for a unique
    ``prefix.name`` match among the table's columns.  Raises
    ``AttributeError`` when the short name is ambiguous (matches multiple
    prefixed columns).
    """
    if hasattr(tbl, "columns") and name in tbl.columns:
        return tbl[name]

    if hasattr(tbl, "columns"):
        suffix = f".{name}"
        matches = [c for c in tbl.columns if c.endswith(suffix)]
        if len(matches) == 1:
            return tbl[matches[0]]
        if len(matches) > 1:
            raise AttributeError(
                f"Ambiguous column '{name}' matches multiple prefixed columns: "
                f"{matches}. Use the full prefixed name to disambiguate."
            )

    return getattr(tbl, name)


def _resolve_column_item(tbl, name):
    """Resolve a column name via bracket access, falling back to short-name matching.

    Raises ``KeyError`` when the short name is ambiguous.
    """
    if hasattr(tbl, "columns") and name in tbl.columns:
        return tbl[name]

    if hasattr(tbl, "columns"):
        suffix = f".{name}"
        matches = [c for c in tbl.columns if c.endswith(suffix)]
        if len(matches) == 1:
            return tbl[matches[0]]
        if len(matches) > 1:
            raise KeyError(
                f"Ambiguous column '{name}' matches multiple prefixed columns: "
                f"{matches}. Use the full prefixed name to disambiguate."
            )

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
            return _resolve_column_short_name(self.tbl, name)

        maybe_measure = _resolve_measure_name(name, self.known, self.known_set).map(MeasureRef)
        if isinstance(maybe_measure, Some):
            return maybe_measure.unwrap()

        if hasattr(self.tbl, "columns") and name in self.tbl.columns:
            return DeferredColumn(name, self.tbl)

        return _resolve_column_short_name(self.tbl, name)

    def __getitem__(self, name: str):
        if self.post_agg:
            return _resolve_column_item(self.tbl, name)

        maybe_measure = _resolve_measure_name(name, self.known, self.known_set).map(MeasureRef)
        if isinstance(maybe_measure, Some):
            return maybe_measure.unwrap()
        return _resolve_column_item(self.tbl, name)

    def all(self, ref):
        import ibis as ibis_mod

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

        return getattr(self.tbl, name)

    def __getitem__(self, name: str):
        return self.tbl[name]

    def all(self, ref):
        import ibis as ibis_mod

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
