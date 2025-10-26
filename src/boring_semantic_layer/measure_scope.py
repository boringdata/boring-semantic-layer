from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from attrs import field, frozen
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


MeasureExpr = MeasureRef | AllOf | BinOp | float | int


@curry
def _resolve_measure_name(
    name: str,
    known: tuple[str, ...],
    known_set: frozenset[str],
) -> str | None:
    return name if name in known_set else next((k for k in known if k.endswith(f".{name}")), None)


def _make_known_measures(
    measures: Iterable[str],
) -> tuple[tuple[str, ...], frozenset[str]]:
    known_tuple = tuple(measures) if not isinstance(measures, tuple) else measures
    return (known_tuple, frozenset(known_tuple))


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
            return getattr(self.tbl, name)

        resolved = _resolve_measure_name(name, self.known, self.known_set)
        return MeasureRef(resolved) if resolved else getattr(self.tbl, name)

    def __getitem__(self, name: str):
        if self.post_agg:
            return self.tbl[name]

        resolved = _resolve_measure_name(name, self.known, self.known_set)
        return MeasureRef(resolved) if resolved else self.tbl[name]

    def all(self, ref):
        import ibis as ibis_mod

        if isinstance(ref, str):
            if self.post_agg:
                return self.tbl[ref].sum().over(ibis_mod.window())

            resolved = _resolve_measure_name(ref, self.known, self.known_set)
            return (
                AllOf(MeasureRef(resolved))
                if resolved
                else self.tbl[ref].sum().over(ibis_mod.window())
            )

        return (
            AllOf(ref)
            if isinstance(ref, MeasureRef)
            else ref.sum().over(ibis_mod.window())
            if hasattr(ref, "__class__") and "ibis" in str(type(ref).__module__)
            else (_ for _ in ()).throw(
                TypeError(
                    "t.all(...) expects either a measure reference (e.g., t.flight_count), "
                    "a string measure name (e.g., 'flight_count'), "
                    "or an ibis column expression (e.g., t['aggregated_column'])",
                ),
            )
        )


@frozen(kw_only=True, slots=True)
class ColumnScope:
    tbl: Any = field(alias="_tbl")

    def __getattr__(self, name: str):
        if name.startswith("_"):
            raise AttributeError(
                f"'{type(self).__name__}' object has no attribute '{name}'",
            )
        return getattr(self.tbl, name)

    def __getitem__(self, name: str):
        return self.tbl[name]
