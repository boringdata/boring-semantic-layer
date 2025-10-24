from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Union

class _Node:
    def _bin(self, op: str, other: Any) -> "BinOp":
        return BinOp(op, self, other)
    def __add__(self, o: Any): return self._bin("add", o)
    def __sub__(self, o: Any): return self._bin("sub", o)
    def __mul__(self, o: Any): return self._bin("mul", o)
    def __truediv__(self, o: Any): return self._bin("div", o)
    def __radd__(self, o: Any): return BinOp("add", o, self)
    def __rsub__(self, o: Any): return BinOp("sub", o, self)
    def __rmul__(self, o: Any): return BinOp("mul", o, self)
    def __rtruediv__(self, o: Any): return BinOp("div", o, self)

@dataclass(frozen=True)
class MeasureRef(_Node):
    name: str

@dataclass(frozen=True)
class AllOf(_Node):
    ref: MeasureRef

@dataclass(frozen=True)
class BinOp(_Node):
    op: str
    left: Any
    right: Any

MeasureExpr = Union[MeasureRef, AllOf, BinOp, float, int]