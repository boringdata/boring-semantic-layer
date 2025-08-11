from dataclasses import dataclass
from typing import Callable

from xorq.caching import frozen

try:
    import xorq.vendor.ibis as ibis_mod

    IS_XORQ_USED = True
except ImportError:
    import ibis as ibis_mod
    IS_XORQ_USED = False

Expr = ibis_mod.expr.types.core.Expr

@dataclass(frozen=True)
class DimensionSpec:
    expr: Callable[[Expr], Expr]
    description: str

    def __call__(self, table: Expr) -> Expr:
        return self.expr(table)

@dataclass(frozen=True)
class MeasureSpec:
    expr: Callable[[Expr], Expr]
    description: str

    def __call__(self, table: Expr) -> Expr:
        return self.expr(table)
