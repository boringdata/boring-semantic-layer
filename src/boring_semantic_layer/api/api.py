from typing import Any, Callable, Optional
from .ops import SemanticTable


def to_semantic_table(ibis_table, name: Optional[str] = None) -> SemanticTable:
    return SemanticTable(table=ibis_table, dimensions=None, measures=None,
                        calc_measures=None, name=name)


def join_one(left: SemanticTable, other: SemanticTable, left_on: str, right_on: str) -> SemanticTable:
    return left.join_one(other, left_on, right_on)


def join_many(left: SemanticTable, other: SemanticTable, left_on: str, right_on: str) -> SemanticTable:
    return left.join_many(other, left_on, right_on)


def join_cross(left: SemanticTable, other: SemanticTable) -> SemanticTable:
    return left.join_cross(other)


def filter_(table: SemanticTable, predicate: Callable[[Any], Any]) -> Any:
    return table.filter(predicate)


def group_by_(table: SemanticTable, *dims: str) -> SemanticTable:
    return table.group_by(*dims)


def aggregate_(table: SemanticTable, *measure_names: str, **aliased: str) -> Any:
    return table.aggregate(*measure_names, **aliased)


def mutate_(table: Any, **kwargs: Callable[[Any], Any]) -> Any:
    return table.mutate(**kwargs)


def order_by_(table: Any, *keys: Any) -> Any:
    return table.order_by(*keys)


def limit_(table: Any, n: int) -> Any:
    return table.limit(n)
