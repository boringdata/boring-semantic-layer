from typing import Any, Callable, Optional
from .expr import SemanticModel


def to_semantic_table(ibis_table, name: Optional[str] = None) -> SemanticModel:
    return SemanticModel(
        table=ibis_table, dimensions=None, measures=None, calc_measures=None, name=name
    )


def join_one(
    left: SemanticModel, other: SemanticModel, left_on: str, right_on: str
) -> SemanticModel:
    return left.join_one(other, left_on, right_on)


def join_many(
    left: SemanticModel, other: SemanticModel, left_on: str, right_on: str
) -> SemanticModel:
    return left.join_many(other, left_on, right_on)


def join_cross(left: SemanticModel, other: SemanticModel) -> SemanticModel:
    return left.join_cross(other)


def filter_(table: SemanticModel, predicate: Callable[[Any], Any]) -> Any:
    return table.filter(predicate)


def group_by_(table: SemanticModel, *dims: str) -> SemanticModel:
    return table.group_by(*dims)


def aggregate_(table: SemanticModel, *measure_names: str, **aliased: str) -> Any:
    return table.aggregate(*measure_names, **aliased)


def mutate_(table: Any, **kwargs: Callable[[Any], Any]) -> Any:
    return table.mutate(**kwargs)


def order_by_(table: Any, *keys: Any) -> Any:
    return table.order_by(*keys)


def limit_(table: Any, n: int) -> Any:
    return table.limit(n)
