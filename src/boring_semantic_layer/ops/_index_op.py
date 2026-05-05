"""``SemanticIndexOp`` and its index-fragment helpers.

``index()`` builds a tall ``(fieldName, fieldPath, fieldType, fieldValue,
weight)`` table for each indexed dimension. String fields produce one row
per distinct value; numeric fields collapse to a single ``min .. max``
row. The result is a ``UNION ALL`` of these per-field fragments.
"""

from __future__ import annotations

from collections.abc import Callable
from functools import reduce
from typing import TYPE_CHECKING, Any

import ibis
from ibis.expr import types as ir
from ibis.expr.operations.relations import Relation

from .._xorq import FrozenOrderedDict, Schema, selectors as s
from ._format import _semantic_repr
from ._root_models import _find_all_root_models
from ._xorq_compat import _rebind_to_canonical_backend

if TYPE_CHECKING:
    from ..expr import SemanticFilter, SemanticLimit, SemanticOrderBy


def _get_field_type_str(field_type: Any) -> str:
    return (
        "string"
        if field_type.is_string()
        else "number"
        if field_type.is_numeric()
        else "date"
        if field_type.is_temporal()
        else str(field_type)
    )


def _get_weight_expr(
    base_tbl: Any,
    by_measure: str | None,
    all_roots: list,
    is_string: bool,
) -> Any:
    from .._xorq import api as xo
    from ._core import _get_merged_fields

    if not by_measure:
        return xo._.count()

    merged_measures = _get_merged_fields(all_roots, "measures")
    return (
        merged_measures[by_measure](base_tbl) if by_measure in merged_measures else xo._.count()
    )


def _build_string_index_fragment(
    base_tbl: Any,
    field_expr: Any,
    field_name: str,
    field_path: str,
    type_str: str,
    weight_expr: Any,
) -> Any:
    from .._xorq import api as xo

    return (
        base_tbl.group_by(field_expr.name("value"))
        .aggregate(weight=weight_expr)
        .select(
            fieldName=xo.literal(field_name.split(".")[-1]),
            fieldPath=xo.literal(field_path),
            fieldType=xo.literal(type_str),
            fieldValue=xo._["value"].cast("string"),
            weight=xo._["weight"],
        )
    )


def _build_numeric_index_fragment(
    base_tbl: Any,
    field_expr: Any,
    field_name: str,
    field_path: str,
    type_str: str,
    weight_expr: Any,
) -> Any:
    from .._xorq import api as xo

    return (
        base_tbl.select(field_expr.name("value"))
        .filter(xo._["value"].notnull())
        .aggregate(
            min_val=xo._["value"].min(),
            max_val=xo._["value"].max(),
            weight=weight_expr,
        )
        .select(
            fieldName=xo.literal(field_name.split(".")[-1]),
            fieldPath=xo.literal(field_path),
            fieldType=xo.literal(type_str),
            fieldValue=(
                xo._["min_val"].cast("string") + " to " + xo._["max_val"].cast("string")
            ),
            weight=xo._["weight"],
        )
    )


def _resolve_selector(
    selector: str | list[str] | Callable | None,
    base_tbl: ir.Table,
) -> tuple[str, ...]:
    if selector is None:
        return tuple(base_tbl.columns)
    try:
        selected = base_tbl.select(selector)
        return tuple(selected.columns)
    except Exception:
        return []


def _get_fields_to_index(
    selector: str | list[str] | Callable | None,
    merged_dimensions: dict,
    base_tbl: ir.Table,
) -> tuple[str, ...]:
    if selector is None:
        selector = s.all()

    raw_fields = _resolve_selector(selector, base_tbl)

    if not raw_fields:
        result = list(merged_dimensions.keys())
        result.extend(col for col in base_tbl.columns if col not in result)
    else:
        result = [col for col in raw_fields if col in merged_dimensions or col in base_tbl.columns]

    return result


class SemanticIndexOp(Relation):
    source: Relation
    selector: str | list[str] | tuple[str, ...] | Callable | None
    by: str | None = None
    sample: int | None = None

    def __init__(
        self,
        source: Relation,
        selector: str | list[str] | tuple[str, ...] | Callable | None = None,
        by: str | None = None,
        sample: int | None = None,
    ) -> None:
        from ._core import _get_merged_fields

        # Validate sample parameter
        if sample is not None and sample <= 0:
            raise ValueError(f"sample must be positive, got {sample}")

        # Validate 'by' measure exists if provided
        if by is not None:
            all_roots = _find_all_root_models(source)
            if all_roots:
                merged_measures = _get_merged_fields(all_roots, "measures")
                if by not in merged_measures:
                    available = list(merged_measures.keys())
                    raise KeyError(
                        f"Unknown measure '{by}' for weight calculation. "
                        f"Available measures: {', '.join(available) or 'none'}",
                    )

        # Convert selector to tuple if it's a list (Ibis requires hashable types)
        hashable_selector = tuple(selector) if isinstance(selector, list) else selector

        super().__init__(
            source=Relation.__coerce__(source),
            selector=hashable_selector,
            by=by,
            sample=sample,
        )

    def __repr__(self) -> str:
        return _semantic_repr(self)

    @property
    def values(self) -> FrozenOrderedDict[str, Any]:
        from .._xorq import api as xo

        return FrozenOrderedDict(
            {
                "fieldName": xo.literal("").op(),
                "fieldPath": xo.literal("").op(),
                "fieldType": xo.literal("").op(),
                "fieldValue": xo.literal("").op(),
                "weight": xo.literal(0).op(),
            },
        )

    @property
    def schema(self) -> Schema:
        return Schema(
            {
                "fieldName": "string",
                "fieldPath": "string",
                "fieldType": "string",
                "fieldValue": "string",
                "weight": "int64",
            },
        )

    @property
    def keys(self) -> tuple[str, ...]:
        return ("fieldValue", "fieldName", "fieldPath", "fieldType")

    @property
    def aggs(self) -> dict[str, Any]:
        return {"weight": lambda t: t.weight}

    def to_untagged(self):
        from ._core import _get_merged_fields, _to_untagged

        all_roots = _find_all_root_models(self.source)
        base_tbl = (
            _to_untagged(self.source).limit(self.sample)
            if self.sample
            else _to_untagged(self.source)
        )

        merged_dimensions = _get_merged_fields(all_roots, "dimensions")
        fields_to_index = _get_fields_to_index(
            self.selector,
            merged_dimensions,
            base_tbl,
        )

        if not fields_to_index:
            from .._xorq import api as xo

            return xo.memtable(
                {
                    "fieldName": [],
                    "fieldPath": [],
                    "fieldType": [],
                    "fieldValue": [],
                    "weight": [],
                },
            )

        def build_fragment(field_name: str) -> Any:
            field_expr = (
                merged_dimensions[field_name](base_tbl)
                if field_name in merged_dimensions
                else base_tbl[field_name]
            )
            field_type = field_expr.type()
            type_str = _get_field_type_str(field_type)
            weight_expr = _get_weight_expr(
                base_tbl,
                self.by,
                all_roots,
                field_type.is_string(),
            )

            return (
                _build_string_index_fragment(
                    base_tbl,
                    field_expr,
                    field_name,
                    field_name,
                    type_str,
                    weight_expr,
                )
                if field_type.is_string() or not field_type.is_numeric()
                else _build_numeric_index_fragment(
                    base_tbl,
                    field_expr,
                    field_name,
                    field_name,
                    type_str,
                    weight_expr,
                )
            )

        fragments = [build_fragment(f) for f in fields_to_index]
        return reduce(lambda acc, frag: acc.union(frag), fragments[1:], fragments[0])

    def filter(self, predicate: Callable) -> "SemanticFilter":
        from ..expr import SemanticFilter

        return SemanticFilter(source=self, predicate=predicate)

    def order_by(self, *keys: str | ir.Value | Callable) -> "SemanticOrderBy":
        from ..expr import SemanticOrderBy

        return SemanticOrderBy(source=self, keys=keys)

    def limit(self, n: int, offset: int = 0) -> "SemanticLimit":
        from ..expr import SemanticLimit

        return SemanticLimit(source=self, n=n, offset=offset)

    def execute(self):
        return _rebind_to_canonical_backend(self.to_untagged()).execute()

    def as_expr(self):
        """Return self as expression."""
        return self

    def compile(self, **kwargs):
        return self.to_untagged().compile(**kwargs)

    def sql(self, **kwargs):
        return ibis.to_sql(self.to_untagged(), **kwargs)

    def __getitem__(self, key):
        return self.to_untagged()[key]

    def pipe(self, func, *args, **kwargs):
        return func(self, *args, **kwargs)
