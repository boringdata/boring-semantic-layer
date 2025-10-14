from __future__ import annotations
from typing import Dict, Iterable, Optional, Union

import ibis

from .measure_scope import MeasureScope, ColumnScope
from .measure_nodes import MeasureRef, AllOf, BinOp, MeasureExpr
from .compile_all import compile_grouped_with_all

class SemanticTable:
    def __init__(self, ibis_table, name: str):
        self._name = name
        self._base_tbl = ibis_table
        self._dims: Dict[str, callable] = {}
        self._base_measures: Dict[str, callable] = {}
        self._calc_measures: Dict[str, MeasureExpr] = {}

    def with_dimensions(self, **defs):
        self._dims.update(defs)
        return self

    def with_measures(self, **defs):
        known = set(self._base_measures) | set(self._calc_measures) | set(defs.keys())
        scope = MeasureScope(self._base_tbl, known_measures=known)
        for name, fn in defs.items():
            val = fn(scope)
            if isinstance(val, (MeasureRef, AllOf, BinOp, int, float)):
                self._calc_measures[name] = val
            else:
                self._base_measures[name] = (lambda _fn=fn: (lambda base_tbl: _fn(ColumnScope(base_tbl))))()
        return self


    def join_one(self, other: "SemanticTable", left_on: str, right_on: str) -> "SemanticTable":
        left = self._base_tbl
        right = other._base_tbl
        cond = getattr(left, left_on) == getattr(right, right_on)
        joined_tbl = left.join(right, cond, how="inner")

        out = SemanticTable(joined_tbl, name=f"{self._name}_join_{other._name}")
        out._dims = {**self._dims, **other._dims}
        out._base_measures = {**self._base_measures, **other._base_measures}
        out._calc_measures = {**self._calc_measures, **other._calc_measures}
        return out

    def join_many(self, other: "SemanticTable", left_on: str, right_on: str) -> "SemanticTable":
        left = self._base_tbl
        right = other._base_tbl
        cond = getattr(left, left_on) == getattr(right, right_on)
        joined_tbl = left.join(right, cond, how="left")

        out = SemanticTable(joined_tbl, name=f"{self._name}_join_{other._name}")
        out._dims = {**self._dims, **other._dims}
        out._base_measures = {**self._base_measures, **other._base_measures}
        out._calc_measures = {**self._calc_measures, **other._calc_measures}
        return out

    def group_by(self, *dims: str):
        self._group_dims = list(dims)
        return self

    def aggregate(self, *measure_names: str, **aliased: str):
        if not hasattr(self, "_group_dims"):
            raise ValueError("Call .group_by(...) before .aggregate(...)")

        from .api import SemanticTableExpr

        # old-style lambda aggregates => defer to semantic_api DSL
        if any(callable(m) for m in measure_names) or any(callable(f) for f in aliased.values()):
            expr = SemanticTableExpr(self)
            expr = expr.group_by(*self._group_dims)
            return expr.aggregate(*measure_names, **aliased)

        select_measures = list(measure_names) + list(aliased.values())
        base = self._materialize_base_with_dims()

        grouped = compile_grouped_with_all(
            base_tbl=base,
            by_cols=self._group_dims,
            agg_specs=self._base_measures,
            calc_specs=self._calc_measures,
        )

        proj_cols = {d: grouped[d] for d in self._group_dims}
        for m in select_measures:
            proj_cols[m] = grouped[m]
        for alias, m in aliased.items():
            proj_cols[alias] = grouped[m]
        return grouped.select(*proj_cols.keys())

    def _materialize_base_with_dims(self):
        if not self._dims:
            return self._base_tbl
        cols = {name: fn(self._base_tbl) for name, fn in self._dims.items()}
        return self._base_tbl.mutate(**cols)

def to_semantic_table(ibis_table, name: Optional[str] = None) -> SemanticTable:
    return SemanticTable(ibis_table, name=name)