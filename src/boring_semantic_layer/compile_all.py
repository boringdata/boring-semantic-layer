from __future__ import annotations
import ibis
from typing import Dict, Iterable
from .measure_nodes import MeasureRef, AllOf, BinOp, MeasureExpr

def _collect_all_refs(expr: MeasureExpr, out: set[str]) -> None:
    if isinstance(expr, AllOf):
        out.add(expr.ref.name)
    elif isinstance(expr, BinOp):
        _collect_all_refs(expr.left, out)
        _collect_all_refs(expr.right, out)

def _compile_formula(expr: MeasureExpr, by_tbl, all_tbl):
    if isinstance(expr, (int, float)):
        return ibis.literal(expr)
    if isinstance(expr, MeasureRef):
        return by_tbl[expr.name]
    if isinstance(expr, AllOf):
        return all_tbl[expr.ref.name]
    if isinstance(expr, BinOp):
        l = _compile_formula(expr.left, by_tbl, all_tbl)
        r = _compile_formula(expr.right, by_tbl, all_tbl)
        return (l + r if expr.op == "add" else l - r if expr.op == "sub"
                else l * r if expr.op == "mul"
                else l.cast("float64") / r.cast("float64") if expr.op == "div"
                else (_ for _ in ()).throw(ValueError(f"unknown op {expr.op}")))
    return expr

def compile_grouped_with_all(
    base_tbl,
    by_cols: Iterable[str],
    agg_specs: Dict[str, callable],
    calc_specs: Dict[str, MeasureExpr],
    requested_measures: Iterable[str] = None,
):
    grouped_aggs = {name: agg_fn(base_tbl) for name, agg_fn in agg_specs.items()}
    by_tbl = base_tbl.group_by([base_tbl[c] for c in by_cols]).aggregate(**grouped_aggs)

    needed_all = set()
    for ast in calc_specs.values():
        _collect_all_refs(ast, needed_all)

    if needed_all:
        totals_aggs = {m: agg_specs[m](base_tbl) for m in needed_all}
        all_tbl = base_tbl.aggregate(**totals_aggs)
        out = by_tbl.join(all_tbl, how="cross")
    else:
        all_tbl = None
        out = by_tbl

    calc_cols = {name: _compile_formula(ast, by_tbl, all_tbl) for name, ast in calc_specs.items()}
    out = out.mutate(**calc_cols)

    if requested_measures is not None:
        select_cols = list(dict.fromkeys(list(by_cols) + list(requested_measures) + list(calc_specs.keys())))
        out = out.select([out[c] for c in select_cols])

    return out