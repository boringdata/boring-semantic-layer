from __future__ import annotations

from collections.abc import Iterable
from functools import reduce
from typing import Any

import ibis
from attrs import frozen
from toolz import curry, pipe

from .measure_scope import AllOf, BinOp, MeasureExpr, MeasureRef


@curry
def _extract_nested_array(prev_col: str, array_col: str, table):
    if prev_col not in table.columns:
        return table
    prev_struct = table[prev_col]
    if not hasattr(prev_struct, array_col):
        return table
    return table.mutate(**{array_col: getattr(prev_struct, array_col)})


@curry
def _do_unnest_array(array_col: str, table):
    return table.unnest(array_col) if array_col in table.columns else table


def _unnest_nested_arrays(base_tbl, array_path: tuple[str, ...]):
    sorted_path = tuple(sorted(array_path))

    def unnest_step(table, indexed_col):
        idx, array_col = indexed_col
        if idx == 0:
            return _do_unnest_array(array_col, table)
        prev_col = sorted_path[idx - 1]
        if array_col in table.columns:
            return _do_unnest_array(array_col, table)
        return pipe(table, _extract_nested_array(prev_col, array_col), _do_unnest_array(array_col))

    return reduce(unnest_step, enumerate(sorted_path), base_tbl)


def _collect_all_refs(expr: MeasureExpr, out: set[str]) -> None:
    if isinstance(expr, AllOf):
        out.add(expr.ref.name)
    elif isinstance(expr, BinOp):
        _collect_all_refs(expr.left, out)
        _collect_all_refs(expr.right, out)


@curry
def _compile_binop(by_tbl, all_tbl, op: str, left: Any, right: Any):
    left_val = _compile_formula(left, by_tbl, all_tbl)
    right_val = _compile_formula(right, by_tbl, all_tbl)
    ops = {
        "add": lambda left_val, right_val: left_val + right_val,
        "sub": lambda left_val, right_val: left_val - right_val,
        "mul": lambda left_val, right_val: left_val * right_val,
        "div": lambda left_val, right_val: left_val.cast("float64") / right_val.cast("float64"),
    }
    if op not in ops:
        raise ValueError(f"Unknown operator: {op}")
    return ops[op](left_val, right_val)


def _compile_formula(expr: MeasureExpr, by_tbl, all_tbl):
    if isinstance(expr, int | float):
        return ibis.literal(expr)
    if isinstance(expr, MeasureRef):
        return by_tbl[expr.name]
    if isinstance(expr, AllOf):
        return all_tbl[expr.ref.name]
    if isinstance(expr, BinOp):
        return _compile_binop(by_tbl, all_tbl, expr.op, expr.left, expr.right)
    return expr


@frozen
class MeasureClassification:
    regular_measures: dict[str, tuple[callable, Any]]
    nested_measures: dict[tuple[str, ...], dict[str, tuple[callable, Any]]]


def make_measure_classification(
    base_tbl,
    agg_specs: dict[str, callable],
) -> MeasureClassification:
    from .nested_access import NestedAccessMarker

    regular = {}
    nested = {}

    for name, agg_fn in agg_specs.items():
        result = agg_fn(base_tbl)

        if isinstance(result, NestedAccessMarker):
            # Nested measure - group by array path
            array_path = result.array_path
            if array_path not in nested:
                nested[array_path] = {}
            nested[array_path][name] = (agg_fn, result)
        else:
            # Regular session-level measure
            regular[name] = (agg_fn, result)

    return MeasureClassification(
        regular_measures=regular,
        nested_measures=nested,
    )


@curry
def _build_field_expr(array_path: tuple[str, ...], field_path: tuple[str, ...], unnested_tbl):
    # Start from first array column
    expr = getattr(unnested_tbl, array_path[0])

    if not field_path:
        return expr

    # Traverse field path
    return reduce(lambda e, field: getattr(e, field), field_path, expr)


@curry
def _apply_aggregation(marker, expr):
    if marker.operation == "count":
        # Count operates on table, not expression
        return expr.count() if hasattr(expr, "count") else expr
    else:
        # Other operations on expression
        agg_method = getattr(expr, marker.operation)
        return agg_method()


def _build_nested_aggregation(unnested_tbl, marker) -> Any:
    if marker.operation == "count":
        return unnested_tbl.count()

    # Build field access expression
    expr = _build_field_expr(marker.array_path, marker.field_path, unnested_tbl)

    # Apply aggregation
    return _apply_aggregation(marker, expr)


def _build_level_aggregations(
    base_tbl,
    array_path: tuple[str, ...],
    measures: dict[str, tuple[callable, Any]],
) -> dict[str, Any]:
    unnested_tbl = _unnest_nested_arrays(base_tbl, array_path)

    return {
        name: _build_nested_aggregation(unnested_tbl, marker)
        for name, (agg_fn, marker) in measures.items()
    }


@curry
def _make_grouped_table(agg_dict: dict[str, Any], by_cols: Iterable[str], table):
    group_exprs = [table[c] for c in by_cols]
    return table.group_by(group_exprs).aggregate(**agg_dict)


def _build_session_table(base_tbl, by_cols: Iterable[str], regular_measures: dict) -> Any:
    if not regular_measures:
        return None

    session_aggs = {name: result for name, (_, result) in regular_measures.items()}
    return _make_grouped_table(session_aggs, by_cols, base_tbl)


def _build_nested_level_table(
    base_tbl,
    by_cols: Iterable[str],
    array_path: tuple[str, ...],
    measures: dict[str, tuple[callable, Any]],
):
    level_aggs = _build_level_aggregations(base_tbl, array_path, measures)
    unnested_tbl = _unnest_nested_arrays(base_tbl, array_path)
    return _make_grouped_table(level_aggs, by_cols, unnested_tbl)


def _join_tables(by_cols: Iterable[str], tables: list) -> Any:
    if len(tables) == 0:
        raise ValueError("Cannot join zero tables")
    if len(tables) == 1:
        return tables[0]

    by_cols_set = set(by_cols)

    def join_step(left, right):
        # Build join predicates
        predicates = [left[c] == right[c] for c in by_cols]

        # Select only non-key columns from right to avoid duplicates
        right_cols = [c for c in right.columns if c not in by_cols_set]
        right_select = [right[c] for c in right_cols]

        # Join and select
        return left.left_join(right, predicates).select([left] + right_select)

    # Left join all tables sequentially
    return reduce(join_step, tables[1:], tables[0])


def _find_measure_in_nested(
    measure_name: str,
    nested_measures: dict[tuple[str, ...], dict[str, tuple[callable, Any]]],
) -> tuple[tuple[str, ...], tuple[callable, Any]] | None:
    for array_path, measures in nested_measures.items():
        if measure_name in measures:
            return (array_path, measures[measure_name])
    return None


def _build_total_aggregation(
    base_tbl,
    measure_name: str,
    classification: MeasureClassification,
    agg_specs: dict[str, callable],
) -> Any:
    # Check regular measures first
    if measure_name in classification.regular_measures:
        _, result = classification.regular_measures[measure_name]
        return result

    # Check nested measures
    found = _find_measure_in_nested(measure_name, classification.nested_measures)
    if found:
        array_path, (agg_fn, marker) = found
        unnested_tbl = _unnest_nested_arrays(base_tbl, array_path)
        return _build_nested_aggregation(unnested_tbl, marker)

    # Fallback - evaluate the function
    return agg_specs[measure_name](base_tbl)


def _build_totals_table(
    base_tbl,
    needed_totals: set[str],
    classification: MeasureClassification,
    agg_specs: dict[str, callable],
) -> Any | None:
    if not needed_totals:
        return None

    totals_aggs = {
        name: _build_total_aggregation(base_tbl, name, classification, agg_specs)
        for name in needed_totals
    }

    return base_tbl.aggregate(**totals_aggs)


def compile_grouped_with_all(
    base_tbl,
    by_cols: Iterable[str],
    agg_specs: dict[str, callable],
    calc_specs: dict[str, MeasureExpr],
    requested_measures: Iterable[str] = None,
):
    # Step 1: Classify measures
    classification = make_measure_classification(base_tbl, agg_specs)

    # Step 2: Build result tables for each level
    result_tables = []

    # Session-level table
    session_table = _build_session_table(
        base_tbl,
        by_cols,
        classification.regular_measures,
    )
    if session_table is not None:
        result_tables.append(session_table)

    # Nested-level tables
    for array_path, measures in classification.nested_measures.items():
        level_table = _build_nested_level_table(base_tbl, by_cols, array_path, measures)
        result_tables.append(level_table)

    # Step 3: Join tables (or create empty grouped table)
    if len(result_tables) == 0:
        by_tbl = _make_grouped_table({}, by_cols, base_tbl)
    else:
        by_tbl = _join_tables(by_cols, result_tables)

    # Step 4: Add totals if needed
    needed_totals = set()
    for ast in calc_specs.values():
        _collect_all_refs(ast, needed_totals)

    if needed_totals:
        all_tbl = _build_totals_table(base_tbl, needed_totals, classification, agg_specs)
        out = by_tbl.join(all_tbl, how="cross")
    else:
        all_tbl = None
        out = by_tbl

    # Step 5: Apply calculated measures
    calc_cols = {name: _compile_formula(ast, out, all_tbl) for name, ast in calc_specs.items()}
    out = out.mutate(**calc_cols)

    # Step 6: Select requested columns
    if requested_measures is not None:
        # Preserve order and uniqueness
        select_cols = list(
            dict.fromkeys(
                list(by_cols) + list(requested_measures) + list(calc_specs.keys()),
            ),
        )
        out = out.select([out[c] for c in select_cols])

    return out
