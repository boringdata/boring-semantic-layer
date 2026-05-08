"""Helpers for compiling nested-array aggregations.

The semantic layer supports measures that aggregate over nested array
columns (``t.hits.count()``, ``t.hits.value.sum()`` …). At compile time
each array path is unnested in isolation, aggregated at its own grain,
and joined back to the session-level result via the requested group-by
columns. These helpers used to live in ``compile_all.py`` alongside the
curated calc-measure compiler; that compiler is gone (replaced by the
ibis-native :mod:`calc_compiler`) so the nested-array machinery now sits
in its own module.
"""

from __future__ import annotations

from collections.abc import Iterable
from functools import reduce
from typing import Any

import ibis
from toolz import curry, pipe


def get_ibis_module(table):
    """Return the ibis module that built ``table`` (regular vs xorq-vendored).

    BSL coexists with both flavors of ibis. Picking the right module avoids
    cross-flavor literal/struct construction errors.
    """
    table_module = type(table).__module__
    if table_module.startswith("xorq.vendor.ibis"):
        from ._xorq import ibis as xorq_ibis

        return xorq_ibis
    return ibis


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


def unnest_nested_arrays(base_tbl, array_path: tuple[str, ...]):
    """Apply unnest steps for each level of a nested array path."""
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


@curry
def _build_field_expr(array_path: tuple[str, ...], field_path: tuple[str, ...], unnested_tbl):
    expr = getattr(unnested_tbl, array_path[0])
    if not field_path:
        return expr
    return reduce(lambda e, field: getattr(e, field), field_path, expr)


@curry
def _apply_aggregation(marker, expr):
    if marker.operation == "count":
        return expr.count() if hasattr(expr, "count") else expr
    return getattr(expr, marker.operation)()


def build_nested_aggregation(unnested_tbl, marker) -> Any:
    """Compile a single nested-array marker into an ibis aggregation."""
    if marker.operation == "count":
        return unnested_tbl.count()
    expr = _build_field_expr(marker.array_path, marker.field_path, unnested_tbl)
    return _apply_aggregation(marker, expr)


def build_level_aggregations(
    base_tbl,
    array_path: tuple[str, ...],
    measures: dict[str, tuple[Any, Any]],
) -> dict[str, Any]:
    unnested_tbl = unnest_nested_arrays(base_tbl, array_path)
    return {
        name: build_nested_aggregation(unnested_tbl, marker)
        for name, (_agg_fn, marker) in measures.items()
    }


@curry
def _make_grouped_table(agg_dict: dict[str, Any], by_cols: Iterable[str], table):
    group_exprs = [table[c] for c in by_cols]
    return (
        table.group_by(group_exprs).aggregate(**agg_dict)
        if group_exprs
        else table.aggregate(**agg_dict)
    )


def build_session_table(base_tbl, by_cols: Iterable[str], regular_measures: dict) -> Any:
    """Aggregate regular (non-nested) measures at the session grain."""
    if not regular_measures:
        return None
    session_aggs = {name: result for name, (_, result) in regular_measures.items()}
    return _make_grouped_table(session_aggs, by_cols, base_tbl)


def build_nested_level_table(
    base_tbl,
    by_cols: Iterable[str],
    array_path: tuple[str, ...],
    measures: dict[str, tuple[Any, Any]],
):
    """Aggregate nested-array measures at the unnested grain."""
    level_aggs = build_level_aggregations(base_tbl, array_path, measures)
    unnested_tbl = unnest_nested_arrays(base_tbl, array_path)
    return _make_grouped_table(level_aggs, by_cols, unnested_tbl)


def join_tables(by_cols: Iterable[str], tables: list) -> Any:
    """Left-join a list of pre-aggregated tables on shared group-by columns."""
    if len(tables) == 0:
        raise ValueError("Cannot join zero tables")
    if len(tables) == 1:
        return tables[0]

    by_cols_set = set(by_cols)

    def join_step(left, right):
        predicates = [left[c] == right[c] for c in by_cols]
        right_cols = [c for c in right.columns if c not in by_cols_set]
        right_select = [right[c] for c in right_cols]
        return left.left_join(right, predicates).select([left] + right_select)

    return reduce(join_step, tables[1:], tables[0])
