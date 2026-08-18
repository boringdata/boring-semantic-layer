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

from toolz import curry

# Back-compat re-export: user-facing error messages document this import path.
from ._xorq import get_ibis_module as get_ibis_module  # noqa: PLC0414
from ._xorq import null_safe_equal


def _allocate_nested_array_name(table, idx: int) -> str:
    """Allocate a private column name without overwriting user data."""
    occupied = frozenset(table.columns)
    preferred = f"__bsl_nested_array_{idx}"
    candidate = preferred
    suffix = 2
    while candidate in occupied:
        candidate = f"{preferred}_{suffix}"
        suffix += 1
    return candidate


def _extract_nested_array(
    prev_col: str,
    array_col: str,
    materialized_col: str,
    table,
    *,
    requested_path: tuple[str, ...],
    parent_path: tuple[str, ...],
):
    """Materialize ``prev_col[array_col]`` under a private column name."""
    path_label = ".".join(requested_path)
    parent_label = ".".join(parent_path)
    if prev_col not in table.columns:
        raise ValueError(
            f"Cannot traverse nested array path {path_label!r}: "
            f"parent {parent_label!r} is unavailable after unnesting."
        )
    prev_struct = table[prev_col]
    fields = getattr(prev_struct.type(), "fields", {})
    if array_col not in fields:
        raise ValueError(
            f"Cannot traverse nested array path {path_label!r}: "
            f"child {array_col!r} does not exist under {parent_label!r}."
        )
    child_type = fields[array_col]
    if not str(child_type).startswith("array"):
        raise ValueError(
            f"Cannot traverse nested array path {path_label!r}: "
            f"child {array_col!r} under {parent_label!r} is not an array "
            f"(found {child_type})."
        )
    return table.mutate(**{materialized_col: prev_struct[array_col]})


@curry
def _do_unnest_array(array_col: str, table):
    return table.unnest(array_col) if array_col in table.columns else table


def unnest_nested_arrays(base_tbl, array_path: tuple[str, ...]):
    """Apply unnest steps for each level of a nested array path."""
    # ``array_path`` describes a traversal, not an unordered collection.  In
    # particular, for ``events.products`` we must unnest ``events`` before the
    # nested ``products`` array can be extracted from the resulting struct.
    # Sorting the names made the result depend on their spelling and could
    # silently turn a nested count into a count of the base rows.
    table = base_tbl
    parent_col: str | None = None
    for idx, array_col in enumerate(tuple(array_path)):
        if idx == 0:
            path_label = ".".join(array_path)
            if array_col not in table.columns:
                raise ValueError(
                    f"Cannot traverse nested array path {path_label!r}: "
                    f"root {array_col!r} does not exist."
                )
            root_type = table[array_col].type()
            if not str(root_type).startswith("array"):
                raise ValueError(
                    f"Cannot traverse nested array path {path_label!r}: "
                    f"root {array_col!r} is not an array (found {root_type})."
                )
            table = _do_unnest_array(array_col, table)
            parent_col = array_col
            continue

        # A later name is a child of the struct produced by the previous
        # unnest step. It must never fall back to an unrelated top-level
        # sibling that happens to have the same name.
        if parent_col is None:
            raise ValueError(
                f"Cannot traverse nested array path {'.'.join(array_path)!r}: "
                f"parent {'.'.join(array_path[:idx])!r} is unavailable after "
                "unnesting."
            )
        materialized_col = _allocate_nested_array_name(table, idx)
        table = _extract_nested_array(
            parent_col,
            array_col,
            materialized_col,
            table,
            requested_path=tuple(array_path),
            parent_path=tuple(array_path[:idx]),
        )
        table = _do_unnest_array(materialized_col, table)
        parent_col = materialized_col

    return table


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
    by_cols = tuple(by_cols)
    level_aggs = build_level_aggregations(base_tbl, array_path, measures)
    unnested_tbl = unnest_nested_arrays(base_tbl, array_path)
    level_table = _make_grouped_table(level_aggs, by_cols, unnested_tbl)

    if not by_cols:
        # A global aggregate already produces one row for an empty input.
        return level_table

    # UNNEST has inner semantics: a base row whose array is NULL or empty
    # contributes no unnested row.  If every row in a group has an empty
    # array, aggregating only the unnested relation drops the group entirely.
    # Build the group domain from the base relation and attach the nested
    # aggregate to it so nested-only queries retain the same group domain as
    # the semantic model.
    group_spine = _make_grouped_table({}, by_cols, base_tbl)
    result = join_tables(by_cols, [group_spine, level_table])

    # COUNT and COUNT DISTINCT have an empty-set identity of zero.  After the
    # left join, their missing level aggregate is NULL, so restore that
    # identity.  Other aggregates intentionally remain NULL for an empty set.
    count_like = {
        name
        for name, (_agg_fn, marker) in measures.items()
        if marker.operation in {"count", "nunique"}
    }
    if count_like:
        result = result.mutate(**{name: result[name].fill_null(0) for name in count_like})
    return result


def join_tables(by_cols: Iterable[str], tables: list) -> Any:
    """Left-join a list of pre-aggregated tables on shared group-by columns."""
    if len(tables) == 0:
        raise ValueError("Cannot join zero tables")
    if len(tables) == 1:
        return tables[0]

    # Materialize once because callers may provide a generator and this value
    # is traversed repeatedly while joining multiple nesting levels.
    by_cols = tuple(by_cols)
    by_cols_set = set(by_cols)

    def join_step(left, right):
        # Null-safe equality: group keys can legitimately be NULL (real NULL
        # dim values, or keys minted by an outer join). Plain == drops those
        # groups from every table but the first.
        predicates = [null_safe_equal(left[c], right[c]) for c in by_cols]
        right_cols = [c for c in right.columns if c not in by_cols_set]
        right_select = [right[c] for c in right_cols]
        return left.left_join(right, predicates).select([left] + right_select)

    return reduce(join_step, tables[1:], tables[0])
