"""Dependency graph for semantic models."""

from __future__ import annotations

from collections import defaultdict
from typing import Any

from ibis.expr import types as ir
from ibis.expr.operations.relations import Field

from .graph_utils import walk_nodes
from .ops import _collect_measure_refs


def build_dependency_graph(
    dimensions: dict,
    measures: dict,
    calc_measures: dict,
    base_table: ir.Table,
) -> dict[str, dict[str, Any]]:
    """Build a dependency graph: field -> {deps, type}.

    Args:
        dimensions: Dict of dimension name -> Dimension object
        measures: Dict of measure name -> Measure object
        calc_measures: Dict of calc measure name -> MeasureRef/BinOp/etc
        base_table: The base table to resolve expressions

    Returns:
        Dict mapping field names to their metadata:
        {
            'field_name': {
                'deps': {
                    'dep_name': 'column' | 'dimension' | 'measure'
                },
                'type': 'dimension' | 'measure' | 'calc_measure'
            }
        }
    """
    from .graph_utils import to_node

    graph = {}

    # Build extended table with all dimensions
    extended_table = base_table
    for dim_name, dim in dimensions.items():
        try:
            resolved = _resolve_expr(dim.expr, extended_table)
            extended_table = extended_table.mutate(**{dim_name: resolved})
        except Exception:
            pass

    # Extract dependencies for dimensions and measures
    for name, obj in {**dimensions, **measures}.items():
        try:
            # Determine which table to use for resolution
            table = extended_table if name in measures else base_table

            # Add previous dimensions if resolving a dimension
            if name in dimensions:
                for prev_name, prev_dim in dimensions.items():
                    if prev_name == name:
                        break
                    try:
                        resolved = _resolve_expr(prev_dim.expr, table)
                        table = table.mutate(**{prev_name: resolved})
                    except Exception:
                        pass

            # Resolve expression and extract field dependencies
            resolved = _resolve_expr(obj.expr, table)
            table_op = to_node(table)
            fields = [
                f
                for f in walk_nodes(Field, resolved)
                if hasattr(f, "name") and hasattr(f, "rel") and f.rel == table_op
            ]

            # Classify each dependency
            deps_with_types = {}
            for f in fields:
                if f.name in dimensions:
                    deps_with_types[f.name] = "dimension"
                elif f.name in measures or f.name in calc_measures:
                    deps_with_types[f.name] = "measure"
                else:
                    deps_with_types[f.name] = "column"

            graph[name] = {
                "deps": deps_with_types,
                "type": "dimension" if name in dimensions else "measure",
            }
        except Exception:
            graph[name] = {"deps": {}, "type": "dimension" if name in dimensions else "measure"}

    # Extract calculated measure dependencies
    for name, calc_expr in calc_measures.items():
        refs = set()
        _collect_measure_refs(calc_expr, refs)
        # All calc measure deps are other measures
        deps_with_types = {ref: "measure" for ref in refs}
        graph[name] = {"deps": deps_with_types, "type": "calc_measure"}

    return graph


def _resolve_expr(expr, table):
    """Helper to resolve an expression against a table."""
    if hasattr(expr, "resolve"):
        return expr.resolve(table)
    elif callable(expr):
        return expr(table)
    return expr


def get_dependents(graph: dict[str, dict[str, Any]]) -> dict[str, set[str]]:
    """Build reverse index: field -> fields that depend on it.

    Args:
        graph: Dependency graph (field -> {deps: {name: type}, type})

    Returns:
        Dict mapping field names to their dependents
    """
    dependents = defaultdict(set)
    for field, metadata in graph.items():
        for dep_name in metadata["deps"]:
            dependents[dep_name].add(field)
    return dict(dependents)
