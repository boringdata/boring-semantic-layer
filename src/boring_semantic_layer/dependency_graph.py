"""Dependency graph for semantic models."""

from __future__ import annotations

from ibis.expr import types as ir
from ibis.expr.operations.relations import Field

from .graph_utils import walk_nodes
from .ops import _collect_measure_refs


class DependencyGraph(dict):
    """A dependency graph with NetworkX-style API for graph traversal.

    Extends dict to provide methods for querying dependencies between fields.
    """

    def predecessors(self, node: str) -> set[str]:
        """Get nodes that this node depends on (incoming edges).

        Args:
            node: The field name to query

        Returns:
            Set of field names that this node depends on

        Example:
            graph.predecessors('avg_distance')  # {'total_distance', 'flight_count'}
        """
        if node not in self:
            return set()
        return set(self[node]["deps"].keys())

    def successors(self, node: str) -> set[str]:
        """Get nodes that depend on this node (outgoing edges).

        Args:
            node: The field name to query

        Returns:
            Set of field names that depend on this node

        Example:
            graph.successors('total_distance')  # {'avg_distance'}
        """
        result = set()
        for field, metadata in self.items():
            if node in metadata["deps"]:
                result.add(field)
        return result


def build_dependency_graph(
    dimensions: dict,
    measures: dict,
    calc_measures: dict,
    base_table: ir.Table,
) -> DependencyGraph:
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

    graph = DependencyGraph()

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
