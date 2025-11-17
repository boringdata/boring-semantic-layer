"""Dependency graph for semantic models."""

from __future__ import annotations

from ibis.expr import types as ir
from ibis.expr.operations.relations import Field

from .graph_utils import walk_nodes
from .ops import _collect_measure_refs


class DependencyGraph(dict):
    """Dependency graph with NetworkX-style API for graph traversal."""

    def _extract_field_deps(
        self,
        dimensions: dict,
        measures: dict,
        calc_measures: dict,
        base_table: ir.Table,
        extended_table: ir.Table,
    ) -> None:
        from .graph_utils import to_node

        for name, obj in {**dimensions, **measures}.items():
            try:
                # Determine which table to use for resolution
                table = extended_table if name in measures else base_table

                # Add previous dimensions if resolving a dimension
                if name in dimensions:
                    table = _add_previous_dimensions(table, dimensions, name)

                # Resolve expression and extract field dependencies
                resolved = _resolve_expr(obj.expr, table)
                table_op = to_node(table)
                fields = [
                    f
                    for f in walk_nodes(Field, resolved)
                    if hasattr(f, "name") and hasattr(f, "rel") and f.rel == table_op
                ]

                # Classify each dependency
                deps_with_types = _classify_dependencies(
                    fields, dimensions, measures, calc_measures, current_field=name
                )

                self[name] = {
                    "deps": deps_with_types,
                    "type": "dimension" if name in dimensions else "measure",
                }
            except Exception:
                self[name] = {"deps": {}, "type": "dimension" if name in dimensions else "measure"}

    def _extract_calc_measure_deps(self, calc_measures: dict) -> None:
        for name, calc_expr in calc_measures.items():
            refs = set()
            _collect_measure_refs(calc_expr, refs)
            # All calc measure deps are other measures
            deps_with_types = {ref: "measure" for ref in refs}
            self[name] = {"deps": deps_with_types, "type": "calc_measure"}

    def predecessors(self, node: str) -> set[str]:
        """Get nodes that this node depends on."""
        if node not in self:
            return set()
        return set(self[node]["deps"].keys())

    def successors(self, node: str) -> set[str]:
        """Get nodes that depend on this node."""
        result = set()
        for field, metadata in self.items():
            if node in metadata["deps"]:
                result.add(field)
        return result

    def to_networkx_json(self) -> dict:
        """Export graph to NetworkX node-link JSON format."""
        # Collect all unique nodes (fields and their dependencies)
        all_nodes = set(self.keys())
        for field_meta in self.values():
            all_nodes.update(field_meta["deps"].keys())

        # Build nodes list with metadata
        nodes = []
        for node in sorted(all_nodes):
            node_data = {"id": node}
            # Add type if this is a tracked field (dimension/measure)
            if node in self:
                node_data["field_type"] = self[node]["type"]
            else:
                # This is a base column (dependency but not tracked)
                node_data["field_type"] = "column"
            nodes.append(node_data)

        # Build links (edges) from dependencies
        links = []
        for target, metadata in self.items():
            for source, dep_type in metadata["deps"].items():
                links.append({"source": source, "target": target, "dependency_type": dep_type})

        return {
            "directed": True,
            "multigraph": False,
            "graph": {},
            "nodes": nodes,
            "links": links,
        }

    @classmethod
    def build(
        cls,
        dimensions: dict,
        measures: dict,
        calc_measures: dict,
        base_table: ir.Table,
    ) -> DependencyGraph:
        graph = cls()

        # Build extended table with all dimensions
        extended_table = _build_extended_table(base_table, dimensions)

        # Extract dependencies for dimensions and measures
        graph._extract_field_deps(dimensions, measures, calc_measures, base_table, extended_table)

        # Extract calculated measure dependencies
        graph._extract_calc_measure_deps(calc_measures)

        return graph


def _resolve_expr(expr, table):
    if hasattr(expr, "resolve"):
        return expr.resolve(table)
    elif callable(expr):
        return expr(table)
    return expr


def _build_extended_table(base_table: ir.Table, dimensions: dict) -> ir.Table:
    extended_table = base_table
    for dim_name, dim in dimensions.items():
        try:
            resolved = _resolve_expr(dim.expr, extended_table)
            extended_table = extended_table.mutate(**{dim_name: resolved})
        except Exception:
            pass
    return extended_table


def _add_previous_dimensions(table: ir.Table, dimensions: dict, current_name: str) -> ir.Table:
    for prev_name, prev_dim in dimensions.items():
        if prev_name == current_name:
            break
        try:
            resolved = _resolve_expr(prev_dim.expr, table)
            table = table.mutate(**{prev_name: resolved})
        except Exception:
            pass
    return table


def _classify_dependencies(
    fields: list,
    dimensions: dict,
    measures: dict,
    calc_measures: dict,
    current_field: str | None = None,
) -> dict[str, str]:
    """Classify field dependencies, excluding the current field being processed to avoid self-references."""
    deps_with_types = {}
    for f in fields:
        # Don't classify a field as a dimension/measure if it's the field we're currently processing
        # (e.g., dimension "origin" referencing column "origin" should be classified as "column", not "dimension")
        if f.name in dimensions and f.name != current_field:
            deps_with_types[f.name] = "dimension"
        elif f.name in measures or f.name in calc_measures:
            deps_with_types[f.name] = "measure"
        else:
            deps_with_types[f.name] = "column"
    return deps_with_types
