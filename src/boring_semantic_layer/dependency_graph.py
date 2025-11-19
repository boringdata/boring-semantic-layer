from __future__ import annotations

from collections.abc import Iterator

from ibis.expr import types as ir
from ibis.expr.operations.relations import Field

from .graph_utils import to_node, walk_nodes
from .ops import _classify_dependencies, _collect_measure_refs, _resolve_expr


class DependencyGraph(dict):
    """Simple dictionary-based dependency graph for semantic fields.

    Structure: {field_name: {"type": "dimension"|"measure"|"calc_measure", "deps": {dep_name: dep_type, ...}}}
    """

    @classmethod
    def build(
        cls, dimensions: dict, measures: dict, calc_measures: dict, base_table: ir.Table
    ) -> DependencyGraph:
        graph = cls()

        # Build extended table by progressively adding dimensions
        # This allows later dimensions to reference earlier ones
        extended_table = base_table
        for dim_name, dim in dimensions.items():
            resolved = _resolve_expr(dim.expr, extended_table)
            extended_table = extended_table.mutate(**{dim_name: resolved})

        # Extract dependencies for dimensions and measures
        table_op = to_node(extended_table)
        for name, obj in {**dimensions, **measures}.items():
            resolved = _resolve_expr(obj.expr, extended_table)
            fields = [
                f
                for f in walk_nodes(Field, resolved)
                if hasattr(f, "name") and hasattr(f, "rel") and f.rel == table_op
            ]
            deps_with_types = _classify_dependencies(
                fields, dimensions, measures, calc_measures, current_field=name
            )
            graph[name] = {
                "deps": deps_with_types,
                "type": "dimension" if name in dimensions else "measure",
            }

        # Extract dependencies for calculated measures
        for name, calc_expr in calc_measures.items():
            refs = set()
            _collect_measure_refs(calc_expr, refs)
            graph[name] = {"deps": {ref: "measure" for ref in refs}, "type": "calc_measure"}

        return graph

    def bfs(self, start: str | list[str], max_depth: int | None = None) -> Iterator[str]:
        """Breadth-first traversal of dependencies (max_depth=None for unlimited)."""
        start_names = [start] if isinstance(start, str) else start
        visited = set()
        queue = [(n, 0) for n in start_names]

        while queue:
            node, depth = queue.pop(0)
            if node in visited:
                continue
            visited.add(node)
            yield node

            # Add direct dependencies to queue if within depth limit
            if max_depth is None or depth < max_depth:
                for dep in self.get(node, {}).get("deps", {}):
                    if dep not in visited:
                        queue.append((dep, depth + 1))

    def predecessors(self, node: str, max_depth: int | None = None) -> set[str]:
        """Get transitive dependencies of a field (max_depth=None for unlimited)."""
        return {dep for dep in self.bfs(node, max_depth=max_depth) if dep != node}

    def successors(self, node: str, max_depth: int | None = None) -> set[str]:
        """Get fields that transitively depend on this field (max_depth=None for unlimited)."""
        # Use BFS on inverted graph
        inverted = self.invert()
        return {dep for dep in inverted.bfs(node, max_depth=max_depth) if dep != node}

    def invert(self) -> DependencyGraph:
        """Return inverted graph where edges point from dependencies to dependents."""
        inverted = DependencyGraph()

        # Initialize all nodes using BFS from all root nodes
        all_nodes = set(self.bfs(list(self.keys())))
        for node_name in all_nodes:
            inverted[node_name] = {
                "deps": {},
                "type": self[node_name]["type"] if node_name in self else "column",
            }

        # Invert edges: for each node -> dep, create dep -> node
        for node, metadata in self.items():
            for dep_name in metadata["deps"]:
                if dep_name in inverted:
                    inverted[dep_name]["deps"][node] = metadata["type"]

        return inverted
