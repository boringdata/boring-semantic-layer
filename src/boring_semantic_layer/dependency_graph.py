from __future__ import annotations

from collections.abc import Iterator

from attrs import frozen
from ibis.common.graph import Graph, Node
from ibis.expr import types as ir
from ibis.expr.operations.relations import Field

from .graph_utils import walk_nodes
from .ops import _collect_measure_refs


@frozen
class SemanticFieldNode(Node):
    name: str
    field_type: str
    dependencies: tuple[SemanticFieldNode, ...] = ()

    @property
    def __args__(self) -> tuple:
        return (self.name, self.field_type, self.dependencies)

    @property
    def __argnames__(self) -> tuple[str, ...]:
        return ("name", "field_type", "dependencies")

    @property
    def __children__(self) -> tuple[Node, ...]:
        return self.dependencies


class DependencyGraph(dict):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._nodes: dict[str, SemanticFieldNode] = {}
        self._graph: Graph | None = None

    def _build_nodes_and_graph(self) -> None:
        if self._graph is not None:
            return

        node_map = {}
        for name, metadata in self.items():
            node_map[name] = SemanticFieldNode(
                name=name, field_type=metadata["type"], dependencies=()
            )

        all_dep_names = set()
        for metadata in self.values():
            all_dep_names.update(metadata["deps"].keys())

        for dep_name in all_dep_names:
            if dep_name not in node_map:
                node_map[dep_name] = SemanticFieldNode(
                    name=dep_name, field_type="column", dependencies=()
                )

        for name, metadata in self.items():
            deps = tuple(node_map[dep_name] for dep_name in metadata["deps"].keys())
            node_map[name] = SemanticFieldNode(
                name=name, field_type=metadata["type"], dependencies=deps
            )

        self._nodes = node_map

        if self._nodes:
            roots = [node for name, node in self._nodes.items() if name in self]
            self._graph = Graph.from_bfs(roots) if roots else Graph()
        else:
            self._graph = Graph()

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
                table = extended_table if name in measures else base_table
                if name in dimensions:
                    table = _add_previous_dimensions(table, dimensions, name)

                resolved = _resolve_expr(obj.expr, table)
                table_op = to_node(table)
                fields = [
                    f
                    for f in walk_nodes(Field, resolved)
                    if hasattr(f, "name") and hasattr(f, "rel") and f.rel == table_op
                ]

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
            self[name] = {"deps": {ref: "measure" for ref in refs}, "type": "calc_measure"}

    def predecessors(self, node: str) -> set[str]:
        return set(self.get(node, {}).get("deps", {}).keys())

    def successors(self, node: str) -> set[str]:
        return {field for field, meta in self.items() if node in meta["deps"]}

    def bfs(self, start: str | list[str]) -> Iterator[str]:
        self._build_nodes_and_graph()
        start_names = [start] if isinstance(start, str) else start
        valid_start_names = [name for name in start_names if name in self._nodes]

        if not valid_start_names:
            yield from start_names
            return

        start_nodes = [self._nodes[name] for name in valid_start_names]
        graph = Graph.from_bfs(start_nodes)

        for node in graph.nodes():
            yield node.name

    def dfs(self, start: str | list[str]) -> Iterator[str]:
        self._build_nodes_and_graph()
        start_names = [start] if isinstance(start, str) else start
        valid_start_names = [name for name in start_names if name in self._nodes]

        if not valid_start_names:
            yield from start_names
            return

        start_nodes = [self._nodes[name] for name in valid_start_names]
        graph = Graph.from_dfs(start_nodes)

        for node in reversed(list(graph.nodes())):
            yield node.name

    def _all_nodes(self) -> set[str]:
        all_nodes = set(self.keys())
        for field_meta in self.values():
            all_nodes.update(field_meta["deps"].keys())
        return all_nodes

    def invert(self) -> DependencyGraph:
        self._build_nodes_and_graph()
        if not self._graph:
            return DependencyGraph()

        inverted_ibis_graph = self._graph.invert()
        inverted = DependencyGraph()

        for node_name in self._all_nodes():
            inverted[node_name] = {
                "deps": {},
                "type": self[node_name]["type"] if node_name in self else "column",
            }

        for node in inverted_ibis_graph.nodes():
            children = inverted_ibis_graph[node]
            for child in children:
                inverted[node.name]["deps"][child.name] = (
                    self[child.name]["type"] if child.name in self else "column"
                )

        return inverted

    def to_dict(self) -> dict:
        nodes = [
            {"id": node, "type": self[node]["type"] if node in self else "column"}
            for node in sorted(self._all_nodes())
        ]
        edges = [
            {"source": source, "target": target, "type": dep_type}
            for target, metadata in self.items()
            for source, dep_type in metadata["deps"].items()
        ]
        return {"nodes": nodes, "edges": edges}

    @classmethod
    def build(
        cls, dimensions: dict, measures: dict, calc_measures: dict, base_table: ir.Table
    ) -> DependencyGraph:
        graph = cls()
        extended_table = _build_extended_table(base_table, dimensions)
        graph._extract_field_deps(dimensions, measures, calc_measures, base_table, extended_table)
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
    return {
        f.name: (
            "dimension"
            if f.name in dimensions and f.name != current_field
            else "measure"
            if f.name in measures or f.name in calc_measures
            else "column"
        )
        for f in fields
    }
