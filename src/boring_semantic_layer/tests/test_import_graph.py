"""Structural ratchet on the intra-package import graph.

The module import graph (including function-local "lazy" imports, which are
cycle workarounds, not absences of a dependency) currently contains one large
strongly connected component: a set of modules that are all mutually
importable from each other, i.e. a knot with no layering.

This test is a ratchet:

* No module outside ``KNOWN_SCC_MEMBERS`` may join a non-trivial SCC.
* No second non-trivial SCC may appear.
* When refactoring detaches modules from the knot, they MUST be removed from
  ``KNOWN_SCC_MEMBERS`` in the same change, so the allowlist only shrinks.

The end state is an empty allowlist: an acyclic module graph.
"""

from __future__ import annotations

import ast
from pathlib import Path

PKG_NAME = "boring_semantic_layer"
PKG_ROOT = Path(__file__).resolve().parents[1]

# The measured SCC as of 2026-08 (main @ 25c79a2). Shrink-only.
KNOWN_SCC_MEMBERS = frozenset(
    {
        "boring_semantic_layer.api",
        "boring_semantic_layer.expr",
        "boring_semantic_layer.format",
        "boring_semantic_layer.ops",
        "boring_semantic_layer.query",
        "boring_semantic_layer.serialization",
        "boring_semantic_layer.serialization.context",
        "boring_semantic_layer.serialization.extract",
        "boring_semantic_layer.serialization.helpers",
        "boring_semantic_layer.serialization.reconstruct",
    }
)


def _discover_modules() -> dict[str, Path]:
    modules: dict[str, Path] = {}
    for path in PKG_ROOT.rglob("*.py"):
        rel = path.relative_to(PKG_ROOT.parent)
        if "tests" in rel.parts or "__pycache__" in rel.parts:
            continue
        dotted = ".".join(rel.with_suffix("").parts)
        if dotted.endswith(".__init__"):
            dotted = dotted[: -len(".__init__")]
        modules[dotted] = path
    return modules


def _sibling_imports(
    node: ast.Import | ast.ImportFrom, current_mod: str, is_pkg_init: bool, mod_names: set[str]
):
    """Yield dotted names of sibling modules this import statement depends on."""
    cur_parts = current_mod.split(".")
    if isinstance(node, ast.Import):
        for alias in node.names:
            name = alias.name
            if name == PKG_NAME or name.startswith(PKG_NAME + "."):
                # `import a.b.c` binds `a`, but the dependency is on `a.b.c`.
                while name and name not in mod_names:
                    name = name.rpartition(".")[0]
                if name:
                    yield name
        return

    if node.level:
        base = cur_parts if is_pkg_init else cur_parts[:-1]
        base = base[: len(base) - (node.level - 1)]
        prefix = ".".join(base)
    else:
        if not (
            node.module and (node.module == PKG_NAME or node.module.startswith(PKG_NAME + "."))
        ):
            return
        prefix = ""
    full = ".".join(p for p in (prefix, node.module or "") if p)
    if full in mod_names:
        # `from X import Y`: Y is either a submodule of X, or a name defined
        # in X. Depend on the submodule when it exists, on X itself otherwise.
        for alias in node.names:
            child = f"{full}.{alias.name}"
            if child in mod_names:
                yield child
            else:
                yield full
    else:
        for alias in node.names:
            child = f"{full}.{alias.name}"
            if child in mod_names:
                yield child


def _build_edges(modules: dict[str, Path]) -> set[tuple[str, str]]:
    mod_names = set(modules)
    edges: set[tuple[str, str]] = set()
    for mod, path in modules.items():
        tree = ast.parse(path.read_text())
        is_pkg_init = path.name == "__init__.py"
        for node in ast.walk(tree):
            if isinstance(node, (ast.Import, ast.ImportFrom)):
                for dst in _sibling_imports(node, mod, is_pkg_init, mod_names):
                    if dst != mod:
                        edges.add((mod, dst))
    return edges


def _strongly_connected_components(nodes: set[str], edges: set[tuple[str, str]]) -> list[set[str]]:
    """Iterative Tarjan."""
    graph: dict[str, list[str]] = {n: [] for n in nodes}
    for src, dst in edges:
        graph[src].append(dst)

    counter = 0
    index: dict[str, int] = {}
    lowlink: dict[str, int] = {}
    on_stack: dict[str, bool] = {}
    stack: list[str] = []
    sccs: list[set[str]] = []

    for root in sorted(nodes):
        if root in index:
            continue
        work = [(root, 0)]
        while work:
            node, next_child = work[-1]
            if next_child == 0:
                index[node] = lowlink[node] = counter
                counter += 1
                stack.append(node)
                on_stack[node] = True
            descended = False
            children = graph[node]
            for i in range(next_child, len(children)):
                child = children[i]
                if child not in index:
                    work[-1] = (node, i + 1)
                    work.append((child, 0))
                    descended = True
                    break
                if on_stack.get(child):
                    lowlink[node] = min(lowlink[node], index[child])
            if descended:
                continue
            if lowlink[node] == index[node]:
                component = set()
                while True:
                    member = stack.pop()
                    on_stack[member] = False
                    component.add(member)
                    if member == node:
                        break
                sccs.append(component)
            work.pop()
            if work:
                parent = work[-1][0]
                lowlink[parent] = min(lowlink[parent], lowlink[node])
    return sccs


def _current_scc_members() -> set[str]:
    modules = _discover_modules()
    edges = _build_edges(modules)
    sccs = _strongly_connected_components(set(modules), edges)
    nontrivial = [s for s in sccs if len(s) > 1]
    return set().union(*nontrivial) if nontrivial else set()


def test_no_module_joins_the_import_scc():
    members = _current_scc_members()
    joined = members - KNOWN_SCC_MEMBERS
    assert not joined, (
        "New module(s) entered a strongly connected import component: "
        f"{sorted(joined)}. Do not add cycles; depend downward only "
        "(see test_import_graph.py for the target layering)."
    )


def test_scc_allowlist_is_ratcheted_down():
    members = _current_scc_members()
    stale = KNOWN_SCC_MEMBERS - members
    assert not stale, (
        "Module(s) no longer in the import SCC — lock in the progress by "
        f"removing them from KNOWN_SCC_MEMBERS: {sorted(stale)}"
    )
