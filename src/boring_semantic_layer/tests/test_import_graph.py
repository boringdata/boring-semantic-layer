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

# Phase 3 emptied the knot: the import graph is a DAG. Nothing may join
# a strongly connected component again.
KNOWN_SCC_MEMBERS: frozenset[str] = frozenset()


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


def _is_type_checking_if(node: ast.AST) -> bool:
    """Match ``if TYPE_CHECKING:`` / ``if typing.TYPE_CHECKING:`` blocks."""
    if not isinstance(node, ast.If):
        return False
    test = node.test
    return (isinstance(test, ast.Name) and test.id == "TYPE_CHECKING") or (
        isinstance(test, ast.Attribute) and test.attr == "TYPE_CHECKING"
    )


def _build_edges(modules: dict[str, Path]) -> set[tuple[str, str]]:
    mod_names = set(modules)
    edges: set[tuple[str, str]] = set()
    for mod, path in modules.items():
        tree = ast.parse(path.read_text())
        is_pkg_init = path.name == "__init__.py"
        stack: list[ast.AST] = [tree]
        while stack:
            node = stack.pop()
            if _is_type_checking_if(node):
                # Type-only imports are not runtime edges; keep walking the
                # else-branch, which does execute.
                stack.extend(node.orelse)
                continue
            if isinstance(node, (ast.Import, ast.ImportFrom)):
                for dst in _sibling_imports(node, mod, is_pkg_init, mod_names):
                    if dst != mod:
                        edges.add((mod, dst))
            stack.extend(ast.iter_child_nodes(node))
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


# ---------------------------------------------------------------------------
# Layer contract
# ---------------------------------------------------------------------------

#: Module-prefix -> layer. Edges must point to an equal or lower layer.
#: Within-layer edges are unrestricted (global acyclicity is enforced above).
_LAYERS: dict[str, int] = {
    # 0: primitives — import nothing from the package but each other
    "_xorq": 0,
    "errors": 0,
    "fieldref": 0,
    "io": 0,
    "safe_eval": 0,
    "nested_access": 0,
    "predicate": 0,
    "config": 0,
    # 1: analysis/util modules over primitives
    "graph_utils": 1,
    "measure_scope": 1,
    "calc_analyzer": 1,
    "nested_compile": 1,
    "projection_utils": 1,
    "profile": 1,
    # 2: compilers-of-expressions
    "calc_compiler": 2,
    "convert": 2,
    # 3: the semantic ops + their compiler
    "ops": 3,
    # 4: user-facing expressions and repr
    "expr": 4,
    "format": 4,
    # 5: sugar and orchestration over expressions
    "api": 5,
    "query": 5,
    "yaml": 5,
    # 6: serialization of everything below
    "serialization": 6,
    # 7: optional extras and the root facade
    "chart": 7,
    "agents": 7,
    "server": 7,
    "<root>": 7,
}

_EXTRAS_PREFIXES = ("chart", "agents", "server")

#: Underscore-prefixed core modules extras may import: the ibis-flavor shim
#: is the designated package-wide import point for xorq symbols.
_EXTRAS_PRIVATE_MODULE_ALLOWLIST = frozenset({"_xorq"})


def _layer_of(module: str) -> int:
    short = module.removeprefix(PKG_NAME + ".") if module != PKG_NAME else "<root>"
    head = short.split(".", 1)[0]
    if head not in _LAYERS:
        raise AssertionError(
            f"Module {short!r} is not assigned to a layer — add it to _LAYERS "
            "in test_import_graph.py when introducing a new top-level module."
        )
    return _LAYERS[head]


def test_layer_contract():
    """Every import edge points downward (or stays within its layer)."""
    modules = _discover_modules()
    edges = _build_edges(modules)
    violations = sorted(
        f"{a.removeprefix(PKG_NAME + '.')} (L{_layer_of(a)}) -> "
        f"{b.removeprefix(PKG_NAME + '.')} (L{_layer_of(b)})"
        for a, b in edges
        if _layer_of(b) > _layer_of(a)
    )
    assert not violations, "Upward imports break the layer contract:\n" + "\n".join(violations)


def test_extras_do_not_import_core_privates():
    """chart/agents/server must use only the core public surface."""
    modules = _discover_modules()
    violations: list[str] = []
    for mod, path in modules.items():
        short = mod.removeprefix(PKG_NAME + ".")
        if not short.startswith(_EXTRAS_PREFIXES):
            continue
        tree = ast.parse(path.read_text())
        for node in ast.walk(tree):
            if not isinstance(node, ast.ImportFrom):
                continue
            target = node.module or ""
            if node.level == 0 and not (target == PKG_NAME or target.startswith(PKG_NAME + ".")):
                continue  # stdlib / third-party import
            absolute = target.removeprefix(PKG_NAME + ".") if target else ""
            # Imports within the extra's own subpackage are unrestricted.
            if node.level and node.level == 1:
                continue
            if absolute.startswith(_EXTRAS_PREFIXES):
                continue
            parts = [p for p in absolute.split(".") if p]
            private_module = any(
                p.startswith("_") and p not in _EXTRAS_PRIVATE_MODULE_ALLOWLIST for p in parts
            )
            private_names = [a.name for a in node.names if a.name.startswith("_") and a.name != "_"]
            if private_module or private_names:
                violations.append(
                    f"{short}: from {target or '.' * node.level} import "
                    f"{', '.join(a.name for a in node.names)}"
                )
    assert not violations, "Extras must import only the core public surface:\n" + "\n".join(
        sorted(violations)
    )
