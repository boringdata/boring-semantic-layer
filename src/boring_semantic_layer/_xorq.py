"""Single import point for the xorq surface used by BSL.

All BSL modules should import xorq symbols from this shim. If xorq renames
or moves something, only this file needs to change.

This shim does NOT replace the plain ``ibis`` package (PyPI ibis-framework).
BSL coexists with both flavors: use ``import ibis`` for the plain side, and
this module for the ``xorq.vendor.ibis`` side.

When xorq is not installed, this shim falls back to plain ``ibis-framework``
equivalents. Xorq-only features (``to_tagged``, ``from_tagged``, tagging,
caching) remain gated behind explicit ``ImportError`` checks in their
respective modules.
"""

from __future__ import annotations

try:
    import xorq.api as api
    from xorq.api import selectors
    from xorq.common.utils.graph_utils import to_node
    from xorq.common.utils.ibis_utils import from_ibis, map_ibis
    from xorq.common.utils.node_utils import replace_nodes, walk_nodes
    from xorq.expr.builders import TagHandler
    from xorq.expr.relations import CachedNode, Read, RemoteTable, Tag
    from xorq.vendor import ibis
    from xorq.vendor.ibis import _
    from xorq.vendor.ibis.backends.profiles import Profile
    from xorq.vendor.ibis.common.collections import FrozenDict, FrozenOrderedDict
    from xorq.vendor.ibis.common.deferred import (
        Attr,
        BinaryOperator,
        Call,
        Deferred,
        Item,
        Just,
        JustUnhashable,
        Mapping,
        Sequence,
        UnaryOperator,
        Variable,
    )
    from xorq.vendor.ibis.common.graph import Graph
    from xorq.vendor.ibis.config import Config
    from xorq.vendor.ibis.expr import operations, types
    from xorq.vendor.ibis.expr.format import fmt, render_fields
    from xorq.vendor.ibis.expr.operations import relations
    from xorq.vendor.ibis.expr.operations.core import Node
    from xorq.vendor.ibis.expr.operations.generic import Literal
    from xorq.vendor.ibis.expr.operations.relations import (
        DatabaseTable,
        Field,
        JoinChain,
        JoinReference,
    )
    from xorq.vendor.ibis.expr.operations.sortkeys import SortKey
    from xorq.vendor.ibis.expr.schema import Schema
    from xorq.vendor.ibis.expr.types import Expr, Table
    from xorq.vendor.ibis.expr.types.generic import Column
    from xorq.vendor.ibis.expr.types.groupby import GroupedTable

    HAS_XORQ = True

except ImportError:
    import ibis
    from ibis import selectors
    from ibis.common.collections import FrozenDict, FrozenOrderedDict
    from ibis.common.deferred import (
        Attr,
        BinaryOperator,
        Call,
        Deferred,
        Item,
        Just,
        JustUnhashable,
        Mapping,
        Sequence,
        UnaryOperator,
        Variable,
    )
    from ibis.common.graph import Graph
    from ibis.config import Config
    from ibis.expr import operations, types
    from ibis.expr.format import fmt, render_fields
    from ibis.expr.operations import relations
    from ibis.expr.operations.core import Node
    from ibis.expr.operations.generic import Literal
    from ibis.expr.operations.relations import (
        DatabaseTable,
        Field,
        JoinChain,
        JoinReference,
    )
    from ibis.expr.operations.sortkeys import SortKey
    from ibis.expr.schema import Schema
    from ibis.expr.types import Expr, Table
    from ibis.expr.types.generic import Column
    from ibis.expr.types.groupby import GroupedTable

    api = ibis
    _ = ibis._

    HAS_XORQ = False

    # Xorq-only symbols: None sentinels so isinstance checks return False and
    # call sites that are already gated (to_tagged, from_tagged, tag_handler)
    # will fail with a clear AttributeError rather than a confusing NameError.
    TagHandler = None
    CachedNode = None
    Read = None
    RemoteTable = None
    Tag = None
    Profile = None

    def from_ibis(table):
        """Identity: without xorq, plain-ibis tables stay as plain ibis."""
        return table

    class _MapIbisStub:
        """Stub for xorq's map_ibis singledispatch mechanism.

        Allows _patch_xorq_sortkey_compat() to register a handler without
        error; the handler body never runs because map_ibis() is only called
        by xorq internals during xorq-table conversion, which doesn't happen
        when xorq is absent.
        """

        def __init__(self):
            self.registry = {}

        def register(self, type_):
            def decorator(fn):
                self.registry[type_] = fn
                return fn

            return decorator

        def __call__(self, *args, **kwargs):
            raise ImportError(
                "xorq is required for map_ibis; "
                "install with: pip install 'boring-semantic-layer[xorq]'"
            )

    map_ibis = _MapIbisStub()

    def to_node(maybe_expr):
        """Convert an expression or node to a Node."""
        if isinstance(maybe_expr, Node):
            return maybe_expr
        if isinstance(maybe_expr, Expr):
            return maybe_expr.op()
        raise ValueError(
            f"Cannot convert {type(maybe_expr).__name__!r} to an expression node"
        )

    def walk_nodes(node_types, expr):
        """Walk the expression graph depth-first, yielding nodes of given types."""
        start = to_node(expr)
        visited = set()
        stack = [start]
        types_ = node_types if isinstance(node_types, tuple) else (node_types,)
        while stack:
            node = stack.pop()
            if node in visited:
                continue
            visited.add(node)
            if isinstance(node, types_):
                yield node
            children = getattr(node, "__children__", ())
            for child in children:
                try:
                    child_node = to_node(child)
                    if child_node not in visited:
                        stack.append(child_node)
                except (ValueError, AttributeError):
                    pass

    def replace_nodes(replacer, node):
        """Replace nodes in the expression tree using ibis Node.replace.

        The xorq replacer signature is ``(node, kwargs) -> node`` where kwargs
        is a dict of the node's arguments (possibly with already-replaced
        children). ibis's Node.replace passes ``None`` when no children were
        replaced; we normalise that to ``{}`` to match xorq's contract.
        """
        return node.replace(
            lambda n, kwargs: replacer(n, kwargs if kwargs is not None else {})
        )


__all__ = [
    "Attr",
    "BinaryOperator",
    "CachedNode",
    "Call",
    "Column",
    "Config",
    "DatabaseTable",
    "Deferred",
    "Expr",
    "Field",
    "FrozenDict",
    "FrozenOrderedDict",
    "Graph",
    "GroupedTable",
    "HAS_XORQ",
    "Item",
    "JoinChain",
    "JoinReference",
    "Just",
    "JustUnhashable",
    "Literal",
    "Mapping",
    "Node",
    "Profile",
    "Read",
    "RemoteTable",
    "Schema",
    "Sequence",
    "SortKey",
    "Table",
    "Tag",
    "TagHandler",
    "UnaryOperator",
    "Variable",
    "_",
    "api",
    "fmt",
    "from_ibis",
    "ibis",
    "map_ibis",
    "operations",
    "relations",
    "render_fields",
    "replace_nodes",
    "selectors",
    "to_node",
    "types",
    "walk_nodes",
]
