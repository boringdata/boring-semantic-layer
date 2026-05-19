"""Single import point for the xorq surface used by BSL.

All BSL modules should import xorq symbols from this shim. If xorq renames
or moves something, only this file needs to change.

This shim does NOT replace the plain ``ibis`` package (PyPI ibis-framework).
BSL coexists with both flavors: use ``import ibis`` for the plain side, and
this module for the ``xorq.vendor.ibis`` side.
"""

from __future__ import annotations

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
