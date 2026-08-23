"""BSL serialization package — structured metadata extraction and reconstruction.

Public API:
    to_tagged     — serialize a BSL expression into xorq tagged metadata
    from_tagged   — reconstruct a BSL expression from tagged metadata
    BSLSerializationContext — configuration context for serialization
"""

from __future__ import annotations

from typing import Any

from attrs import frozen
from returns.result import Failure, Result, safe

from ._trust import UntrustedCallableError, trust_callable_module  # noqa: F401
from .context import BSLSerializationContext
from .extract import (
    deserialize_calc_measures,
    extract_op_tree,
    serialize_calc_measures,
    serialize_dimensions,
    serialize_measures,
)
from .freeze import freeze
from .reconstruct import (
    extract_xorq_metadata,
    reconstruct_bsl_operation,
)

# ---------------------------------------------------------------------------
# xorq import helper
# ---------------------------------------------------------------------------


@frozen
class XorqModule:
    api: Any


def try_import_xorq() -> Result[XorqModule, ImportError]:
    @safe
    def do_import():
        from xorq import api

        return XorqModule(api=api)

    return do_import()


# ---------------------------------------------------------------------------
# to_tagged
# ---------------------------------------------------------------------------

#: Marker tag stamped on each leaf model's AUTHORED table expression inside
#: the lowered payload. Recovery reads the marked subtree back verbatim
#: instead of re-deriving the leaf from the lowered plan (base-relation
#: walking / join splitting), which discarded authored shaping and could not
#: see through aggregation lowering at all. metadata: {"tag": BSL_LEAF_TAG,
#: "leaf": <model name>}.
BSL_LEAF_TAG = "__bsl_leaf__"


def _collect_leaf_tables(op, out=None):
    """Map each leaf SemanticTableOp's name to its authored table op.

    Walks the SEMANTIC op tree (source/left/right chains; join wrappers
    descend into their _source_join). First declaration wins on a duplicated
    name — an ambiguous name cannot be marked meaningfully and falls back to
    heuristic recovery.
    """
    from .. import ops as bsl_ops

    if out is None:
        out = {}
    if op is None:
        return out
    if isinstance(op, bsl_ops.SemanticTableOp):
        source_join = getattr(op, "_source_join", None)
        if source_join is not None:
            return _collect_leaf_tables(source_join, out)
        name = getattr(op, "name", None)
        table = getattr(op, "table", None)
        if table is not None and hasattr(table, "op"):
            table = table.op()
        if name and table is not None and name not in out:
            out[name] = table
        return out
    for attr in ("source", "left", "right"):
        child = getattr(op, attr, None)
        if child is not None:
            _collect_leaf_tables(child, out)
    return out


def _mark_leaf_tables(xorq_table, leaf_tables):
    """Wrap every occurrence of an authored leaf table in a marker tag.

    The lowered expression embeds each leaf's table op structurally, so a
    node-equality rewrite finds them wherever lowering placed them — under
    rename projections, under pre-aggregation legs, under a query's
    Aggregate. A leaf whose table was itself rewritten by lowering (so no
    node matches) is simply left unmarked and recovers via the heuristics.
    """
    from .._xorq import replace_nodes

    by_op = {}
    for name, table in leaf_tables.items():
        by_op.setdefault(table, name)
    if not by_op:
        return xorq_table

    def replacer(node, _kwargs):
        # Plain-callable replacers must recreate the node from _kwargs
        # themselves — that is how child substitutions propagate upward
        # (see ibis graph._coerce_replacer; Pattern/Mapping replacers get
        # this for free, callables do not).
        rebuilt = node.__recreate__(_kwargs) if _kwargs else node
        name = by_op.get(node)
        if name is None:
            return rebuilt
        return rebuilt.to_expr().hashing_tag(tag=BSL_LEAF_TAG, leaf=name).op()

    return replace_nodes(replacer, xorq_table).to_expr()


def to_tagged(semantic_expr, aggregate_cache_storage=None):
    """Tag a BSL expression with serialized metadata.

    Takes a BSL semantic expression and tags it with serialized metadata
    (dimensions, measures, etc.) in xorq format. The tagged expression can
    later be reconstructed using from_tagged().

    Args:
        semantic_expr: BSL SemanticTable or expression
        aggregate_cache_storage: Optional xorq storage backend (ParquetStorage or
                                SourceStorage). If provided, automatically injects
                                .cache() at aggregation points for smart cube caching.

    Returns:
       xorq expression with BSL metadata tags
    """
    from .. import expr as bsl_expr
    from ..ops import SemanticAggregateOp

    context = BSLSerializationContext()

    @safe
    def do_convert(xorq_mod: XorqModule):
        if isinstance(semantic_expr, bsl_expr.SemanticTable):
            op = semantic_expr.op()
        else:
            op = semantic_expr

        ibis_expr = bsl_expr.to_untagged(semantic_expr)

        import re

        from .._xorq import DatabaseTable, replace_nodes

        xorq_table = ibis_expr

        def replace_read_parquet(node, _kwargs):
            if not isinstance(node, DatabaseTable):
                return node
            if not node.name.startswith("ibis_read_parquet_"):
                return node

            @safe
            def extract_path_from_view(table_name):
                backend = node.source
                query = "SELECT sql FROM duckdb_views() WHERE view_name = ?"
                views_df = backend.con.execute(query, [table_name]).fetchdf()
                if views_df.empty:
                    return None
                sql = views_df.iloc[0]["sql"]
                match = re.search(r"list_value\(['\"](.*?)['\"]\)", sql)
                return match.group(1) if match else None

            path_result = extract_path_from_view(node.name)
            if path := path_result.value_or(None):
                return xorq_mod.api.deferred_read_parquet(path).op()
            return node

        xorq_table = replace_nodes(replace_read_parquet, xorq_table).to_expr()
        xorq_table = _mark_leaf_tables(xorq_table, _collect_leaf_tables(op))

        metadata = extract_op_tree(op, context)
        tag_data = {k: freeze(v) for k, v in metadata.items()}

        if aggregate_cache_storage is not None and isinstance(op, SemanticAggregateOp):
            xorq_table = xorq_table.cache(storage=aggregate_cache_storage)

        xorq_table = xorq_table.hashing_tag(tag="bsl", **tag_data)

        return xorq_table

    result = try_import_xorq().bind(do_convert)

    if isinstance(result, Failure):
        error = result.failure()
        if isinstance(error, ImportError):
            raise ImportError(
                "Xorq conversion requires the 'xorq' optional dependency. "
                "Install with: pip install 'boring-semantic-layer[xorq]'"
            ) from error
        raise error

    return result.value_or(None)


# ---------------------------------------------------------------------------
# from_tagged
# ---------------------------------------------------------------------------


def from_tagged(tagged_expr, context: BSLSerializationContext | None = None):
    """Reconstruct BSL expression from tagged expression.

    Extracts BSL metadata from tags and reconstructs the original
    BSL operation chain.

    Args:
        tagged_expr: Expression with BSL metadata tags (created by to_tagged)
        context: Optional serialization context. Defaults to BSLSerializationContext().

    Returns:
        BSL expression reconstructed from metadata

    Raises:
        ImportError: If xorq is not installed
        ValueError: If no BSL metadata found in expression
    """
    result = try_import_xorq()
    if isinstance(result, Failure):
        error = result.failure()
        if isinstance(error, ImportError):
            raise ImportError(
                "Xorq conversion requires the 'xorq' optional dependency. "
                "Install with: pip install 'boring-semantic-layer[xorq]'"
            ) from error
        raise error

    if context is None:
        context = BSLSerializationContext()

    @safe
    def do_convert():
        metadata = extract_xorq_metadata(tagged_expr)

        if not metadata:
            raise ValueError("No BSL metadata found in tagged expression")

        return reconstruct_bsl_operation(metadata, tagged_expr, context)

    result = do_convert()

    if isinstance(result, Failure):
        raise result.failure()

    return result.value_or(None)


__all__ = [
    "BSLSerializationContext",
    "XorqModule",
    "deserialize_calc_measures",
    "from_tagged",
    "serialize_calc_measures",
    "serialize_dimensions",
    "serialize_measures",
    "to_tagged",
    "try_import_xorq",
]
