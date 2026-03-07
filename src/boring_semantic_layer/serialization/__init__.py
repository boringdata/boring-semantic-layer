"""BSL serialization package — structured metadata extraction and reconstruction.

Public API:
    to_tagged     — serialize a BSL expression into xorq tagged metadata
    from_tagged   — reconstruct a BSL expression from tagged metadata
    BSLSerializationContext — configuration context for serialization
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from attrs import frozen
from returns.result import Failure, Result, Success, safe

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

        from xorq.common.utils.node_utils import replace_nodes
        from xorq.vendor.ibis.expr.operations.relations import DatabaseTable

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

        metadata = extract_op_tree(op, context)
        tag_data = {k: freeze(v) for k, v in metadata.items()}

        if aggregate_cache_storage is not None and isinstance(op, SemanticAggregateOp):
            xorq_table = xorq_table.cache(storage=aggregate_cache_storage)

        xorq_table = xorq_table.tag(tag="bsl", **tag_data)

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
        ValueError: If no BSL metadata found in expression
    """
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
