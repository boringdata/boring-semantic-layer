from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from typing import Any

from attrs import frozen
from returns.result import Failure, Result, safe
from toolz.curried import get, pipe, valmap

from .utils import expr_to_ibis_string, ibis_string_to_expr

@frozen
class XorqModule:
    api: Any


def try_import_xorq() -> Result[XorqModule, ImportError]:
    @safe
    def do_import():
        from xorq import api

        return XorqModule(api=api)

    return do_import()


def serialize_dimensions(dimensions: Mapping[str, Any]) -> Result[dict, Exception]:
    @safe
    def do_serialize():
        dim_metadata = {}
        for name, dim in dimensions.items():
            expr_str = expr_to_ibis_string(dim.expr).value_or(None)

            dim_metadata[name] = {
                "description": dim.description,
                "is_time_dimension": dim.is_time_dimension,
                "smallest_time_grain": dim.smallest_time_grain,
                "expr": expr_str,
            }
        return dim_metadata

    return do_serialize()


def serialize_measures(measures: Mapping[str, Any]) -> Result[dict, Exception]:
    @safe
    def do_serialize():
        meas_metadata = {}
        for name, meas in measures.items():
            expr_str = expr_to_ibis_string(meas.expr).value_or(None)

            meas_metadata[name] = {
                "description": meas.description,
                "requires_unnest": list(meas.requires_unnest),
                "expr": expr_str,
            }
        return meas_metadata

    return do_serialize()


def serialize_predicate(predicate: Callable) -> Result[str, Exception]:
    # Unwrap _CallableWrapper if present
    from . import ops

    if isinstance(predicate, ops._CallableWrapper):
        predicate = predicate._fn

    return expr_to_ibis_string(predicate)


def to_xorq(semantic_expr):
    """Convert BSL expression to xorq expression with metadata tags.

    Converts the BSL expression to ibis first, then wraps with xorq tagging
    to preserve BSL operation metadata for reconstruction.

    Args:
        semantic_expr: BSL SemanticTable or expression

    Returns:
        Xorq table expression with BSL metadata preserved

    Raises:
        ImportError: If xorq is not installed
        Exception: If conversion fails

    Example:
        >>> from boring_semantic_layer import SemanticModel
        >>> model = SemanticModel(...)
        >>> xorq_expr = to_xorq(model)  # No .unwrap() needed!
        >>> # xorq_expr can now be used with xorq features
    """
    from . import expr as bsl_expr

    @safe
    def do_convert(xorq_mod: XorqModule):
        if isinstance(semantic_expr, bsl_expr.SemanticTable):
            op = semantic_expr.op()
        else:
            op = semantic_expr

        # Convert BSL -> ibis -> xorq (expression level, no execution)
        ibis_expr = bsl_expr.to_ibis(semantic_expr)

        from xorq.common.utils.ibis_utils import from_ibis
        from xorq.common.utils.node_utils import replace_nodes
        from xorq.vendor.ibis.expr.operations.relations import DatabaseTable
        import re

        xorq_table = from_ibis(ibis_expr)

        # Replace read_parquet temporary tables with deferred ReadOp for serialization
        def replace_read_parquet(node, _kwargs):
            if isinstance(node, DatabaseTable):
                table_name = node.name
                # Check if this is a temporary read_parquet table
                if table_name.startswith('ibis_read_parquet_'):
                    backend = node.source
                    # Extract the parquet path from the DuckDB view SQL
                    try:
                        views_df = backend.con.execute(
                            f"SELECT sql FROM duckdb_views() WHERE view_name = '{table_name}'"
                        ).fetchdf()
                        if len(views_df) > 0:
                            sql = views_df.iloc[0]['sql']
                            # Extract path from SQL like: read_parquet(main.list_value('path'))
                            match = re.search(r"list_value\(['\"](.*?)['\"]\)", sql)
                            if match:
                                path = match.group(1)
                                # Replace with deferred read
                                return xorq_mod.api.deferred_read_parquet(path).op()
                    except Exception:
                        # If extraction fails, leave the node as is
                        pass
            return node

        xorq_table = replace_nodes(replace_read_parquet, xorq_table).to_expr()

        metadata = _extract_op_metadata(op)
        tag_data = _metadata_to_hashable_dict(metadata)

        return xorq_table.tag(tag="bsl", **tag_data)

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


def _extract_op_metadata(op) -> dict[str, Any]:
    """Extract metadata from BSL operation.

    Args:
        op: BSL operation (SemanticTableOp, SemanticFilterOp, etc.)

    Returns:
        Dict with operation metadata
    """
    from . import ops
    from .ops import _unwrap

    op_type = type(op).__name__

    metadata = {
        "bsl_op_type": op_type,
        "bsl_version": "1.0",
    }

    # Extract operation-specific metadata
    if isinstance(op, ops.SemanticTableOp):
        dims_result = serialize_dimensions(op.get_dimensions())
        meas_result = serialize_measures(op.get_measures())

        # Use value_or for functional style - only store if serialization succeeded
        metadata["dimensions"] = dims_result.value_or({})
        metadata["measures"] = meas_result.value_or({})
        if op.name:
            metadata["name"] = op.name

    elif isinstance(op, ops.SemanticFilterOp):
        pred_result = serialize_predicate(op.predicate)
        # Use value_or for functional style
        metadata["predicate"] = pred_result.value_or("")

    elif isinstance(op, ops.SemanticGroupByOp):
        if op.keys:
            metadata["keys"] = list(op.keys)

    elif isinstance(op, ops.SemanticAggregateOp):
        if op.keys:
            metadata["by"] = list(op.keys)
        # Serialize aggregation functions
        if op.aggs:
            agg_metadata = {}
            for name, fn in op.aggs.items():
                unwrapped = _unwrap(fn) if hasattr(fn, '_fn') else fn
                expr_str = expr_to_ibis_string(unwrapped).value_or(None)
                if expr_str:
                    agg_metadata[name] = expr_str
            metadata["aggs"] = agg_metadata

    elif isinstance(op, ops.SemanticMutateOp):
        if op.post:
            post_metadata = {}
            for name, fn in op.post.items():
                expr_str = expr_to_ibis_string(fn).value_or(None)
                if expr_str:
                    post_metadata[name] = expr_str
            metadata["post"] = post_metadata

    elif isinstance(op, ops.SemanticProjectOp):
        if op.fields:
            metadata["fields"] = list(op.fields)

    elif isinstance(op, ops.SemanticLimitOp):
        metadata["n"] = op.n
        metadata["offset"] = op.offset

    elif isinstance(op, ops.SemanticOrderByOp):
        order_keys = []
        for key in op.keys:
            if isinstance(key, str):
                order_keys.append({"type": "string", "value": key})
            else:
                # Unwrap _CallableWrapper if present
                unwrapped = _unwrap(key) if hasattr(key, '_fn') else key
                expr_str = expr_to_ibis_string(unwrapped).value_or(None)
                if expr_str:
                    order_keys.append({"type": "callable", "value": expr_str})
        metadata["order_keys"] = order_keys

    # Add source operation metadata recursively (all Relation ops have source)
    try:
        source_metadata = _extract_op_metadata(op.source)
        metadata["source"] = source_metadata
    except AttributeError:
        pass

    return metadata


def _metadata_to_hashable_dict(metadata: dict[str, Any]) -> dict[str, str]:
    """Convert metadata to hashable dict for xorq tagging.

    Xorq tags require all values to be hashable (strings, ints, etc).
    Converts complex structures to JSON strings.

    Args:
        metadata: Metadata dict

    Returns:
        Dict with string keys and hashable string values
    """
    hashable = {}
    for key, value in metadata.items():
        if isinstance(value, str | int | float | bool | type(None)):
            hashable[key] = value
        else:
            # Convert to JSON string to ensure hashability
            hashable[key] = json.dumps(value) if value is not None else ""

    return hashable


# ==============================================================================
# Xorq to BSL Conversion
# ==============================================================================


def from_xorq(xorq_expr):
    """Reconstruct BSL expression from tagged xorq expression.

    Extracts BSL metadata from xorq tags and reconstructs the original
    BSL operation chain.

    Args:
        xorq_expr: Xorq expression with BSL metadata tags

    Returns:
        BSL expression reconstructed from metadata

    Raises:
        ValueError: If no BSL metadata found in xorq expression
        Exception: If reconstruction fails

    Example:
        >>> xorq_expr = ...  # Tagged xorq expression
        >>> bsl_expr = from_xorq(xorq_expr)  # No .unwrap() needed!
        >>> # Use bsl_expr normally
    """

    @safe
    def do_convert():
        metadata = _extract_xorq_metadata(xorq_expr)

        if not metadata:
            raise ValueError("No BSL metadata found in xorq expression")

        return _reconstruct_bsl_operation(metadata, xorq_expr)

    result = do_convert()

    if isinstance(result, Failure):
        raise result.failure()

    return result.value_or(None)


# ==============================================================================
# Reconstruction helpers
# ==============================================================================


def _parse_json_field(metadata: dict, field: str) -> dict:
    """Parse a metadata field that might be JSON string or dict."""
    value = metadata.get(field)
    if not value:
        return {}
    if isinstance(value, str):
        return json.loads(value)
    return dict(value) if value else {}


def _deserialize_expr(expr_str: str | None, fallback_name: str | None = None) -> Callable:
    """Deserialize expression string to callable, with fallback."""
    if not expr_str:
        return lambda t, n=fallback_name: t[n] if n else t  # noqa: E731

    result = ibis_string_to_expr(expr_str)
    return result.value_or(lambda t, n=fallback_name: t[n] if n else t)  # noqa: E731


def _extract_source_metadata(metadata: dict) -> dict | None:
    """Extract and parse source metadata if present."""
    if "source" not in metadata or not metadata["source"]:
        return None
    return _parse_json_field(metadata, "source")


# ==============================================================================
# Metadata extraction
# ==============================================================================


# ==============================================================================
# Reconstruction helpers
# ==============================================================================


def _parse_json_field(metadata: dict, field: str) -> dict | list:
    """Parse a metadata field that might be JSON string or dict/list."""
    value = metadata.get(field)
    if not value:
        return {} if field != "order_keys" else []
    if isinstance(value, str):
        return json.loads(value)
    return dict(value) if isinstance(value, dict) else list(value) if isinstance(value, list) else {}


def _deserialize_expr(expr_str: str | None, fallback_name: str | None = None) -> Callable:
    """Deserialize expression string to callable, with fallback."""
    if not expr_str:
        return lambda t, n=fallback_name: t[n] if n else t  # noqa: E731

    result = ibis_string_to_expr(expr_str)
    return result.value_or(lambda t, n=fallback_name: t[n] if n else t)  # noqa: E731


# ==============================================================================
# Operation type registry (for singledispatch pattern)
# ==============================================================================

# We'll use a simple dict-based dispatch instead of singledispatch
# since we're dispatching on strings, not types
_RECONSTRUCTORS = {}


def _register_reconstructor(op_type: str):
    """Decorator to register a reconstructor for an operation type."""
    def decorator(func):
        _RECONSTRUCTORS[op_type] = func
        return func
    return decorator


# ==============================================================================
# Metadata extraction
# ==============================================================================


def _extract_xorq_metadata(xorq_expr) -> dict[str, Any] | None:
    """Extract BSL metadata from xorq expression tags.

    Args:
        xorq_expr: Xorq expression potentially with BSL tags

    Returns:
        Dict with BSL metadata or None if no tags found
    """
    @safe
    def get_op(expr):
        return expr.op()

    @safe
    def get_parent_expr(op):
        return op.parent.to_expr()

    def is_bsl_tag(op) -> bool:
        return type(op).__name__ == "Tag" and "bsl_op_type" in getattr(op, "metadata", {})

    # Try to get operation from expression
    maybe_op = get_op(xorq_expr).map(lambda op: op if is_bsl_tag(op) else None)

    # If we found a BSL tag, return its metadata
    if maybe_op.value_or(None):
        return dict(maybe_op.unwrap().metadata)

    # Otherwise, recursively check parent
    parent_result = get_op(xorq_expr).bind(get_parent_expr)
    if isinstance(parent_result, Failure):
        return None

    return _extract_xorq_metadata(parent_result.unwrap())


# ==============================================================================
# Operation reconstructors (registered by type)
# ==============================================================================


@_register_reconstructor("SemanticTableOp")
def _reconstruct_semantic_table(metadata: dict, xorq_expr, source):
    """Reconstruct SemanticTableOp from metadata."""
    from . import expr as bsl_expr
    from . import ops

    def _create_dimension(name: str, dim_data: dict) -> ops.Dimension:
        """Create Dimension from metadata dict."""
        return ops.Dimension(
            expr=_deserialize_expr(dim_data.get("expr"), fallback_name=name),
            description=dim_data.get("description"),
            is_time_dimension=dim_data.get("is_time_dimension", False),
            smallest_time_grain=dim_data.get("smallest_time_grain"),
        )

    def _create_measure(name: str, meas_data: dict) -> ops.Measure:
        """Create Measure from metadata dict."""
        return ops.Measure(
            expr=_deserialize_expr(meas_data.get("expr"), fallback_name=name),
            description=meas_data.get("description"),
            requires_unnest=tuple(meas_data.get("requires_unnest", [])),
        )

    def _load_from_read_op(read_op):
        """Load table from xorq Read operation."""
        import ibis
        import pandas as pd

        read_kwargs = read_op.args[4] if len(read_op.args) > 4 else None
        if not (read_kwargs and isinstance(read_kwargs, tuple)):
            return None

        path = next((v for k, v in read_kwargs if k in ('path', 'source_list')), None)
        return ibis.memtable(pd.read_parquet(path)) if path else None

    def _load_from_in_memory(op):
        """Load table from xorq InMemoryTable."""
        import ibis

        proxy = op.args[2]
        return ibis.memtable(proxy.to_frame())

    def _load_from_db_table(op):
        """Load table from xorq DatabaseTable."""
        import ibis

        table_name, xorq_backend = op.args[0], op.args[2]
        backend_class = getattr(ibis, xorq_backend.name)
        external_backend = backend_class.from_connection(xorq_backend.con)
        return external_backend.table(table_name)

    def _materialize_table():
        """Materialize ibis table from xorq expression."""
        import ibis
        from xorq.common.utils.graph_utils import walk_nodes
        from xorq.vendor.ibis.expr.operations import relations as xorq_rel
        from xorq.expr.relations import Read

        # Find underlying table operations
        read_ops = list(walk_nodes((Read,), xorq_expr))
        in_memory_tables = list(walk_nodes((xorq_rel.InMemoryTable,), xorq_expr))
        db_tables = list(walk_nodes((xorq_rel.DatabaseTable,), xorq_expr))

        # Try each loading strategy in order
        if read_ops:
            table = _load_from_read_op(read_ops[0])
            if table is not None:
                return table
        if in_memory_tables:
            return _load_from_in_memory(in_memory_tables[0])
        if db_tables:
            return _load_from_db_table(db_tables[0])

        # Fallback: materialize entire expression
        return ibis.memtable(xorq_expr.to_pandas())

    # Reconstruct dimensions and measures using dict comprehension + toolz
    dim_meta = _parse_json_field(metadata, "dimensions")
    meas_meta = _parse_json_field(metadata, "measures")

    dimensions = {name: _create_dimension(name, data) for name, data in dim_meta.items()}
    measures = {name: _create_measure(name, data) for name, data in meas_meta.items()}

    return bsl_expr.SemanticModel(
        table=_materialize_table(),
        dimensions=dimensions,
        measures=measures,
        name=metadata.get("name"),
    )


@_register_reconstructor("SemanticFilterOp")
def _reconstruct_filter(metadata: dict, xorq_expr, source):
    """Reconstruct SemanticFilterOp from metadata."""
    if source is None:
        raise ValueError("SemanticFilterOp requires source")

    predicate = _deserialize_expr(metadata.get("predicate"), fallback_name=None)
    return source.filter(predicate)


@_register_reconstructor("SemanticGroupByOp")
def _reconstruct_group_by(metadata: dict, xorq_expr, source):
    """Reconstruct SemanticGroupByOp from metadata."""
    if source is None:
        raise ValueError("SemanticGroupByOp requires source")

    keys = tuple(_parse_json_field(metadata, "keys")) or ()
    return source.group_by(*keys) if keys else source


@_register_reconstructor("SemanticAggregateOp")
def _reconstruct_aggregate(metadata: dict, xorq_expr, source):
    """Reconstruct SemanticAggregateOp from metadata."""
    if source is None:
        raise ValueError("SemanticAggregateOp requires source")

    aggs_meta = _parse_json_field(metadata, "aggs")
    return source.aggregate(*aggs_meta.keys()) if aggs_meta else source


@_register_reconstructor("SemanticMutateOp")
def _reconstruct_mutate(metadata: dict, xorq_expr, source):
    """Reconstruct SemanticMutateOp from metadata."""
    if source is None:
        raise ValueError("SemanticMutateOp requires source")

    post_meta = _parse_json_field(metadata, "post")
    if not post_meta:
        return source

    post_callables = {
        name: _deserialize_expr(expr_str, fallback_name=name)
        for name, expr_str in post_meta.items()
    }
    return source.mutate(**post_callables)


@_register_reconstructor("SemanticProjectOp")
def _reconstruct_project(metadata: dict, xorq_expr, source):
    """Reconstruct SemanticProjectOp from metadata."""
    if source is None:
        raise ValueError("SemanticProjectOp requires source")

    fields = tuple(_parse_json_field(metadata, "fields")) or ()
    return source.select(*fields) if fields else source


@_register_reconstructor("SemanticOrderByOp")
def _reconstruct_order_by(metadata: dict, xorq_expr, source):
    """Reconstruct SemanticOrderByOp from metadata."""
    if source is None:
        raise ValueError("SemanticOrderByOp requires source")

    def _deserialize_key(key_meta: dict):
        """Deserialize a single order key from metadata."""
        return (
            key_meta["value"]
            if key_meta["type"] == "string"
            else _deserialize_expr(key_meta["value"])
        )

    order_keys_meta = _parse_json_field(metadata, "order_keys")
    if not order_keys_meta:
        return source

    keys = [_deserialize_key(key_meta) for key_meta in order_keys_meta]
    return source.order_by(*keys) if keys else source


@_register_reconstructor("SemanticLimitOp")
def _reconstruct_limit(metadata: dict, xorq_expr, source):
    """Reconstruct SemanticLimitOp from metadata."""
    if source is None:
        raise ValueError("SemanticLimitOp requires source")

    return source.limit(
        n=int(metadata.get("n", 0)),
        offset=int(metadata.get("offset", 0)),
    )


def _reconstruct_bsl_operation(metadata: dict[str, Any], xorq_expr):
    """Reconstruct BSL operation from metadata.

    Args:
        metadata: Extracted BSL metadata
        xorq_expr: Original xorq expression

    Returns:
        Reconstructed BSL expression
    """
    op_type = metadata.get("bsl_op_type")

    # Reconstruct source operation first if present
    source = None
    source_metadata = _parse_json_field(metadata, "source")
    if source_metadata:
        source = _reconstruct_bsl_operation(source_metadata, xorq_expr)

    # Dispatch to appropriate reconstructor
    reconstructor = _RECONSTRUCTORS.get(op_type)
    if not reconstructor:
        raise ValueError(f"Unknown BSL operation type: {op_type}")

    return reconstructor(metadata, xorq_expr, source)


# ==============================================================================
# Public API
# ==============================================================================

__all__ = [
    "to_xorq",
    "from_xorq",
    "try_import_xorq",
    "XorqModule",
]
