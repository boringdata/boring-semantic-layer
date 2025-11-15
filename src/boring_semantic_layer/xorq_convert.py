"""Xorq conversion module for BSL semantic layer.

Provides bidirectional conversion between BSL expressions and xorq expressions,
preserving semantic metadata through xorq's tagging mechanism.

This module is only available when xorq is installed. All functions return
Result types to handle optional dependencies gracefully.

Architecture:
- Immutable data structures using @frozen
- Result types for all fallible operations
- Optional import handling
- Functional composition
- No side effects
"""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from typing import Any

from attrs import frozen
from returns.result import Failure, Result, safe

# ==============================================================================
# Optional Import Handling
# ==============================================================================


@frozen
class XorqModule:
    """Immutable wrapper for optional xorq module."""

    api: Any  # xorq.api module


def try_import_xorq() -> Result[XorqModule, ImportError]:
    """Attempt to import xorq module.

    Returns:
        Success(XorqModule) if xorq is installed
        Failure(ImportError) if xorq is not available
    """

    @safe
    def do_import():
        from xorq import api

        return XorqModule(api=api)

    return do_import()


def try_import_cloudpickle() -> Result[Any, ImportError]:
    """Attempt to import cloudpickle for callable serialization.

    Returns:
        Success(cloudpickle module) if available
        Failure(ImportError) otherwise
    """

    @safe
    def do_import():
        import cloudpickle

        return cloudpickle

    return do_import()


# ==============================================================================
# Callable Serialization
# ==============================================================================


def serialize_callable(fn: Callable) -> Result[str, Exception]:
    """Serialize callable using cloudpickle and base64 encoding.

    Args:
        fn: Callable to serialize

    Returns:
        Success(base64 string) with pickled callable
        Failure(Exception) if serialization fails
    """
    import base64

    def do_pickle(cloudpickle):
        pickled_bytes = cloudpickle.dumps(fn)
        return base64.b64encode(pickled_bytes).decode("ascii")

    return try_import_cloudpickle().bind(lambda cp: safe(lambda: do_pickle(cp))())


def deserialize_callable(pickled_str: str) -> Result[Callable, Exception]:
    """Deserialize callable from base64-encoded cloudpickle string.

    Args:
        pickled_str: Base64-encoded pickled callable

    Returns:
        Success(Callable) if deserialization succeeds
        Failure(Exception) otherwise
    """
    import base64

    def do_unpickle(cloudpickle):
        pickled_bytes = base64.b64decode(pickled_str.encode("ascii"))
        return cloudpickle.loads(pickled_bytes)

    return try_import_cloudpickle().bind(lambda cp: safe(lambda: do_unpickle(cp))())


# ==============================================================================
# Metadata Serialization for BSL Operations
# ==============================================================================


def serialize_dimensions(dimensions: Mapping[str, Any]) -> Result[str, Exception]:
    """Serialize dimensions dict to JSON with pickled callables.

    Stores dimension metadata including the pickled callable for full reconstruction.

    Args:
        dimensions: Mapping of dimension name to Dimension object

    Returns:
        Success(JSON string) with dimension metadata and pickled callables
        Failure(Exception) if serialization fails
    """

    @safe
    def do_serialize():
        # Store dimension metadata including pickled callable
        dim_metadata = {}
        for name, dim in dimensions.items():
            pickled_expr = serialize_callable(dim.expr).value_or(None)

            dim_metadata[name] = {
                "description": dim.description,
                "is_time_dimension": dim.is_time_dimension,
                "smallest_time_grain": dim.smallest_time_grain,
                "expr_pickled": pickled_expr,
            }
        return json.dumps(dim_metadata)

    return do_serialize()


def serialize_measures(measures: Mapping[str, Any]) -> Result[str, Exception]:
    """Serialize measures dict to JSON with pickled callables.

    Stores measure metadata including the pickled callable for full reconstruction.

    Args:
        measures: Mapping of measure name to Measure object

    Returns:
        Success(JSON string) with measure metadata and pickled callables
        Failure(Exception) if serialization fails
    """

    @safe
    def do_serialize():
        # Store measure metadata including pickled callable
        meas_metadata = {}
        for name, meas in measures.items():
            pickled_expr = serialize_callable(meas.expr).value_or(None)

            meas_metadata[name] = {
                "description": meas.description,
                "requires_unnest": meas.requires_unnest,
                "expr_pickled": pickled_expr,
            }
        return json.dumps(meas_metadata)

    return do_serialize()


def serialize_predicate(predicate: Callable) -> Result[str, Exception]:
    """Serialize filter predicate using cloudpickle.

    Args:
        predicate: Filter predicate callable

    Returns:
        Success(base64 string) with pickled predicate
        Failure(Exception) if serialization fails
    """
    return serialize_callable(predicate)


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

        xorq_table = from_ibis(ibis_expr)
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
        metadata["dimensions"] = dims_result.value_or("")
        metadata["measures"] = meas_result.value_or("")
        if op.name:
            metadata["name"] = op.name

    elif isinstance(op, ops.SemanticFilterOp):
        pred_result = serialize_predicate(op.predicate)
        # Use value_or for functional style
        metadata["predicate"] = pred_result.value_or("")

    elif isinstance(op, ops.SemanticGroupByOp):
        if op.keys:
            metadata["keys"] = json.dumps(list(op.keys))

    elif isinstance(op, ops.SemanticAggregateOp):
        if op.keys:
            metadata["by"] = json.dumps(list(op.keys))

    elif isinstance(op, ops.SemanticMutateOp):
        if op.post:
            # Serialize the mutation callables
            post_metadata = {}
            for name, fn in op.post.items():
                pickled = serialize_callable(fn).value_or(None)
                if pickled:
                    post_metadata[name] = pickled
            metadata["post"] = json.dumps(post_metadata)

    elif isinstance(op, ops.SemanticProjectOp):
        if op.fields:
            metadata["fields"] = json.dumps(list(op.fields))

    elif isinstance(op, ops.SemanticLimitOp):
        metadata["n"] = str(op.n)
        metadata["offset"] = str(op.offset)

    elif isinstance(op, ops.SemanticOrderByOp):
        # Serialize order by keys (can be strings or callables)
        order_keys = []
        for key in op.keys:
            if isinstance(key, str):
                order_keys.append({"type": "string", "value": key})
            else:
                # It's a callable wrapper - serialize it
                pickled = serialize_callable(key).value_or(None)
                if pickled:
                    order_keys.append({"type": "callable", "value": pickled})
                else:
                    # Fallback: skip this key
                    pass
        metadata["order_keys"] = json.dumps(order_keys)

    # Add source operation metadata recursively (all Relation ops have source)
    try:
        source_metadata = _extract_op_metadata(op.source)
        metadata["source"] = json.dumps(source_metadata)
    except AttributeError:
        pass

    return metadata


def _metadata_to_hashable_dict(metadata: dict[str, Any]) -> dict[str, str]:
    """Convert metadata to hashable dict for xorq tagging.

    Xorq tags require all values to be hashable (strings, ints, etc).

    Args:
        metadata: Metadata dict

    Returns:
        Dict with string keys and hashable values
    """
    hashable = {}
    for key, value in metadata.items():
        if isinstance(value, str | int | float | bool | type(None)):
            hashable[key] = value
        else:
            # Convert to JSON string
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
        # Extract metadata from xorq tags
        metadata = _extract_xorq_metadata(xorq_expr)

        if not metadata:
            raise ValueError("No BSL metadata found in xorq expression")

        # Reconstruct BSL operation from metadata
        return _reconstruct_bsl_operation(metadata, xorq_expr)

    # Internal use of Result types, but unwrap for user-facing API
    result = do_convert()

    # Match the pattern of to_ibis() - unwrap internally
    if isinstance(result, Failure):
        raise result.failure()

    return result.value_or(None)


def _extract_xorq_metadata(xorq_expr) -> dict[str, Any] | None:
    """Extract BSL metadata from xorq expression tags.

    Args:
        xorq_expr: Xorq expression potentially with BSL tags

    Returns:
        Dict with BSL metadata or None if no tags found
    """
    try:
        op = xorq_expr.op()
    except AttributeError:
        return None

    # Check if this is a Tag operation
    if type(op).__name__ == "Tag":
        tag_metadata = dict(op.metadata)
        if "bsl_op_type" in tag_metadata:
            return tag_metadata

    # Check parent if available
    try:
        return _extract_xorq_metadata(op.parent.to_expr())
    except AttributeError:
        return None


def _reconstruct_bsl_operation(metadata: dict[str, Any], xorq_expr):
    """Reconstruct BSL operation from metadata.

    Args:
        metadata: Extracted BSL metadata
        xorq_expr: Original xorq expression

    Returns:
        Reconstructed BSL expression
    """
    from . import expr as bsl_expr
    from . import ops

    op_type = metadata.get("bsl_op_type")

    # Reconstruct source operation first if present
    source = None
    if "source" in metadata and metadata["source"]:
        source_metadata = json.loads(metadata["source"])
        source = _reconstruct_bsl_operation(source_metadata, xorq_expr)

    # Reconstruct based on operation type
    if op_type == "SemanticTableOp":
        # Get dimensions and measures from metadata
        dimensions = {}
        measures = {}

        if "dimensions" in metadata:
            dim_meta = json.loads(metadata["dimensions"])
            # Reconstruct dimensions with unpickled callables
            for name, dim_data in dim_meta.items():
                # Try to unpickle the callable - use value_or for functional style
                pickled_expr = dim_data.get("expr_pickled")
                if pickled_expr:
                    expr_result = deserialize_callable(pickled_expr)
                    # Use value_or to provide fallback
                    expr = expr_result.value_or(lambda t, n=name: t[n])  # noqa: E731
                else:
                    expr = lambda t, n=name: t[n]  # noqa: E731  # Fallback to column accessor

                dimensions[name] = ops.Dimension(
                    expr=expr,
                    description=dim_data.get("description"),
                    is_time_dimension=dim_data.get("is_time_dimension", False),
                    smallest_time_grain=dim_data.get("smallest_time_grain"),
                )

        if "measures" in metadata:
            meas_meta = json.loads(metadata["measures"])
            # Reconstruct measures with unpickled callables
            for name, meas_data in meas_meta.items():
                # Try to unpickle the callable - use value_or for functional style
                pickled_expr = meas_data.get("expr_pickled")
                if pickled_expr:
                    expr_result = deserialize_callable(pickled_expr)
                    # Use value_or to provide fallback
                    expr = expr_result.value_or(lambda t, n=name: t[n])  # noqa: E731
                else:
                    expr = lambda t, n=name: t[n]  # noqa: E731  # Fallback to column accessor

                measures[name] = ops.Measure(
                    expr=expr,
                    description=meas_data.get("description"),
                    requires_unnest=tuple(meas_data.get("requires_unnest", [])),
                )

        import ibis
        from xorq.common.utils.graph_utils import walk_nodes
        from xorq.vendor.ibis.expr.operations import relations as xorq_rel

        # Find underlying table operation (unwraps Tag operations)
        in_memory_tables = list(walk_nodes((xorq_rel.InMemoryTable,), xorq_expr))
        db_tables = list(walk_nodes((xorq_rel.DatabaseTable,), xorq_expr))

        if in_memory_tables:
            # Extract pandas dataframe from xorq's proxy (already in memory)
            op = in_memory_tables[0]
            proxy = op.args[2]
            df = proxy.to_frame()
            ibis_table = ibis.memtable(df)
        elif db_tables:
            # Reference the same database table in external ibis
            op = db_tables[0]
            table_name = op.args[0]
            xorq_backend = op.args[2]

            # Get underlying connection and backend type
            underlying_con = xorq_backend.con
            backend_name = xorq_backend.name

            # Create external ibis backend from same connection
            backend_class = getattr(ibis, backend_name)
            external_backend = backend_class.from_connection(underlying_con)
            ibis_table = external_backend.table(table_name)
        else:
            # Fallback: materialize for complex expressions
            ibis_table = ibis.memtable(xorq_expr.to_pandas())

        return bsl_expr.SemanticModel(
            table=ibis_table,
            dimensions=dimensions,
            measures=measures,
            name=metadata.get("name"),
        )

    elif op_type == "SemanticFilterOp":
        if source is None:
            raise ValueError("SemanticFilterOp requires source")

        # Deserialize predicate - use value_or for functional style
        if "predicate" in metadata:
            pred_result = deserialize_callable(metadata["predicate"])
            predicate = pred_result.value_or(lambda t: t)  # noqa: E731
        else:
            predicate = lambda t: t  # noqa: E731

        return source.filter(predicate)

    elif op_type == "SemanticGroupByOp":
        if source is None:
            raise ValueError("SemanticGroupByOp requires source")

        # Get group by keys
        keys = ()
        if "keys" in metadata:
            keys = tuple(json.loads(metadata["keys"]))

        # Group by
        if keys:
            return source.group_by(*keys)
        else:
            return source

    elif op_type == "SemanticAggregateOp":
        if source is None:
            raise ValueError("SemanticAggregateOp requires source")

        # Get group by keys
        by = ()
        if "by" in metadata:
            by = tuple(json.loads(metadata["by"]))

        # Group by
        if by:
            grouped = source.group_by(*by)
            # Would need to aggregate, but we don't have measure info here
            # This is a limitation of current design
            return grouped
        else:
            return source

    elif op_type == "SemanticMutateOp":
        if source is None:
            raise ValueError("SemanticMutateOp requires source")

        # Deserialize mutation callables
        if "post" in metadata:
            post_meta = json.loads(metadata["post"])
            post_callables = {}
            for name, pickled in post_meta.items():
                result = deserialize_callable(pickled)
                post_callables[name] = result.value_or(lambda t, n=name: t[n])  # noqa: E731

            if post_callables:
                return source.mutate(**post_callables)

        # Fallback: return source as-is
        return source

    elif op_type == "SemanticProjectOp":
        if source is None:
            raise ValueError("SemanticProjectOp requires source")

        # Get fields to project
        if "fields" in metadata:
            fields = tuple(json.loads(metadata["fields"]))
            return source.select(*fields)

        return source

    elif op_type == "SemanticOrderByOp":
        if source is None:
            raise ValueError("SemanticOrderByOp requires source")

        # Deserialize order by keys
        if "order_keys" in metadata:
            order_keys_meta = json.loads(metadata["order_keys"])
            keys = []
            for key_meta in order_keys_meta:
                if key_meta["type"] == "string":
                    keys.append(key_meta["value"])
                elif key_meta["type"] == "callable":
                    result = deserialize_callable(key_meta["value"])
                    keys.append(result.value_or(lambda t: t))  # noqa: E731

            if keys:
                return source.order_by(*keys)

        return source

    elif op_type == "SemanticLimitOp":
        if source is None:
            raise ValueError("SemanticLimitOp requires source")

        n = int(metadata.get("n", 0))
        offset = int(metadata.get("offset", 0))
        return source.limit(n, offset=offset)

    else:
        raise ValueError(f"Unknown BSL operation type: {op_type}")


# ==============================================================================
# Public API
# ==============================================================================

__all__ = [
    "to_xorq",
    "from_xorq",
    "try_import_xorq",
    "XorqModule",
]
