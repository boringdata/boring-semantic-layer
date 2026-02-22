from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any

from attrs import frozen
from returns.result import Failure, Result, safe
from xorq.ibis_yaml.common import deserialize_callable, serialize_callable


def _pickle_callable(fn) -> str:
    """Serialize a callable via xorq's cloudpickle-based serializer."""
    from .ops import _CallableWrapper

    if isinstance(fn, _CallableWrapper):
        fn = fn._fn
    return serialize_callable(fn)


def _unpickle_callable(data: str) -> Callable:
    """Deserialize a callable via xorq's cloudpickle-based deserializer."""
    return deserialize_callable(data)


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
# Serialization helpers
# ---------------------------------------------------------------------------


def _extract_simple_column_name(expr) -> str | None:
    """Extract column name from a simple Deferred like ``_.col_name``.

    Returns the column name string if the expression is a simple column access,
    or None if it requires pickle.
    """
    from .ops import _CallableWrapper, _is_deferred

    if isinstance(expr, _CallableWrapper):
        expr = expr._fn

    if not _is_deferred(expr):
        return None

    resolver = expr._resolver
    if type(resolver).__name__ != "Attr":
        return None

    if type(resolver.obj).__name__ != "Variable":
        return None

    name_resolver = resolver.name
    if type(name_resolver).__name__ != "Just":
        return None

    value = name_resolver.value
    return value if isinstance(value, str) else None


def serialize_dimensions(dimensions: Mapping[str, Any]) -> Result[dict, Exception]:
    @safe
    def do_serialize():
        dim_metadata = {}
        for name, dim in dimensions.items():
            entry = {
                "description": dim.description,
                "is_entity": dim.is_entity,
                "is_event_timestamp": dim.is_event_timestamp,
                "is_time_dimension": dim.is_time_dimension,
                "smallest_time_grain": dim.smallest_time_grain,
            }
            col_name = _extract_simple_column_name(dim.expr)
            if col_name is not None:
                entry["expr"] = col_name
            else:
                entry["expr_pickle"] = _pickle_callable(dim.expr)
            dim_metadata[name] = entry
        return dim_metadata

    return do_serialize()


def serialize_measures(measures: Mapping[str, Any]) -> Result[dict, Exception]:
    @safe
    def do_serialize():
        from returns.result import Success

        from .utils import expr_to_structured

        meas_metadata = {}
        for name, meas in measures.items():
            entry = {
                "description": meas.description,
                "requires_unnest": list(meas.requires_unnest),
            }
            original = getattr(meas, "original_expr", None)
            struct_result = expr_to_structured(original) if original is not None else None
            if struct_result is not None and isinstance(struct_result, Success):
                entry["expr_struct"] = struct_result.unwrap()
            else:
                entry["expr_pickle"] = _pickle_callable(meas.expr)
            meas_metadata[name] = entry
        return meas_metadata

    return do_serialize()


def serialize_calc_measures(calc_measures: Mapping[str, Any]) -> Result[dict, Exception]:
    @safe
    def do_serialize():
        from .measure_scope import AllOf, BinOp, MeasureRef, MethodCall

        def _serialize_calc_expr(expr):
            if isinstance(expr, MeasureRef):
                return ("measure_ref", expr.name)
            if isinstance(expr, AllOf):
                return ("all_of", _serialize_calc_expr(expr.ref))
            if isinstance(expr, MethodCall):
                return (
                    "method_call",
                    _serialize_calc_expr(expr.receiver),
                    expr.method,
                    tuple(expr.args),
                    tuple(expr.kwargs),
                )
            if isinstance(expr, BinOp):
                return ("calc_binop", expr.op, _serialize_calc_expr(expr.left), _serialize_calc_expr(expr.right))
            if isinstance(expr, int | float):
                return ("num", expr)
            return None

        result = {}
        for name, expr in calc_measures.items():
            serialized = _serialize_calc_expr(expr)
            if serialized is not None:
                result[name] = serialized
        return result

    return do_serialize()


def deserialize_calc_measures(calc_data: Mapping[str, Any]) -> dict[str, Any]:
    from .measure_scope import AllOf, BinOp, MeasureRef, MethodCall

    def _deserialize_calc_expr(data):
        if isinstance(data, int | float):
            return data
        tag = data[0]
        if tag == "measure_ref":
            return MeasureRef(data[1])
        if tag == "all_of":
            return AllOf(_deserialize_calc_expr(data[1]))
        if tag == "method_call":
            return MethodCall(
                receiver=_deserialize_calc_expr(data[1]),
                method=data[2],
                args=tuple(data[3]) if data[3] else (),
                kwargs=tuple(data[4]) if data[4] else (),
            )
        if tag == "calc_binop":
            return BinOp(data[1], _deserialize_calc_expr(data[2]), _deserialize_calc_expr(data[3]))
        if tag == "num":
            return data[1]
        raise ValueError(f"Unknown calc measure tag: {tag}")

    return {name: _deserialize_calc_expr(expr) for name, expr in calc_data.items()}


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

    Example:
        >>> from boring_semantic_layer import SemanticModel
        >>> model = SemanticModel(...)
        >>> tagged_expr = to_tagged(model)
    """
    from . import expr as bsl_expr
    from .ops import SemanticAggregateOp

    @safe
    def do_convert(xorq_mod: XorqModule):
        if isinstance(semantic_expr, bsl_expr.SemanticTable):
            op = semantic_expr.op()
        else:
            op = semantic_expr

        ibis_expr = bsl_expr.to_untagged(semantic_expr)

        import re

        from xorq.common.utils.ibis_utils import from_ibis
        from xorq.common.utils.node_utils import replace_nodes
        from xorq.vendor.ibis.expr.operations.relations import DatabaseTable

        xorq_table = from_ibis(ibis_expr)

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

        metadata = _extract_op_metadata(op)

        def _to_hashable(value):
            if isinstance(value, str | int | float | bool | type(None)):
                return value
            elif isinstance(value, dict):
                return tuple((k, _to_hashable(v)) for k, v in value.items())
            elif isinstance(value, list | tuple):
                return tuple(_to_hashable(item) for item in value)
            else:
                return str(value)

        tag_data = {k: _to_hashable(v) for k, v in metadata.items()}

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
# Extractors  (op → metadata dict)
# ---------------------------------------------------------------------------

_EXTRACTORS = {}


def _register_extractor(op_type: type):
    def decorator(func):
        _EXTRACTORS[op_type] = func
        return func

    return decorator


@_register_extractor("SemanticTableOp")
def _extract_semantic_table(op) -> dict[str, Any]:
    dims_result = serialize_dimensions(op.get_dimensions())
    meas_result = serialize_measures(op.get_measures())
    calc_result = serialize_calc_measures(op.get_calculated_measures())
    metadata = {
        "dimensions": dims_result.value_or({}),
        "measures": meas_result.value_or({}),
    }
    calc_data = calc_result.value_or({})
    if calc_data:
        metadata["calc_measures"] = calc_data
    if op.name:
        metadata["name"] = op.name
    return metadata


@_register_extractor("SemanticFilterOp")
def _extract_filter(op) -> dict[str, Any]:
    return {"predicate_pickle": _pickle_callable(op.predicate)}


@_register_extractor("SemanticGroupByOp")
def _extract_group_by(op) -> dict[str, Any]:
    return {"keys": list(op.keys)} if op.keys else {}


@_register_extractor("SemanticAggregateOp")
def _extract_aggregate(op) -> dict[str, Any]:
    metadata = {}
    if op.keys:
        metadata["by"] = list(op.keys)
    if op.aggs:
        metadata["aggs_pickle"] = {name: _pickle_callable(fn) for name, fn in op.aggs.items()}
    return metadata


@_register_extractor("SemanticMutateOp")
def _extract_mutate(op) -> dict[str, Any]:
    if not op.post:
        return {}
    return {"post_pickle": {name: _pickle_callable(fn) for name, fn in op.post.items()}}


@_register_extractor("SemanticProjectOp")
def _extract_project(op) -> dict[str, Any]:
    return {"fields": list(op.fields)} if op.fields else {}


@_register_extractor("SemanticLimitOp")
def _extract_limit(op) -> dict[str, Any]:
    return {"n": op.n, "offset": op.offset}


@_register_extractor("SemanticOrderByOp")
def _extract_order_by(op) -> dict[str, Any]:
    order_keys = []
    for key in op.keys:
        if isinstance(key, str):
            order_keys.append({"type": "string", "value": key})
        else:
            order_keys.append({"type": "callable", "value_pickle": _pickle_callable(key)})
    return {"order_keys": order_keys}


@_register_extractor("SemanticJoinOp")
def _extract_join(op) -> dict[str, Any]:
    metadata = {"how": op.how}
    if op.on is not None:
        metadata["on_pickle"] = _pickle_callable(op.on)
    return metadata


def _extract_op_metadata(op) -> dict[str, Any]:
    op_type = type(op).__name__
    metadata = {
        "bsl_op_type": op_type,
        "bsl_version": "1.0",
    }

    extractor = _EXTRACTORS.get(op_type)
    if extractor:
        metadata.update(extractor(op))

    @safe
    def extract_source():
        return _extract_op_metadata(op.source)

    @safe
    def extract_left():
        return _extract_op_metadata(op.left)

    @safe
    def extract_right():
        return _extract_op_metadata(op.right)

    if source_metadata := extract_source().value_or(None):
        metadata["source"] = source_metadata

    if left_metadata := extract_left().value_or(None):
        metadata["left"] = left_metadata

    if right_metadata := extract_right().value_or(None):
        metadata["right"] = right_metadata

    return metadata


# ---------------------------------------------------------------------------
# from_tagged
# ---------------------------------------------------------------------------


def from_tagged(tagged_expr):
    """Reconstruct BSL expression from tagged expression.

    Extracts BSL metadata from tags and reconstructs the original
    BSL operation chain.

    Args:
        tagged_expr: Expression with BSL metadata tags (created by to_tagged)

    Returns:
        BSL expression reconstructed from metadata

    Raises:
        ValueError: If no BSL metadata found in expression
        Exception: If reconstruction fails

    Example:
        >>> tagged_expr = to_tagged(model)
        >>> bsl_expr = from_tagged(tagged_expr)
    """

    @safe
    def do_convert():
        metadata = _extract_xorq_metadata(tagged_expr)

        if not metadata:
            raise ValueError("No BSL metadata found in tagged expression")

        return _reconstruct_bsl_operation(metadata, tagged_expr)

    result = do_convert()

    if isinstance(result, Failure):
        raise result.failure()

    return result.value_or(None)


# ---------------------------------------------------------------------------
# Tag metadata parsing helpers
# ---------------------------------------------------------------------------


def _parse_field(metadata: dict, field: str) -> dict | list:
    """Extract a field from tag metadata, converting frozen tuples back to mutable types.

    xorq's FrozenOrderedDict stores dicts as tuples-of-pairs and lists as tuples.
    This function reverses that transformation so reconstructors see plain dicts/lists.
    """
    value = metadata.get(field)
    if not value:
        return {} if field != "order_keys" else []

    def _tuple_to_mutable(obj):
        if isinstance(obj, tuple):
            if len(obj) == 0:
                return {}
            if all(
                isinstance(item, tuple) and len(item) == 2 and isinstance(item[0], str)
                for item in obj
            ):
                return {k: _tuple_to_mutable(v) for k, v in obj}
            else:
                return [_tuple_to_mutable(item) for item in obj]
        else:
            return obj

    return _tuple_to_mutable(value)


def _list_to_tuple(obj):
    """Recursively convert lists back to tuples (reverses _tuple_to_mutable for expr_struct)."""
    if isinstance(obj, list):
        return tuple(_list_to_tuple(item) for item in obj)
    if isinstance(obj, dict):
        return tuple((k, _list_to_tuple(v)) for k, v in obj.items())
    return obj


# ---------------------------------------------------------------------------
# Metadata extraction from xorq expressions
# ---------------------------------------------------------------------------


def _extract_xorq_metadata(xorq_expr) -> dict[str, Any] | None:
    from xorq.expr.relations import Tag

    @safe
    def get_op(expr):
        return expr.op()

    @safe
    def get_parent_expr(op):
        return op.parent.to_expr()

    def is_bsl_tag(op) -> bool:
        return isinstance(op, Tag) and "bsl_op_type" in op.metadata

    maybe_op = get_op(xorq_expr).map(lambda op: op if is_bsl_tag(op) else None)

    if bsl_op := maybe_op.value_or(None):
        return dict(bsl_op.metadata)

    parent_expr = get_op(xorq_expr).bind(get_parent_expr).value_or(None)
    if parent_expr is None:
        return None

    return _extract_xorq_metadata(parent_expr)


# ---------------------------------------------------------------------------
# Reconstructors  (metadata dict → BSL expression)
# ---------------------------------------------------------------------------

_RECONSTRUCTORS = {}


def _register_reconstructor(op_type: str):
    def decorator(func):
        _RECONSTRUCTORS[op_type] = func
        return func

    return decorator


@_register_reconstructor("SemanticTableOp")
def _reconstruct_semantic_table(metadata: dict, xorq_expr, source):
    from . import expr as bsl_expr
    from . import ops

    def _create_dimension(name: str, dim_data: dict) -> ops.Dimension:
        expr_col = dim_data.get("expr")
        expr_pickle = dim_data.get("expr_pickle")
        if expr_col is not None:
            expr = lambda t, c=expr_col: t[c]  # noqa: E731
        elif expr_pickle:
            expr = _unpickle_callable(expr_pickle)
        else:
            expr = lambda t, n=name: t[n]  # noqa: E731
        return ops.Dimension(
            expr=expr,
            description=dim_data.get("description"),
            is_entity=dim_data.get("is_entity", False),
            is_event_timestamp=dim_data.get("is_event_timestamp", False),
            is_time_dimension=dim_data.get("is_time_dimension", False),
            smallest_time_grain=dim_data.get("smallest_time_grain"),
        )

    def _create_measure(name: str, meas_data: dict) -> ops.Measure:
        from .utils import structured_to_expr

        expr_struct = meas_data.get("expr_struct")
        expr_pickle = meas_data.get("expr_pickle")
        if expr_struct is not None:
            result = structured_to_expr(_list_to_tuple(expr_struct))
            expr = result.value_or(None)
            if expr is None:
                raise ValueError(f"Measure '{name}': failed to deserialize expr_struct")
        elif expr_pickle:
            expr = _unpickle_callable(expr_pickle)
        else:
            raise ValueError(f"Measure '{name}' has no expr_struct or expr_pickle")
        return ops.Measure(
            expr=expr,
            description=meas_data.get("description"),
            requires_unnest=tuple(meas_data.get("requires_unnest", [])),
        )

    def _unwrap_cached_nodes(expr):
        """Unwrap CachedNode wrappers to get to the underlying expression.

        When aggregate_cache_storage is used, the expression is wrapped as:
        Tag(parent=CachedNode(parent=RemoteTable(args[3]=actual_computation)))
        """
        from xorq.expr.relations import CachedNode, RemoteTable, Tag

        op = expr.op()

        if isinstance(op, Tag):
            expr = op.parent.to_expr() if hasattr(op.parent, "to_expr") else op.parent
            op = expr.op()

        if isinstance(op, CachedNode):
            expr = op.parent
            op = expr.op()

        if isinstance(op, RemoteTable):
            expr = op.args[3]

        return expr

    def _reconstruct_table():
        from xorq.common.utils.graph_utils import walk_nodes
        from xorq.common.utils.ibis_utils import from_ibis
        from xorq.expr.relations import Read
        from xorq.vendor import ibis
        from xorq.vendor.ibis.expr.operations import relations as xorq_rel

        unwrapped_expr = _unwrap_cached_nodes(xorq_expr)

        read_ops = list(walk_nodes((Read,), unwrapped_expr))
        in_memory_tables = list(walk_nodes((xorq_rel.InMemoryTable,), unwrapped_expr))
        db_tables = list(walk_nodes((xorq_rel.DatabaseTable,), unwrapped_expr))

        total_leaf_tables = len(read_ops) + len(in_memory_tables) + len(db_tables)
        if total_leaf_tables > 1:
            expr = unwrapped_expr.to_expr() if hasattr(unwrapped_expr, "to_expr") else unwrapped_expr
            return from_ibis(expr) if not hasattr(expr.op(), "source") else expr

        if read_ops:
            read_op = read_ops[0]
            read_kwargs = read_op.args[4] if len(read_op.args) > 4 else None
            if read_kwargs and isinstance(read_kwargs, tuple):
                path = next((v for k, v in read_kwargs if k in ("path", "source_list")), None)
                if path:
                    import pandas as pd

                    return from_ibis(ibis.memtable(pd.read_parquet(path)))

        if in_memory_tables:
            proxy = in_memory_tables[0].args[2]
            return from_ibis(ibis.memtable(proxy.to_frame()))

        if db_tables:
            db_table = db_tables[0]
            table_name, xorq_backend = db_table.args[0], db_table.args[2]
            backend_class = getattr(ibis, xorq_backend.name)
            backend = backend_class.from_connection(xorq_backend.con)
            return from_ibis(backend.table(table_name))

        return xorq_expr.to_expr()

    dim_meta = _parse_field(metadata, "dimensions")
    meas_meta = _parse_field(metadata, "measures")
    calc_meta = _parse_field(metadata, "calc_measures")

    dimensions = {name: _create_dimension(name, data) for name, data in dim_meta.items()}
    measures = {name: _create_measure(name, data) for name, data in meas_meta.items()}
    calc_measures = deserialize_calc_measures(calc_meta) if calc_meta else {}

    return bsl_expr.SemanticModel(
        table=_reconstruct_table(),
        dimensions=dimensions,
        measures=measures,
        calc_measures=calc_measures,
        name=metadata.get("name"),
    )


@_register_reconstructor("SemanticFilterOp")
def _reconstruct_filter(metadata: dict, xorq_expr, source):
    if source is None:
        raise ValueError("SemanticFilterOp requires source")
    pickle_data = metadata.get("predicate_pickle")
    if not pickle_data:
        raise ValueError("SemanticFilterOp has no predicate_pickle")
    return source.filter(_unpickle_callable(pickle_data))


@_register_reconstructor("SemanticGroupByOp")
def _reconstruct_group_by(metadata: dict, xorq_expr, source):
    if source is None:
        raise ValueError("SemanticGroupByOp requires source")
    keys = tuple(_parse_field(metadata, "keys")) or ()
    return source.group_by(*keys) if keys else source


@_register_reconstructor("SemanticAggregateOp")
def _reconstruct_aggregate(metadata: dict, xorq_expr, source):
    if source is None:
        raise ValueError("SemanticAggregateOp requires source")
    aggs_pickle = _parse_field(metadata, "aggs_pickle")
    if not aggs_pickle:
        raise ValueError("SemanticAggregateOp has no aggs_pickle")
    return source.aggregate(*aggs_pickle.keys())


@_register_reconstructor("SemanticMutateOp")
def _reconstruct_mutate(metadata: dict, xorq_expr, source):
    if source is None:
        raise ValueError("SemanticMutateOp requires source")
    post_pickle = _parse_field(metadata, "post_pickle")
    if not post_pickle:
        return source
    return source.mutate(**{name: _unpickle_callable(data) for name, data in post_pickle.items()})


@_register_reconstructor("SemanticProjectOp")
def _reconstruct_project(metadata: dict, xorq_expr, source):
    if source is None:
        raise ValueError("SemanticProjectOp requires source")
    fields = tuple(_parse_field(metadata, "fields")) or ()
    return source.select(*fields) if fields else source


@_register_reconstructor("SemanticOrderByOp")
def _reconstruct_order_by(metadata: dict, xorq_expr, source):
    if source is None:
        raise ValueError("SemanticOrderByOp requires source")

    def _deserialize_key(key_meta: dict):
        if key_meta["type"] == "string":
            return key_meta["value"]
        pickle_data = key_meta.get("value_pickle")
        if not pickle_data:
            raise ValueError("Order-by callable key has no value_pickle")
        return _unpickle_callable(pickle_data)

    order_keys_meta = _parse_field(metadata, "order_keys")
    if not order_keys_meta:
        return source
    keys = [_deserialize_key(key_meta) for key_meta in order_keys_meta]
    return source.order_by(*keys) if keys else source


@_register_reconstructor("SemanticLimitOp")
def _reconstruct_limit(metadata: dict, xorq_expr, source):
    if source is None:
        raise ValueError("SemanticLimitOp requires source")
    return source.limit(n=int(metadata.get("n", 0)), offset=int(metadata.get("offset", 0)))


@_register_reconstructor("SemanticJoinOp")
def _reconstruct_join(metadata: dict, xorq_expr, source):
    from . import expr as bsl_expr

    left_metadata = _parse_field(metadata, "left")
    right_metadata = _parse_field(metadata, "right")

    if not left_metadata or not right_metadata:
        raise ValueError("SemanticJoinOp requires both 'left' and 'right' metadata")

    left_model = _reconstruct_bsl_operation(left_metadata, xorq_expr)
    right_model = _reconstruct_bsl_operation(right_metadata, xorq_expr)

    how = metadata.get("how", "inner")
    on_pickle = metadata.get("on_pickle")

    if on_pickle is None:
        return bsl_expr.SemanticJoin(
            left=left_model.op() if hasattr(left_model, "op") else left_model,
            right=right_model.op() if hasattr(right_model, "op") else right_model,
            on=None,
            how=how,
        )

    predicate = _unpickle_callable(on_pickle)
    return left_model.join_many(right_model, on=predicate, how=how)


# ---------------------------------------------------------------------------
# Dispatch
# ---------------------------------------------------------------------------


def _reconstruct_bsl_operation(metadata: dict[str, Any], xorq_expr):
    op_type = metadata.get("bsl_op_type")
    source = None
    source_metadata = _parse_field(metadata, "source")
    if source_metadata:
        source = _reconstruct_bsl_operation(source_metadata, xorq_expr)
    reconstructor = _RECONSTRUCTORS.get(op_type)
    if not reconstructor:
        raise ValueError(f"Unknown BSL operation type: {op_type}")
    return reconstructor(metadata, xorq_expr, source)


__all__ = [
    "to_tagged",
    "from_tagged",
    "try_import_xorq",
    "XorqModule",
]
