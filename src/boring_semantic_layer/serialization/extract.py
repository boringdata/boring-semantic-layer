"""Singledispatch metadata extractors for BSL op types.

Each BSL op type gets a registered handler that knows how to serialize
its fields into a plain dict. The ``extract_op_tree`` function walks
the op tree recursively, calling ``extract_metadata`` at each node.
"""

from __future__ import annotations

import functools
from collections.abc import Mapping
from typing import Any

from returns.result import Result, Success, safe

from .context import BSLSerializationContext
from .helpers import extract_simple_column_name


# ---------------------------------------------------------------------------
# singledispatch extractors
# ---------------------------------------------------------------------------


@functools.singledispatch
def extract_metadata(op, context: BSLSerializationContext) -> dict[str, Any]:
    """Extract serializable metadata from a single BSL op node.

    Dispatches on the concrete op type. Raises for unregistered types.
    """
    raise NotImplementedError(f"No extractor for {type(op).__name__}")


def _register_lazy(op_class_name: str):
    """Decorator that defers singledispatch registration until first call.

    BSL op classes live in ``ops.py`` which has heavy imports. This avoids
    importing them at module level.
    """

    def decorator(func):
        _LAZY_HANDLERS[op_class_name] = func
        return func

    return decorator


_LAZY_HANDLERS: dict[str, Any] = {}
_REGISTERED = False


def _ensure_registered():
    global _REGISTERED
    if _REGISTERED:
        return
    _REGISTERED = True
    from ..ops import (
        SemanticAggregateOp,
        SemanticFilterOp,
        SemanticGroupByOp,
        SemanticJoinOp,
        SemanticLimitOp,
        SemanticMutateOp,
        SemanticOrderByOp,
        SemanticProjectOp,
        SemanticTableOp,
    )

    _OP_CLASSES = {
        "SemanticTableOp": SemanticTableOp,
        "SemanticFilterOp": SemanticFilterOp,
        "SemanticGroupByOp": SemanticGroupByOp,
        "SemanticAggregateOp": SemanticAggregateOp,
        "SemanticMutateOp": SemanticMutateOp,
        "SemanticProjectOp": SemanticProjectOp,
        "SemanticOrderByOp": SemanticOrderByOp,
        "SemanticLimitOp": SemanticLimitOp,
        "SemanticJoinOp": SemanticJoinOp,
    }
    for name, handler in _LAZY_HANDLERS.items():
        cls = _OP_CLASSES[name]
        extract_metadata.register(cls)(handler)


# ---------------------------------------------------------------------------
# Per-op extractors
# ---------------------------------------------------------------------------


@_register_lazy("SemanticTableOp")
def _extract_semantic_table(op, context: BSLSerializationContext) -> dict[str, Any]:
    dims_result = serialize_dimensions(op.get_dimensions())
    meas_result = serialize_measures(op.get_measures())
    calc_result = serialize_calc_measures(op.get_calculated_measures())
    metadata: dict[str, Any] = {
        "dimensions": dims_result.value_or({}),
        "measures": meas_result.value_or({}),
    }
    calc_data = calc_result.value_or({})
    if calc_data:
        metadata["calc_measures"] = calc_data
    if op.name:
        metadata["name"] = op.name
    return metadata


@_register_lazy("SemanticFilterOp")
def _extract_filter(op, context: BSLSerializationContext) -> dict[str, Any]:
    from ..utils import expr_to_structured

    struct_result = expr_to_structured(op.predicate)
    match struct_result:
        case Success():
            return {"predicate_struct": struct_result.unwrap()}
        case _:
            raise ValueError("SemanticFilterOp: failed to serialize predicate")


@_register_lazy("SemanticGroupByOp")
def _extract_group_by(op, context: BSLSerializationContext) -> dict[str, Any]:
    return {"keys": list(op.keys)} if op.keys else {}


@_register_lazy("SemanticAggregateOp")
def _extract_aggregate(op, context: BSLSerializationContext) -> dict[str, Any]:
    from ..utils import expr_to_structured

    metadata: dict[str, Any] = {}
    if op.keys:
        metadata["by"] = list(op.keys)
    if op.aggs:
        metadata["aggs_struct"] = {
            name: expr_to_structured(fn).value_or(None) for name, fn in op.aggs.items()
        }
    return metadata


@_register_lazy("SemanticMutateOp")
def _extract_mutate(op, context: BSLSerializationContext) -> dict[str, Any]:
    from ..utils import expr_to_structured

    if not op.post:
        return {}
    return {
        "post_struct": {
            name: expr_to_structured(fn).value_or(None) for name, fn in op.post.items()
        }
    }


@_register_lazy("SemanticProjectOp")
def _extract_project(op, context: BSLSerializationContext) -> dict[str, Any]:
    return {"fields": list(op.fields)} if op.fields else {}


@_register_lazy("SemanticOrderByOp")
def _extract_order_by(op, context: BSLSerializationContext) -> dict[str, Any]:
    from ..utils import expr_to_structured

    order_keys = [
        {"type": "string", "value": key}
        if isinstance(key, str)
        else {"type": "callable", "value_struct": expr_to_structured(key).value_or(None)}
        for key in op.keys
    ]
    return {"order_keys": order_keys}


@_register_lazy("SemanticLimitOp")
def _extract_limit(op, context: BSLSerializationContext) -> dict[str, Any]:
    return {"n": op.n, "offset": op.offset}


@_register_lazy("SemanticJoinOp")
def _extract_join(op, context: BSLSerializationContext) -> dict[str, Any]:
    from ..utils import join_predicate_to_structured

    metadata: dict[str, Any] = {"how": op.how}
    if op.on is not None:
        struct_result = join_predicate_to_structured(op.on)
        match struct_result:
            case Success():
                metadata["on_struct"] = struct_result.unwrap()
            case _:
                raise ValueError("SemanticJoinOp: failed to serialize join predicate")
    return metadata


# ---------------------------------------------------------------------------
# Tree walker
# ---------------------------------------------------------------------------


def extract_op_tree(op, context: BSLSerializationContext) -> dict[str, Any]:
    """Walk the BSL op tree and extract metadata at each node.

    Replaces the old ``_extract_op_metadata`` recursive function.
    """
    _ensure_registered()

    op_type = type(op).__name__
    metadata: dict[str, Any] = {
        "bsl_op_type": op_type,
        "bsl_version": context.version,
    }

    try:
        metadata.update(extract_metadata(op, context))
    except NotImplementedError:
        pass  # unknown op type — still record bsl_op_type

    @safe
    def extract_source():
        return extract_op_tree(op.source, context)

    @safe
    def extract_left():
        return extract_op_tree(op.left, context)

    @safe
    def extract_right():
        return extract_op_tree(op.right, context)

    if source_metadata := extract_source().value_or(None):
        metadata["source"] = source_metadata

    if left_metadata := extract_left().value_or(None):
        metadata["left"] = left_metadata

    if right_metadata := extract_right().value_or(None):
        metadata["right"] = right_metadata

    return metadata


# ---------------------------------------------------------------------------
# Dimension / Measure / CalcMeasure serializers
# ---------------------------------------------------------------------------


def serialize_dimensions(dimensions: Mapping[str, Any]) -> Result[dict, Exception]:
    from ..utils import expr_to_structured

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
            col_name = extract_simple_column_name(dim.expr)
            match col_name:
                case str():
                    entry["expr"] = col_name
                case _:
                    struct_result = expr_to_structured(dim.expr)
                    match struct_result:
                        case Success():
                            entry["expr_struct"] = struct_result.unwrap()
                        case _:
                            raise ValueError(
                                f"Dimension '{name}': failed to serialize expression"
                            )
            dim_metadata[name] = entry
        return dim_metadata

    return do_serialize()


def serialize_measures(measures: Mapping[str, Any]) -> Result[dict, Exception]:
    from ..utils import expr_to_structured

    @safe
    def do_serialize():
        meas_metadata = {}
        for name, meas in measures.items():
            entry = {
                "description": meas.description,
                "requires_unnest": list(meas.requires_unnest),
            }
            original = getattr(meas, "original_expr", None)
            struct_result = (
                expr_to_structured(original)
                if original is not None
                else expr_to_structured(meas.expr)
            )
            match struct_result:
                case Success():
                    entry["expr_struct"] = struct_result.unwrap()
                case _:
                    raise ValueError(f"Measure '{name}': failed to serialize expression")
            meas_metadata[name] = entry
        return meas_metadata

    return do_serialize()


def serialize_calc_measures(calc_measures: Mapping[str, Any]) -> Result[dict, Exception]:
    @safe
    def do_serialize():
        from ..measure_scope import AggregationExpr, AllOf, BinOp, MeasureRef, MethodCall

        def _serialize_calc_expr(expr):
            if isinstance(expr, MeasureRef):
                return ("measure_ref", expr.name)
            if isinstance(expr, AggregationExpr):
                return ("agg_expr", expr.column, expr.operation, expr.post_ops)
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
                return (
                    "calc_binop",
                    expr.op,
                    _serialize_calc_expr(expr.left),
                    _serialize_calc_expr(expr.right),
                )
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
    from ..measure_scope import AggregationExpr, AllOf, BinOp, MeasureRef, MethodCall

    from .freeze import list_to_tuple

    def _deserialize_calc_expr(data):
        if isinstance(data, int | float):
            return data
        tag = data[0]
        if tag == "measure_ref":
            return MeasureRef(data[1])
        if tag == "agg_expr":
            return AggregationExpr(
                column=data[1],
                operation=data[2],
                post_ops=list_to_tuple(data[3]) if data[3] else (),
            )
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
