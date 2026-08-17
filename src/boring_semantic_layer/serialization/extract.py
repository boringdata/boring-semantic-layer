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
        SemanticOrderByOp,
        SemanticProjectOp,
        SemanticTableOp,
    )

    _OP_CLASSES = {
        "SemanticTableOp": SemanticTableOp,
        "SemanticFilterOp": SemanticFilterOp,
        "SemanticGroupByOp": SemanticGroupByOp,
        "SemanticAggregateOp": SemanticAggregateOp,
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


def _unwrap_or_raise(result: Result[dict, Exception], kind: str, model_name) -> dict:
    """Return the serialized fields, or explain which kind could not be written.

    ``value_or({})`` here meant that one unserializable field emptied the
    whole set: a model with a measure holding, say, a Python ``set`` was
    tagged with ``measures: ()``, reconstructed with no measures at all, and
    only failed later as "Column 'total' is not found" — pointing at the
    query rather than at the field that could not be serialized.
    """
    if isinstance(result, Success):
        return result.unwrap()
    where = f" on model {model_name!r}" if model_name else ""
    raise ValueError(
        f"Cannot serialize the {kind}{where}: {result.failure()}. Tagging would "
        f"otherwise drop every {kind} silently and the reconstructed model would "
        "be missing them."
    )


@_register_lazy("SemanticTableOp")
def _extract_semantic_table(op, context: BSLSerializationContext) -> dict[str, Any]:
    metadata: dict[str, Any] = {
        "dimensions": _unwrap_or_raise(
            serialize_dimensions(op.get_dimensions()), "dimensions", op.name
        ),
        "measures": _unwrap_or_raise(
            serialize_measures(op.get_measures()), "measures", op.name
        ),
    }
    calc_data = _unwrap_or_raise(
        serialize_calc_measures(op.get_calculated_measures()), "calculated measures", op.name
    )
    if calc_data:
        metadata["calc_measures"] = calc_data
    if op.name:
        metadata["name"] = op.name
    if op.description:
        metadata["description"] = op.description
    # Wrapper tables from SemanticJoin.with_measures()/with_dimensions()
    # carry the join topology in _source_join. Serializing the wrapper
    # flat loses it, and the reconstructed model then executes on the
    # lowered (fanned-out) join — bypassing pre-aggregation entirely.
    source_join = getattr(op, "_source_join", None)
    if source_join is not None:
        metadata["source_join"] = extract_op_tree(source_join, context)
    return metadata


@_register_lazy("SemanticFilterOp")
def _extract_filter(op, context: BSLSerializationContext) -> dict[str, Any]:
    from ..ops import _exact_filter_fields, _unwrap
    from ..utils import expr_to_structured

    predicate = _unwrap(op.predicate)
    try:
        serialization_predicate = object.__getattribute__(
            predicate, "__bsl_serialization_predicate__"
        )
    except (AttributeError, TypeError):
        serialization_predicate = predicate
    struct_result = expr_to_structured(serialization_predicate)
    match struct_result:
        case Success():
            metadata = {"predicate_struct": struct_result.unwrap()}
            # JSON filters carry exact semantic field spellings as callable
            # metadata.  The generic Deferred resolver tree preserves the
            # expression but not those attributes, so serialize them beside
            # the predicate.  They are needed to synthesize qualified raw
            # columns and to reject unknown model prefixes after round-trip.
            if filter_fields := _exact_filter_fields(op.predicate):
                metadata["filter_fields"] = sorted(filter_fields)
            return metadata
        case _:
            raise ValueError("SemanticFilterOp: failed to serialize predicate")


@_register_lazy("SemanticGroupByOp")
def _extract_group_by(op, context: BSLSerializationContext) -> dict[str, Any]:
    return {"keys": list(op.keys)} if op.keys else {}


@_register_lazy("SemanticAggregateOp")
def _extract_aggregate(op, context: BSLSerializationContext) -> dict[str, Any]:
    from ..ops import _detect_bare_name_lambda, _unwrap
    from ..utils import expr_to_structured

    metadata: dict[str, Any] = {}
    if op.keys:
        metadata["by"] = list(op.keys)
    if op.aggs:
        metadata["aggs_struct"] = {
            name: expr_to_structured(fn).value_or(None) for name, fn in op.aggs.items()
        }
        # ``aggregate("revenue")`` and ``aggregate(revenue=lambda t: ...)``
        # both land in ``aggs`` and can serialize to similar-looking trees,
        # but they mean different things: the first must replay through
        # measure resolution (which is what makes it fan-out safe on a
        # joined model), the second is a query-local expression that must
        # be rebuilt verbatim. Record which is which instead of guessing
        # from the name on the way back in.
        # Always emitted, including empty: its absence is what tells the
        # reader this payload predates the marker and needs the structural
        # fallback in ``_bare_ref_names``.
        metadata["agg_bare_refs"] = sorted(
            name
            for name, fn in op.aggs.items()
            if _detect_bare_name_lambda(_unwrap(fn)) is not None
        )
    return metadata


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

    metadata: dict[str, Any] = {"how": op.how, "cardinality": op.cardinality}
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
            if dim.derived_dimensions:
                entry["derived_dimensions"] = list(dim.derived_dimensions)
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
    """Serialize calc measures (``CalcMeasure`` objects) by resolver-tree.

    Each calc measure stores the original user lambda. We run it once
    against a fresh ``Deferred`` variable to capture the structural shape
    (calls to ``.all(...)``, attribute access, arithmetic ...) and
    serialize the resulting resolver tree.
    """
    from ..utils import expr_to_structured

    @safe
    def do_serialize():
        result: dict[str, Any] = {}
        for name, calc in calc_measures.items():
            fn = getattr(calc, "expr", calc)
            struct_result = expr_to_structured(fn)
            entry: dict[str, Any] = {}
            match struct_result:
                case Success():
                    entry["expr_struct"] = struct_result.unwrap()
                case _:
                    # Skipping left the model looking complete while quietly
                    # missing this calc measure.
                    raise ValueError(
                        f"Calc measure {name!r}: failed to serialize expression "
                        f"({struct_result.failure()})"
                    )
            description = getattr(calc, "description", None)
            if description is not None:
                entry["description"] = description
            requires_unnest = getattr(calc, "requires_unnest", ())
            if requires_unnest:
                entry["requires_unnest"] = list(requires_unnest)
            depends_on = getattr(calc, "depends_on", None)
            if depends_on:
                entry["depends_on"] = sorted(depends_on)
            result[name] = entry
        return result

    return do_serialize()


def deserialize_calc_measures(calc_data: Mapping[str, Any]) -> dict[str, Any]:
    """Reconstruct calc measures from their serialized resolver trees.

    Returns a dict mapping ``name → CalcMeasure``. Each entry's expression
    is a Deferred whose resolver mirrors the original lambda's structural
    shape; at query time the planner runs it against an
    ``IbisCalcScope`` exactly like a user-supplied lambda.
    """
    from ..ops import CalcMeasure
    from ..utils import structured_to_expr

    from .freeze import list_to_tuple

    out: dict[str, Any] = {}
    for name, data in calc_data.items():
        if isinstance(data, dict):
            entry = data
            struct = entry.get("expr_struct")
            description = entry.get("description")
            requires_unnest = tuple(entry.get("requires_unnest", ()) or ())
            depends_on = frozenset(entry.get("depends_on", ()) or ())
        else:
            # Backwards-compat: old curated-AST tags arrive as bare tuples.
            struct = data
            description = None
            requires_unnest = ()
            depends_on = frozenset()

        if struct is None:
            raise ValueError(
                f"Calc measure {name!r} has no serialized expression in this "
                "payload; reconstructing without it would silently return a "
                "model that is missing the measure."
            )
        # ``thaw`` converts the resolver tuple into a list of lists; the
        # resolver deserializer expects nested tuples, so convert back.
        struct = list_to_tuple(struct)
        result = structured_to_expr(struct)
        match result:
            case Success():
                expr = result.unwrap()
            case _:
                raise ValueError(
                    f"Calc measure {name!r}: failed to deserialize expression "
                    f"({result.failure()})"
                )
        out[name] = CalcMeasure(
            expr=expr,
            description=description,
            requires_unnest=requires_unnest,
            depends_on=depends_on,
        )
    return out
