from __future__ import annotations

import contextlib
import logging
import re
from collections.abc import Callable, Iterable, Mapping, Sequence
from difflib import get_close_matches
from functools import reduce
from typing import TYPE_CHECKING, Any

import ibis
from attrs import field, frozen
from ibis.common.deferred import Deferred
from ibis.expr import datatypes as dt
from ibis.expr import operations as ibis_ops
from ibis.expr import types as ir
from ibis.expr.operations.relations import Relation
from ibis.expr.schema import Schema

from .. import projection_utils
from .._xorq import (
    Column as XorqColumn,
)
from .._xorq import (
    FrozenDict,
    FrozenOrderedDict,
    null_safe_equal,
)
from .._xorq import (
    Schema as XorqSchema,
)
from .._xorq import (
    operations as xorq_ops,
)
from .._xorq import (
    selectors as s,
)
from ..calc_analyzer import (
    _is_reduction,
    _is_window,
    _to_node,
    analyze_calc_expr,
)
from ..calc_analyzer import (
    _walk as _walk_calc_expr,
)
from ..calc_compiler import (
    TOTALS_PREFIX,
    TotalsNotAvailableError,
    UnknownMeasureRefError,
    WindowedBaseReductionError,
    _drop_totals_columns,
    _to_op,
    attach_calc_totals,
    attach_windowed_totals,
    evaluate_calc_lambda,
    lift_inline_reductions,
    topological_order_from_deps,
)
from ..calc_compiler import (
    compile_calc_measure as _compile_calc_measure_impl,
)
from ..fieldref import resolve_suffix
from ..measure_scope import (
    ColumnScope,
    MeasureScope,
)
from ..nested_access import NestedAccessMarker
from ._compat import _rebind_to_backend, _rebind_to_canonical_backend
from ._normalize import (
    _JOIN_REMOVED_MESSAGE,
    _allocate_right_collision_names,
    _allocate_temporary_join_names,
    _RenamedResolver,
)
from ._reductions import (
    _is_mean_expr,
    _reagg_op_for_expr,
)
from ._tracking import (
    _extract_columns_from_callable,
    _extract_join_key_columns,
)
from ._values import (
    CalcMeasure,
    Dimension,
    Measure,
    _CallableWrapper,
    _ensure_wrapped,
    _is_deferred,
    _reject_bool_resolution,
)

logger = logging.getLogger(__name__)

_SchemaClass = XorqSchema
_FrozenOrderedDict = FrozenOrderedDict


if TYPE_CHECKING:
    from ..expr import (
        SemanticFilter,
        SemanticGroupBy,
        SemanticLimit,
        SemanticOrderBy,
        SemanticTable,
    )


def _to_untagged(source: Any) -> ir.Table:
    return source.to_untagged() if hasattr(source, "to_untagged") else source.to_expr()


def _expr_module():
    """Call-time accessor for the expression layer (the layer above ops).

    Fluent convenience methods on op classes construct user-facing wrappers;
    resolving the module at call time keeps ops free of any import-time
    dependency on expr. Follow-up: move those methods onto the wrappers.
    """
    import importlib

    return importlib.import_module("boring_semantic_layer.expr")


def _compile_module(name: str):
    """Call-time accessor for a compile-strategy module.

    Strategy modules import _core at module level (downward); _core reaches
    them only at call time, keeping the import graph acyclic.
    """
    import importlib

    return importlib.import_module(f"boring_semantic_layer.ops.{name}")


def _query_module():
    """Call-time accessor for the query layer (see ``_expr_module``)."""
    import importlib

    return importlib.import_module("boring_semantic_layer.query")


def _semantic_table(*args, **kwargs) -> SemanticTable:
    return _expr_module().SemanticModel(*args, **kwargs)


def _unwrap(wrapped: Any) -> Any:
    return wrapped.unwrap if isinstance(wrapped, _CallableWrapper) else wrapped


def _exact_filter_fields(predicate: Any) -> frozenset[str]:
    """Return exact JSON-AST field spellings attached by ``query.Filter``."""
    fn = _unwrap(predicate)
    # Ibis/XORQ Deferred implements ``__getattr__`` by constructing another
    # Deferred expression.  A normal ``getattr(..., default)`` therefore does
    # not return the default for ordinary deferred/callable filters and trying
    # to iterate that synthetic expression raises.  Only metadata explicitly
    # attached to the generated JSON-filter callable counts here.
    try:
        fields = object.__getattribute__(fn, "__bsl_filter_fields__")
    except (AttributeError, TypeError):
        return frozenset()
    if not isinstance(fields, (set, frozenset, tuple, list)):
        return frozenset()
    return frozenset(fields)


def _collect_chain(op: Relation) -> list[Relation]:
    """Walk op.source (or op.left for joins) back to root, return list from root to current."""
    chain = [op]
    current = op
    while True:
        if hasattr(current, "source") and current.source is not None:
            chain.append(current.source)
            current = current.source
        elif hasattr(current, "left") and current.left is not None:
            chain.append(current.left)
            current = current.left
        else:
            break
    chain.reverse()
    return chain


def _format_op_summary(op: Relation) -> str:
    """Return a one-line summary string for a non-root semantic op."""
    # Import here to avoid circular imports at module level
    cls = type(op).__name__

    if isinstance(op, SemanticFilterOp):
        predicate = object.__getattribute__(op, "predicate")
        pred_name = "<predicate>"
        if hasattr(predicate, "__name__"):
            pred_name = predicate.__name__
        elif hasattr(predicate, "unwrap"):
            unwrapped = predicate.unwrap
            if hasattr(unwrapped, "__name__"):
                pred_name = unwrapped.__name__
        return f"Filter(\u03bb {pred_name})"

    if isinstance(op, SemanticGroupByOp):
        keys = object.__getattribute__(op, "keys")
        return f"GroupBy({', '.join(keys)})"

    if isinstance(op, SemanticAggregateOp):
        aggs = object.__getattribute__(op, "aggs")
        agg_names = list(aggs.keys())
        return f"Aggregate({', '.join(agg_names)})"

    if isinstance(op, SemanticOrderByOp):
        keys = object.__getattribute__(op, "keys")
        key_strs = [k if isinstance(k, str) else repr(k) for k in keys]
        return f"OrderBy({', '.join(key_strs)})"

    if isinstance(op, SemanticLimitOp):
        return f"Limit({op.n})"

    if isinstance(op, SemanticProjectOp):
        fields = object.__getattribute__(op, "fields")
        return f"Project({', '.join(fields)})"

    if isinstance(op, SemanticUnnestOp):
        column = object.__getattribute__(op, "column")
        return f"Unnest({column})"

    if isinstance(op, SemanticJoinOp):
        how = object.__getattribute__(op, "how")
        right = object.__getattribute__(op, "right")
        right_name = ""
        if isinstance(right, SemanticTableOp):
            right_name = object.__getattribute__(right, "name") or ""
        if not right_name:
            # Try to find a root name from right side
            roots = _find_all_root_models(right)
            if roots:
                right_name = object.__getattribute__(roots[0], "name") or ""
        if right_name:
            return f"Join({how}, right={right_name})"
        return f"Join({how})"

    if isinstance(op, SemanticIndexOp):
        parts = []
        selector = object.__getattribute__(op, "selector")
        by = object.__getattribute__(op, "by")
        sample = object.__getattribute__(op, "sample")
        if selector is not None:
            if isinstance(selector, tuple):
                parts.append(", ".join(selector))
            else:
                parts.append(str(selector))
        if by is not None:
            parts.append(f"by={by}")
        if sample is not None:
            parts.append(f"sample={sample}")
        return f"Index({', '.join(parts)})"

    # Fallback for unknown op types
    return cls.replace("Semantic", "").replace("Op", "")


def _format_root(root_op: SemanticTableOp) -> str:
    """Format a SemanticTableOp root using the ``fmt`` singledispatch registry.

    ``format.py`` (imported by the package ``__init__``) registers the
    semantic-op handlers on ibis's ``fmt``; import it from the same place
    so we hit the same registry without depending on the format module.
    """
    try:
        from ibis.expr.format import fmt
    except ImportError:
        from .._xorq import fmt

    try:
        return fmt(root_op)
    except Exception:
        # Fallback if format module isn't available
        name = object.__getattribute__(root_op, "name")
        return f"SemanticTable: {name}" if name else "SemanticTable"


def _semantic_repr(op: Relation) -> str:
    chain = _collect_chain(op)

    # Find the root (first element should be a SemanticTableOp)
    root = chain[0]
    if isinstance(root, SemanticTableOp):
        lines = [_format_root(root)]
    else:
        # Fallback: no SemanticTableOp root found
        from ibis.expr.format import pretty

        try:
            return pretty(op)
        except Exception:
            return object.__repr__(op)

    # Append pipeline steps
    for step in chain[1:]:
        if not isinstance(step, SemanticTableOp):
            lines.append(f"-> {_format_op_summary(step)}")

    return "\n".join(lines)


def _make_schema(fields_dict: dict[str, str]):
    """Create Schema instance from fields dict.

    Strips length parameters from string types (e.g. ``string(50)`` → ``string``)
    so that backends like Postgres whose ``VARCHAR(N)`` serialises as ``string(N)``
    can be parsed by the Schema constructor.
    """
    cleaned = {k: re.sub(r"\bstring\(\d+\)", "string", v) for k, v in fields_dict.items()}
    return _SchemaClass(cleaned)


def _resolve_expr(expr: Deferred | Callable | Any, scope: ir.Table) -> ir.Value:
    was_resolved = _is_deferred(expr) or callable(expr)
    result = expr.resolve(scope) if _is_deferred(expr) else expr(scope) if callable(expr) else expr

    if was_resolved:
        _reject_bool_resolution(result, expr)

    if hasattr(result, "__class__") and hasattr(scope, "__class__"):
        result_module = result.__class__.__module__
        scope_module = scope.__class__.__module__
        result_is_regular_ibis = "ibis.expr" in result_module and "xorq" not in result_module
        scope_is_xorq = "xorq.vendor.ibis" in scope_module

        if result_is_regular_ibis and scope_is_xorq:
            from .._xorq import from_ibis

            result = from_ibis(result)

    return result


def _get_field_dict(root: Any, field_type: str) -> dict:
    method_map = {
        "dimensions": "get_dimensions",
        "measures": "get_measures",
        "calc_measures": "get_calculated_measures",
    }
    method_name = method_map[field_type]
    return dict(getattr(root, method_name)())


def _get_merged_fields(
    all_roots: list,
    field_type: str,
    source: Relation | None = None,
) -> dict:
    return (
        _merge_fields_with_prefixing(
            all_roots,
            lambda r: _get_field_dict(r, field_type),
            source=source,
        )
        if len(all_roots) > 1
        else _get_field_dict(all_roots[0], field_type)
        if all_roots
        else {}
    )


def _augment_dimensions_with_raw_columns(
    merged_dimensions: Mapping[str, Any],
    keys: Iterable[str],
    all_roots: Sequence[Any],
    source: Any = None,
) -> dict:
    """Expose requested ``<table>.<column>`` group keys as auto-dimensions.

    On a single un-joined model, raw table columns are queryable without a
    ``with_dimensions`` declaration. A joined table flattens to physical
    columns with collision suffixes (``_right``, ``_right2``, …), so a
    prefixed raw-column reference has nothing to resolve against unless a
    dimension was declared. For each requested key that names a root table
    and one of its raw columns, synthesize an identity dimension and run it
    through the same rename-aware merge that declared dimensions use, so
    collided right-side columns resolve to their suffixed physical name.

    Declared dimensions always win over synthesized ones.
    """
    expanded_roots: list[Any] = []

    def expand(root):
        source_join = getattr(root, "_source_join", None)
        if source_join is None:
            expanded_roots.append(root)
            return
        for child in _find_all_root_models(source_join):
            expand(child)

    for root in all_roots:
        expand(root)

    requested: dict[str, dict[str, Dimension]] = {}
    for key in keys:
        if key in merged_dimensions or "." not in key:
            continue
        prefix, col = key.split(".", 1)
        for root in expanded_roots:
            if root.name != prefix:
                continue
            declared = _get_field_dict(root, "dimensions")
            if col in declared:
                # A single standalone model stores semantic dimensions under
                # bare names. Its accepted ``model.dimension`` convenience
                # spelling must preserve the declared expression rather than
                # synthesizing an identity over a same-named raw column.
                requested.setdefault(prefix, {})[col] = declared[col]
                break
            cols = getattr(getattr(root, "table", None), "columns", ())
            if col in cols:
                requested.setdefault(prefix, {})[col] = Dimension(expr=lambda t, _c=col: t[_c])
            break
    if not requested:
        return dict(merged_dimensions)
    synthesized = _merge_fields_with_prefixing(
        expanded_roots,
        lambda r: requested.get(r.name, {}),
        source=source,
    )
    return {**dict(synthesized), **dict(merged_dimensions)}


def _validate_qualified_filter_fields(
    fields: Iterable[str],
    dimensions: Mapping[str, Any],
    all_roots: Sequence[Any],
) -> None:
    """Fail closed when an exact JSON field names no semantic source."""
    expanded_roots: list[Any] = []

    def expand(root):
        source_join = getattr(root, "_source_join", None)
        if source_join is None:
            expanded_roots.append(root)
            return
        for child in _find_all_root_models(source_join):
            expand(child)

    for root in all_roots:
        expand(root)

    roots_by_name = {root.name: root for root in expanded_roots if getattr(root, "name", None)}
    for field_name in fields:
        if "." not in field_name or field_name in dimensions:
            continue
        prefix, raw_name = field_name.split(".", 1)
        root = roots_by_name.get(prefix)
        if root is None:
            available = ", ".join(sorted(roots_by_name)) or "none"
            raise KeyError(
                f"Unknown semantic model prefix {prefix!r} in filter field "
                f"{field_name!r}. Available model prefixes: {available}."
            )
        raw_columns = frozenset(getattr(getattr(root, "table", None), "columns", ()))
        if raw_name not in raw_columns:
            raise KeyError(
                f"Unknown field {raw_name!r} on semantic model {prefix!r} "
                f"in filter field {field_name!r}."
            )


def _reject_unresolvable_group_keys(
    keys: Iterable[str],
    merged_dimensions: Mapping[str, Any],
    tbl,
    all_roots: Sequence[Any],
) -> None:
    """Raise a semantic-layer error for group keys that resolve to nothing.

    Without this, an unknown key reaches ibis as a physical column lookup on
    the joined table and fails with an error that leaks the flattened join
    schema (``name_right``, ``tournament_id_right2``, …) instead of naming
    the model's queryable surface.
    """
    tbl_columns = frozenset(getattr(tbl, "columns", ()))
    unresolved = [k for k in keys if k not in merged_dimensions and k not in tbl_columns]
    if not unresolved:
        return

    candidates: set[str] = set(merged_dimensions)
    for root in all_roots:
        cols = getattr(getattr(root, "table", None), "columns", ())
        candidates.update(cols)
        if root.name:
            candidates.update(f"{root.name}.{c}" for c in cols)

    suggestions = []
    for key in unresolved:
        close = get_close_matches(key, sorted(candidates), n=3, cutoff=0.6)
        if close:
            suggestions.append(f"{key!r} (did you mean: {', '.join(map(repr, close))}?)")
        else:
            suggestions.append(repr(key))
    declared = ", ".join(sorted(merged_dimensions)) or "none"
    raise KeyError(
        f"Unknown group_by key(s): {'; '.join(suggestions)}. "
        f"Declared dimensions: {declared}. Raw table columns can also be "
        "referenced directly ('column' or '<table>.<column>' on joins)."
    )


def _extract_missing_column_name(exc: Exception) -> str | None:
    """Extract a missing column/attribute name from common resolution errors."""
    message = str(exc)
    patterns = (
        r"has no attribute ['\"]([^'\"]+)['\"]",
        r"non-existent column ['\"]([^'\"]+)['\"]",
        r"Column ['\"]([^'\"]+)['\"] is not found",
        r"KeyError: ['\"]([^'\"]+)['\"]",
    )
    for pattern in patterns:
        match = re.search(pattern, message)
        if match:
            return match.group(1)
    return None


def _mutate_dimensions_with_dependencies(
    tbl: ir.Table,
    dimension_names: Iterable[str],
    merged_dimensions: Mapping[str, Any],
    *,
    overwrite_existing: bool = True,
) -> ir.Table:
    """Mutate requested dimensions, recursively materializing derived deps first.

    ``overwrite_existing=False`` leaves dimensions that share a name with an
    existing column unmaterialized. Filter resolution needs this: it resolves
    such dimensions through the dimension lambda against raw columns, and
    materializing them first would both re-apply the expression (``amount*2``
    filtering as ``amount*4``) and hand downstream measures the mutated
    column in place of the raw one.
    """
    resolving: list[str] = []

    # Dim lambdas reference sibling dims by their BARE name (t.region_band),
    # but merged dimension maps key them by prefixed name on joins
    # (customers.region_band). Alias unambiguous suffixes so dependency
    # resolution can materialize them under the name the lambda reads.
    merged_dimensions = dict(merged_dimensions)
    _by_suffix: dict[str, list[str]] = {}
    for _name in merged_dimensions:
        if "." in _name:
            _by_suffix.setdefault(_name.split(".", 1)[1], []).append(_name)
    for _short, _fulls in _by_suffix.items():
        if _short not in merged_dimensions and len(_fulls) == 1:
            merged_dimensions[_short] = merged_dimensions[_fulls[0]]

    def resolve_one(dim_name: str, current_tbl: ir.Table) -> ir.Table:
        if dim_name not in merged_dimensions:
            return current_tbl
        if not overwrite_existing and dim_name in current_tbl.columns:
            return current_tbl
        if dim_name in resolving:
            cycle = " -> ".join([*resolving, dim_name])
            raise ValueError(f"Circular dimension dependency detected: {cycle}")

        resolving.append(dim_name)
        try:
            while True:
                try:
                    dim_fn = merged_dimensions[dim_name]
                    dim_expr = (
                        dim_fn(current_tbl, _dims=merged_dimensions)
                        if isinstance(dim_fn, Dimension)
                        else dim_fn(current_tbl)
                    )
                    return current_tbl.mutate(**{dim_name: dim_expr})
                except Exception as exc:
                    missing = _extract_missing_column_name(exc)
                    if (
                        missing
                        and missing in merged_dimensions
                        and missing != dim_name
                        and missing not in resolving
                    ):
                        current_tbl = resolve_one(missing, current_tbl)
                        continue
                    raise
        finally:
            resolving.pop()

    for dim_name in dimension_names:
        tbl = resolve_one(dim_name, tbl)
    return tbl


def _reject_shadowed_group_keys(
    tbl, keys, merged_dimensions, aggs, merged_base_measures, raw_columns=None
):
    """Reject group keys whose dimension redefines a column a measure reads.

    Materializing such a dimension overwrites the raw column before measures
    are computed, so the measure would silently aggregate the dimension's
    values (e.g. ``amount * 2``) instead of the column it was defined over.
    Identity dimensions (``lambda t: t.amount``) and measures that don't
    touch the shadowed column are unaffected and stay allowed.

    ``raw_columns`` is the union of the root tables' own column names: a key
    absent from it can only exist in ``tbl`` as an upstream materialization
    of the dimension itself (e.g. by a pre-aggregation filter), so there is
    no raw column to shadow and expressions reading it are well-defined
    (e.g. ``mutate`` entries desugared onto the measure path).
    """
    for key in keys:
        if key not in merged_dimensions or key not in tbl.columns:
            continue
        if raw_columns is not None and key not in raw_columns:
            continue
        dim_fn = merged_dimensions[key]
        try:
            dim_expr = dim_fn(tbl)
        except Exception as exc:
            logger.debug("Shadowed-key guard could not evaluate dimension %r: %s", key, exc)
            continue
        target = tbl[key].op()
        try:
            if dim_expr.op() == target:
                continue
        except Exception as exc:
            logger.debug("Shadowed-key guard could not compare dimension %r: %s", key, exc)
            continue
        for name, agg in aggs.items():
            measure = merged_base_measures.get(name)
            try:
                measure_expr = measure(tbl) if measure is not None else _unwrap(agg)(tbl)
            except Exception as exc:
                logger.debug("Shadowed-key guard could not evaluate measure %r: %s", name, exc)
                continue
            try:
                reads_shadowed = any(
                    node == target for node in measure_expr.op().find(type(target))
                )
            except Exception as exc:
                logger.debug("Shadowed-key guard could not inspect measure %r: %s", name, exc)
                continue
            if reads_shadowed:
                raise ValueError(
                    f"Group key {key!r} is a dimension that redefines column "
                    f"{key!r} with a different expression, and measure {name!r} "
                    "reads that column. Grouping would replace the column with "
                    "the dimension's values and silently change the measure. "
                    f"Rename the dimension (e.g. '{key}_bucket') or define the "
                    "measure against a column the dimension does not shadow."
                )


def _classify_dependencies(
    fields: list,
    dimensions: dict,
    measures: dict,
    calc_measures: dict,
    current_field: str | None = None,
) -> dict[str, str]:
    """Classify field dependencies as dimension, measure, or column."""
    return {
        f.name: (
            "dimension"
            if f.name in dimensions and f.name != current_field
            else "measure"
            if f.name in measures or f.name in calc_measures
            else "column"
        )
        for f in fields
    }


def _collect_struct(struct_dict: dict[str, Any], **collect_kwargs):
    """Build ``struct(...).collect()`` from columns of a single ibis flavor.

    ``ibis.struct`` and ``xorq.vendor.ibis.struct`` are not interchangeable:
    each can only infer types from columns of its own module.
    """
    first_col = next(iter(struct_dict.values()))
    if isinstance(first_col, XorqColumn):
        from .._xorq import ibis as xibis

        return xibis.struct(struct_dict).collect(**collect_kwargs)
    return ibis.struct(struct_dict).collect(**collect_kwargs)


class NestAggSpec:
    """Compiled plan for a semantic ``nest=`` aggregation entry.

    Built by ``SemanticGroupBy.aggregate`` when a nest lambda returns a
    semantic aggregation. ``inner_op`` is that aggregation re-grouped at
    (outer keys + inner keys) grain — including any filters the lambda
    applied — and ``struct_fields`` are the columns collected into the
    array-of-structs (inner keys + inner aggregates). Pipeline steps
    chained after the inner aggregate are carried as per-group modifiers:
    ``having`` predicates run at the inner grain before collection,
    ``order_keys`` order each group's array, and ``limit_spec`` (n,
    offset) truncates it. ``SemanticAggregateOp.to_untagged`` compiles it
    as its own query and joins it back to the outer aggregate on the
    outer keys.
    """

    __slots__ = ("having", "inner_op", "limit_spec", "order_keys", "struct_fields")

    def __init__(
        self,
        inner_op: SemanticAggregateOp,
        struct_fields: Iterable[str],
        order_keys: Iterable[Any] = (),
        limit_spec: tuple[int, int] | None = None,
        having: Iterable[Any] = (),
    ) -> None:
        self.inner_op = inner_op
        self.struct_fields = tuple(struct_fields)
        self.order_keys = tuple(order_keys)
        self.limit_spec = limit_spec
        self.having = tuple(having)

    def __call__(self, *args, **kwargs):
        # Callable so it passes the ``aggs: dict[str, Callable]`` signature
        # validation, but it is a compile plan, not an aggregation lambda:
        # SemanticAggregateOp.to_untagged routes it to _to_untagged_with_nest
        # before any agg spec is invoked.
        raise TypeError(
            "NestAggSpec is not an aggregation lambda; nest= entries are "
            "compiled by SemanticAggregateOp._to_untagged_with_nest",
        )

    def __repr__(self) -> str:
        return f"NestAggSpec(keys={self.inner_op.keys!r}, struct_fields={self.struct_fields!r})"


def _resolve_nest_order_key(key, table):
    """Resolve a nest order_by key against the compiled inner table."""
    if isinstance(key, str):
        return table[key]
    return _resolve_expr(_unwrap(key), ColumnScope(_tbl=table))


def _infer_unnest(fn: Callable, table: Any) -> tuple[str, ...]:
    """Infer required unnest operations from the table.

    Examples:
        to_semantic_table(tbl).with_measures(...) -> ()  # Session level
        to_semantic_table(tbl).unnest("hits").with_measures(...) -> ("hits",)
        unnested.unnest("product").with_measures(...) -> ("product",)
    """
    op = table.op() if hasattr(table, "op") else None
    if isinstance(op, SemanticUnnestOp):
        # SemanticUnnestOp always has column attribute
        return (op.column,)

    return ()


def _extract_measure_metadata(
    fn_or_expr: Any,
) -> tuple[Any, str | None, tuple, Mapping[str, Any]]:
    """Extract metadata from various measure representations."""
    if isinstance(fn_or_expr, dict):
        return (
            fn_or_expr["expr"],
            fn_or_expr.get("description"),
            tuple(fn_or_expr.get("requires_unnest", [])),
            dict(fn_or_expr.get("metadata") or {}),
        )
    elif isinstance(fn_or_expr, Measure):
        return (
            fn_or_expr.expr,
            fn_or_expr.description,
            fn_or_expr.requires_unnest,
            dict(fn_or_expr.metadata),
        )
    else:
        return (fn_or_expr, None, (), {})


def _make_base_measure(
    expr: Any,
    description: str | None,
    requires_unnest: tuple,
    metadata: Mapping[str, Any] | None = None,
) -> Measure:
    """Wrap a base-measure callable as a :class:`Measure`.

    The lambda is invoked against a :class:`ColumnScope` so that nested
    array columns (``t.hits.count()`` over an array) surface as
    ``NestedAccessMarker`` values for the nested-aggregation pipeline.
    Plain reductions (``t.distance.sum()``) flow through unchanged.
    """
    raw_expr = expr._fn if isinstance(expr, _CallableWrapper) else expr

    if _is_deferred(expr):
        wrapped = lambda t, fn=expr: fn.resolve(ColumnScope(_tbl=t))
    elif callable(expr):
        wrapped = lambda t, fn=expr: fn(ColumnScope(_tbl=t))
    else:
        wrapped = lambda t, v=expr: v

    return Measure(
        expr=wrapped,
        description=description,
        requires_unnest=requires_unnest,
        original_expr=raw_expr,
        metadata=dict(metadata or {}),
    )


def _classify_measure(
    fn_or_expr: Any, scope: Any, measure_name: str | None = None
) -> tuple[str, Any]:
    """Classify a measure lambda as ``base`` or ``calc``.

    Runs the lambda once against an :class:`IbisCalcScope`, then walks
    the resulting ibis tree with :func:`analyze_calc_expr`. Pushable
    expressions become base measures (the same lambda runs at agg time
    against the raw ibis table). Post-aggregation expressions become
    :class:`CalcMeasure` records that re-evaluate at query time.

    The legacy ``MeasureScope`` is accepted as the scope argument for
    backwards compatibility with call sites — only its ``tbl`` and
    ``known`` fields are read.
    """
    expr, description, requires_unnest, metadata = _extract_measure_metadata(fn_or_expr)

    base_tbl = getattr(scope, "tbl", None)
    if base_tbl is None:
        base_tbl = getattr(scope, "_tbl", None)
    known = getattr(scope, "known", None)
    if known is None:
        known = getattr(scope, "_known", ())
    known_set = frozenset(known)
    prefer_known = getattr(scope, "prefer_known", None)
    if prefer_known is None:
        prefer_known = getattr(scope, "_prefer_known", ())
    prefer_known_set = frozenset(prefer_known)
    try:
        # Use object.__getattribute__ so ibis Deferred.__getattr__ does not
        # synthesize a resolver for this private marker.
        expr_prefer_known = object.__getattribute__(expr, "__bsl_prefer_known__")
    except AttributeError:
        expr_prefer_known = ()
    if expr_prefer_known is True:
        prefer_known_set = prefer_known_set | known_set
    else:
        prefer_known_set = prefer_known_set | frozenset(expr_prefer_known or ())

    # Pure constants fold into both grouped and ungrouped contexts.
    if isinstance(expr, (int, float)) and not isinstance(expr, bool):
        return ("base", _make_base_measure(expr, description, requires_unnest, metadata))

    if base_tbl is None:
        return ("base", _make_base_measure(expr, description, requires_unnest, metadata))

    # Build virtual aggregated table schema from already-known measures.
    # The dtypes are placeholders — the analyzer cares about structure.
    virtual_schema = {name: "float64" for name in known_set}

    try:
        ibis_expr, vt, totals_vt = evaluate_calc_lambda(
            expr,
            base_tbl,
            known_set,
            virtual_schema,
            priority_measures=prefer_known_set,
        )
    except UnknownMeasureRefError:
        raise
    except Exception:
        # Could not evaluate against the analyzer scope (e.g. lambda
        # uses backend-specific methods MeasureScope didn't reflect).
        # Fall back to base classification — the lambda runs verbatim
        # against the raw ibis table at agg time.
        if not requires_unnest and callable(expr):
            inferred_unnest = _infer_unnest(expr, base_tbl)
            requires_unnest = requires_unnest or inferred_unnest
        return ("base", _make_base_measure(expr, description, requires_unnest, metadata))

    base_op = base_tbl.op() if hasattr(base_tbl, "op") and callable(base_tbl.op) else None
    totals_op = totals_vt.op() if hasattr(totals_vt, "op") and callable(totals_vt.op) else None
    analysis = analyze_calc_expr(
        ibis_expr,
        known_measures=known_set,
        base_table_op=base_op,
        totals_vt_op=totals_op,
    )

    if analysis.pushable or analysis.post_agg_only is False:
        # ``post_agg_only=False`` without ``pushable`` means no window /
        # AllOf / measure deps but the expression touched multiple source
        # tables. Routing to base lets the lambda run verbatim at agg
        # time; if it really does span tables, ibis will surface the
        # error there. Log so the silent fallthrough is visible.
        if not analysis.pushable:
            logger.debug(
                "calc-measure %r references multiple source tables but no measures; "
                "routing to base classification — ibis will validate at agg time.",
                measure_name,
            )
        if not requires_unnest and callable(expr):
            inferred_unnest = _infer_unnest(expr, base_tbl)
            requires_unnest = requires_unnest or inferred_unnest
        return ("base", _make_base_measure(expr, description, requires_unnest, metadata))

    return (
        "calc",
        CalcMeasure(
            expr=expr,
            description=description,
            requires_unnest=requires_unnest,
            depends_on=analysis.depends_on,
            prefer_known=prefer_known_set,
            metadata=metadata,
        ),
    )


def _build_json_definition(
    dims_dict: dict,
    meas_dict: dict,
    calc_meas_dict: Mapping[str, CalcMeasure] | None = None,
    name: str | None = None,
    description: str | None = None,
) -> dict:
    calc_meas_dict = dict(calc_meas_dict or {})
    result = {
        "dimensions": {n: spec.to_json() for n, spec in dims_dict.items()},
        "measures": {
            **{n: spec.to_json() for n, spec in meas_dict.items()},
            **{n: spec.to_json() for n, spec in calc_meas_dict.items()},
        },
        "calculated_measures": {n: spec.to_json() for n, spec in calc_meas_dict.items()},
        "entity_dimensions": {n: spec.to_json() for n, spec in dims_dict.items() if spec.is_entity},
        "event_timestamp": {
            n: spec.to_json() for n, spec in dims_dict.items() if spec.is_event_timestamp
        },
        "time_dimensions": {
            n: spec.to_json() for n, spec in dims_dict.items() if spec.is_time_dimension
        },
        "name": name,
    }
    if description is not None:
        result["description"] = description
    return result


class SemanticTableOp(Relation):
    """Relation with semantic metadata (dimensions and measures).

    Stores ir.Table expression directly to avoid .op() → .to_expr() conversions.

    Note: Accepts both regular ibis.Table and xorq's vendored ibis.Table.
    Regular ibis tables are automatically converted to xorq in __init__.
    """

    table: Any  # Accepts both ir.Table and regular ibis.expr.types.Table
    dimensions: FrozenDict[str, Dimension]
    measures: FrozenDict[str, Measure]
    calc_measures: FrozenDict[str, Any]
    name: str | None = None
    description: str | None = None
    _source_join: Any = field(
        default=None, repr=False
    )  # Track if this wraps a join (SemanticJoinOp) for optimization

    def __init__(
        self,
        table: ir.Table,
        dimensions: dict[str, Dimension] | FrozenDict[str, Dimension],
        measures: dict[str, Measure] | FrozenDict[str, Measure],
        calc_measures: dict[str, Any] | FrozenDict[str, Any],
        name: str | None = None,
        description: str | None = None,
        _source_join: Any = None,
    ) -> None:
        # Accept both regular ibis and xorq tables without conversion
        # This allows using regular ibis by default, xorq only when provided
        super().__init__(
            table=table,
            dimensions=FrozenDict(dimensions)
            if not isinstance(dimensions, FrozenDict)
            else dimensions,
            measures=FrozenDict(measures) if not isinstance(measures, FrozenDict) else measures,
            calc_measures=FrozenDict(calc_measures)
            if not isinstance(calc_measures, FrozenDict)
            else calc_measures,
            name=name,
            description=description,
            _source_join=_source_join,
        )

    def __repr__(self) -> str:
        return _semantic_repr(self)

    @property
    def values(self) -> FrozenOrderedDict[str, Any]:
        dims = self.get_dimensions()
        measures = self.get_measures()
        calc_measures = self.get_calculated_measures()
        # Build enriched table with all dimensions resolved (handles derived deps)
        enriched = _mutate_dimensions_with_dependencies(self.table, dims.keys(), dims)
        base_values = {
            **{col: self.table[col].op() for col in self.table.columns},
            **{name: enriched[name].op() for name in dims},
            **{name: fn(enriched).op() for name, fn in measures.items()},
        }
        # Calc measures are stored as ``CalcMeasure`` objects holding the
        # original lambda. Re-run each one against an ``IbisCalcScope``
        # over ``enriched`` plus a virtual aggregated table whose schema
        # mirrors the base measures. Type inference falls out of ibis
        # naturally; failures are best-effort.
        if calc_measures:
            measure_schema = {
                name: base_values[name].dtype for name in measures if name in base_values
            }
            known_set = frozenset(measures.keys()) | frozenset(calc_measures.keys())
            for name, calc in calc_measures.items():
                fn = calc.expr if isinstance(calc, CalcMeasure) else calc
                try:
                    expr, _vt, _tvt = evaluate_calc_lambda(fn, enriched, known_set, measure_schema)
                    base_values[name] = expr.op()
                except Exception as e:
                    logger.debug("calc-measure type inference failed for %r: %s", name, e)
        return FrozenOrderedDict(base_values)

    @property
    def schema(self):
        fields_dict = {name: str(v.dtype) for name, v in self.values.items()}
        return _make_schema(fields_dict)

    @property
    def json_definition(self) -> Mapping[str, Any]:
        return _build_json_definition(
            self.get_dimensions(),
            self.get_measures(),
            self.get_calculated_measures(),
            self.name,
            self.description,
        )

    @property
    def _dims(self) -> dict[str, Dimension]:
        return dict(self.get_dimensions())

    @property
    def _base_measures(self) -> dict[str, Measure]:
        return dict(self.get_measures())

    @property
    def _calc_measures(self) -> dict[str, Any]:
        return dict(self.get_calculated_measures())

    def get_measures(self) -> Mapping[str, Measure]:
        """Get dictionary of base measures with metadata."""
        return object.__getattribute__(self, "measures")

    def get_dimensions(self) -> Mapping[str, Dimension]:
        """Get dictionary of dimensions with metadata."""
        return object.__getattribute__(self, "dimensions")

    def get_calculated_measures(self) -> Mapping[str, Any]:
        """Get dictionary of calculated measures with metadata."""
        return self.calc_measures

    def get_graph(self) -> dict[str, dict[str, Any]]:
        from ..graph_utils import build_dependency_graph

        return build_dependency_graph(
            self.get_dimensions(),
            self.get_measures(),
            self.get_calculated_measures(),
            self.table,
        )

    def __getattribute__(self, name: str):
        """Override attribute access to return tuples for dimensions/measures.

        This provides a cleaner API where .dimensions returns ('dim1', 'dim2')
        instead of the full FrozenDict. Use get_dimensions() to get the full dict.
        """
        # For special/internal attributes (dunder methods), use default behavior
        # This is critical for xorq's vendored ibis which uses __precomputed_hash__, etc.
        if name.startswith("__") and name.endswith("__"):
            return object.__getattribute__(self, name)

        # Custom behavior for dimensions and measures
        if name == "dimensions":
            dims = object.__getattribute__(self, "dimensions")
            return tuple(dims.keys())
        if name == "measures":
            base_meas = object.__getattribute__(self, "measures")
            calc_meas = object.__getattribute__(self, "calc_measures")
            return tuple(base_meas.keys()) + tuple(calc_meas.keys())

        # Default behavior for everything else
        return object.__getattribute__(self, name)

    def to_untagged(self):
        # Conversion happens at SemanticModel construction; self.table is
        # already xorq when supported, plain ibis when not.
        return self.table


class _SourcePassThroughOp:
    """Mixin for relation ops that pass semantic metadata through unchanged.

    Ops that neither add nor materialize dimensions/measures (filter,
    order-by, limit, unnest) delegate the whole metadata protocol to their
    ``source``. Subclasses may still override individual members (e.g.
    unnest overrides ``values``/``schema`` because it changes types).
    """

    @property
    def values(self) -> FrozenOrderedDict[str, Any]:
        return self.source.values

    @property
    def schema(self) -> Schema:
        return self.source.schema

    def get_dimensions(self) -> Mapping[str, Dimension]:
        """Get dictionary of dimensions from source."""
        return self.source.get_dimensions()

    def get_measures(self) -> Mapping[str, Measure]:
        """Get dictionary of measures from source."""
        return self.source.get_measures()

    def get_calculated_measures(self) -> Mapping[str, Any]:
        """Get dictionary of calculated measures from source."""
        return self.source.get_calculated_measures()


class SemanticFilterOp(_SourcePassThroughOp, Relation):
    source: Relation
    predicate: Callable

    def __init__(self, source: Relation, predicate: Callable) -> None:
        super().__init__(
            source=Relation.__coerce__(source),
            predicate=_ensure_wrapped(predicate),
        )

    def __repr__(self) -> str:
        return _semantic_repr(self)

    def to_untagged(self):
        from ..convert import _Resolver

        all_roots = _find_all_root_models(self.source)
        base_tbl = _to_untagged(self.source)
        pred_fn = _unwrap(self.predicate)
        exact_fields = _exact_filter_fields(pred_fn)
        dim_map = (
            {}
            if isinstance(self.source, SemanticAggregateOp)
            else _get_merged_fields(all_roots, "dimensions", source=self.source)
        )
        if not isinstance(self.source, SemanticAggregateOp) and exact_fields:
            dim_map = _augment_dimensions_with_raw_columns(
                dim_map,
                exact_fields,
                all_roots,
                self.source,
            )
            _validate_qualified_filter_fields(exact_fields, dim_map, all_roots)

        # Enrich table with derived dimensions so multi-level deps
        # (e.g. d_two -> d_one -> distance) resolve correctly in filters.
        # Best-effort: skip dimensions whose columns aren't available yet
        # (e.g. join-based dims); those resolve through the Resolver fallback.
        enriched = base_tbl
        for dim_name in dim_map:
            with contextlib.suppress(TypeError, KeyError, AttributeError):
                enriched = _mutate_dimensions_with_dependencies(
                    enriched, [dim_name], dim_map, overwrite_existing=False
                )

        resolver = _Resolver(enriched, dim_map)
        pred = _resolve_expr(pred_fn, resolver)
        return enriched.filter(pred)


def _classify_fields(
    fields: tuple[str, ...],
    dimensions: dict,
    measures: dict,
) -> tuple[list[str], list[str], list[str]]:
    """Classify fields into dimensions, measures, and raw columns."""
    dims = [f for f in fields if f in dimensions]
    meas = [f for f in fields if f in measures]
    raw = [f for f in fields if f not in dimensions and f not in measures]
    return dims, meas, raw


def _process_nested_access_marker(
    marker: NestedAccessMarker,
    name: str,
    tbl: ir.Table,
) -> tuple[ir.Table, ir.Value]:
    """Process a NestedAccessMarker to unnest and build aggregation expression."""
    unnested = tbl
    for array_col in marker.array_path:
        if array_col in unnested.columns:
            unnested = unnested.unnest(array_col)

    if marker.operation == "count":
        return unnested, unnested.count().name(name)

    expr = getattr(unnested, marker.array_path[0])
    for field_name in marker.field_path:
        expr = getattr(expr, field_name)

    if marker.operation in ("sum", "mean", "min", "max", "nunique"):
        agg_fn = getattr(expr, marker.operation)
        return unnested, agg_fn().name(name)

    raise ValueError(f"Unknown operation: {marker.operation}")


def _evaluate_measures_with_unnesting(
    measure_names: list[str],
    measures: dict,
    tbl: ir.Table,
) -> dict:
    """Evaluate measures and apply automatic unnesting if needed.

    Returns dict with:
        - table: potentially unnested table
        - measure_exprs: list of evaluated measure expressions
        - needs_unnesting: whether unnesting occurred
    """
    meas_exprs = []
    current_tbl = tbl
    needs_unnesting = False

    for name in measure_names:
        result = measures[name](tbl)

        if isinstance(result, NestedAccessMarker):
            current_tbl, meas_expr = _process_nested_access_marker(result, name, current_tbl)
            meas_exprs.append(meas_expr)
            needs_unnesting = True
        else:
            meas_exprs.append(result.name(name))

    return {
        "table": current_tbl,
        "measure_exprs": meas_exprs,
        "needs_unnesting": needs_unnesting,
    }


def _build_select_or_aggregate(
    tbl: ir.Table,
    dim_exprs: list,
    meas_exprs: list,
    raw_exprs: list,
) -> ir.Table:
    """Build appropriate select/aggregate based on what expressions exist."""
    if meas_exprs and dim_exprs:
        return tbl.group_by(dim_exprs).aggregate(meas_exprs)
    if meas_exprs:
        return tbl.aggregate(meas_exprs)
    if dim_exprs or raw_exprs:
        return tbl.select(dim_exprs + raw_exprs)
    return tbl


class SemanticProjectOp(Relation):
    source: Relation
    fields: tuple[str, ...]

    def __init__(self, source: Relation, fields: Iterable[str]) -> None:
        super().__init__(source=Relation.__coerce__(source), fields=tuple(fields))

    def __repr__(self) -> str:
        return _semantic_repr(self)

    @property
    def values(self) -> FrozenOrderedDict[str, Any]:
        src_vals = self.source.values
        return FrozenOrderedDict(
            {k: v for k, v in src_vals.items() if k in self.fields},
        )

    @property
    def schema(self) -> Schema:
        return _SchemaClass(fields=_FrozenOrderedDict({k: v.dtype for k, v in self.values.items()}))

    def to_untagged(self):
        all_roots = _find_all_root_models(self.source)
        tbl = _to_untagged(self.source)

        if not all_roots:
            return tbl.select([getattr(tbl, f) for f in self.fields])

        merged_dimensions = _get_merged_fields(all_roots, "dimensions", source=self.source)
        merged_measures = _get_merged_fields(all_roots, "measures")

        dims, meas, raw_fields = _classify_fields(self.fields, merged_dimensions, merged_measures)

        # Evaluate measures and handle automatic unnesting
        meas_result = _evaluate_measures_with_unnesting(meas, merged_measures, tbl)

        active_tbl = meas_result["table"]
        meas_exprs = meas_result["measure_exprs"]
        needs_unnesting = meas_result["needs_unnesting"]

        # Re-evaluate dimensions on unnested table if needed
        dim_exprs = (
            [merged_dimensions[name](active_tbl).name(name) for name in dims]
            if needs_unnesting
            else [merged_dimensions[name](tbl).name(name) for name in dims]
        )

        # Get raw columns that still exist after unnesting
        raw_exprs = [getattr(active_tbl, name) for name in raw_fields if name in active_tbl.columns]

        return _build_select_or_aggregate(active_tbl, dim_exprs, meas_exprs, raw_exprs)


class SemanticGroupByOp(Relation):
    source: Relation
    keys: tuple[str, ...]

    def __init__(self, source: Relation, keys: Iterable[str]) -> None:
        super().__init__(source=Relation.__coerce__(source), keys=tuple(keys))

    def __repr__(self) -> str:
        return _semantic_repr(self)

    @property
    def values(self) -> FrozenOrderedDict[str, Any]:
        return self.source.values

    @property
    def schema(self) -> Schema:
        return self.source.schema

    def to_untagged(self):
        return _to_untagged(self.source)


@frozen
class _MeasureSpec:
    name: str
    kind: str  # 'agg' or 'calc'
    value: Any


@frozen
class _AggregationPlan:
    agg_specs: FrozenDict[str, Callable]
    calc_specs: FrozenDict[str, Any]
    requested_measures: tuple[str, ...]
    group_by_cols: tuple[str, ...]


def _make_agg_callable(measure: Any) -> Callable:
    """Wrap a base-measure value into a callable that returns an ibis aggregation.

    ``Measure.expr`` is already wrapped with ``ColumnScope`` inside
    :func:`_make_base_measure`, so ``Measure`` instances and raw callables
    (e.g. lifted-reduction stubs that close over a pre-built ibis op) are
    invoked with the raw ibis table directly. Only ``Deferred`` values
    are resolved through ``ColumnScope`` here, since they have no other
    way to bind to the table.
    """
    if _is_deferred(measure):
        return lambda t: measure.resolve(ColumnScope(_tbl=t))
    if isinstance(measure, Measure):
        return lambda t: measure(t)
    if callable(measure):
        return lambda t: measure(t)
    return lambda t, v=measure: v


def _is_dimension_grain_expr(expr: Any) -> bool:
    """True when a probed agg expression is a plain column-grain expression.

    Dimension-grain expressions (e.g. ``t.carrier.upper()``) contain no
    reduction or window; they cannot go through ``Table.aggregate`` and
    are instead applied to the aggregated result via ``mutate`` — ibis
    dereferences group-key field references onto the result table.
    Non-ibis probe values (constants) keep the aggregate path.
    """
    op = _to_node(expr)
    if op is None:
        return False
    try:
        return not any(_is_reduction(n) or _is_window(n) for n in _walk_calc_expr(op))
    except Exception:
        return False


def _build_aggregation_plan(
    aggs: dict,
    keys: tuple,
    scope: Any,
    is_post_agg: bool,
    merged_base_measures: dict,
    merged_calc_measures: dict,
    tbl: ir.Table,
) -> _AggregationPlan:
    """Split requested aggregations into base aggs and calc-measure lambdas.

    Each entry in ``aggs`` is a callable. We resolve it once against the
    measure scope to determine whether it refers to a base measure (yields
    a ``Measure``-like callable that produces an ibis aggregation) or a
    calc measure (a ``CalcMeasure`` recorded in ``merged_calc_measures``
    or an inline post-aggregation expression).

    Inline ad-hoc lambdas that look like calc expressions (use
    ``t.measure_name`` or ``t.all(...)``) are classified on the fly via
    :func:`_classify_measure` and routed to ``calc_specs``.
    """
    agg_specs: dict[str, Callable] = {}
    calc_specs: dict[str, CalcMeasure] = {}

    base_tbl = getattr(scope, "tbl", None)
    if base_tbl is None:
        base_tbl = getattr(scope, "_tbl", None)
    if base_tbl is None:
        base_tbl = tbl

    # Query-local entries can reference each other (e.g. a derived
    # column built on a sibling derivation from the same aggregate /
    # chained mutate) and the group-by keys (which are columns of the
    # result table — windows order by them). Make both resolvable
    # during inline classification by augmenting the scope's
    # known-measure set, which also seeds the virtual aggregated
    # table's schema.
    scope_known = tuple(getattr(scope, "known", ()) or ())
    prior_local_names: list[str] = []

    def remember_local_name(local_name: str) -> None:
        if local_name.startswith("_measure_") or local_name in prior_local_names:
            return
        prior_local_names.append(local_name)

    for name, fn_wrapped in aggs.items():
        fn = _unwrap(fn_wrapped)

        if is_post_agg:
            # Wrap raw user callables with ColumnScope (via Measure) so a
            # re-aggregation lambda like ``t.flights.carrier.nunique()``
            # routes through the NestedAccessMarker pipeline in
            # _compile_aggregation. Without the wrap, t.flights returns a
            # raw ArrayColumn and struct-field access blows up before the
            # marker can be produced.
            if callable(fn) and not _is_deferred(fn) and not isinstance(fn, Measure):
                fn = _make_base_measure(fn, None, (), {})
            agg_specs[name] = _make_agg_callable(fn)
            remember_local_name(name)
            continue

        # Recognize bare-name lambdas (``lambda t, n=name: t[n]``) that
        # the SemanticAggregate.aggregate API generates for measure
        # lookups by name. These should resolve to the named measure,
        # suffix-matching prefixed names on joined models.
        ref_name = _detect_bare_name_lambda(fn)
        if ref_name is not None:
            resolved = _resolve_short_name(ref_name, merged_base_measures, merged_calc_measures)
            if resolved is not None:
                if resolved in merged_base_measures:
                    agg_specs[name] = _make_agg_callable(merged_base_measures[resolved])
                    remember_local_name(name)
                    continue
                if resolved in merged_calc_measures:
                    calc_specs[name] = merged_calc_measures[resolved]
                    remember_local_name(name)
                    continue

        # Otherwise classify the inline lambda on the fly.
        if not is_post_agg:
            classify_scope = MeasureScope(
                _tbl=base_tbl,
                _known=tuple(dict.fromkeys(scope_known + tuple(prior_local_names) + tuple(keys))),
                _prefer_known=tuple(prior_local_names),
            )
        else:
            classify_scope = scope
        kind, value = _classify_measure(fn, classify_scope, name)
        if kind == "calc":
            calc_specs[name] = value
        else:
            agg_specs[name] = _make_agg_callable(value)
        remember_local_name(name)

    # Auto-include base-measure dependencies referenced by calc measures
    # so the aggregation produces the columns the calc lambdas read.
    # Walk transitively so calc-of-calc chains pull all needed bases.
    if calc_specs:

        def _resolve_dep(ref: str) -> str | None:
            """Resolve a dependency name against base/calc measures.

            On joined models, calc measures captured ``depends_on`` with
            short names (``flight_count``); the merged dictionaries hold
            prefixed names (``flights.flight_count``). Suffix-match when
            the exact name is missing.
            """
            if ref in merged_base_measures or ref in merged_calc_measures:
                return ref
            suffix = f".{ref}"
            base_matches = [k for k in merged_base_measures if k.endswith(suffix)]
            calc_matches = [k for k in merged_calc_measures if k.endswith(suffix)]
            matches = base_matches + calc_matches
            if len(matches) == 1:
                return matches[0]
            return None

        worklist = list(calc_specs.values())
        seen_calcs: set[str] = set(calc_specs.keys())
        while worklist:
            cm = worklist.pop()
            for ref in cm.depends_on:
                resolved_ref = _resolve_dep(ref)
                if resolved_ref is None or resolved_ref in agg_specs:
                    continue
                if resolved_ref in merged_base_measures:
                    agg_specs[resolved_ref] = _make_agg_callable(merged_base_measures[resolved_ref])
                elif resolved_ref in merged_calc_measures and resolved_ref not in seen_calcs:
                    dep_cm = merged_calc_measures[resolved_ref]
                    calc_specs[resolved_ref] = dep_cm
                    seen_calcs.add(resolved_ref)
                    if isinstance(dep_cm, CalcMeasure):
                        worklist.append(dep_cm)

    return _AggregationPlan(
        agg_specs=FrozenDict(agg_specs),
        calc_specs=FrozenDict(calc_specs),
        requested_measures=tuple(aggs.keys()),
        group_by_cols=tuple(keys),
    )


def _make_rebindable_reduction_spec(reduction_expr, origin_op) -> Callable:
    """Wrap a lifted inline reduction as an agg-spec callable.

    The reduction was built against the pre-totals base table. Field-based
    reductions (``Sum(Field(base, x))``) survive on a mutated descendant via
    ibis's field dereferencing, but relation-argument reductions
    (``CountStar(base)``) hold the relation itself and fail the aggregate
    integrity check unless rebound to the table actually being aggregated.
    """

    def spec(t, _r=reduction_expr, _origin=origin_op):
        target = _to_op(t)
        if target is _origin:
            return _r
        return _to_op(_r).replace({_origin: target}).to_expr()

    return spec


def _compile_aggregation(
    base_tbl,
    by_cols: list[str],
    agg_specs: dict[str, Callable],
    calc_specs: dict[str, CalcMeasure],
    known_measures: frozenset[str],
    requested_measures: list[str] | None = None,
    is_post_agg: bool = False,
):
    """Run base aggregations on ``base_tbl``, then apply calc measures.

    Replaces the legacy ``compile_grouped_with_all`` pipeline. Calc
    measures are recomputed at query time by re-running their lambda
    against an :class:`IbisCalcScope` over ``base_tbl`` plus a virtual
    aggregated table that mirrors the real result schema. Nested-array
    aggregations surface as :class:`NestedAccessMarker` values and are
    routed through :func:`_compile_aggregation_with_nested`.
    """
    # --- Pre-process calc specs ---------------------------------------
    # Run the analyzer once per calc, then route inline reductions
    # through the lift pass. ``lifted_calc_specs[name]`` carries the
    # rewritten expression and the virtual tables it references;
    # ``classifications[name]`` carries the structural analysis.
    # ``None`` lift means the lambda blew up — we'll re-evaluate from
    # scratch in the apply loop.
    #
    # Build the virtual schema with *real* dtypes derived from the base
    # aggregations. Using a placeholder dtype (``float64`` for
    # everything) lets ibis silently elide ``column.cast(float64)`` as a
    # no-op during ``evaluate_calc_lambda``; after the substitution
    # ``Field(virtual_agg) → Field(real_agg)`` the Cast is gone but the
    # real column is int64, so ``int / int * 100`` returns 0. Probing
    # ``agg_specs[n](base_tbl).type()`` gives the analyzer the same
    # dtype the user's calc will see at compile time.
    base_op = _to_op(base_tbl)
    virtual_schema_real: dict[str, Any] = {}
    for n in known_measures:
        if n in agg_specs:
            try:
                virtual_schema_real[n] = agg_specs[n](base_tbl).type()
            except Exception as exc:
                logger.debug(
                    "could not probe dtype for measure %r; falling back to float64: %s",
                    n,
                    exc,
                )
                virtual_schema_real[n] = "float64"
        else:
            virtual_schema_real[n] = "float64"

    # Group-by keys are columns of the result table; calc expressions
    # may reference them (windows ordering by a key, derived-dimension
    # references). Surface them on the virtual aggregated table so the
    # lambda evaluates, and in the known set so non-column keys (derived
    # dimensions) resolve to virtual fields.
    for c in by_cols:
        if c not in virtual_schema_real:
            try:
                virtual_schema_real[c] = base_tbl[c].type()
            except Exception:
                virtual_schema_real[c] = "string"
    known_with_keys = frozenset(known_measures) | frozenset(by_cols)

    lifted_calc_specs: dict[str, tuple[Any, Any, Any] | None] = {}
    classifications: dict[str, Any] = {}
    preproc_errors: dict[str, Exception] = {}
    needs_totals = False
    if calc_specs:
        for name, cm in calc_specs.items():
            try:
                virtual_schema = dict(virtual_schema_real)
                expr, vt, totals_vt = evaluate_calc_lambda(
                    cm.expr,
                    base_tbl,
                    known_with_keys,
                    virtual_schema,
                    priority_measures=cm.prefer_known,
                )
                new_expr, new_vt, new_totals_vt, lifted = lift_inline_reductions(
                    expr,
                    vt,
                    base_tbl,
                    totals_virtual_agg_tbl=totals_vt,
                    group_keys=by_cols,
                )
                analysis = analyze_calc_expr(
                    new_expr,
                    known_measures=known_measures,
                    base_table_op=base_op,
                    totals_vt_op=_to_op(new_totals_vt),
                )
                lifted_calc_specs[name] = (new_expr, new_vt, new_totals_vt)
                classifications[name] = analysis
                if analysis.references_AllOf:
                    needs_totals = True
                for anon_name, reduction_expr in lifted.items():
                    if anon_name not in agg_specs:
                        agg_specs[anon_name] = _make_rebindable_reduction_spec(
                            reduction_expr, base_op
                        )
            except WindowedBaseReductionError:
                # The apply-time fallback re-evaluates the lambda against
                # the aggregated result, which would silently give the
                # windowed reduction different (output-grain) semantics —
                # surface the soundness error instead.
                raise
            except Exception as exc:
                logger.debug(
                    "calc-measure lift/classify failed for %r; will re-evaluate at apply time: %s",
                    name,
                    exc,
                )
                lifted_calc_specs[name] = None
                preproc_errors[name] = exc

    nested_marker_specs: dict[str, Any] = {}
    regular_specs: dict[str, Callable] = {}
    dimension_grain_specs: dict[str, Callable] = {}
    post_agg_deriv_specs: dict[str, Callable] = {}
    for name, fn in agg_specs.items():
        try:
            probe = fn(base_tbl)
        except Exception:
            # On a post-aggregate aggregation, a folded ``.mutate()``
            # derivation may reference sibling aggregate outputs (e.g.
            # ``session_end_min - session_start_min``) that don't exist on
            # ``base_tbl`` — they are produced by this aggregation. Such a
            # spec can't go through ``Table.aggregate``; evaluate it against
            # the built result instead (historical post-agg ``.mutate()``
            # semantics). Non-post-agg failures keep the base-spec path.
            if is_post_agg:
                post_agg_deriv_specs[name] = fn
            else:
                regular_specs[name] = fn
            continue
        if isinstance(probe, NestedAccessMarker):
            nested_marker_specs[name] = probe
        elif by_cols and _is_dimension_grain_expr(probe):
            # Column-grain expression (no reduction/window) — cannot go
            # through Table.aggregate. Apply to the aggregated result
            # instead: ibis dereferences group-key fields onto it, so
            # expressions over group keys (e.g. t.carrier.upper()) work.
            dimension_grain_specs[name] = fn
        else:
            regular_specs[name] = fn

    # --- Attach windowed totals to base ------------------------------
    # When any calc references ``t.all(measure_ref)``, compute that
    # measure's formula as a window function over the entire base
    # *before* group_by, then carry it through the per-group aggregation
    # via ``arbitrary()``. This expresses "ungrouped aggregate alongside
    # grouped one" as a single-pass query — no cross-join, no
    # shared-ancestor collapse, compiles to SQL on every backend
    # supporting window functions. Skipped on the nested-array path:
    # totals across multiple grains aren't well-defined; we surface a
    # clear error in the apply loop.
    #
    # Calc-of-calc-AllOf (an AllOf-using calc that references a calc,
    # not a base measure — e.g. ``t.all(t.avg_distance)`` where
    # ``avg_distance`` is itself a calc) is handled in two passes:
    # first transitively expand to the base measures; attach window
    # totals for those; then post-aggregation derive the calc's totals
    # value via :func:`attach_calc_totals`.
    totals_arbitrary_specs: dict[str, Callable] = {}
    if needs_totals and regular_specs and not nested_marker_specs:
        totals_for_base: set[str] = set()
        # Transitive expansion: for AllOf-using calcs, follow calc deps
        # through ``classifications`` until we land on base measures.
        work: list[str] = []
        for c in classifications.values():
            if c.references_AllOf:
                for d in c.depends_on:
                    if d in regular_specs:
                        totals_for_base.add(d)
                    elif d in calc_specs:
                        work.append(d)
        seen: set[str] = set()
        while work:
            calc_dep = work.pop()
            if calc_dep in seen:
                continue
            seen.add(calc_dep)
            cls = classifications.get(calc_dep)
            if cls is None:
                continue
            for d in cls.depends_on:
                if d in regular_specs:
                    totals_for_base.add(d)
                elif d in calc_specs:
                    work.append(d)

        if totals_for_base:
            base_tbl, totals_arbitrary_specs = attach_windowed_totals(
                base_tbl, regular_specs, totals_for_base, TOTALS_PREFIX
            )

    if not nested_marker_specs:
        if by_cols or regular_specs or totals_arbitrary_specs:
            agg_exprs = {n: f(base_tbl) for n, f in regular_specs.items()}
            for tn, tf in totals_arbitrary_specs.items():
                agg_exprs[tn] = tf(base_tbl)
            if by_cols:
                real_agg_tbl = base_tbl.group_by([base_tbl[c] for c in by_cols]).aggregate(
                    **agg_exprs
                )
            else:
                real_agg_tbl = base_tbl.aggregate(**agg_exprs)
        else:
            real_agg_tbl = base_tbl.aggregate()
    else:
        real_agg_tbl = _compile_aggregation_with_nested(
            base_tbl, by_cols, regular_specs, nested_marker_specs
        )

    # --- Apply dimension-grain specs ---------------------------------
    # Evaluated against the current base table (after any windowed-totals
    # mutation) and added to the result via mutate; ibis dereferences
    # group-key field references onto the aggregated table.
    for name, fn in dimension_grain_specs.items():
        real_agg_tbl = real_agg_tbl.mutate(**{name: fn(base_tbl)})

    # --- Apply post-aggregate derivations ----------------------------
    # Specs that reference sibling aggregate outputs (and so could not be
    # probed against ``base_tbl``) are evaluated against the built result,
    # where those columns now exist. Applied in insertion order so a
    # derivation may build on an earlier one in the same call.
    for name, fn in post_agg_deriv_specs.items():
        real_agg_tbl = real_agg_tbl.mutate(**{name: fn(real_agg_tbl)})

    # --- Derive calc-of-calc totals ----------------------------------
    # If any AllOf-using calc references another calc (transitively),
    # the windowed-totals pass attached only the base totals. Now that
    # ``real_agg_tbl`` has those base totals as columns, we evaluate
    # each needed calc lambda against the totals columns to derive the
    # calc's totals value (constant across rows).
    if calc_specs and totals_arbitrary_specs:
        real_agg_tbl = attach_calc_totals(real_agg_tbl, calc_specs, classifications, TOTALS_PREFIX)

    # --- Apply calc measures -----------------------------------------
    if calc_specs:
        # ``real_agg_tbl`` already carries ``__bsl_totals__<name>``
        # columns when totals were attached above. Calc compilation
        # rewrites ``Field(totals_vt, name) → Field(real_agg, "__bsl_totals__<name>")``
        # directly; no separate cross-joined table is needed.
        real_with_totals = real_agg_tbl if totals_arbitrary_specs else None
        cur_known = known_with_keys | frozenset(calc_specs.keys())

        ordered = _topological_calc_order(calc_specs, base_tbl, known_measures)
        for name in ordered:
            spec = lifted_calc_specs.get(name)
            if spec is None:
                # Preprocessing could not classify this calc against the
                # virtual-aggregate scope. The common cause is a window
                # that references both a prior derived measure and a
                # group-key/base column: the virtual scope resolves the
                # former onto the virtual table and the latter onto the
                # base table, so ibis cannot bind the mixed-relation window
                # and yields a bare Deferred. By this point the calcs are
                # processed in dependency order, so every name the lambda
                # needs already exists as a real column on ``real_agg_tbl``.
                # Evaluate it there directly (the historical post-aggregate
                # ``.mutate()`` semantics) and move on. Falls through to the
                # re-lift path below only when that fails — e.g. a genuine
                # inline base reduction over a column consumed by the
                # aggregation.
                try:
                    post_scope = MeasureScope(_tbl=real_agg_tbl, _known=[], _post_agg=True)
                    resolved = _resolve_expr(calc_specs[name].expr, post_scope)
                    real_agg_tbl = real_agg_tbl.mutate(resolved.name(name))
                    if real_with_totals is not None:
                        real_with_totals = real_agg_tbl
                    continue
                except Exception as exc:
                    logger.debug(
                        "post-aggregate fallback for calc %r failed; re-lifting against base: %s",
                        name,
                        exc,
                    )

                # Lift failed at preprocessing; re-evaluate AND re-lift so
                # inline base reductions (``t.distance.sum() / t.all(...)``)
                # don't reach _compile_calc_measure_impl as bare base
                # reductions ibis can't compile through mutate.
                fn = calc_specs[name].expr
                virtual_schema = {
                    col: real_agg_tbl[col].type()
                    for col in real_agg_tbl.columns
                    if col in cur_known
                }
                expr0, vt0, totals_vt0 = evaluate_calc_lambda(
                    fn,
                    base_tbl,
                    cur_known,
                    virtual_schema,
                    priority_measures=calc_specs[name].prefer_known,
                )
                rewritten_expr, rewritten_vt, rewritten_totals_vt, lifted = lift_inline_reductions(
                    expr0,
                    vt0,
                    base_tbl,
                    totals_virtual_agg_tbl=totals_vt0,
                    group_keys=by_cols,
                )
                if lifted:
                    # The lift produced anonymous base reductions that
                    # would need to be added to the per-group aggregation,
                    # but that has already been built. Surface the original
                    # preprocessing failure rather than letting unbound
                    # Field references reach ibis.
                    orig = preproc_errors.get(name)
                    raise RuntimeError(
                        f"Calc measure {name!r} contains inline base reductions "
                        "that could not be lifted at preprocessing time."
                    ) from orig
                analysis = analyze_calc_expr(
                    rewritten_expr,
                    known_measures=known_measures,
                    base_table_op=base_op,
                    totals_vt_op=_to_op(rewritten_totals_vt),
                )
            else:
                rewritten_expr, rewritten_vt, rewritten_totals_vt = spec
                analysis = classifications[name]

            if analysis.references_AllOf:
                if real_with_totals is None:
                    raise TotalsNotAvailableError(
                        f"Calc measure {name!r} references t.all(...) but no totals "
                        "columns were attached. This typically means the model contains "
                        "nested-array measures (which compile at multiple grains and "
                        "don't yet support totals), or the AllOf reference targets a "
                        "calc measure rather than a base measure (calc-of-calc-totals "
                        "is not yet supported via the windowed-totals path)."
                    )
                compiled = _compile_calc_measure_impl(
                    rewritten_expr,
                    rewritten_vt,
                    real_agg_tbl,
                    totals_virtual_agg_tbl=rewritten_totals_vt,
                    real_with_totals=real_agg_tbl,
                )
                real_agg_tbl = real_agg_tbl.mutate(**{name: compiled})
                real_with_totals = real_agg_tbl
            else:
                compiled = _compile_calc_measure_impl(rewritten_expr, rewritten_vt, real_agg_tbl)
                real_agg_tbl = real_agg_tbl.mutate(**{name: compiled})
                if real_with_totals is not None:
                    real_with_totals = real_agg_tbl

    # Drop the synthetic ``__bsl_totals__<name>`` columns so the
    # result schema only carries user-requested measures.
    if calc_specs:
        real_agg_tbl = _drop_totals_columns(real_agg_tbl, TOTALS_PREFIX)

    if requested_measures is not None:
        select_cols = list(
            dict.fromkeys(list(by_cols) + list(requested_measures) + list(calc_specs.keys()))
        )
        available = frozenset(real_agg_tbl.columns)
        select_cols = [c for c in select_cols if c in available]
        if select_cols:
            real_agg_tbl = real_agg_tbl.select([real_agg_tbl[c] for c in select_cols])

    return real_agg_tbl


def _compile_aggregation_with_nested(
    base_tbl,
    by_cols: list[str],
    regular_specs: dict[str, Callable],
    nested_specs: dict[str, Any],
):
    """Compile aggregations when nested-array measures are present.

    Each array path is unnested in isolation, aggregated at its own
    grain, and joined back to the session-level table on ``by_cols``.
    The new calc-compiler path layers on top of the resulting joined
    table via :func:`apply_calc_measures`.
    """
    from ..nested_compile import (
        build_nested_level_table,
        build_session_table,
        join_tables,
    )

    nested_by_path: dict[tuple[str, ...], dict[str, tuple]] = {}
    for name, marker in nested_specs.items():
        nested_by_path.setdefault(marker.array_path, {})[name] = (
            regular_specs.get(name) or (lambda t, m=marker: m),
            marker,
        )

    result_tables: list = []
    if regular_specs:
        regular_results = {n: (f, f(base_tbl)) for n, f in regular_specs.items()}
        session_table = build_session_table(base_tbl, by_cols, regular_results)
        if session_table is not None:
            result_tables.append(session_table)

    for array_path, measures in nested_by_path.items():
        level_table = build_nested_level_table(base_tbl, by_cols, array_path, measures)
        result_tables.append(level_table)

    if not result_tables:
        if by_cols:
            return base_tbl.group_by([base_tbl[c] for c in by_cols]).aggregate()
        return base_tbl.aggregate()

    return join_tables(by_cols, result_tables)


def _resolve_short_name(
    name: str,
    merged_base_measures: dict,
    merged_calc_measures: dict,
) -> str | None:
    """Match ``name`` against merged measure dicts, allowing suffix lookup."""
    return resolve_suffix(name, merged_base_measures, merged_calc_measures)


def _topological_calc_order(
    calc_specs: dict[str, CalcMeasure],
    base_tbl,
    known_measures: frozenset[str],
) -> list[str]:
    """Order calc measures by ``CalcMeasure.depends_on`` so deps compile first."""
    deps = {name: set(cm.depends_on) for name, cm in calc_specs.items()}
    return topological_order_from_deps(calc_specs, deps)


def _detect_bare_name_lambda(fn: Any) -> str | None:
    """Return the captured name when ``fn`` was generated by ``make_bare_ref_lambda``.

    Read the ``_bsl_bare_ref`` sentinel attribute set at the API site —
    sniffing ``__defaults__`` was unreliable because user lambdas with
    arbitrary string defaults (e.g. ``lambda t, c=col, op=op: getattr(...)
    ``) collide with the trivial ``lambda t, n=name: t[n]`` shape and
    silently misroute as bare references.
    """
    if not callable(fn):
        return None
    name = getattr(fn, "_bsl_bare_ref", None)
    if isinstance(name, str):
        return name
    return None


def make_bare_ref_lambda(name: str):
    """Build a ``lambda t: t[name]`` tagged for fast-path measure lookup.

    Use this anywhere the BSL surface needs to construct a measure-name
    passthrough callable: it sets ``_bsl_bare_ref`` so
    :func:`_detect_bare_name_lambda` can route the call straight to the
    referenced base or calc measure without re-running the analyzer.
    """
    fn = lambda t, _n=name: t[_n]  # noqa: E731
    fn._bsl_bare_ref = name
    return fn


# ---------------------------------------------------------------------------
# Pre-aggregation helpers (fan-out / chasm trap prevention)
# ---------------------------------------------------------------------------


@frozen
class _JoinTreeInfo:
    """Information collected from the join tree for pre-aggregation decisions."""

    has_join_many: bool
    table_cardinalities: dict  # table_name → "one"|"many"|"root"
    table_join_keys: dict  # table_name → {raw_col_names}
    table_ops: dict  # table_name → SemanticTableOp


def _collect_join_tree_info(join_op: SemanticJoinOp) -> _JoinTreeInfo:
    """Walk the join tree to collect cardinality and join key information."""
    table_cardinalities: dict[str, str] = {}
    table_ops: dict[str, SemanticTableOp] = {}

    def walk(node, inherited_cardinality: str | None = None):
        if isinstance(node, SemanticJoinOp):
            walk(node.left, inherited_cardinality=inherited_cardinality)
            if inherited_cardinality == "many" or node.cardinality == "many":
                right_cardinality = "many"
            elif inherited_cardinality == "cross" or node.cardinality == "cross":
                right_cardinality = "cross"
            else:
                right_cardinality = "one"
            walk(node.right, inherited_cardinality=right_cardinality)
        elif isinstance(node, SemanticTableOp):
            source_join = getattr(node, "_source_join", None)
            if source_join is not None:
                walk(source_join, inherited_cardinality=inherited_cardinality)
                return
            name = node.name
            if name:
                table_ops[name] = node
                if inherited_cardinality in ("many", "cross"):
                    table_cardinalities[name] = inherited_cardinality
                elif name not in table_cardinalities:
                    table_cardinalities[name] = "one"
        else:
            source = getattr(node, "source", None)
            if source is None:
                return
            roots = _find_all_root_models(node)
            if len(roots) == 1 and roots[0].name:
                # A pass-through wrapper used as a join operand (notably a
                # pre-join filter) is the executable source for this leg.
                # Retain it instead of silently replacing it with the raw leaf.
                name = roots[0].name
                table_ops[name] = node
                if inherited_cardinality in ("many", "cross"):
                    table_cardinalities[name] = inherited_cardinality
                elif name not in table_cardinalities:
                    table_cardinalities[name] = "one"
            else:
                walk(source, inherited_cardinality=inherited_cardinality)

    walk(join_op)

    # The leftmost leaf of the root is the "root" table
    def find_leftmost(node):
        if isinstance(node, SemanticJoinOp):
            return find_leftmost(node.left)
        if isinstance(node, SemanticTableOp) and node._source_join is not None:
            return find_leftmost(node._source_join)
        source = getattr(node, "source", None)
        if source is not None:
            return find_leftmost(source)
        return getattr(node, "name", None)

    root_name = find_leftmost(join_op)
    if root_name:
        table_cardinalities[root_name] = "root"

    has_join_many = any(c == "many" for c in table_cardinalities.values())
    # table_join_keys is used by the pre-aggregation path and by
    # deferred join detection.  Collect for all join trees.
    table_join_keys = join_op._collect_join_keys_for_leaves()

    return _JoinTreeInfo(
        has_join_many=has_join_many,
        table_cardinalities=table_cardinalities,
        table_join_keys=table_join_keys,
        table_ops=table_ops,
    )


def _validate_preaggregation_join_predicates(join_op: SemanticJoinOp) -> None:
    """Require source-preaggregated joins to be plain field equijoins.

    The preaggregation planner uses join-key bridges to reconnect source-grain
    aggregates.  Those bridges preserve conjunctions of direct field equality
    pairs; they cannot reproduce inequalities, OR predicates, casts, or other
    transformed expressions.  Accepting those shapes and retaining only the
    accessed column names produces a different row set from the actual join, so
    fail closed until the planner carries the complete predicate expression.
    """
    from ..convert import _Resolver

    def _is_field(node) -> bool:
        return type(node).__name__ == "Field"

    def _valid(node, left_rel, right_rel) -> bool:
        node_name = type(node).__name__
        if node_name == "And":
            return _valid(node.left, left_rel, right_rel) and _valid(
                node.right, left_rel, right_rel
            )
        if node_name != "Equals":
            return False
        left_arg, right_arg = node.left, node.right
        if not (_is_field(left_arg) and _is_field(right_arg)):
            return False
        left_arg_rel = getattr(left_arg, "rel", None)
        right_arg_rel = getattr(right_arg, "rel", None)
        return (left_arg_rel is left_rel and right_arg_rel is right_rel) or (
            left_arg_rel is right_rel and right_arg_rel is left_rel
        )

    def _walk(node) -> None:
        if not isinstance(node, SemanticJoinOp):
            return
        _walk(node.left)
        _walk(node.right)
        if node.cardinality == "cross":
            return
        if node.on is None:
            raise ValueError(
                "Source-aware aggregation requires an explicit equijoin predicate; "
                "use join_cross() for a Cartesian product."
            )

        left_tbl = (
            node.left.to_untagged(parent_requirements=None)
            if isinstance(node.left, SemanticJoinOp)
            else _to_untagged(node.left)
        )
        right_tbl = (
            node.right.to_untagged(parent_requirements=None)
            if isinstance(node.right, SemanticJoinOp)
            else _to_untagged(node.right)
        )
        try:
            predicate = node.on(_Resolver(left_tbl), _Resolver(right_tbl))
            _reject_bool_resolution(predicate, node.on)
            predicate_op = predicate.op()
        except Exception as exc:
            raise ValueError(
                "Could not validate join predicate for source-aware aggregation. "
                "Use a conjunction of direct field equalities such as "
                "`lambda left, right: left.id == right.id`."
            ) from exc
        if not _valid(predicate_op, _to_op(left_tbl), _to_op(right_tbl)):
            raise ValueError(
                "Source-aware aggregation supports only conjunctions of direct "
                "field equijoins. Inequality, OR, cast, and transformed join "
                "predicates cannot be preaggregated soundly; aggregate each "
                "model first or restate the relationship as plain equality keys."
            )

    _walk(join_op)


@frozen
class _DeferrableJoin:
    """A join_one that can be deferred until after aggregation."""

    table_name: str  # Name of the right (dimension) table
    table_op: Any  # SemanticTableOp of the right table
    join_keys_left: tuple  # Column names on the left side of the join
    on_predicate: Any  # Original join predicate (preserves key pairing)
    deferred_dims: tuple  # Prefixed dimension names to add post-agg


def _table_filter_resolver(
    raw_tbl,
    table_op,
    table_name,
    requested_fields: Iterable[str] = (),
):
    """Build a filter resolver scoped to one source table.

    Exposes the table's declared dimensions under both their bare and
    table-prefixed names (raw columns resolve via the fallback), so
    ownership checks resolve ``t["orders.status"]`` only against
    ``orders``.
    """
    from ..convert import _Resolver

    dims = dict(_get_field_dict(table_op, "dimensions"))
    if table_name:
        for dname, dim in list(dims.items()):
            dims[f"{table_name}.{dname}"] = dim
        raw_columns = frozenset(raw_tbl.columns)
        for field_name in requested_fields:
            if "." not in field_name:
                continue
            prefix, raw_name = field_name.split(".", 1)
            if prefix == table_name and raw_name in raw_columns and field_name not in dims:
                dims[field_name] = Dimension(expr=lambda t, _name=raw_name: t[_name])
    return _Resolver(raw_tbl, dims)


_FIELD_TYPES = tuple({ibis_ops.Field, xorq_ops.Field})
_AND_TYPES = tuple({ibis_ops.And, xorq_ops.And})


def _leaf_rel_types():
    """Base relation classes for both ibis flavors (plus xorq Read)."""
    from .._xorq import Read as _XorqRead

    types = {
        ibis_ops.DatabaseTable,
        ibis_ops.InMemoryTable,
        xorq_ops.DatabaseTable,
        xorq_ops.InMemoryTable,
    }
    if _XorqRead is not None:
        types.add(_XorqRead)
    return tuple(types)


def _flatten_and_legs(expr):
    """Flatten a boolean expression's top-level AND chain into legs."""
    op = expr.op()
    if isinstance(op, _AND_TYPES):
        return _flatten_and_legs(op.left.to_expr()) + _flatten_and_legs(op.right.to_expr())
    return [expr]


def _value_fields(value_op):
    """Fields referenced by a value op, without descending into relations.

    Descending into a Field's relation would surface every column of the
    join tree; provenance only wants the fields the value itself reads.
    """
    from ..graph_utils import gen_children_of

    out, stack, seen = [], [value_op], set()
    while stack:
        node = stack.pop()
        if node in seen:
            continue
        seen.add(node)
        if isinstance(node, _FIELD_TYPES):
            out.append(node)
            continue
        if isinstance(node, (Relation, xorq_ops.Relation)):
            continue
        stack.extend(gen_children_of(node))
    return out


def _field_base_relations(field_op, leaf_types, guard=0):
    """Resolve a Field down to the base relation(s) its value derives from."""
    if guard > 100:
        return {None}
    rel, name = field_op.rel, field_op.name
    if isinstance(rel, leaf_types):
        return {rel}
    values = getattr(rel, "values", None)
    if values is not None and name in values:
        bases = set()
        for f in _value_fields(values[name]):
            bases |= _field_base_relations(f, leaf_types, guard + 1)
        return bases
    parent = getattr(rel, "parent", None)
    if parent is not None:
        return _field_base_relations(field_op.__class__(parent, name), leaf_types, guard + 1)
    return {rel}


def _base_rel_key(rel):
    """Structural fingerprint for a base relation.

    Separately-built untagged tables wrap distinct backend instances, so
    node equality fails even for the same physical table; match on class,
    table name and schema instead.
    """
    try:
        schema_names = tuple(rel.schema.names)
    except Exception:
        schema_names = ()
    return (type(rel).__name__, getattr(rel, "name", None), schema_names)


def _leg_source_tables(leg_expr, base_rel_to_table, leaf_types):
    """Names of the source tables a filter leg's fields derive from."""
    sources = set()
    for f in _value_fields(leg_expr.op()):
        for base in _field_base_relations(f, leaf_types):
            sources.add(base_rel_to_table.get(_base_rel_key(base), "__unknown__"))
    return sources


def _inline_to_base_op(node, leaf_types, target_tbl=None, guard=0):
    """Rewrite a value op so every Field references a base relation.

    Projection/filter chains between the joined table and the base are
    inlined, producing an expression that can be re-applied to the owning
    table's raw table (row-precise filter pushdown). When ``target_tbl``
    is given, base fields are rebased onto it by column name — the join's
    copy of a base relation wraps a different backend instance, so node
    identity alone would fail the Filter integrity check.
    """
    if guard > 200:
        raise ValueError("expression too deep to rebind")
    if isinstance(node, _FIELD_TYPES):
        rel, name = node.rel, node.name
        if isinstance(rel, leaf_types):
            if target_tbl is not None:
                return target_tbl[name].op()
            return node
        values = getattr(rel, "values", None)
        if values is not None and name in values:
            return _inline_to_base_op(values[name], leaf_types, target_tbl, guard + 1)
        parent = getattr(rel, "parent", None)
        if parent is not None:
            return _inline_to_base_op(
                node.__class__(parent, name), leaf_types, target_tbl, guard + 1
            )
        return node
    if isinstance(node, (Relation, xorq_ops.Relation)):
        raise ValueError("cannot rebind a predicate containing a subquery")

    def _tx(a):
        if isinstance(a, tuple):
            return tuple(_tx(x) for x in a)
        if isinstance(a, _FIELD_TYPES) or hasattr(a, "__argnames__"):
            return _inline_to_base_op(a, leaf_types, target_tbl, guard + 1)
        return a

    new_args = [_tx(a) for a in node.args]
    if all(n is o for n, o in zip(new_args, node.args, strict=False)):
        return node
    return node.__class__(**dict(zip(node.__argnames__, new_args, strict=False)))


def _find_deferrable_joins(
    join_op,
    group_by_keys: tuple[str, ...],
    agg_names: dict,
    all_roots: list,
    join_tree_info: _JoinTreeInfo,
    filters: list | None = None,
) -> list[_DeferrableJoin]:
    """Identify join_one ops that can be deferred until after aggregation.

    A join is deferrable when:
    - cardinality == "one"
    - Right table has is_entity dims matching the join key
    - No measures from right table are used in the aggregation
    - No filters reference the right table's columns
    """
    filters = filters or []
    deferrable: list[_DeferrableJoin] = []

    def _filter_references_table(table_name, table_op):
        """Check if any filter predicate references columns from this table."""
        if not filters:
            return False
        raw_tbl = _to_untagged(table_op)
        for pred in filters:
            pred_fn = _unwrap(pred)
            resolver = _table_filter_resolver(
                raw_tbl,
                table_op,
                table_name,
                _exact_filter_fields(pred_fn),
            )
            try:
                # If the predicate resolves against this table's columns or
                # dimensions (bare or table-prefixed), it references the
                # table → can't defer
                _resolve_expr(pred_fn, resolver)
                return True
            except Exception:
                pass
        return False

    def walk(node):
        if not isinstance(node, SemanticJoinOp):
            return
        # Recurse into left side (may have nested joins)
        walk(node.left)

        if node.cardinality != "one":
            return

        right = node.right
        if not isinstance(right, SemanticTableOp):
            return

        right_name = right.name
        if not right_name:
            return

        # Check: no filters reference the right table
        if _filter_references_table(right_name, right):
            return

        # Check: right table has is_entity dims
        right_dims = right.get_dimensions()
        entity_dims = frozenset(
            name for name, dim in right_dims.items() if getattr(dim, "is_entity", False)
        )
        if not entity_dims:
            return

        # Check: no measures from right table are used in aggregation
        for agg_name in agg_names:
            if agg_name.startswith(f"{right_name}."):
                return  # Right table has measures in agg → can't defer

        # Get join keys for the right table
        right_join_keys = join_tree_info.table_join_keys.get(right_name, set())
        if not right_join_keys:
            return

        # Check: entity dims match join keys (PK = join key)
        if entity_dims != frozenset(right_join_keys):
            return

        # Get left-side join keys by extracting from the predicate
        try:
            left_tbl = (
                node.left.to_untagged(parent_requirements=None)
                if isinstance(node.left, SemanticJoinOp)
                else _to_untagged(node.left)
            )
            right_tbl = _to_untagged(right)
            join_keys_result = _extract_join_key_columns(node.on, left_tbl, right_tbl)
            if not join_keys_result.is_success():
                return
            left_cols = tuple(sorted(join_keys_result.left_columns))
        except Exception:
            return

        # Identify which group-by dimensions from the right table will be deferred
        deferred_dims = tuple(k for k in group_by_keys if k.startswith(f"{right_name}."))

        # Only deferrable if there are dims to add post-agg
        if not deferred_dims:
            return

        # Deferral attaches dim labels to entity-grain rows WITHOUT
        # re-aggregating, so it is only sound when the requested group keys
        # already pin the entity grain. Grouping by a coarser attribute
        # alone (e.g. customers.region) must go through the pre-agg path,
        # which re-groups correctly; deferring it returns one row per
        # entity with duplicated dim values.
        left_key_names = frozenset(left_cols)

        def _key_covers_entity(entity_name):
            candidates = {entity_name, f"{right_name}.{entity_name}"}
            for k in group_by_keys:
                short = k.split(".", 1)[-1]
                if k in candidates or short == entity_name or short in left_key_names:
                    return True
            return False

        if not all(_key_covers_entity(e) for e in entity_dims):
            return

        deferrable.append(
            _DeferrableJoin(
                table_name=right_name,
                table_op=right,
                join_keys_left=left_cols,
                on_predicate=node.on,
                deferred_dims=deferred_dims,
            )
        )

    walk(join_op)
    return deferrable


def _left_join_bridge(left, bridge, common_keys):
    """Left-join *bridge* onto *left*, selecting only new columns from bridge."""
    # Null-safe equality so NULL-valued keys still pair up
    preds = [null_safe_equal(left[c], bridge[c]) for c in common_keys]
    bridge_only = tuple(c for c in bridge.columns if c not in frozenset(common_keys))
    return left.left_join(bridge, preds).select([left] + [bridge[c] for c in bridge_only])


def _find_chain_bridge(pt, gb_col, prefix, raw, measure_names, join_tree_info):
    """Find an intermediate table that chains *pt* grain to *raw* (dim table).

    Returns the bridged table, or *pt* unchanged if no chain is found.
    """
    current_grain = frozenset(c for c in pt.columns if c not in measure_names)
    dim_keys = join_tree_info.table_join_keys.get(prefix, frozenset())
    raw_columns = frozenset(raw.columns)

    for tname, tkeys in join_tree_info.table_join_keys.items():
        overlap_dim = tkeys & dim_keys & raw_columns
        overlap_grain = tkeys & current_grain
        if not (overlap_dim and overlap_grain):
            continue

        inter_op = join_tree_info.table_ops.get(tname)
        if inter_op is None:
            continue

        inter_raw = _to_untagged(inter_op)
        inter_cols = sorted(overlap_grain | overlap_dim)
        inter_bridge = inter_raw.select(
            [inter_raw[c] for c in inter_cols if c in inter_raw.columns]
        ).distinct()

        # Join dim table onto intermediate
        dim_bridge_cols = sorted({gb_col} | overlap_dim)
        dim_bridge = raw.select([raw[c] for c in dim_bridge_cols if c in raw.columns]).distinct()
        chained = _left_join_bridge(inter_bridge, dim_bridge, sorted(overlap_dim))

        # Join chained bridge onto pt — bridge on preserved (left) side
        return _left_join_bridge(chained, pt, sorted(overlap_grain))

    return pt


def _attach_dim_column(pt, gb_col, measure_names, join_tree_info, merged_dimensions):
    """Attach a single group-by dimension column to a pre-agg result.

    Looks up the raw table for the dimension's prefix, mutates the dim
    column onto it, and bridges it to *pt* via shared join keys — either
    directly or through an intermediate table.
    """
    if "." not in gb_col:
        return pt

    prefix, _short = gb_col.split(".", 1)
    dim_table_op = join_tree_info.table_ops.get(prefix)
    dim_fn = merged_dimensions.get(gb_col)
    if dim_table_op is None or dim_fn is None:
        return pt

    raw = _mutate_dimensions_with_dependencies(
        _to_untagged(dim_table_op),
        [gb_col],
        merged_dimensions,
    )
    raw_columns = frozenset(raw.columns)
    current_grain = tuple(c for c in pt.columns if c not in measure_names)
    common_keys = tuple(c for c in current_grain if c in raw_columns)

    match common_keys:
        case ():
            # No direct overlap — chain through an intermediate table.
            return _find_chain_bridge(
                pt,
                gb_col,
                prefix,
                raw,
                measure_names,
                join_tree_info,
            )
        case _:
            bridge = raw.select([raw[c] for c in (gb_col, *common_keys)]).distinct()
            return _left_join_bridge(bridge, pt, common_keys)


def _is_direct_physical_field(expr, table, column_name: str) -> bool:
    """Return whether *expr* is exactly ``table[column_name]``.

    Expression names are not lineage: transformations such as ``upper()``
    commonly retain their input field's name.  Preaggregation may reuse a raw
    column only for a direct Field op bound to the current raw relation.
    """
    try:
        op = _to_op(expr)
        return (
            type(op).__name__ == "Field"
            and getattr(op, "name", None) == column_name
            and getattr(op, "rel", None) is _to_op(table)
        )
    except Exception:
        return False


def _allocate_local_group_alias(
    group_key: str,
    occupied: Iterable[str],
) -> str:
    """Allocate a deterministic raw-table alias for a derived group key."""
    safe_key = re.sub(r"[^0-9A-Za-z_]", "_", group_key).strip("_") or "key"
    preferred = f"__bsl_gb_{safe_key}"
    occupied = frozenset(occupied)
    candidate = preferred
    suffix = 2
    while candidate in occupied:
        candidate = f"{preferred}_{suffix}"
        suffix += 1
    return candidate


def _compile_evaluated_measure_table(
    base_tbl,
    by_cols: Iterable[str],
    evaluated_measures: Mapping[str, Any],
):
    """Aggregate already-evaluated regular and nested measures together.

    ``NestedAccessMarker`` is intentionally not an ibis expression.  Most
    aggregation paths classify it before calling ``Table.aggregate``; the
    source-aware join pre-aggregation path also needs that dispatch because
    nested arrays must be unnested on their owning raw relation, never on the
    flattened (and potentially fanned-out) join.
    """
    by_cols = tuple(by_cols)
    nested_specs = {
        name: value
        for name, value in evaluated_measures.items()
        if isinstance(value, NestedAccessMarker)
    }
    regular_exprs = {
        name: value
        for name, value in evaluated_measures.items()
        if not isinstance(value, NestedAccessMarker)
    }

    if nested_specs:
        # `_compile_aggregation_with_nested` accepts callables for regular
        # measures.  These expressions are already bound to `base_tbl`, so a
        # constant-returning wrapper preserves that exact relation binding.
        regular_specs = {
            name: (lambda _table, expr=expr: expr) for name, expr in regular_exprs.items()
        }
        return _compile_aggregation_with_nested(
            base_tbl,
            list(by_cols),
            regular_specs,
            nested_specs,
        )

    if by_cols:
        return base_tbl.group_by([base_tbl[c] for c in by_cols]).aggregate(**regular_exprs)
    return base_tbl.aggregate(**regular_exprs)


def _compile_exact_measure_table(
    base_tbl,
    by_cols: Iterable[str],
    exact_measures: Mapping[str, Callable],
):
    """Evaluate exact-grain measure specs and compile nested markers safely."""
    evaluated = {name: fn(base_tbl) for name, fn in exact_measures.items()}
    return _compile_evaluated_measure_table(base_tbl, by_cols, evaluated)


def _exact_grain_preagg(
    raw_tbl,
    tbl,
    group_by_cols,
    join_keys,
    exact_measures,
    joined_key_names: Mapping[str, str] | None = None,
    local_group_keys: Mapping[str, str] | None = None,
):
    """Aggregate non-decomposable measures at the exact target grain.

    Median, stddev, variance and compound expressions (``sum()/count()``)
    cannot be re-aggregated from a finer pre-aggregate. Build a
    (group keys -> join keys) bridge from the joined table and aggregate
    the raw rows directly per group. Source-local group keys are also part
    of the bridge predicate: a join key can participate in multiple local
    dimension values, and joining on the key alone would leak raw rows from
    sibling groups into COUNT DISTINCT, median, and other exact reductions.
    Raises instead of degrading — the previous behavior summed per-key
    values silently.
    """
    names = ", ".join(sorted(exact_measures))
    if tbl is None:
        raise ValueError(
            f"Cannot compute non-decomposable measure(s) {names} at a "
            "cross-table grain: the joined table is unavailable."
        )
    missing = [c for c in group_by_cols if c not in tbl.columns]
    if missing:
        raise ValueError(
            f"Cannot compute non-decomposable measure(s) {names}: group "
            f"key(s) {missing} are not materialized on the joined table."
        )
    joined_key_names = dict(joined_key_names or {})
    local_group_keys = {
        joined_name: raw_name
        for joined_name, raw_name in dict(local_group_keys or {}).items()
        if joined_name in group_by_cols and raw_name in raw_tbl.columns
    }
    shared_jk = [
        (raw_name, joined_key_names.get(raw_name, raw_name))
        for raw_name in join_keys
        if raw_name in raw_tbl.columns and joined_key_names.get(raw_name, raw_name) in tbl.columns
    ]
    if not shared_jk:
        raise ValueError(
            f"Cannot compute non-decomposable measure(s) {names}: no join "
            "keys shared with the joined table to bridge the target grain."
        )
    # Allocate bridge-only names outside every user/executable namespace so
    # group columns cannot shadow raw columns referenced by measure
    # expressions (users may legitimately own ``__exact_gb_0`` already).
    occupied = set(raw_tbl.columns) | set(tbl.columns) | set(exact_measures)
    tmp: dict[str, str] = {}
    for i, column in enumerate(group_by_cols):
        preferred = f"__exact_gb_{i}"
        candidate = preferred
        suffix = 2
        while candidate in occupied:
            candidate = f"{preferred}_{suffix}"
            suffix += 1
        tmp[column] = candidate
        occupied.add(candidate)

    def allocate_exact_name(preferred: str) -> str:
        candidate = preferred
        suffix = 2
        while candidate in occupied:
            candidate = f"{preferred}_{suffix}"
            suffix += 1
        occupied.add(candidate)
        return candidate

    presence_name = allocate_exact_name("__exact_present")
    empty_names = {
        measure: allocate_exact_name(f"__exact_empty_{index}")
        for index, measure in enumerate(exact_measures)
    }
    bridge = tbl.select(
        [tbl[c].name(tmp[c]) for c in group_by_cols]
        + [tbl[joined].name(raw) for raw, joined in shared_jk]
    ).distinct()
    preds = [null_safe_equal(bridge[raw], raw_tbl[raw]) for raw, _ in shared_jk]
    preds.extend(
        null_safe_equal(bridge[tmp[joined_name]], raw_tbl[raw_name])
        for joined_name, raw_name in local_group_keys.items()
    )
    joined = bridge.inner_join(raw_tbl, preds)
    pt = _compile_exact_measure_table(joined, tmp.values(), exact_measures)
    from ..nested_compile import get_ibis_module

    pt = pt.mutate(**{presence_name: get_ibis_module(pt).literal(True)})
    # ibis rename convention: {new_name: old_name}
    pt = pt.rename({orig: tmp_name for orig, tmp_name in tmp.items()})

    # Exact source aggregation has no row for an unmatched LEFT JOIN group.
    # Preserve the joined query's full group domain and evaluate every measure
    # once on an actual empty source. Compound reductions can have non-NULL
    # empty-set results (count()+1 -> 1, sum().fill_null(0) -> 0), so blanket
    # NULL filling is unsound. A separate presence marker distinguishes an
    # absent aggregate row from a matched row whose measure itself is NULL.
    empty_tbl = raw_tbl.limit(0)
    empty_values = _compile_exact_measure_table(empty_tbl, (), exact_measures).rename(
        {empty_names[measure]: measure for measure in exact_measures}
    )
    group_spine = tbl.select([tbl[c] for c in group_by_cols]).distinct()
    spine_preds = [null_safe_equal(group_spine[c], pt[c]) for c in group_by_cols]
    attached = group_spine.left_join(pt, spine_preds).cross_join(empty_values)
    missing_aggregate = pt[presence_name].isnull()
    return attached.select(
        [group_spine]
        + [
            missing_aggregate.ifelse(empty_values[empty_names[measure]], pt[measure]).name(measure)
            for measure in exact_measures
        ]
    )


def _source_join_key_pairs(
    table_name: str,
    join_keys: Iterable[str],
    raw_columns: Iterable[str],
    joined_columns: Iterable[str],
    join_column_lineage: Mapping[str, Mapping[str, str]],
) -> tuple[tuple[str, str], ...]:
    """Return ``(raw key, executable joined alias)`` pairs for one source.

    Join-key metadata is intentionally expressed in each source table's raw
    namespace.  Once joins are flattened, a key such as a right-side ``id``
    may be materialized as ``id_right`` (or a collision-safe later suffix).
    Keeping the two names explicit prevents bridges from accidentally binding
    the raw right key to an unrelated same-named column from the left table.
    """
    raw_columns = frozenset(raw_columns)
    joined_columns = frozenset(joined_columns)
    source_names = join_column_lineage.get(table_name, {})
    return tuple(
        (raw_name, source_names.get(raw_name, raw_name))
        for raw_name in sorted(join_keys)
        if raw_name in raw_columns and source_names.get(raw_name, raw_name) in joined_columns
    )


def _rename_preagg_grain_to_joined_aliases(
    table,
    grain: Iterable[str],
    source_names: Mapping[str, str],
):
    """Put raw source-grain columns in the joined table's namespace."""
    grain = tuple(grain)
    renames = {
        source_names[name]: name
        for name in grain
        if name in table.columns and source_names.get(name, name) != name
    }
    if not renames:
        return table

    untouched = set(table.columns) - set(renames.values())
    collisions = sorted(set(renames) & untouched)
    if collisions:
        raise ValueError(
            "Cannot attach a source pre-aggregate to the joined table because "
            "its executable join-key alias collides with an aggregate column: "
            f"{collisions}. Rename the aggregate field."
        )
    return table.rename(renames)


def _partition_agg_specs_by_source(
    agg_specs: dict[str, Callable],
    all_roots: list[SemanticTableOp],
) -> dict[str | None, dict[str, Callable]]:
    """Partition aggregation specs by their source table.

    Prefixed measure names like ``"orders.total_amount"`` are mapped to
    ``table="orders"``.  Measures without a prefix go to ``None``.
    """
    root_names = {r.name for r in all_roots if r.name}
    partitioned: dict[str | None, dict[str, Callable]] = {}

    for measure_name, fn in agg_specs.items():
        table_name = None
        if "." in measure_name:
            prefix = measure_name.split(".", 1)[0]
            if prefix in root_names:
                table_name = prefix
        if table_name not in partitioned:
            partitioned[table_name] = {}
        partitioned[table_name][measure_name] = fn

    return partitioned


def _infer_join_wrapper_measure_owner(
    measure: Measure,
    join_tree_info: _JoinTreeInfo,
) -> str | None:
    """Infer one owning leaf for a base reduction declared after a join.

    Field-bearing reductions such as ``lambda t: t.amount.sum()`` should keep
    the grain of the one source that actually owns ``amount``.  Relation-only
    reductions such as ``t.count()`` intentionally describe joined-row grain
    and return ``None``.  If the same field expression resolves on multiple
    roots, choosing one would be a silent namespace guess, so reject it.
    """
    owners: list[str] = []
    original = measure.original_expr
    if _is_deferred(original):

        def probe(t, expr=original):
            return expr.resolve(t)
    elif callable(original):
        probe = original
    else:

        def probe(_t, value=original):
            return value

    for table_name, table_op in join_tree_info.table_ops.items():
        try:
            raw_tbl = _to_untagged(table_op)
            extraction = _extract_columns_from_callable(probe, raw_tbl)
        except Exception:
            continue
        if not extraction.is_success():
            continue
        if not extraction.columns:
            continue
        owners.append(table_name)

    if len(owners) == 1:
        return owners[0]
    if len(owners) > 1:
        raise ValueError(
            "A base measure declared after a join references columns that "
            f"resolve on multiple semantic models ({', '.join(sorted(owners))}). "
            "Define the measure on its owning model before joining, or use "
            "qualified calculated-measure references."
        )
    return None


def _join_wrapper_local_dimensions(
    roots: Iterable[SemanticTableOp],
) -> dict[str, Dimension]:
    """Return unprefixed dimensions declared on materialized join wrappers.

    A ``SemanticJoin.with_dimensions(...)`` result is represented by a
    ``SemanticTableOp`` whose physical table is the flattened join and whose
    ``_source_join`` retains semantic provenance.  Its inherited leaf
    dimensions are prefixed; unprefixed entries therefore belong to the
    wrapper namespace and must be interpreted against that flattened table,
    not independently against every leaf that happens to share a column name.
    """
    result: dict[str, Dimension] = {}
    for root in roots:
        if not isinstance(root, SemanticTableOp) or root._source_join is None:
            continue
        result.update(
            {
                name: dimension
                for name, dimension in root.get_dimensions().items()
                if "." not in name and isinstance(dimension, Dimension)
            }
        )
    return result


class _JoinWrapperDimensionPrefix:
    """Resolve one qualified prefix through a wrapper dimension scope."""

    __slots__ = ("_resolver", "_prefix")

    def __init__(self, resolver, prefix: str):
        object.__setattr__(self, "_resolver", resolver)
        object.__setattr__(self, "_prefix", prefix)

    def __getattr__(self, name: str):
        return self._resolver._resolve_dimension(f"{self._prefix}.{name}")


class _JoinWrapperDimensionResolver:
    """Resolve wrapper expressions with physical-self and semantic siblings.

    A wrapper definition such as ``status=lambda t: t.status.upper()`` reads
    the physical ``status`` column, while a later definition such as
    ``label=lambda t: t.status + '!'`` reads the semantic sibling.  Tracking
    the currently resolving names distinguishes those two cases and keeps the
    same dependency behavior as dimension materialization.
    """

    __slots__ = ("_dims", "_resolving", "_table")

    def __init__(self, table, dimensions: Mapping[str, Any], resolving=()):
        object.__setattr__(self, "_table", table)
        object.__setattr__(self, "_dims", dimensions)
        object.__setattr__(self, "_resolving", frozenset(resolving))

    def _resolve_dimension(self, name: str):
        if name not in self._dims:
            raise AttributeError(f"No dimension {name!r} exists in wrapper scope")
        if name in self._resolving:
            return self._table[name]
        dimension = self._dims[name]
        nested = _JoinWrapperDimensionResolver(
            self._table,
            self._dims,
            (*self._resolving, name),
        )
        if isinstance(dimension, Dimension):
            return dimension(nested, _dims=dict(self._dims))
        return _resolve_expr(dimension, nested)

    def __getattr__(self, name: str):
        if name in self._dims and name not in self._resolving:
            return self._resolve_dimension(name)
        prefix = f"{name}."
        if any(key.startswith(prefix) for key in self._dims):
            return _JoinWrapperDimensionPrefix(self, name)
        return getattr(self._table, name)

    def __getitem__(self, name: str):
        if name in self._dims and name not in self._resolving:
            return self._resolve_dimension(name)
        return self._table[name]

    @property
    def columns(self):
        return self._table.columns


def _infer_join_wrapper_dimension_owners(
    dimensions: Mapping[str, Dimension],
    joined_table,
    merged_dimensions: Mapping[str, Any],
    join_column_lineage: Mapping[str, Mapping[str, str]],
) -> tuple[dict[str, str | None], dict[str, str]]:
    """Bind wrapper dimensions to their unique source in joined namespace.

    The returned owner is a leaf model name, or ``None`` for a constant
    dimension.  Expressions that fail resolution, access a non-lineage
    column, or combine multiple leaves are returned in the error mapping so
    source preaggregation can fail closed rather than guess a namespace.
    """
    executable_owners: dict[str, set[str]] = {}
    for table_name, columns in join_column_lineage.items():
        for executable_name in columns.values():
            executable_owners.setdefault(executable_name, set()).add(table_name)

    owners: dict[str, str | None] = {}
    errors: dict[str, str] = {}
    dimensions_for_scope = dict(merged_dimensions)
    for name, dimension in dimensions.items():

        def resolve_on_join(t, dim=dimension, name=name):
            scope = _JoinWrapperDimensionResolver(t, dimensions_for_scope, resolving=(name,))
            return dim(scope, _dims=dimensions_for_scope)

        extraction = _extract_columns_from_callable(resolve_on_join, joined_table)
        if not extraction.is_success():
            errors[name] = "its expression does not resolve on the flattened join"
            continue
        if not extraction.columns:
            owners[name] = None
            continue

        expression_owners: set[str] = set()
        unknown_columns: list[str] = []
        for column in extraction.columns:
            candidates = executable_owners.get(column)
            if not candidates:
                unknown_columns.append(column)
            else:
                expression_owners.update(candidates)
        if unknown_columns:
            errors[name] = (
                "its expression accesses joined column(s) without leaf lineage: "
                f"{sorted(unknown_columns)}"
            )
        elif len(expression_owners) != 1:
            errors[name] = (
                f"its expression spans multiple semantic models: {sorted(expression_owners)}"
            )
        else:
            owners[name] = next(iter(expression_owners))
    return owners, errors


class SemanticAggregateOp(Relation):
    source: Relation
    keys: tuple[str, ...]
    aggs: dict[
        str,
        Callable,
    ]  # Transformed to FrozenDict[str, _CallableWrapper] in __init__
    nested_columns: tuple[str, ...] = ()  # Track which columns are nested arrays

    def __init__(
        self,
        source: Relation,
        keys: Iterable[str],
        aggs: dict[str, Callable] | None,
        nested_columns: Iterable[str] | None = None,
    ) -> None:
        frozen_aggs = FrozenDict(
            {name: _ensure_wrapped(fn) for name, fn in (aggs or {}).items()},
        )
        super().__init__(
            source=Relation.__coerce__(source),
            keys=tuple(keys),
            aggs=frozen_aggs,
            nested_columns=tuple(nested_columns or []),
        )

    def __repr__(self) -> str:
        return _semantic_repr(self)

    @property
    def values(self) -> FrozenOrderedDict[str, Any]:
        tbl = self.to_untagged()
        return FrozenOrderedDict({col: tbl[col].op() for col in tbl.columns})

    @property
    def schema(self) -> Schema:
        return _SchemaClass(fields=_FrozenOrderedDict({n: v.dtype for n, v in self.values.items()}))

    @property
    def measures(self) -> tuple[str, ...]:
        return ()

    def get_dimensions(self) -> Mapping[str, Dimension]:
        """After aggregation, dimensions are materialized - return empty."""
        return {}

    def get_measures(self) -> Mapping[str, Measure]:
        """After aggregation, measures are materialized - return empty."""
        return {}

    def get_calculated_measures(self) -> Mapping[str, Any]:
        """After aggregation, calculated measures are materialized - return empty."""
        return {}

    @property
    def required_columns(self) -> dict[str, set[str]]:
        """
        Column requirements for this aggregation operation.

        This property makes column requirements intrinsic to the aggregate operation,
        similar to how `schema` is intrinsic to a relation.

        Returns:
            Dict mapping table names to sets of required column names.
        """
        all_roots = _find_all_root_models(self.source)
        merged_dimensions = _get_merged_fields(all_roots, "dimensions", source=self.source)

        base_tbl = (
            self.source.to_expr() if hasattr(self.source, "to_expr") else _to_untagged(self.source)
        )

        table_names = []
        for root in all_roots:
            if root.name:
                table_names.append(root.name)
            elif root._source_join is not None:
                # All roots are SemanticTableOp with _source_join attribute
                join_roots = _find_all_root_models(root._source_join)
                table_names.extend([r.name for r in join_roots if r.name])

        key_requirements = projection_utils.extract_requirements_from_keys(
            keys=list(self.keys),
            dimensions=merged_dimensions,
            table=base_tbl,
            table_names=table_names,
        )

        measure_requirements = projection_utils.extract_requirements_from_measures(
            measures={name: _unwrap(fn) for name, fn in self.aggs.items()},
            table=base_tbl,
            table_names=table_names,
        )

        combined = key_requirements.merge(measure_requirements)

        if hasattr(self.source, "required_columns"):
            source_reqs = projection_utils.TableRequirements.from_dict(self.source.required_columns)
            combined = combined.merge(source_reqs)

        return combined.to_dict()

    def to_untagged(self):
        nest_specs = {
            name: _unwrap(fn)
            for name, fn in self.aggs.items()
            if isinstance(_unwrap(fn), NestAggSpec)
        }
        if nest_specs:
            return self._to_untagged_with_nest(nest_specs)

        all_roots = _find_all_root_models(self.source)

        def find_join_in_tree(node):
            """Find a SemanticJoinOp in the operation tree.

            All Relation subclasses have source attribute except leaf operations.
            """
            if isinstance(node, SemanticJoinOp):
                return node
            if isinstance(node, SemanticTableOp):
                # Wrapper tables from SemanticJoin.with_measures()/
                # with_dimensions() carry the join in _source_join; plain
                # leaf tables carry None. Following it here keeps queries
                # with filters between the wrapper and the aggregate on
                # the fan-out-safe pre-aggregation path.
                return node._source_join
            if node.source is not None:
                return find_join_in_tree(node.source)
            return None

        def collect_filters_to_join(node):
            """Collect filter predicates between this node and the join.

            Returns tuple of filter predicate wrappers found between the
            aggregate and the underlying join/table.
            """
            filters = []
            current = node
            while current is not None:
                match current:
                    case SemanticFilterOp():
                        filters.append(current.predicate)
                        current = current.source
                    case SemanticJoinOp() | SemanticTableOp():
                        break
                    case SemanticGroupByOp():
                        current = current.source
                    case _ if hasattr(current, "source"):
                        current = current.source
                    case _:
                        break
            return tuple(filters)

        join_op = find_join_in_tree(self.source)

        if join_op is None and isinstance(self.source, SemanticGroupByOp):
            grouped_source = self.source.source
            if isinstance(grouped_source, SemanticTableOp):
                # SemanticTableOp always has _source_join attribute
                join_op = grouped_source._source_join

        is_post_agg = _has_prior_aggregate(self.source)
        collected_filters = collect_filters_to_join(self.source)

        # Dimension-only shortcut: when no measures are requested and all
        # dimensions originate from a single joined table, query that table
        # directly so that dimension members with no matching fact rows are
        # still returned.  (Fixes #224.)
        if join_op is not None and not is_post_agg and not self.aggs:
            shortcut = _dimension_only_source_table(
                self.keys,
                all_roots,
                collected_filters,
            )
            if shortcut is not None:
                root_op, unprefixed_keys, dim_filters = shortcut
                try:
                    tbl = _to_untagged(root_op)
                    root_dims = root_op.get_dimensions()
                    tbl = _mutate_dimensions_with_dependencies(
                        tbl,
                        unprefixed_keys,
                        root_dims,
                    )
                    # Apply pre-aggregation filters on the dimension table.
                    # Resolve through the table-scoped resolver so prefixed
                    # (t["customers.region"]) and derived-dim references
                    # work the same as bare column access.
                    for flt in dim_filters:
                        fn = _unwrap(flt) if hasattr(flt, "unwrap") else flt
                        tbl = tbl.filter(
                            _resolve_expr(fn, _table_filter_resolver(tbl, root_op, root_op.name))
                        )
                    result = tbl.select(unprefixed_keys).distinct()
                    # Rename columns to their prefixed (dotted) names so that
                    # downstream consumers see the expected column names.
                    prefix = root_op.name
                    rename_map = {f"{prefix}.{uk}": uk for uk in unprefixed_keys}
                    return result.rename(rename_map)
                except Exception:
                    logger.debug(
                        "dimension-only shortcut failed for keys=%s; falling back to standard path",
                        self.keys,
                        exc_info=True,
                    )

        # Source-aware aggregation path: aggregate every joined model's base
        # measures against their owning leaf relation, then use the joined
        # table only as a dimension/participation bridge.  This is required
        # for ``join_one`` too: evaluating a right-side measure on the
        # flattened LEFT JOIN can bind colliding fields to the left relation,
        # and relation reductions such as ``t.count()`` count unmatched left
        # rows as if a right row existed.
        if join_op is not None and not is_post_agg and self.aggs:
            join_tree_info = _collect_join_tree_info(join_op)
            root_names = {
                name
                for name, cardinality in join_tree_info.table_cardinalities.items()
                if cardinality == "root"
            }
            requested_from_non_root = any(
                "." in name and name.split(".", 1)[0] not in root_names for name in self.aggs
            )
            merged_base_for_routing = _get_merged_fields(all_roots, "measures")
            requested_wrapper_base = any(
                "." not in name and name in merged_base_for_routing for name in self.aggs
            )
            if join_tree_info.has_join_many or requested_from_non_root or requested_wrapper_base:
                return self._to_untagged_with_preagg(
                    all_roots,
                    join_op,
                    join_tree_info,
                    filters=collected_filters,
                )

            # Deferred join path: when join_one dimension tables can be
            # joined AFTER aggregation for better performance.
            deferrable = _find_deferrable_joins(
                join_op,
                self.keys,
                self.aggs,
                all_roots,
                join_tree_info,
                filters=collected_filters,
            )
            if deferrable:
                return self._to_untagged_with_deferred_joins(
                    all_roots,
                    join_op,
                    join_tree_info,
                    deferrable,
                    filters=collected_filters,
                )

        # Only use the join optimization if there are no filters after the join
        # Otherwise we'd skip the filter operations
        needed_tables: dict[str, set[str]] = {}
        if join_op is not None and not collected_filters:
            # Build table-level requirements from prefixed keys and measure
            # names so the join can prune tables that the query never touches.
            # When all names are unprefixed (single-table or post-agg), the
            # dict is empty and `or None` disables pruning — correct since
            # there's nothing to prune in that case.
            for name in (*self.keys, *self.aggs.keys()):
                if "." in name:
                    tbl_prefix = name.split(".", 1)[0]
                    needed_tables.setdefault(tbl_prefix, set()).add(name)
            tbl = join_op.to_untagged(parent_requirements=needed_tables or None)
        else:
            tbl = _to_untagged(self.source)

        merged_dimensions = _get_merged_fields(all_roots, "dimensions", source=join_op)
        merged_base_measures = _get_merged_fields(all_roots, "measures")
        merged_calc_measures = _get_merged_fields(all_roots, "calc_measures")

        if needed_tables:
            # Keep validation behavior for queried tables even when join pruning
            # removes unrelated siblings. Validate each referenced root table
            # against its own leaf schema so wrapped join aliases do not affect
            # the error surface for local dimensions.
            for root in all_roots:
                if root.name not in needed_tables:
                    continue
                root_dimensions = _get_field_dict(root, "dimensions")
                if root_dimensions:
                    _mutate_dimensions_with_dependencies(
                        _to_untagged(root),
                        root_dimensions.keys(),
                        root_dimensions,
                    )

        if not is_post_agg:
            raw_columns = set()
            for root in all_roots:
                cols = getattr(getattr(root, "table", None), "columns", ())
                raw_columns.update(cols)
                if root.name:
                    raw_columns.update(f"{root.name}.{c}" for c in cols)
            _reject_shadowed_group_keys(
                tbl,
                self.keys,
                merged_dimensions,
                self.aggs,
                merged_base_measures,
                raw_columns=raw_columns,
            )
            merged_dimensions = _augment_dimensions_with_raw_columns(
                merged_dimensions, self.keys, all_roots, join_op
            )
        tbl = _mutate_dimensions_with_dependencies(
            tbl,
            [k for k in self.keys if k in merged_dimensions],
            merged_dimensions,
        )
        if not is_post_agg:
            _reject_unresolvable_group_keys(self.keys, merged_dimensions, tbl, all_roots)

        scope = (
            ColumnScope(_tbl=tbl)
            if is_post_agg
            else MeasureScope(
                _tbl=tbl,
                _known=list(merged_base_measures.keys()) + list(merged_calc_measures.keys()),
            )
        )

        plan = _build_aggregation_plan(
            aggs=self.aggs,
            keys=self.keys,
            scope=scope,
            is_post_agg=is_post_agg,
            merged_base_measures=merged_base_measures,
            merged_calc_measures=merged_calc_measures,
            tbl=tbl,
        )

        return _compile_aggregation(
            tbl,
            list(plan.group_by_cols),
            dict(plan.agg_specs),
            dict(plan.calc_specs),
            known_measures=frozenset(merged_base_measures)
            | frozenset(merged_calc_measures)
            | frozenset(plan.agg_specs)
            | frozenset(plan.calc_specs),
            requested_measures=list(plan.requested_measures),
            is_post_agg=is_post_agg,
        )

    def _to_untagged_with_nest(self, nest_specs: dict[str, NestAggSpec]):
        return _compile_module("_compile_nest").to_untagged_with_nest(self, nest_specs)

    def _to_untagged_with_preagg(
        self,
        all_roots: list,
        join_op: SemanticJoinOp,
        join_tree_info: _JoinTreeInfo,
        filters: list | None = None,
    ):
        return _compile_module("_compile_preagg").to_untagged_with_preagg(
            self, all_roots, join_op, join_tree_info, filters
        )

    def _to_untagged_with_deferred_joins(
        self,
        all_roots: list,
        join_op: SemanticJoinOp,
        join_tree_info: _JoinTreeInfo,
        deferrable: list[_DeferrableJoin],
        filters: list | None = None,
    ):
        return _compile_module("_compile_deferred").to_untagged_with_deferred_joins(
            self, all_roots, join_op, join_tree_info, deferrable, filters
        )


class SemanticUnnestOp(_SourcePassThroughOp, Relation):
    """Unnest an array column, expanding rows (like Malloy's nested data pattern)."""

    source: Relation
    column: str

    def __repr__(self) -> str:
        return _semantic_repr(self)

    @property
    def schema(self) -> Schema:
        # After unnesting, the schema changes - the array column is replaced by its element schema
        # For now, delegate to source schema (ideally we'd update it)
        return self.source.schema

    @property
    def values(self) -> FrozenDict:
        return FrozenDict({})

    def to_untagged(self):
        """Convert to Ibis expression with functional struct unpacking.

        Uses pure helper functions to extract struct fields when unnesting
        produces struct columns that need to be expanded.
        """

        def build_struct_fields(col_expr, col_type):
            """Pure function: build dict of struct field selections."""
            return {name: col_expr[name] for name in col_type.names}

        def unpack_struct_if_needed(unnested_tbl, column_name):
            """Conditionally unpack struct fields into top-level columns."""
            if column_name not in unnested_tbl.columns:
                return unnested_tbl

            col_expr = unnested_tbl[column_name]
            col_type = col_expr.type()

            # Only Struct types have fields to unpack
            if isinstance(col_type, dt.Struct) and col_type.fields:
                struct_fields = build_struct_fields(col_expr, col_type)
                return unnested_tbl.select(unnested_tbl, **struct_fields)

            return unnested_tbl

        tbl = _to_untagged(self.source)

        if self.column not in tbl.columns:
            raise ValueError(f"Column '{self.column}' not found in table")

        try:
            unnested = tbl.unnest(self.column)
        except Exception as e:
            raise ValueError(f"Failed to unnest column '{self.column}': {e}") from e

        return unpack_struct_if_needed(unnested, self.column)


class SemanticJoinOp(Relation):
    left: Relation
    right: Relation
    how: str
    on: (
        Callable[[Any, Any], Any] | None
    )  # Returns BooleanValue from either ibis or xorq.vendor.ibis
    cardinality: str  # "one", "many", or "cross"

    def __init__(
        self,
        left: Relation,
        right: Relation,
        how: str = "left",
        on: Callable[[Any, Any], Any] | None = None,
        cardinality: str = "one",
    ) -> None:
        left = Relation.__coerce__(left)
        right = Relation.__coerce__(right)

        def _root_names(node) -> list[str]:
            if isinstance(node, SemanticTableOp):
                source_join = getattr(node, "_source_join", None)
                if source_join is not None:
                    return _root_names(source_join)
                return [node.name] if node.name else []
            if isinstance(node, SemanticJoinOp):
                return [*_root_names(node.left), *_root_names(node.right)]
            source = getattr(node, "source", None)
            return _root_names(source) if source is not None else []

        root_names = [*_root_names(left), *_root_names(right)]
        duplicate_names = sorted(name for name in set(root_names) if root_names.count(name) > 1)
        if duplicate_names:
            raise ValueError(
                "Joined semantic models must have unique names; duplicate "
                f"name(s): {duplicate_names}. Assign explicit aliases before "
                "joining so dimensions, measures, and grain metadata retain "
                "an unambiguous source."
            )

        super().__init__(
            left=left,
            right=right,
            how=how,
            on=on,
            cardinality=cardinality,
        )

    def __repr__(self) -> str:
        return _semantic_repr(self)

    @property
    def values(self) -> FrozenOrderedDict[str, Any]:
        # Derive fields from the executable join so metadata uses the same
        # collision aliases (``x_right``, ``x_right2``, …) as query execution.
        # A dict update of left/right semantic values overwrote colliding names
        # and exposed a schema that contradicted ``to_untagged()``.
        table = self.to_untagged(parent_requirements=None)
        return FrozenOrderedDict({name: table[name].op() for name in table.columns})

    @property
    def schema(self):
        """Get schema of semantic table.

        Uses runtime imports to handle both regular ibis and xorq (which vendors ibis).
        Converts dtypes to strings to allow Schema to parse them into the correct dtype objects.
        """
        fields_dict = {name: str(v.dtype) for name, v in self.values.items()}
        return _make_schema(fields_dict)

    def get_dimensions(self) -> Mapping[str, Dimension]:
        """Get dictionary of dimensions with metadata."""
        all_roots = _find_all_root_models(self)
        return _merge_fields_with_prefixing(
            all_roots,
            lambda r: _get_field_dict(r, "dimensions"),
            source=self,  # Pass self to extract join keys
        )

    def get_measures(self) -> Mapping[str, Measure]:
        """Get dictionary of base measures with metadata."""
        all_roots = _find_all_root_models(self)
        return _merge_fields_with_prefixing(
            all_roots,
            lambda r: _get_field_dict(r, "measures"),
            source=self,
        )

    def get_calculated_measures(self) -> Mapping[str, Any]:
        """Get dictionary of calculated measures with metadata."""
        all_roots = _find_all_root_models(self)
        return _merge_fields_with_prefixing(
            all_roots,
            lambda r: _get_field_dict(r, "calc_measures"),
            source=self,
        )

    @property
    def dimensions(self) -> tuple[str, ...]:
        """Get tuple of dimension names."""
        return tuple(self.get_dimensions().keys())

    @property
    def _dims(self) -> dict[str, Dimension]:
        return dict(self.get_dimensions())

    @property
    def _base_measures(self) -> dict[str, Measure]:
        return dict(self.get_measures())

    @property
    def _calc_measures(self) -> dict[str, Any]:
        return dict(self.get_calculated_measures())

    @property
    def calc_measures(self) -> dict[str, Any]:
        """Get calculated measures as dict (for consistency with SemanticModel)."""
        return dict(self.get_calculated_measures())

    @property
    def measures(self) -> tuple[str, ...]:
        return tuple(self.get_measures().keys()) + tuple(
            self.get_calculated_measures().keys(),
        )

    @property
    def json_definition(self) -> Mapping[str, Any]:
        return _build_json_definition(
            self.get_dimensions(),
            self.get_measures(),
            self.get_calculated_measures(),
            None,
        )

    @property
    def name(self) -> str | None:
        return None

    @property
    def description(self) -> str | None:
        """Get description for joined model by combining root model descriptions."""
        roots = _find_all_root_models(self)
        base_descriptions = []
        for root in roots:
            root_name = getattr(root, "name", None) or "unnamed"
            root_desc = getattr(root, "description", None)
            if root_desc:
                base_descriptions.append(f"{root_name} ({root_desc})")
            else:
                base_descriptions.append(root_name)
        if base_descriptions:
            return "Joined model combining: " + ", ".join(base_descriptions)
        return None

    @property
    def table(self):
        return self.to_untagged()

    def with_dimensions(self, **dims) -> SemanticTable:
        return _semantic_table(
            table=self.to_untagged(),
            dimensions={**self.get_dimensions(), **dims},
            measures=self.get_measures(),
            calc_measures=self.get_calculated_measures(),
            name=None,
            _source_join=self,
        )

    def with_measures(self, **meas) -> SemanticTable:
        joined_tbl = self.to_untagged()
        all_known = (
            list(self.get_measures().keys())
            + list(self.get_calculated_measures().keys())
            + list(meas.keys())
        )
        scope = MeasureScope(_tbl=joined_tbl, _known=all_known)

        new_base, new_calc = (
            dict(self.get_measures()),
            dict(self.get_calculated_measures()),
        )
        for name, fn_or_expr in meas.items():
            kind, value = _classify_measure(fn_or_expr, scope, name)
            (new_calc if kind == "calc" else new_base)[name] = value

        return _semantic_table(
            table=joined_tbl,
            dimensions=self.get_dimensions(),
            measures=new_base,
            calc_measures=new_calc,
            name=None,
            _source_join=self,  # Pass join reference for projection pushdown
        )

    def group_by(self, *keys: str) -> SemanticGroupBy:
        return _expr_module().SemanticGroupBy(source=self, keys=keys)

    def filter(self, predicate: Callable) -> SemanticFilter:
        return _expr_module().SemanticFilter(source=self, predicate=predicate)

    def join_one(
        self,
        other: SemanticTable,
        on: Callable[[Any, Any], ir.BooleanValue],
        how: str = "left",
    ):
        """Join with one-to-one relationship semantics (left outer join)."""
        return _expr_module()._join_one_with_detected_grain(self, other, on, how)

    def join_many(
        self,
        other: SemanticTable,
        on: Callable[[Any, Any], ir.BooleanValue],
        how: str = "left",
    ):
        """Join with one-to-many relationship semantics."""
        return _expr_module().SemanticJoin(
            left=self,
            right=other.op(),
            on=on,
            how=how,
            cardinality="many",
        )

    def join_cross(self, other: SemanticTable):
        """Cross join (Cartesian product) with another semantic model."""
        return _expr_module().SemanticJoin(
            left=self,
            right=other.op(),
            on=None,
            how="cross",
            cardinality="cross",
        )

    def join(self, *args, **kwargs):
        """Deprecated: Use join_one(), join_many(), or join_cross() instead."""
        raise TypeError(_JOIN_REMOVED_MESSAGE)

    def index(
        self,
        selector: str | list[str] | Callable | None = None,
        by: str | None = None,
        sample: int | None = None,
    ) -> SemanticIndexOp:
        """Create an index for search/discovery.

        Supports ibis selectors (s.all(), s.cols(), etc.).
        """

        # Handle ibis selectors
        processed_selector = selector
        if selector is not None and "ibis.selectors" in str(type(selector).__module__):
            # Handle s.all() - select all columns
            if type(selector).__name__ == "AllColumns":
                processed_selector = None
            # Handle s.cols() - select specific columns
            elif type(selector).__name__ == "Cols":
                # Extract column names from the Cols selector
                processed_selector = sorted(selector.names)
            # For other selectors, keep as-is
            else:
                processed_selector = selector

        return SemanticIndexOp(source=self, selector=processed_selector, by=by, sample=sample)

    def _collect_leaf_table_names(self) -> set[str]:
        """Collect names of all leaf (base) tables in this join tree."""
        tables = set()

        if isinstance(self.left, SemanticJoinOp):
            tables |= self.left._collect_leaf_table_names()
        else:
            left_name = getattr(self.left, "name", None)
            if left_name:
                tables.add(left_name)

        if isinstance(self.right, SemanticJoinOp):
            tables |= self.right._collect_leaf_table_names()
        else:
            right_name = getattr(self.right, "name", None)
            if right_name:
                tables.add(right_name)

        return tables

    @property
    def required_columns(self):
        """
        Column requirements for projection pushdown.

        This property makes projection pushdown intrinsic to the join operation,
        similar to how `schema` is intrinsic to a relation.

        Computes what columns are needed from each leaf table based on:
        1. Columns needed for measures defined on the joined tables
        2. Join key columns

        Returns:
            Dict mapping table names to sets of required column names.
        """
        return self._compute_required_columns()

    def _compute_required_columns(self, parent_requirements: dict[str, set[str]] | None = None):
        """
        Compute column requirements for projection pushdown.

        Args:
            parent_requirements: Optional dict of specific columns requested by parent operations.

        Returns:
            Dict mapping table names to sets of required column names.
        """
        # Start with parent requirements using immutable TableRequirements
        requirements = projection_utils.TableRequirements.from_dict(
            parent_requirements if parent_requirements else {}
        )

        # Get all root models in join tree
        all_roots = _find_all_root_models(self)

        # Collect leaf tables
        def collect_leaf_tables(node):
            if isinstance(node, SemanticJoinOp):
                return collect_leaf_tables(node.left) + collect_leaf_tables(node.right)
            table_name = getattr(node, "name", None)
            return [(table_name, _to_untagged(node))] if table_name else []

        leaf_tables = collect_leaf_tables(self)

        # Group measures by table
        measures_by_table = {}
        for root in all_roots:
            root_measures = _get_field_dict(root, "measures")
            if root.name:
                measures_by_table[root.name] = root_measures
            else:
                # Parse prefixed measures (e.g., "marketing.spend")
                for measure_name, measure_obj in root_measures.items():
                    if "." in measure_name:
                        table_name = measure_name.split(".", 1)[0]
                        if table_name not in measures_by_table:
                            measures_by_table[table_name] = {}
                        measures_by_table[table_name][measure_name] = measure_obj

        # Extract columns needed by measures (using immutable operations)
        for table_name, table_ibis in leaf_tables:
            if table_name in measures_by_table:
                for measure_obj in measures_by_table[table_name].values():
                    # All measure_obj values are Measure instances with expr attribute
                    measure_fn = measure_obj.expr
                    if callable(measure_fn):
                        cols = projection_utils.extract_columns_from_callable_safe(
                            measure_fn, table_ibis
                        )
                        if cols:
                            requirements = requirements.add_columns(table_name, cols)

        # Extract and add join key columns
        if self.on is not None:
            # Get full schema for join key extraction
            temp_left = (
                self.left.to_untagged()
                if isinstance(self.left, SemanticJoinOp)
                else _to_untagged(self.left)
            )
            temp_right = (
                self.right.to_untagged()
                if isinstance(self.right, SemanticJoinOp)
                else _to_untagged(self.right)
            )

            join_keys_result = _extract_join_key_columns(self.on, temp_left, temp_right)

            if join_keys_result.is_success():
                # Add join keys to leaf tables (immutable operations)
                if isinstance(self.left, SemanticJoinOp):
                    for col in join_keys_result.left_columns:
                        for leaf_name in self.left._collect_leaf_table_names():
                            leaf_table = self._get_leaf_table_by_name(self.left, leaf_name)
                            if leaf_table and col in _to_untagged(leaf_table).columns:
                                requirements = requirements.add_columns(leaf_name, {col})
                else:
                    left_name = getattr(self.left, "name", None)
                    if left_name:
                        requirements = requirements.add_columns(
                            left_name, join_keys_result.left_columns
                        )

                if isinstance(self.right, SemanticJoinOp):
                    for col in join_keys_result.right_columns:
                        for leaf_name in self.right._collect_leaf_table_names():
                            leaf_table = self._get_leaf_table_by_name(self.right, leaf_name)
                            if leaf_table and col in _to_untagged(leaf_table).columns:
                                requirements = requirements.add_columns(leaf_name, {col})
                else:
                    right_name = getattr(self.right, "name", None)
                    if right_name:
                        requirements = requirements.add_columns(
                            right_name, join_keys_result.right_columns
                        )

        return requirements.to_dict()

    def _get_leaf_table_by_name(self, join_op: SemanticJoinOp, target_name: str):
        """Find a leaf table by name in a join tree."""
        if isinstance(join_op.left, SemanticJoinOp):
            result = self._get_leaf_table_by_name(join_op.left, target_name)
            if result is not None:
                return result
        else:
            left_name = getattr(join_op.left, "name", None)
            if left_name == target_name:
                return join_op.left

        if isinstance(join_op.right, SemanticJoinOp):
            result = self._get_leaf_table_by_name(join_op.right, target_name)
            if result is not None:
                return result
        else:
            right_name = getattr(join_op.right, "name", None)
            if right_name == target_name:
                return join_op.right

        return None

    def _collect_join_keys_for_leaves(self) -> dict[str, set[str]]:
        """Collect join keys needed by each leaf table.

        For nested joins, we trace join keys back to their source leaf tables.
        Returns dict mapping leaf table names to sets of columns needed for joins.
        """
        join_columns: dict[str, set[str]] = {}

        # Recursively collect from nested joins
        if isinstance(self.left, SemanticJoinOp):
            nested_keys = self.left._collect_join_keys_for_leaves()
            for table_name, cols in nested_keys.items():
                existing = join_columns.get(table_name, set())
                join_columns[table_name] = existing | cols

        if isinstance(self.right, SemanticJoinOp):
            nested_keys = self.right._collect_join_keys_for_leaves()
            for table_name, cols in nested_keys.items():
                existing = join_columns.get(table_name, set())
                join_columns[table_name] = existing | cols

        # Add join keys for THIS level
        if self.on is not None:
            # Convert without projection to get full schema
            temp_left = (
                self.left.to_untagged(parent_requirements=None)
                if isinstance(self.left, SemanticJoinOp)
                else _to_untagged(self.left)
            )
            temp_right = (
                self.right.to_untagged(parent_requirements=None)
                if isinstance(self.right, SemanticJoinOp)
                else _to_untagged(self.right)
            )

            join_keys = _extract_join_key_columns(self.on, temp_left, temp_right)

            if join_keys.is_success():
                # Add join keys to the appropriate leaf tables
                if not isinstance(self.left, SemanticJoinOp):
                    # Left is a leaf table
                    left_name = getattr(self.left, "name", None)
                    if left_name:
                        existing = join_columns.get(left_name, set())
                        join_columns[left_name] = existing | join_keys.left_columns
                else:
                    # Left is a nested join - need to map columns back to source tables
                    # Get all leaf tables from the nested join and their schemas
                    left_leaves = self.left._collect_leaf_table_names()
                    for col in join_keys.left_columns:
                        # Add column to each leaf table that actually has this column
                        for table_name in left_leaves:
                            if table_name:
                                # Check if this table actually has this column
                                # We do this by converting the table and checking its schema
                                leaf_table = self._get_leaf_table_by_name(self.left, table_name)
                                if leaf_table is not None:
                                    leaf_ibis = _to_untagged(leaf_table)
                                    if col in leaf_ibis.columns:
                                        existing = join_columns.get(table_name, set())
                                        join_columns[table_name] = existing | {col}

                if not isinstance(self.right, SemanticJoinOp):
                    # Right is a leaf table
                    right_name = getattr(self.right, "name", None)
                    if right_name:
                        existing = join_columns.get(right_name, set())
                        join_columns[right_name] = existing | join_keys.right_columns
                else:
                    # Right is a nested join
                    right_leaves = self.right._collect_leaf_table_names()
                    for col in join_keys.right_columns:
                        for table_name in right_leaves:
                            if table_name:
                                leaf_table = self._get_leaf_table_by_name(self.right, table_name)
                                if leaf_table is not None:
                                    leaf_ibis = _to_untagged(leaf_table)
                                    if col in leaf_ibis.columns:
                                        existing = join_columns.get(table_name, set())
                                        join_columns[table_name] = existing | {col}

        return join_columns

    def _augment_parent_requirements_for_pruning(
        self,
        parent_requirements: dict[str, set[str]] | None,
    ) -> dict[str, set[str]] | None:
        """Expand table requirements with bridge tables needed by descendant joins."""
        if parent_requirements is None:
            return None

        augmented = {table: set(cols) for table, cols in parent_requirements.items()}

        right_tables = (
            self.right._collect_leaf_table_names()
            if isinstance(self.right, SemanticJoinOp)
            else {getattr(self.right, "name", None)} - {None}
        )
        if augmented.keys() & right_tables and self.on is not None:
            temp_left = (
                self.left.to_untagged(parent_requirements=None)
                if isinstance(self.left, SemanticJoinOp)
                else _to_untagged(self.left)
            )
            temp_right = (
                self.right.to_untagged(parent_requirements=None)
                if isinstance(self.right, SemanticJoinOp)
                else _to_untagged(self.right)
            )
            join_keys = _extract_join_key_columns(self.on, temp_left, temp_right)
            if join_keys.is_success():
                join_key_columns = join_keys.left_columns | join_keys.right_columns
                if isinstance(self.left, SemanticJoinOp):
                    for col in join_keys.left_columns:
                        for leaf_name in self.left._collect_leaf_table_names():
                            leaf_table = self._get_leaf_table_by_name(self.left, leaf_name)
                            if leaf_table is not None and col in _to_untagged(leaf_table).columns:
                                augmented.setdefault(leaf_name, set())
                else:
                    left_name = getattr(self.left, "name", None)
                    if left_name:
                        augmented.setdefault(left_name, set())

                right_needed_tables = right_tables & augmented.keys()
                if isinstance(self.left, SemanticJoinOp) and right_needed_tables:
                    left_leaf_names = self.left._collect_leaf_table_names()
                    right_needed_leaf_names = [
                        leaf_name
                        for leaf_name in right_needed_tables
                        if self._get_leaf_table_by_name(self, leaf_name) is not None
                    ]
                    for left_leaf_name in left_leaf_names:
                        left_leaf = self._get_leaf_table_by_name(self.left, left_leaf_name)
                        if left_leaf is None:
                            continue
                        left_columns = set(_to_untagged(left_leaf).columns) - join_key_columns
                        if not left_columns:
                            continue
                        for right_leaf_name in right_needed_leaf_names:
                            right_leaf = self._get_leaf_table_by_name(self, right_leaf_name)
                            if right_leaf is None:
                                continue
                            right_columns = set(_to_untagged(right_leaf).columns) - join_key_columns
                            if left_columns & right_columns:
                                augmented.setdefault(left_leaf_name, set())
                                break

        if isinstance(self.left, SemanticJoinOp):
            augmented = self.left._augment_parent_requirements_for_pruning(augmented) or augmented
        if isinstance(self.right, SemanticJoinOp):
            augmented = self.right._augment_parent_requirements_for_pruning(augmented) or augmented

        return augmented

    def _should_prune_right(self, parent_requirements: dict[str, set[str]] | None) -> bool:
        """Return whether the right side can be skipped for the given requirements."""
        if parent_requirements is None or self.cardinality != "one" or self.how != "left":
            return False

        needed_tables = frozenset(parent_requirements.keys())
        right_tables = (
            self.right._collect_leaf_table_names()
            if isinstance(self.right, SemanticJoinOp)
            else {getattr(self.right, "name", None)} - {None}
        )
        if not right_tables or right_tables & needed_tables:
            return False

        # If an earlier sibling shares non-join columns with a still-needed
        # table, pruning it changes the later table's rname-based aliases
        # (e.g. ``state_right2`` vs ``state_right``). Keep the sibling so
        # wrapped dimensions keep resolving to the expected columns.
        remaining_tables = needed_tables - right_tables
        if remaining_tables:
            temp_left = (
                self.left.to_untagged(parent_requirements=None)
                if isinstance(self.left, SemanticJoinOp)
                else _to_untagged(self.left)
            )
            temp_right = (
                self.right.to_untagged(parent_requirements=None)
                if isinstance(self.right, SemanticJoinOp)
                else _to_untagged(self.right)
            )
            join_keys = _extract_join_key_columns(self.on, temp_left, temp_right)
            join_key_columns = (
                (join_keys.left_columns | join_keys.right_columns)
                if self.on is not None and join_keys.is_success()
                else set()
            )

            for right_name in right_tables:
                right_leaf = self._get_leaf_table_by_name(self, right_name)
                if right_leaf is None:
                    continue
                right_columns = set(_to_untagged(right_leaf).columns) - join_key_columns
                if not right_columns:
                    continue
                for needed_name in remaining_tables:
                    needed_leaf = self._get_leaf_table_by_name(self, needed_name)
                    if needed_leaf is None:
                        continue
                    needed_columns = set(_to_untagged(needed_leaf).columns) - join_key_columns
                    if right_columns & needed_columns:
                        return False

        return True

    def _effective_join_depth(self, parent_requirements: dict[str, set[str]] | None) -> int:
        """Count the surviving left-spine joins after pruning."""
        augmented = self._augment_parent_requirements_for_pruning(parent_requirements)
        left_depth = (
            self.left._effective_join_depth(augmented)
            if isinstance(self.left, SemanticJoinOp)
            else 0
        )
        if self._should_prune_right(augmented):
            return left_depth
        return left_depth + 1

    @staticmethod
    def _join_depth(op) -> int:
        """Count nested left SemanticJoinOps to determine join depth."""
        depth = 0
        current = op
        while isinstance(current, SemanticJoinOp):
            depth += 1
            current = current.left
        return depth

    @staticmethod
    def _rname_for_depth(depth: int) -> str:
        """Return the ``rname`` template for the given join depth.

        ibis uses ``{name}_right`` by default.  When three or more tables
        share a column name the second ``_right`` collides with the first.
        We avoid this by appending the depth: ``_right``, ``_right2``,
        ``_right3``, …
        """
        return "{name}_right" if depth <= 1 else f"{{name}}_right{depth}"

    def to_untagged(self, parent_requirements: dict[str, set[str]] | None = None):
        """Convert join to Ibis expression, pruning unnecessary joins.

        When *parent_requirements* is provided (a dict mapping table names to
        sets of needed columns), right-side leaf tables that contribute no
        required columns are skipped entirely.  This avoids expensive joins
        to dimension tables whose columns are never referenced by the query.

        Args:
            parent_requirements: Optional mapping of ``{table_name: {col, …}}``.
                When provided, joins to tables absent from this dict are elided.

        Returns:
            Ibis join expression (potentially simplified).
        """
        from ..convert import _Resolver

        augmented_requirements = self._augment_parent_requirements_for_pruning(parent_requirements)

        # --- Join pruning: skip right-side tables not needed by the query ---
        # Only prune join_one with LEFT join semantics.  Inner joins act as
        # a filter (excluding unmatched left rows) and must not be removed.
        # join_many / join_cross affect row counts and are never pruned here
        # (join_many is intercepted by the pre-aggregation path earlier).
        if self._should_prune_right(augmented_requirements):
            # Right side contributes nothing the parent needs — skip it.
            if isinstance(self.left, SemanticJoinOp):
                return self.left.to_untagged(parent_requirements=augmented_requirements)
            return _to_untagged(self.left)

        # Build both sides, passing requirements down for recursive pruning.
        left_tbl = (
            _to_untagged(self.left)
            if not isinstance(self.left, SemanticJoinOp)
            else self.left.to_untagged(parent_requirements=augmented_requirements)
        )
        right_tbl = (
            _to_untagged(self.right)
            if not isinstance(self.right, SemanticJoinOp)
            else self.right.to_untagged(parent_requirements=augmented_requirements)
        )

        # Rebind right side's DatabaseTable ops to use the same backend as
        # the left side.  from_ibis() creates a separate Backend object per
        # call; xorq >=0.3.11 raises "Multiple backends found" unless all
        # tables in a join share the same backend instance.
        left_tbl, right_tbl = self._rebind_join_backends(left_tbl, right_tbl)

        depth = (
            self._effective_join_depth(augmented_requirements)
            if augmented_requirements is not None
            else self._join_depth(self)
        )
        rname = self._rname_for_depth(depth)

        # Detect column name conflicts that cause ibis/xorq to raise
        # ``Ambiguous field reference`` during predicate resolution. The
        # rename dance + ``_RenamedResolver`` below is a workaround for
        # upstream ibis behaviors pinned by ``test_upstream_ibis_pins``;
        # remove this branch when those tests fail.
        conflicting = frozenset(left_tbl.columns) & frozenset(right_tbl.columns)

        right_collision_names = _allocate_right_collision_names(
            conflicting,
            left_tbl.columns,
            right_tbl.columns,
            depth,
        )

        if self.on is None:
            # Cross joins have no predicate requiring temporary left aliases,
            # but ibis's rname template can still overwrite a real left column
            # such as ``x_right``. Rename the colliding right columns to the
            # already-allocated unique names before joining.
            if right_collision_names:
                right_tbl = right_tbl.rename(
                    {new: old for old, new in right_collision_names.items()}
                )
            return left_tbl.join(right_tbl, how=self.how)

        if not conflicting:
            pred = self.on(_Resolver(left_tbl), _Resolver(right_tbl))
            return left_tbl.join(right_tbl, pred, how=self.how, rname=rname)

        # Temporarily rename conflicting left columns so the predicate
        # can be resolved without ambiguity.
        # ibis rename convention: {new_name: old_name}
        temporary_names = _allocate_temporary_join_names(
            conflicting,
            left_tbl.columns,
            right_tbl.columns,
        )
        # Keep final right aliases distinct from temporary columns too. This
        # matters for adversarial-but-valid source names containing both the
        # public ``_right`` convention and BSL's private temporary prefix.
        right_collision_names = _allocate_right_collision_names(
            conflicting,
            left_tbl.columns,
            right_tbl.columns,
            depth,
            reserved=temporary_names.values(),
        )
        rename_left = {temporary_names[c]: c for c in conflicting}
        left_safe = left_tbl.rename(rename_left)

        # Resolver that transparently maps original names → temp names,
        # so predicates like ``lambda f, a: f.tail_num == a.tail_num``
        # still work even though left's ``tail_num`` was renamed.
        orig_to_tmp = dict(temporary_names)

        pred = self.on(
            _RenamedResolver(left_safe, orig_to_tmp),
            _Resolver(right_tbl),
        )
        joined = left_safe.join(right_tbl, pred, how=self.how, rname=rname)

        # Restore final column names (ibis convention: {new: old}):
        # - left temp columns → original names
        # - right conflicting columns → unique depth-based suffixes
        rename_final = {c: temporary_names[c] for c in conflicting} | {
            new: old for old, new in right_collision_names.items()
        }

        return joined.rename(rename_final)

    @staticmethod
    def _rebind_join_backends(left_tbl, right_tbl):
        """Rebind DatabaseTable ops so both sides share a single backend.

        When tables are individually wrapped via ``from_ibis()``, each gets
        a distinct ``Backend`` object.  xorq >=0.3.11 raises "Multiple
        backends found" unless all tables in a join share the same instance.
        Uses ``op.replace()`` (ibis graph rewriting) to swap out the
        ``source`` field on every ``DatabaseTable`` node in the right tree.

        For plain ibis backends (e.g. Snowflake, Databricks, BigQuery)
        that xorq doesn't wrap, ``walk_nodes`` can't traverse the tree —
        fall back to returning the inputs unchanged so ibis executes the
        join natively. Rebinding is only needed for xorq-vendored backends.
        """
        from .._xorq import HAS_XORQ

        # Without xorq, from_ibis() is an identity, so both sides already share
        # their backends — nothing to rebind (see _rebind_to_canonical_backend).
        if not HAS_XORQ:
            return left_tbl, right_tbl

        try:
            from .._xorq import relations as xorq_rel
            from .._xorq import walk_nodes
        except ImportError:
            return left_tbl, right_tbl

        # Find a canonical backend from the left tree. Plain ibis Table
        # objects raise ValueError/TypeError ("Don't know how to handle
        # type ...") inside xorq's walk_nodes — skip rebinding for them.
        try:
            db_tables = list(walk_nodes((xorq_rel.DatabaseTable,), left_tbl))
        except (ValueError, TypeError):
            return left_tbl, right_tbl
        canonical = db_tables[0].source if db_tables else None

        if canonical is None:
            return left_tbl, right_tbl

        # Shared primitive: only tables on the same physical connection are
        # rebound, so a join across two distinct databases fails in the
        # engine rather than silently reading both sides from one of them.
        return (
            _rebind_to_backend(left_tbl, canonical),
            _rebind_to_backend(right_tbl, canonical),
        )

    def execute(self):
        return _rebind_to_canonical_backend(self.to_untagged()).execute()

    def compile(self, **kwargs):
        return _rebind_to_canonical_backend(self.to_untagged()).compile(**kwargs)

    def sql(self, **kwargs):
        return ibis.to_sql(_rebind_to_canonical_backend(self.to_untagged()), **kwargs)

    def __getitem__(self, key):
        dims_dict = self.get_dimensions()
        if key in dims_dict:
            return dims_dict[key]

        meas_dict = self.get_measures()
        if key in meas_dict:
            return meas_dict[key]

        calc_meas_dict = self.get_calculated_measures()
        if key in calc_meas_dict:
            return calc_meas_dict[key]

        raise KeyError(
            f"'{key}' not found in dimensions, measures, or calculated measures",
        )

    def pipe(self, func, *args, **kwargs):
        return func(self, *args, **kwargs)

    def as_table(self) -> SemanticTable:
        """Convert to SemanticTable, preserving merged metadata from both sides."""
        return _semantic_table(
            table=self.to_untagged(),
            dimensions=self.get_dimensions(),
            measures=self.get_measures(),
            calc_measures=self.get_calculated_measures(),
        )


class SemanticOrderByOp(_SourcePassThroughOp, Relation):
    source: Relation
    keys: tuple[
        str | ir.Value | Callable,
        ...,
    ]  # Transformed to tuple[str | _CallableWrapper, ...] in __init__

    def __init__(self, source: Relation, keys: Iterable[str | ir.Value | Callable]) -> None:
        def wrap_key(k):
            return k if isinstance(k, str | _CallableWrapper) else _ensure_wrapped(k)

        super().__init__(
            source=Relation.__coerce__(source),
            keys=tuple(wrap_key(k) for k in keys),
        )

    def __repr__(self) -> str:
        return _semantic_repr(self)

    def to_untagged(self):
        tbl = _to_untagged(self.source)

        def resolve_order_key(key):
            if isinstance(key, str):
                return tbl[key] if key in tbl.columns else getattr(tbl, key, key)
            elif isinstance(key, _CallableWrapper):
                unwrapped = _unwrap(key)
                return _resolve_expr(unwrapped, tbl)
            return key

        return tbl.order_by([resolve_order_key(key) for key in self.keys])


class SemanticLimitOp(_SourcePassThroughOp, Relation):
    source: Relation
    n: int
    offset: int

    def __init__(self, source: Relation, n: int, offset: int = 0) -> None:
        if n < 0:
            raise ValueError(f"limit must be non-negative, got {n}")
        if offset < 0:
            raise ValueError(f"offset must be non-negative, got {offset}")
        super().__init__(source=Relation.__coerce__(source), n=n, offset=offset)

    def __repr__(self) -> str:
        return _semantic_repr(self)

    def to_untagged(self):
        tbl = _to_untagged(self.source)
        return tbl.limit(self.n) if self.offset == 0 else tbl.limit(self.n, offset=self.offset)


def _get_field_type_str(field_type: Any) -> str:
    return (
        "string"
        if field_type.is_string()
        else "number"
        if field_type.is_numeric()
        else "date"
        if field_type.is_temporal()
        else str(field_type)
    )


def _get_weight_expr(
    base_tbl: Any,
    by_measure: str | None,
    all_roots: list,
    is_string: bool,
) -> Any:
    from .._xorq import api as xo

    if not by_measure:
        return xo._.count()

    merged_measures = _get_merged_fields(all_roots, "measures")
    return merged_measures[by_measure](base_tbl) if by_measure in merged_measures else xo._.count()


def _build_string_index_fragment(
    base_tbl: Any,
    field_expr: Any,
    field_name: str,
    field_path: str,
    type_str: str,
    weight_expr: Any,
) -> Any:
    from .._xorq import api as xo

    return (
        base_tbl.group_by(field_expr.name("value"))
        .aggregate(weight=weight_expr)
        .select(
            fieldName=xo.literal(field_name.split(".")[-1]),
            fieldPath=xo.literal(field_path),
            fieldType=xo.literal(type_str),
            fieldValue=xo._["value"].cast("string"),
            weight=xo._["weight"],
        )
    )


def _build_numeric_index_fragment(
    base_tbl: Any,
    field_expr: Any,
    field_name: str,
    field_path: str,
    type_str: str,
    weight_expr: Any,
) -> Any:
    from .._xorq import api as xo

    return (
        base_tbl.select(field_expr.name("value"))
        .filter(xo._["value"].notnull())
        .aggregate(
            min_val=xo._["value"].min(),
            max_val=xo._["value"].max(),
            weight=weight_expr,
        )
        .select(
            fieldName=xo.literal(field_name.split(".")[-1]),
            fieldPath=xo.literal(field_path),
            fieldType=xo.literal(type_str),
            fieldValue=(xo._["min_val"].cast("string") + " to " + xo._["max_val"].cast("string")),
            weight=xo._["weight"],
        )
    )


def _resolve_selector(
    selector: str | list[str] | Callable | None,
    base_tbl: ir.Table,
    known_fields=frozenset(),
) -> tuple[str, ...]:
    if selector is None:
        return tuple(base_tbl.columns)
    names = None
    if isinstance(selector, str):
        names = [selector]
    elif isinstance(selector, (list, tuple)) and all(isinstance(n, str) for n in selector):
        names = list(selector)
    if names is not None:
        known = set(known_fields) | set(base_tbl.columns)
        unknown = [n for n in names if n not in known]
        if unknown:
            raise ValueError(
                f"index() selector matched no dimension or column: {unknown}. "
                f"Available fields: {sorted(known)}"
            )
        return tuple(names)
    # Callable / ibis selector: let resolution errors propagate loudly — an
    # empty fallback here made a failing selector index every field.
    if callable(selector) and not isinstance(selector, s.Selector):
        resolved = selector(base_tbl)
        exprs = resolved if isinstance(resolved, (list, tuple)) else [resolved]
        return tuple(e.get_name() for e in exprs)
    return tuple(base_tbl.select(selector).columns)


def _get_fields_to_index(
    selector: str | list[str] | Callable | None,
    merged_dimensions: dict,
    base_tbl: ir.Table,
) -> tuple[str, ...]:
    if selector is None:
        selector = s.all()

    raw_fields = _resolve_selector(selector, base_tbl, known_fields=merged_dimensions.keys())

    if not raw_fields:
        result = list(merged_dimensions.keys())
        result.extend(col for col in base_tbl.columns if col not in result)
    else:
        result = [col for col in raw_fields if col in merged_dimensions or col in base_tbl.columns]

    return result


class SemanticIndexOp(Relation):
    source: Relation
    selector: str | list[str] | tuple[str, ...] | Callable | None
    by: str | None = None
    sample: int | None = None

    def __init__(
        self,
        source: Relation,
        selector: str | list[str] | tuple[str, ...] | Callable | None = None,
        by: str | None = None,
        sample: int | None = None,
    ) -> None:
        # Validate sample parameter
        if sample is not None and sample <= 0:
            raise ValueError(f"sample must be positive, got {sample}")

        # Validate 'by' measure exists if provided
        if by is not None:
            all_roots = _find_all_root_models(source)
            if all_roots:
                merged_measures = _get_merged_fields(all_roots, "measures")
                if by not in merged_measures:
                    available = list(merged_measures.keys())
                    raise KeyError(
                        f"Unknown measure '{by}' for weight calculation. "
                        f"Available measures: {', '.join(available) or 'none'}",
                    )

        # Convert selector to tuple if it's a list (Ibis requires hashable types)
        hashable_selector = tuple(selector) if isinstance(selector, list) else selector

        super().__init__(
            source=Relation.__coerce__(source),
            selector=hashable_selector,
            by=by,
            sample=sample,
        )

    def __repr__(self) -> str:
        return _semantic_repr(self)

    @property
    def values(self) -> FrozenOrderedDict[str, Any]:
        from .._xorq import api as xo

        return FrozenOrderedDict(
            {
                "fieldName": xo.literal("").op(),
                "fieldPath": xo.literal("").op(),
                "fieldType": xo.literal("").op(),
                "fieldValue": xo.literal("").op(),
                "weight": xo.literal(0).op(),
            },
        )

    @property
    def schema(self) -> Schema:
        return Schema(
            {
                "fieldName": "string",
                "fieldPath": "string",
                "fieldType": "string",
                "fieldValue": "string",
                "weight": "int64",
            },
        )

    @property
    def keys(self) -> tuple[str, ...]:
        return ("fieldValue", "fieldName", "fieldPath", "fieldType")

    @property
    def aggs(self) -> dict[str, Any]:
        return {"weight": lambda t: t.weight}

    def to_untagged(self):
        all_roots = _find_all_root_models(self.source)
        base_tbl = (
            _to_untagged(self.source).limit(self.sample)
            if self.sample
            else _to_untagged(self.source)
        )

        merged_dimensions = _get_merged_fields(all_roots, "dimensions", source=self.source)
        fields_to_index = _get_fields_to_index(
            self.selector,
            merged_dimensions,
            base_tbl,
        )

        if not fields_to_index:
            from .._xorq import api as xo

            return xo.memtable(
                {
                    "fieldName": [],
                    "fieldPath": [],
                    "fieldType": [],
                    "fieldValue": [],
                    "weight": [],
                },
            )

        def build_fragment(field_name: str) -> Any:
            field_expr = (
                merged_dimensions[field_name](base_tbl)
                if field_name in merged_dimensions
                else base_tbl[field_name]
            )
            field_type = field_expr.type()
            type_str = _get_field_type_str(field_type)
            weight_expr = _get_weight_expr(
                base_tbl,
                self.by,
                all_roots,
                field_type.is_string(),
            )

            return (
                _build_string_index_fragment(
                    base_tbl,
                    field_expr,
                    field_name,
                    field_name,
                    type_str,
                    weight_expr,
                )
                if field_type.is_string() or not field_type.is_numeric()
                else _build_numeric_index_fragment(
                    base_tbl,
                    field_expr,
                    field_name,
                    field_name,
                    type_str,
                    weight_expr,
                )
            )

        fragments = [build_fragment(f) for f in fields_to_index]
        return reduce(lambda acc, frag: acc.union(frag), fragments[1:], fragments[0])

    def filter(self, predicate: Callable) -> SemanticFilter:
        return _expr_module().SemanticFilter(source=self, predicate=predicate)

    def order_by(self, *keys: str | ir.Value | Callable) -> SemanticOrderBy:
        return _expr_module().SemanticOrderBy(source=self, keys=keys)

    def limit(self, n: int, offset: int = 0) -> SemanticLimit:
        return _expr_module().SemanticLimit(source=self, n=n, offset=offset)

    def execute(self):
        return _rebind_to_canonical_backend(self.to_untagged()).execute()

    def as_expr(self):
        """Return self as expression."""
        return self

    def compile(self, **kwargs):
        return self.to_untagged().compile(**kwargs)

    def sql(self, **kwargs):
        return ibis.to_sql(self.to_untagged(), **kwargs)

    def __getitem__(self, key):
        return self.to_untagged()[key]

    def pipe(self, func, *args, **kwargs):
        return func(self, *args, **kwargs)


def _find_root_model(node: Any) -> SemanticTableOp | None:
    """Find root SemanticTableOp in the operation tree."""
    cur = node
    while cur is not None:
        if isinstance(cur, SemanticTableOp):
            return cur
        parent = getattr(cur, "source", None)
        cur = parent
    return None


def _find_all_root_models(node: Any) -> tuple[SemanticTableOp, ...]:
    """Find all root SemanticTableOps in the operation tree (handles joins with multiple roots)."""
    if isinstance(node, SemanticTableOp):
        return [node]

    roots = []

    if hasattr(node, "left") and hasattr(node, "right"):
        roots.extend(_find_all_root_models(node.left))
        roots.extend(_find_all_root_models(node.right))
    elif hasattr(node, "source") and node.source is not None:
        roots.extend(_find_all_root_models(node.source))

    return roots


def _non_additive_result_columns(node: Any) -> frozenset[str]:
    """Result columns of a prior aggregate that must not be summed to get a total.

    A post-aggregation ``.mutate()`` only sees the aggregated rows, so its
    ``t.all(x)`` can only be a window sum over those rows. That equals the
    true overall value for SUM/COUNT measures and nothing else: summing
    per-group means, medians, min/max or distinct counts gives a number with
    no meaning, which is what ``t.all()`` used to return silently.

    Classification resolves each measure against its root's raw table, which
    builds an expression but compiles nothing. Measures that cannot be
    classified are omitted rather than assumed non-additive — callers keep
    their historical behaviour for those instead of failing on a guess.
    """
    current = node
    agg_op = None
    while current is not None:
        if isinstance(current, SemanticAggregateOp):
            agg_op = current
            break
        current = getattr(current, "source", None)
    if agg_op is None:
        return frozenset()

    try:
        roots = _find_all_root_models(agg_op.source)
        if not roots:
            return frozenset()
        merged_base = _get_merged_fields(roots, "measures")
        merged_calc = _get_merged_fields(roots, "calc_measures")
        probes = []
        for root in roots:
            raw = getattr(root, "table", None)
            if raw is None:
                continue
            probes.append(raw.to_expr() if hasattr(raw, "to_expr") else raw)
    except Exception as exc:
        logger.debug("additivity classification unavailable: %s", exc)
        return frozenset()

    non_additive: set[str] = set()
    for name in agg_op.aggs:
        resolved = _resolve_short_name(name, merged_base, merged_calc)
        if resolved is None:
            continue
        if resolved in merged_calc:
            # A calculated measure is a ratio/window expression; summing it
            # across groups is never the overall value.
            non_additive.add(name)
            continue
        measure = merged_base.get(resolved)
        expr = None
        for probe in probes:
            try:
                expr = _resolve_expr(getattr(measure, "expr", measure), probe)
                break
            except Exception:
                continue
        if expr is None:
            continue
        try:
            if _is_mean_expr(expr) or _reagg_op_for_expr(expr) != "sum":
                non_additive.add(name)
        except Exception as exc:
            # _reagg_op_for_expr raises "this is a bug" for undecomposed
            # mean / undeferred count-distinct: both are non-additive.
            logger.debug("treating %r as non-additive: %s", name, exc)
            non_additive.add(name)
    return frozenset(non_additive)


def _has_prior_aggregate(node: Any) -> bool:
    """True when a SemanticAggregateOp sits beneath ``node`` in the chain.

    Used to distinguish post-aggregation contexts (filter/order_by/limit
    applied after an aggregate) from pre-aggregation ones. Stops at leaf
    table and join boundaries.
    """
    if isinstance(node, SemanticAggregateOp):
        return True
    if isinstance(node, SemanticTableOp | SemanticJoinOp):
        return False
    source = getattr(node, "source", None)
    if source is not None:
        return _has_prior_aggregate(source)
    return False


def _dimension_only_source_table(
    keys: tuple[str, ...],
    all_roots: Sequence[SemanticTableOp],
    filters: tuple,
) -> tuple[SemanticTableOp, list[str], tuple] | None:
    """Check if a dimension-only query can be routed to a single source table.

    When all requested dimension keys share a single table prefix and that
    prefix maps to a root model whose dimensions cover every key, we can
    bypass the join and query the dimension table directly.  This ensures
    dimension members with no matching fact rows are still returned.

    *filters* are the ``_CallableWrapper`` predicates collected between the
    aggregate and the underlying join.  Filters whose column references all
    belong to the target table are forwarded; if any filter references columns
    outside the target table the shortcut is disabled.

    Returns ``(root_op, unprefixed_keys, applicable_filters)`` or ``None``.
    """
    if not keys:
        return None

    prefixes: set[str] = set()
    unprefixed: list[str] = []
    for key in keys:
        if "." not in key:
            return None  # Non-prefixed key — can't determine source
        prefix, name = key.split(".", 1)
        prefixes.add(prefix)
        unprefixed.append(name)

    if len(prefixes) != 1:
        return None  # Keys span multiple tables

    target_prefix = next(iter(prefixes))

    for root in all_roots:
        if root.name == target_prefix:
            root_dims = root.get_dimensions()
            if all(k in root_dims for k in unprefixed):
                # Validate that every filter only touches columns present
                # on the target dimension table.  If any filter references
                # columns from other tables we cannot use the shortcut.
                if filters:
                    tbl = _to_untagged(root)
                    # Accept bare and table-prefixed spellings: filters
                    # written t["customers.region"] (the qualified form
                    # BSL's own errors recommend) must not silently
                    # disable the zero-fact-rows guarantee.
                    tbl_cols = (
                        frozenset(tbl.columns)
                        | frozenset(root_dims)
                        | frozenset(f"{target_prefix}.{d}" for d in root_dims)
                        | frozenset(f"{target_prefix}.{c}" for c in tbl.columns)
                    )
                    resolver = _table_filter_resolver(tbl, root, target_prefix)
                    for flt in filters:
                        fn = _unwrap(flt) if hasattr(flt, "unwrap") else flt
                        # Dict/string filters resolve deferred through the
                        # backend; their columns can't be statically
                        # introspected, so disable the shortcut rather than
                        # risk a wrong source table. See query.Filter.to_callable.
                        if getattr(fn, "__bsl_deferred_resolution__", False):
                            return None
                        extraction = _extract_columns_from_callable(fn, resolver)
                        if extraction.extraction_failed:
                            return None  # Can't determine — bail out
                        if not extraction.columns <= tbl_cols:
                            return None  # References columns outside target
                return root, unprefixed, filters

    return None


def _build_join_depth_map(node: Any) -> dict[str, int]:
    """Map each leaf table name to its actual ibis rname depth.

    ``SemanticJoinOp.to_untagged`` calls ``_join_depth`` to determine the
    rname suffix for each join level.  ``_join_depth`` counts the number
    of ``SemanticJoinOp`` ancestors on the *left* spine.  The right child
    at depth *d* gets ``rname = _rname_for_depth(d)``.

    For nested subtrees on the right side of a join, ibis applies the
    inner subtree's rname independently.  So ``aircraft_models`` at inner
    depth 1 gets ``_right``, not ``_right3`` even if the outer depth is 3.

    This function mirrors ``_join_depth`` logic: walk down the left spine,
    recording the right child's depth at each level.  If the right child is
    itself a join tree, recurse to get inner depths for its leaves.
    """
    depth_map: dict[str, int] = {}

    def _record_leaf(n, depth: int):
        """Record a leaf table at the given depth."""
        if isinstance(n, SemanticTableOp):
            name = n.name
            if name and name not in depth_map:
                depth_map[name] = depth

    def _walk_join_spine(n):
        """Walk the left spine of a join tree, recording depths."""
        if not isinstance(n, SemanticJoinOp):
            # Leftmost leaf: depth 0 (root, never renamed)
            _record_leaf(n, 0)
            return

        depth = SemanticJoinOp._join_depth(n)
        # The right child is joined at this depth
        right = n.right
        if isinstance(right, SemanticJoinOp):
            # Right is a subtree — its leaves get inner depths
            inner_map = _build_join_depth_map(right)
            for tname, idepth in inner_map.items():
                if tname not in depth_map:
                    if idepth == 0:
                        # Leftmost leaf of subtree sits at the outer depth
                        # (it receives the outer rname suffix if conflicting)
                        depth_map[tname] = depth
                    else:
                        # Inner leaves keep their inner depth (inner rname)
                        depth_map[tname] = idepth
        else:
            _record_leaf(right, depth)

        # Recurse down the left spine
        _walk_join_spine(n.left)

    _walk_join_spine(node)
    return depth_map


def _extract_join_key_column_names(source: Relation) -> set[str]:
    """
    Extract column names that ibis will merge (coalesce) during joins.

    Ibis only merges join-key columns when **both** sides of an equi-join share
    the **same** column name (e.g., ``l.code == r.code``).  When names differ
    (e.g., ``l.carrier == r.code``), the right column gets a ``_right`` suffix
    instead.  We return only the intersection of left/right key names so that
    ``_check_and_add_rename`` correctly detects columns that need renaming.

    Args:
        source: The relation to search for join operations

    Returns:
        Set of column names that ibis merges (same-name equi-join keys)
    """
    join_keys: set[str] = set()

    def find_joins(node):
        """Recursively find join operations and extract merged key columns."""
        if isinstance(node, SemanticJoinOp) and node.on:
            try:
                left_expr = node.left.to_expr() if hasattr(node.left, "to_expr") else node.left
                right_expr = node.right.to_expr() if hasattr(node.right, "to_expr") else node.right
                result = _extract_join_key_columns(node.on, left_expr, right_expr)
                if result.is_success():
                    # ibis merges only same-name equi-join columns
                    join_keys.update(result.left_columns & result.right_columns)
            except (AttributeError, TypeError):
                pass

        if hasattr(node, "left") and isinstance(node.left, Relation):
            find_joins(node.left)
        if hasattr(node, "right") and isinstance(node.right, Relation):
            find_joins(node.right)
        if hasattr(node, "source") and isinstance(node.source, Relation):
            find_joins(node.source)

    find_joins(source)
    return join_keys


def _build_join_column_lineage(
    node: Relation,
) -> tuple[dict[str, dict[str, str]], tuple[str, ...]]:
    """Mirror join lowering and track every root column's executable alias."""
    if isinstance(node, SemanticJoinOp):
        left_lineage, left_columns = _build_join_column_lineage(node.left)
        right_lineage, right_columns = _build_join_column_lineage(node.right)
        conflicting = frozenset(left_columns) & frozenset(right_columns)
        reserved = (
            _allocate_temporary_join_names(conflicting, left_columns, right_columns).values()
            if node.on is not None
            else ()
        )
        aliases = _allocate_right_collision_names(
            conflicting,
            left_columns,
            right_columns,
            node._join_depth(node),
            reserved=reserved,
        )

        remapped_right = {
            table_name: {
                source_name: aliases.get(current_name, current_name)
                for source_name, current_name in columns.items()
            }
            for table_name, columns in right_lineage.items()
        }
        output_columns = (*left_columns, *(aliases.get(c, c) for c in right_columns))
        return ({**left_lineage, **remapped_right}, output_columns)

    if isinstance(node, SemanticTableOp):
        source_join = getattr(node, "_source_join", None)
        if source_join is not None:
            lineage, _columns = _build_join_column_lineage(source_join)
            return lineage, tuple(node.table.columns)
        columns = tuple(node.table.columns)
        lineage = {node.name: {column: column for column in columns}} if node.name else {}
        return lineage, columns

    source = getattr(node, "source", None)
    if source is not None:
        lineage, _columns = _build_join_column_lineage(source)
        return lineage, tuple(_to_untagged(node).columns)

    return {}, tuple(_to_untagged(node).columns)


def _build_column_rename_map(
    all_roots: Sequence[SemanticTable],
    field_accessor: callable,
    source: Relation | None = None,
) -> dict[str, dict[str, str]]:
    """
    Build per-dimension source-column mappings for a flattened joined table.

    When Ibis joins tables with duplicate column names, columns from later tables
    receive a depth-specific ``_right`` suffix.  A dimension may reference more
    than one source column, so each entry retains the complete mapping needed to
    re-evaluate the original expression against the flattened join.

    Uses graph_utils for generic traversal and the returns library for safe handling.

    Args:
        all_roots: List of root semantic tables in join order
        field_accessor: Function to get fields (dimensions) from a root
        source: Optional source relation to extract join keys from

    Returns:
        Dict mapping dimension names to ``{source_column: joined_column}`` maps.
    """
    # Prefer exact lineage from the same alias allocator used by executable
    # join lowering. The older flat-index/depth heuristic below remains as a
    # fallback for callers without a SemanticJoinOp source.
    join_lineage: dict[str, dict[str, str]] = {}

    def _lineage_join(node):
        if isinstance(node, SemanticJoinOp):
            return node
        if isinstance(node, SemanticTableOp):
            wrapped = getattr(node, "_source_join", None)
            if wrapped is not None:
                return wrapped
        parent = getattr(node, "source", None)
        return _lineage_join(parent) if isinstance(parent, Relation) else None

    lineage_join = _lineage_join(source)
    if lineage_join is not None:
        try:
            join_lineage, _columns = _build_join_column_lineage(lineage_join)
        except Exception:
            logger.debug("join column-lineage analysis failed", exc_info=True)

    # Build column index using graph_utils (returns Result)
    from returns.result import Failure

    from ..graph_utils import build_column_index_from_roots

    column_index_result = build_column_index_from_roots(all_roots)
    if isinstance(column_index_result, Failure):
        # If we can't build the index, return empty map (dimensions will use fallback behavior)
        return {}

    column_index = column_index_result.value_or({})

    # Build a map from table name → actual ibis join depth by walking the
    # join tree.  The flat index in all_roots does NOT equal ibis join depth
    # for nested joins (e.g. aircraft → aircraft_models inside a flights
    # join tree), so we must compute it from the tree structure.
    join_depth_map: dict[str, int] = {}
    if source is not None:
        join_depth_map = _build_join_depth_map(source)

    # Process dimensions and determine which need renamed columns
    rename_map = {}

    for idx, root in enumerate(all_roots):
        if not root.name:
            continue

        fields_dict = field_accessor(root)
        if not fields_dict:
            continue

        root_tbl = root.to_untagged()
        # Use the actual join depth if available, otherwise fall back to table_idx
        effective_depth = join_depth_map.get(root.name, idx)

        for field_name, field_value in fields_dict.items():
            # Track every source field used by the original dimension.  The
            # previous implementation selected an arbitrary first field and
            # replaced the whole expression with it, turning e.g. ``upper()``
            # and multi-column dimensions into identity dimensions.
            extraction = _extract_columns_from_callable(lambda t, dim=field_value: dim(t), root_tbl)
            if not extraction.is_success():
                continue

            field_renames: dict[str, str] = {}
            for base_column in extraction.columns:
                executable_name = join_lineage.get(root.name, {}).get(base_column)
                if executable_name is not None:
                    if executable_name != base_column:
                        field_renames[base_column] = executable_name
                    continue

                candidate: dict[str, str] = {}
                _check_and_add_rename(
                    rename_map=candidate,
                    base_column=base_column,
                    prefixed_name=base_column,
                    table_idx=idx,
                    column_index=column_index,
                    # SemanticJoinOp's collision-safe join path preserves the
                    # right key as a suffixed physical column too.  Mapping it
                    # is necessary to distinguish an unmatched right row from
                    # the left join key.
                    join_keys=set(),
                    join_depth=effective_depth,
                )
                if base_column in candidate:
                    field_renames[base_column] = candidate[base_column]

            if field_renames:
                rename_map[f"{root.name}.{field_name}"] = field_renames

    return rename_map


def _check_and_add_rename(
    rename_map: dict[str, str],
    base_column: str,
    prefixed_name: str,
    table_idx: int,
    column_index: dict[str, list[int]],
    join_keys: set[str],
    join_depth: int | None = None,
) -> None:
    """
    Check if a column needs renaming and add to rename map if so.

    ``table_idx`` is the flat index in ``all_roots`` used to detect
    whether an earlier table has the same column.  ``join_depth`` is
    the actual ibis join depth (from ``_build_join_depth_map``) used
    to compute the ``_right`` / ``_right2`` / … suffix.

    Args:
        rename_map: Map to update with renames
        base_column: The base column name
        prefixed_name: The prefixed dimension name (e.g., 'airports.city')
        table_idx: Flat index in all_roots (for conflict detection)
        column_index: Index of column occurrences
        join_keys: Set of column names used as join keys (these don't get renamed)
        join_depth: Actual ibis join depth for suffix computation (defaults to table_idx)
    """
    # Skip columns that are join keys - they get merged, not renamed
    if base_column in join_keys:
        return

    depth = join_depth if join_depth is not None else table_idx

    if base_column in column_index:
        tables_with_column = column_index[base_column]
        # Check if any table before this one (in flat order) has the same column
        earlier_tables = [t for t in tables_with_column if t < table_idx]
        if earlier_tables:
            suffix = "_right" if depth <= 1 else f"_right{depth}"
            rename_map[prefixed_name] = f"{base_column}{suffix}"


def _wrap_dimension_for_renamed_column(
    dimension: Dimension, column_renames: Mapping[str, str]
) -> Dimension:
    """
    Wrap a dimension to access a renamed column in a joined table.

    Args:
        dimension: The original dimension
        column_renames: Source-to-joined physical column mapping.

    Returns:
        A new Dimension that accesses the renamed column
    """

    # Re-evaluate the complete original expression against a resolver that
    # redirects only the source fields renamed by the join.  Operations and
    # non-colliding fields continue to delegate to the joined table.
    def renamed_accessor(table: ir.Table) -> ir.Value:
        return dimension(_RenamedResolver(table, column_renames))

    # Return a new Dimension with the wrapped callable but same metadata
    return Dimension(
        expr=renamed_accessor,
        description=dimension.description,
        is_entity=dimension.is_entity,
        is_time_dimension=dimension.is_time_dimension,
        is_event_timestamp=dimension.is_event_timestamp,
        smallest_time_grain=dimension.smallest_time_grain,
        derived_dimensions=dimension.derived_dimensions,
        metadata=dimension.metadata,
    )


def _qualify_calc_measure_for_root(calc: CalcMeasure, root: SemanticTableOp) -> CalcMeasure:
    """Bind a stored calculated measure's dependencies to its owning root.

    Calculated measures are authored before composition and therefore capture
    short dependency names.  Once roots are merged those names become
    ``<root>.<measure>``; suffix matching is ambiguous when two roots expose
    the same short name.  Qualifying at the composition boundary preserves the
    original lexical scope and also gives the calc compiler an explicit
    preferred match for short references in the user's callable.
    """
    if not root.name:
        return calc

    local_names = set(root.get_measures()) | set(root.get_calculated_measures())

    def qualify(name: str) -> str:
        if "." in name or name not in local_names:
            return name
        return f"{root.name}.{name}"

    qualified_dependencies = frozenset(qualify(name) for name in calc.depends_on)
    qualified_preferred = frozenset(qualify(name) for name in calc.prefer_known)
    qualified_preferred |= qualified_dependencies
    return CalcMeasure(
        expr=calc.expr,
        description=calc.description,
        requires_unnest=calc.requires_unnest,
        depends_on=qualified_dependencies,
        prefer_known=qualified_preferred,
        metadata=calc.metadata,
    )


def _merge_fields_with_prefixing(
    all_roots: Sequence[SemanticTable],
    field_accessor: callable,
    source: Relation | None = None,
) -> FrozenDict[str, Any]:
    """
    Generic function to merge any type of fields (dimensions, measures) with prefixing.

    Args:
        all_roots: List of SemanticTable root models
        field_accessor: Function that takes a root and returns the fields dict (e.g. lambda r: r.dimensions)
        source: Optional source relation to extract join keys from for proper column renaming

    Returns:
        FrozenDict mapping field names (always prefixed with table name) to field values
    """
    if not all_roots:
        return FrozenDict()

    merged_fields = {}

    # Sample the first root with declared fields — not all_roots[0]
    # unconditionally. When the fact table declares no dimensions, an
    # empty first sample would leave ``is_dimensions`` False, skip the
    # rename map, and let a colliding right-table dimension silently
    # read the LEFT table's column after the join.
    is_dimensions = False
    for root in all_roots:
        sample_fields = field_accessor(root)
        if sample_fields:
            first_val = next(iter(sample_fields.values()), None)
            is_dimensions = isinstance(first_val, Dimension)
            break

    column_rename_map = {}
    if is_dimensions:
        column_rename_map = _build_column_rename_map(all_roots, field_accessor, source)

    for root in all_roots:
        root_name = root.name
        fields_dict = field_accessor(root)

        for field_name, field_value in fields_dict.items():
            if root_name:
                prefixed_name = f"{root_name}.{field_name}"

                if is_dimensions and prefixed_name in column_rename_map:
                    field_value = _wrap_dimension_for_renamed_column(
                        field_value, column_rename_map[prefixed_name]
                    )
                elif isinstance(field_value, CalcMeasure):
                    field_value = _qualify_calc_measure_for_root(field_value, root)

                merged_fields[prefixed_name] = field_value
            else:
                merged_fields[field_name] = field_value

    return FrozenDict(merged_fields)
