"""Semantic relation ops and their compiler.

``_core`` is the historical single-module implementation, being split into
focused submodules zone by zone (phase 3b). This ``__init__`` is the stable
import surface: every name other modules pull from ``boring_semantic_layer.
ops`` is re-exported here, and unknown attributes fall through to ``_core``
so the split never breaks an importer.
"""

from . import _core
from ._core import (
    CalcMeasure,
    Dimension,
    Measure,
    NestAggSpec,
    SemanticAggregateOp,
    SemanticFilterOp,
    SemanticGroupByOp,
    SemanticIndexOp,
    SemanticJoinOp,
    SemanticLimitOp,
    SemanticOrderByOp,
    SemanticProjectOp,
    SemanticTableOp,
    SemanticUnnestOp,
    _CallableWrapper,
    _classify_measure,
    _collect_struct,
    _detect_bare_name_lambda,
    _ensure_xorq_table,
    _exact_filter_fields,
    _extract_columns_from_callable,
    _extract_join_key_columns,
    _find_all_root_models,
    _get_merged_fields,
    _has_prior_aggregate,
    _is_deferred,
    _make_schema,
    _non_additive_result_columns,
    _normalize_join_predicate,
    _normalize_to_name,
    _rebind_to_backend,
    _rebind_to_canonical_backend,
    _resolve_expr,
    _unwrap,
    make_bare_ref_lambda,
)

__all__ = [
    "CalcMeasure",
    "Dimension",
    "Measure",
    "NestAggSpec",
    "SemanticAggregateOp",
    "SemanticFilterOp",
    "SemanticGroupByOp",
    "SemanticIndexOp",
    "SemanticJoinOp",
    "SemanticLimitOp",
    "SemanticOrderByOp",
    "SemanticProjectOp",
    "SemanticTableOp",
    "SemanticUnnestOp",
    "_CallableWrapper",
    "_classify_measure",
    "_collect_struct",
    "_detect_bare_name_lambda",
    "_ensure_xorq_table",
    "_exact_filter_fields",
    "_extract_columns_from_callable",
    "_extract_join_key_columns",
    "_find_all_root_models",
    "_get_merged_fields",
    "_has_prior_aggregate",
    "_is_deferred",
    "_make_schema",
    "_non_additive_result_columns",
    "_normalize_join_predicate",
    "_normalize_to_name",
    "_rebind_to_backend",
    "_rebind_to_canonical_backend",
    "_resolve_expr",
    "_unwrap",
    "make_bare_ref_lambda",
]


def __getattr__(name):
    return getattr(_core, name)
