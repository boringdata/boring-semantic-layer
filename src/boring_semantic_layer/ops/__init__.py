"""Semantic layer operations.

This package was split out of a single ``ops.py``. ``__init__`` re-exports
the names that callers import from ``boring_semantic_layer.ops`` so existing
imports keep working unchanged.
"""

from ._column_extraction import (
    ColumnExtractionResult,
    ColumnTracker,
    JoinColumnExtractionResult,
    TableColumnRequirements,
    _extract_columns_from_callable,
    _extract_join_key_columns,
    _extract_requirements_from_keys,
    _extract_requirements_from_measures,
    _make_tracking_proxy,
    _parse_prefixed_field,
)
from ._core import (
    # Public value objects
    Dimension,
    Measure,
    # Public Op classes
    SemanticAggregateOp,
    SemanticFilterOp,
    SemanticGroupByOp,
    SemanticIndexOp,
    SemanticJoinOp,
    SemanticLimitOp,
    SemanticMutateOp,
    SemanticOrderByOp,
    SemanticProjectOp,
    SemanticTableOp,
    SemanticUnnestOp,
    # Private helpers used by other modules in this package
    _CallableWrapper,
    _classify_measure,
    _collect_measure_refs,
    _ensure_xorq_table,
    _find_all_root_models,
    _get_field_dict,
    _get_merged_fields,
    _is_deferred,
    _make_schema,
    _merge_fields_with_prefixing,
    _normalize_join_predicate,
    _normalize_to_name,
    _rebind_to_backend,
    _rebind_to_canonical_backend,
)

__all__ = [
    "Dimension",
    "Measure",
    "SemanticAggregateOp",
    "SemanticFilterOp",
    "SemanticGroupByOp",
    "SemanticIndexOp",
    "SemanticJoinOp",
    "SemanticLimitOp",
    "SemanticMutateOp",
    "SemanticOrderByOp",
    "SemanticProjectOp",
    "SemanticTableOp",
    "SemanticUnnestOp",
]
