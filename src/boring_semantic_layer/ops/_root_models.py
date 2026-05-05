"""Root-model traversal and join-aware field merging.

These helpers walk a relation tree to find the underlying ``SemanticTableOp``
roots (one per leaf table) and merge their dimensions/measures with proper
table prefixing. They also handle the ibis quirk of renaming non-key columns
on the right side of a join with ``_right`` / ``_right2`` / … suffixes.

``SemanticTableOp`` and ``SemanticJoinOp`` are imported lazily inside each
function to avoid a circular module dependency with ``_core.py``.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from ibis.expr import types as ir
from ibis.expr.operations.relations import Relation

from .._xorq import FrozenDict
from ._column_extraction import _extract_columns_from_callable, _extract_join_key_columns
from ._values import Dimension


def _find_root_model(node: Any):
    """Find root SemanticTableOp in the operation tree."""
    from ._core import SemanticTableOp

    cur = node
    while cur is not None:
        if isinstance(cur, SemanticTableOp):
            return cur
        parent = getattr(cur, "source", None)
        cur = parent
    return None


def _find_all_root_models(node: Any) -> tuple[Any, ...]:
    """Find all root SemanticTableOps in the operation tree (handles joins with multiple roots)."""
    from ._core import SemanticTableOp

    if isinstance(node, SemanticTableOp):
        return [node]

    roots = []

    if hasattr(node, "left") and hasattr(node, "right"):
        roots.extend(_find_all_root_models(node.left))
        roots.extend(_find_all_root_models(node.right))
    elif hasattr(node, "source") and node.source is not None:
        roots.extend(_find_all_root_models(node.source))

    return roots


def _dimension_only_source_table(
    keys: tuple[str, ...],
    all_roots: Sequence[Any],
    filters: tuple,
):
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
    from ._core import _to_untagged, _unwrap

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
                    tbl_cols = frozenset(tbl.columns) | frozenset(root_dims)
                    for flt in filters:
                        fn = _unwrap(flt) if hasattr(flt, "unwrap") else flt
                        extraction = _extract_columns_from_callable(fn, tbl)
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
    from ._core import SemanticJoinOp, SemanticTableOp

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


def _update_measure_refs_in_calc(expr, prefix_map: dict[str, str]):
    """
    Recursively update MeasureRef names in a calculated measure expression.

    Args:
        expr: A MeasureExpr (MeasureRef, AllOf, BinOp, MethodCall, or literal)
        prefix_map: Mapping from old name to new prefixed name

    Returns:
        Updated expression with prefixed MeasureRef names
    """
    from ..measure_scope import AllOf, BinOp, MeasureRef, MethodCall

    if isinstance(expr, MeasureRef):
        # Update the measure reference name if it's in the map
        new_name = prefix_map.get(expr.name, expr.name)
        return MeasureRef(new_name)
    elif isinstance(expr, AllOf):
        # Update the inner MeasureRef
        updated_ref = _update_measure_refs_in_calc(expr.ref, prefix_map)
        return AllOf(updated_ref)
    elif isinstance(expr, MethodCall):
        updated_receiver = _update_measure_refs_in_calc(expr.receiver, prefix_map)
        return MethodCall(
            receiver=updated_receiver,
            method=expr.method,
            args=expr.args,
            kwargs=expr.kwargs,
        )
    elif isinstance(expr, BinOp):
        # Recursively update left and right
        updated_left = _update_measure_refs_in_calc(expr.left, prefix_map)
        updated_right = _update_measure_refs_in_calc(expr.right, prefix_map)
        return BinOp(op=expr.op, left=updated_left, right=updated_right)
    else:
        # Literal number or other - return as-is
        return expr


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
    from ._core import SemanticJoinOp

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


def _build_column_rename_map(
    all_roots: Sequence[Any],
    field_accessor: callable,
    source: Relation | None = None,
) -> dict[str, str]:
    """
    Build a mapping of dimension names to their renamed column names in joined tables.

    When Ibis joins tables with duplicate column names, it renames columns from later
    tables with '_right' suffix. However, columns used as join keys are merged and
    NOT renamed, so we exclude them from the rename map.

    Uses graph_utils for generic traversal and the returns library for safe handling.

    Args:
        all_roots: List of root semantic tables in join order
        field_accessor: Function to get fields (dimensions) from a root
        source: Optional source relation to extract join keys from

    Returns:
        Dict mapping dimension names like 'airports.city' to renamed columns like 'city_right'
    """
    # Build column index using graph_utils (returns Result)
    from returns.result import Failure

    from ..graph_utils import build_column_index_from_roots, extract_column_from_dimension

    column_index_result = build_column_index_from_roots(all_roots)
    if isinstance(column_index_result, Failure):
        # If we can't build the index, return empty map (dimensions will use fallback behavior)
        return {}

    column_index = column_index_result.value_or({})

    # Extract join key columns to exclude from renaming
    join_keys = _extract_join_key_column_names(source) if source else set()

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
            # Extract column name using graph_utils (returns Maybe)
            column_maybe = extract_column_from_dimension(field_value, root_tbl)

            # Use Maybe pattern from returns library
            column_maybe.bind_optional(
                lambda base_column: _check_and_add_rename(  # noqa: B023
                    rename_map=rename_map,
                    base_column=base_column,
                    prefixed_name=f"{root.name}.{field_name}",  # noqa: B023
                    table_idx=idx,  # noqa: B023
                    column_index=column_index,
                    join_keys=join_keys,
                    join_depth=effective_depth,  # noqa: B023
                )
            )

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


def _wrap_dimension_for_renamed_column(dimension: Dimension, renamed_column: str) -> Dimension:
    """
    Wrap a dimension to access a renamed column in a joined table.

    Args:
        dimension: The original dimension
        renamed_column: The renamed column name (e.g., 'city_right')

    Returns:
        A new Dimension that accesses the renamed column
    """

    # Create a new callable that accesses the renamed column
    def renamed_accessor(table: ir.Table) -> ir.Value:
        return table[renamed_column]

    # Return a new Dimension with the wrapped callable but same metadata
    return Dimension(
        expr=renamed_accessor,
        description=dimension.description,
        is_entity=dimension.is_entity,
        is_time_dimension=dimension.is_time_dimension,
        is_event_timestamp=dimension.is_event_timestamp,
        smallest_time_grain=dimension.smallest_time_grain,
        derived_dimensions=dimension.derived_dimensions,
    )


def _merge_fields_with_prefixing(
    all_roots: Sequence[Any],
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

    is_calc_measures = False
    is_dimensions = False
    if all_roots:
        sample_fields = field_accessor(all_roots[0])
        if sample_fields:
            from ..measure_scope import AllOf, BinOp, MeasureRef, MethodCall

            first_val = next(iter(sample_fields.values()), None)
            is_calc_measures = isinstance(
                first_val,
                MeasureRef | AllOf | BinOp | MethodCall | int | float,
            )
            is_dimensions = isinstance(first_val, Dimension)

    # For dimensions, build a column rename map to handle Ibis join conflicts
    column_rename_map = {}
    if is_dimensions:
        column_rename_map = _build_column_rename_map(all_roots, field_accessor, source)

    for root in all_roots:
        root_name = root.name
        fields_dict = field_accessor(root)

        if is_calc_measures and root_name:
            base_map = (
                {k: f"{root_name}.{k}" for k in root.get_measures()}
                if hasattr(root, "get_measures")
                else {}
            )
            calc_map = (
                {k: f"{root_name}.{k}" for k in root.get_calculated_measures()}
                if hasattr(root, "get_calculated_measures")
                else {}
            )
            prefix_map = {**base_map, **calc_map}

        for field_name, field_value in fields_dict.items():
            if root_name:
                # Always use prefixed name with . separator
                prefixed_name = f"{root_name}.{field_name}"

                # If it's a calculated measure, update internal MeasureRefs
                if is_calc_measures:
                    field_value = _update_measure_refs_in_calc(field_value, prefix_map)
                # If it's a dimension that needs column renaming, wrap the callable
                elif is_dimensions and prefixed_name in column_rename_map:
                    field_value = _wrap_dimension_for_renamed_column(
                        field_value, column_rename_map[prefixed_name]
                    )

                merged_fields[prefixed_name] = field_value
            else:
                # Fallback to original name if no root name
                merged_fields[field_name] = field_value

    return FrozenDict(merged_fields)
