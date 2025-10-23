"""
Compile QueryExpr instances into Ibis expressions.
"""

from typing import Any, Dict, List, Optional

from .filters import Filter


def _find_similar(name: str, available: List[str], max_suggestions: int = 3) -> List[str]:
    """Find similar names using simple string distance."""
    import difflib
    return difflib.get_close_matches(name, available, n=max_suggestions, cutoff=0.6)


def _validate_dimension(dim: str, model: Any, available_dims: List[str]) -> None:
    """Validate a dimension exists and provide helpful error message if not."""
    if "." in dim:
        alias, field = dim.split(".", 1)
        join = model.joins.get(alias)
        if not join:
            available_joins = list(model.joins.keys())
            suggestions = _find_similar(alias, available_joins)
            suggestion_text = f" Did you mean: {', '.join(suggestions)}?" if suggestions else ""
            raise KeyError(
                f"Unknown join alias '{alias}'.{suggestion_text} "
                f"Available joins: {', '.join(available_joins) or 'none'}"
            )
        if field not in join.model.dimensions:
            join_dims = list(join.model.dimensions.keys())
            suggestions = _find_similar(field, join_dims)
            suggestion_text = f" Did you mean: {', '.join(suggestions)}?" if suggestions else ""
            raise KeyError(
                f"Unknown dimension '{field}' in join '{alias}'.{suggestion_text} "
                f"Available dimensions in '{alias}': {', '.join(join_dims) or 'none'}"
            )
    else:
        if dim not in available_dims:
            suggestions = _find_similar(dim, available_dims)
            suggestion_text = f" Did you mean: {', '.join(suggestions)}?" if suggestions else ""
            raise KeyError(
                f"Unknown dimension '{dim}'.{suggestion_text} "
                f"Available dimensions: {', '.join(available_dims) or 'none'}"
            )


def _validate_measure(measure: str, model: Any, available_measures: List[str]) -> None:
    """Validate a measure exists and provide helpful error message if not."""
    if "." in measure:
        alias, field = measure.split(".", 1)
        join = model.joins.get(alias)
        if not join:
            available_joins = list(model.joins.keys())
            suggestions = _find_similar(alias, available_joins)
            suggestion_text = f" Did you mean: {', '.join(suggestions)}?" if suggestions else ""
            raise KeyError(
                f"Unknown join alias '{alias}'.{suggestion_text} "
                f"Available joins: {', '.join(available_joins) or 'none'}"
            )
        if field not in join.model.measures:
            join_measures = list(join.model.measures)
            suggestions = _find_similar(field, join_measures)
            suggestion_text = f" Did you mean: {', '.join(suggestions)}?" if suggestions else ""
            raise KeyError(
                f"Unknown measure '{field}' in join '{alias}'.{suggestion_text} "
                f"Available measures in '{alias}': {', '.join(join_measures) or 'none'}"
            )
    else:
        if measure not in available_measures:
            suggestions = _find_similar(measure, available_measures)
            suggestion_text = f" Did you mean: {', '.join(suggestions)}?" if suggestions else ""
            raise KeyError(
                f"Unknown measure '{measure}'.{suggestion_text} "
                f"Available measures: {', '.join(available_measures) or 'none'}"
            )

try:
    import xorq.vendor.ibis as ibis_mod
    from xorq.vendor.ibis.expr.operations.relations import Field as RelField

    IS_XORQ_USED = True
except ImportError:
    import ibis as ibis_mod
    from ibis.expr.operations.relations import Field as RelField

    IS_XORQ_USED = False

Expr = ibis_mod.expr.types.core.Expr
_ = ibis_mod._


def _compile_query(qe: Any) -> Expr:
    """Compile a QueryExpr into an Ibis expression."""
    model = qe.model

    # Validate time grain
    model._validate_time_grain(qe.time_grain)

    # Start with the base table
    t = model.table

    # Apply joins
    for alias, join in model.joins.items():
        right = join.model.table
        if join.how == "cross":
            t = t.cross_join(right)
        else:
            cond = join.on(t, right)
            t = t.join(right, cond, how=join.how)

    # Transform time dimension if needed
    t, dim_map = model._transform_time_dimension(t, qe.time_grain)

    # Apply time range filter if provided
    if qe.time_range and model.time_dimension:
        start, end = qe.time_range
        time_filter = {
            "operator": "AND",
            "conditions": [
                {"field": model.time_dimension, "operator": ">=", "value": start},
                {"field": model.time_dimension, "operator": "<=", "value": end},
            ],
        }
        t = t.filter(Filter(filter=time_filter).to_ibis(t, model))

    # Apply other filters
    for flt in qe.filters:
        t = t.filter(flt.to_ibis(t, model))

    # Prepare dimensions and measures lists
    dimensions = list(qe.dimensions)
    if (
        qe.time_grain
        and model.time_dimension
        and model.time_dimension not in dimensions
    ):
        dimensions.append(model.time_dimension)
    measures = list(qe.measures)

    # Validate dimensions
    available_dimensions = list(model.dimensions.keys())
    for d in dimensions:
        _validate_dimension(d, model, available_dimensions)

    # Validate measures
    available_measures = model.measures
    for m in measures:
        _validate_measure(m, model, available_measures)

    # Build aggregate expressions
    agg_kwargs: Dict[str, Expr] = {}
    for m in measures:
        if "." in m:
            alias, field = m.split(".", 1)
            join = model.joins[alias]
            expr = join.model.measures[field](join.model.table)
            name = f"{alias}_{field}"
            agg_kwargs[name] = expr
        else:
            expr = model.measures[m](t)
            agg_kwargs[m] = expr

    def _is_simple_field(expr: Expr) -> bool:
        return isinstance(expr.op(), RelField)

    if dimensions:
        dim_exprs: list[Expr] = []
        for d in dimensions:
            if "." in d:
                alias, field = d.split(".", 1)
                name = f"{alias}_{field}"
                target_model = model.joins[alias].model
                expr = target_model.dimensions[field](target_model.table).name(name)
            else:
                # Use possibly transformed dimension function
                expr = dim_map[d](t).name(d)
            dim_exprs.append(expr)
        if all(_is_simple_field(expr) for expr in agg_kwargs.values()):
            result = t.select(*dim_exprs, *agg_kwargs.values())
        else:
            result = t.aggregate(by=dim_exprs, **agg_kwargs)
    else:
        if all(_is_simple_field(expr) for expr in agg_kwargs.values()):
            result = t.select(*agg_kwargs.values())
        else:
            result = t.aggregate(**agg_kwargs)

    # Apply ordering
    if qe.order_by:
        result = result.order_by([
            (col := result[field.replace(".", "_")]).desc() if direction.lower().startswith("desc") else col.asc()
            for field, direction in qe.order_by
        ])

    # Apply limit
    if qe.limit is not None:
        result = result.limit(qe.limit)

    return result
