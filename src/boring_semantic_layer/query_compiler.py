"""
Compile QueryExpr instances into Ibis expressions.
"""

from typing import Any, Dict

from .filters import Filter

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
    for d in dimensions:
        if "." in d:
            alias, field = d.split(".", 1)
            join = model.joins.get(alias)
            if not join or field not in join.model.dimensions:
                raise KeyError(f"Unknown dimension: {d}")
        elif d not in dimensions:
            raise KeyError(f"Unknown dimension: {d}")

    # Validate measures
    for m in measures:
        if "." in m:
            alias, field = m.split(".", 1)
            join = model.joins.get(alias)
            if not join or field not in join.model.measures:
                raise KeyError(f"Unknown measure: {m}")
        elif m not in model.measures:
            raise KeyError(f"Unknown measure: {m}")

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
        order_exprs = []
        for field, direction in qe.order_by:
            col_name = field.replace(".", "_")
            col = result[col_name]
            order_exprs.append(
                col.desc() if direction.lower().startswith("desc") else col.asc()
            )
        result = result.order_by(order_exprs)

    # Apply limit
    if qe.limit is not None:
        result = result.limit(qe.limit)

    return result
