"""Lightweight semantic layer for BI-style queries using Xorq backend."""

from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

from xorq.vendor.ibis.expr.types.core import Expr


Dimension = Callable[[Expr], Expr]
Measure = Callable[[Expr], Expr]
FilterValue = Union[str, int, float, List[Any]]


@dataclass
class Join:
    """Join definition for semantic model relationships."""

    alias: str
    model: "SemanticModel"
    on: Callable[[Expr, Expr], Expr]
    how: str = "inner"


class SemanticModel:
    """
    Define a semantic model over an Ibis table expression with reusable dimensions and measures.

    Attributes:
        table: Base Ibis table expression.
        dimensions: Mapping of dimension names to callables producing column expressions.
        measures: Mapping of measure names to callables producing aggregate expressions.

    Example:
        con = xo.duckdb.connect()
        flights_tbl = con.table('flights')
        flights = SemanticModel(
            table=flights_tbl,
            dimensions={
                'origin': lambda t: t.origin,
                'destination': lambda t: t.destination,
                'carrier': lambda t: t.carrier,
            },
            measures={
                'flight_count': lambda t: t.count(),
                'avg_distance': lambda t: t.distance.mean(),
            }
        )
        # Query grouping by origin, filtering by carrier, ordering, and limiting.
        expr = flights.query(
            dims=['origin'],
            measures=['flight_count', 'avg_distance'],
            filters={'carrier': 'WN'},
            order_by=[('flight_count', 'desc')],
            limit=5,
        )
        df = expr.execute()
    """

    def __init__(
        self,
        table: Expr,
        dimensions: Dict[str, Dimension],
        measures: Dict[str, Measure],
        joins: Optional[Dict[str, Join]] = None,
        name: Optional[str] = None,
    ) -> None:
        self.name = name or table.get_name()
        self.table = table
        self.dimensions = dimensions
        self.measures = measures
        # Mapping of join alias to Join definitions
        self.joins: Dict[str, Join] = joins or {}

    def query(
        self,
        dims: Optional[List[str]] = None,
        measures: Optional[List[str]] = None,
        filters: Optional[Dict[str, FilterValue]] = None,
        order_by: Optional[List[Tuple[str, str]]] = None,
        limit: Optional[int] = None,
    ) -> Expr:
        """
        Build an Ibis expression that groups by dimensions and aggregates measures.

        Args:
            dims: List of dimension keys to group by.
            measures: List of measure keys to compute.
            filters: Filter mapping from dimension or column name to value or list of values.
            order_by: List of tuples (field, 'asc'|'desc') for ordering.
            limit: Row limit.

        Returns:
            Ibis Expr representing the query.
        """
        t = self.table

        # Apply defined joins
        for alias, join in self.joins.items():
            right = join.model.table
            cond = join.on(t, right)
            t = t.join(right, cond, how=join.how)

        # Apply filters
        if filters:
            for key, value in filters.items():
                # Handle join-scoped filters (alias.field)
                if isinstance(key, str) and "." in key:
                    alias, field = key.split(".", 1)
                    join = self.joins.get(alias)
                    if not join:
                        raise KeyError(f"Unknown join alias in filter: {alias}")
                    model = join.model
                    if field in model.dimensions:
                        col = model.dimensions[field](t)
                    else:
                        col = t[field]
                elif key in self.dimensions:
                    col = self.dimensions[key](t)
                else:
                    # fallback to raw column
                    col = t[key]
                if isinstance(value, (list, tuple, set)):
                    t = t.filter(col.isin(value))
                else:
                    t = t.filter(col == value)

        dims = dims or []
        measures = measures or []

        # Validate keys (dimensions and measures), including joins
        for d in dims:
            if isinstance(d, str) and "." in d:
                alias, field = d.split(".", 1)
                join = self.joins.get(alias)
                if not join or field not in join.model.dimensions:
                    raise KeyError(f"Unknown dimension: {d}")
            elif d not in self.dimensions:
                raise KeyError(f"Unknown dimension: {d}")
        for m in measures:
            if isinstance(m, str) and "." in m:
                alias, field = m.split(".", 1)
                join = self.joins.get(alias)
                if not join or field not in join.model.measures:
                    raise KeyError(f"Unknown measure: {m}")
            elif m not in self.measures:
                raise KeyError(f"Unknown measure: {m}")

        # Build aggregate expressions, including join measures
        agg_kwargs: Dict[str, Expr] = {}
        for m in measures:
            if isinstance(m, str) and "." in m:
                alias, field = m.split(".", 1)
                join = self.joins[alias]
                expr = join.model.measures[field](t)
                name = f"{alias}_{field}"
                agg_kwargs[name] = expr.name(name)
            else:
                expr = self.measures[m](t)
                agg_kwargs[m] = expr.name(m)

        # Grouping and aggregation
        if dims:
            # Name and prepare dimension expressions
            dim_exprs = []
            for d in dims:
                if isinstance(d, str) and "." in d:
                    alias, field = d.split(".", 1)
                    join = self.joins[alias]
                    name = f"{alias}_{field}"
                    expr = join.model.dimensions[field](t).name(name)
                else:
                    expr = self.dimensions[d](t).name(d)
                dim_exprs.append(expr)
            grouped = t.group_by(*dim_exprs)
            result = grouped.aggregate(**agg_kwargs)
        else:
            result = t.aggregate(**agg_kwargs)

        # Ordering
        if order_by:
            order_exprs = []
            for field, direction in order_by:
                if isinstance(field, str) and "." in field:
                    alias, fname = field.split(".", 1)
                    col_name = f"{alias}_{fname}"
                else:
                    col_name = field
                col = result[col_name]
                order_exprs.append(
                    col.desc() if direction.lower().startswith("desc") else col.asc()
                )
            result = result.order_by(order_exprs)

        # Limit
        if limit is not None:
            result = result.limit(limit)

        return result

    @property
    def available_dimensions(self) -> List[str]:
        """List available dimension keys, including joined model dimensions."""
        keys = list(self.dimensions.keys())
        for alias, join in self.joins.items():
            keys.extend([f"{alias}.{d}" for d in join.model.dimensions.keys()])
        return keys

    @property
    def available_measures(self) -> List[str]:
        """List available measure keys, including joined model measures."""
        keys = list(self.measures.keys())
        for alias, join in self.joins.items():
            keys.extend([f"{alias}.{m}" for m in join.model.measures.keys()])
        return keys
