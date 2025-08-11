"""Lightweight semantic layer for Malloy-style data models using Ibis."""

from attrs import frozen, field, evolve
from types import MappingProxyType
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Tuple,
    Union,
    Literal,
    Mapping,
    TYPE_CHECKING,
)
import datetime

if TYPE_CHECKING:
    import altair

try:
    import xorq.vendor.ibis as ibis_mod

    IS_XORQ_USED = True
except ImportError:
    import ibis as ibis_mod

    IS_XORQ_USED = False

# Import Join class from separate module
from .joins import Join
from .filters import Filter

Expr = ibis_mod.expr.types.core.Expr
_ = ibis_mod._
from .filters import OPERATOR_MAPPING

from .time_grain import TimeGrain, TIME_GRAIN_TRANSFORMATIONS, TIME_GRAIN_ORDER

# Join strategies
How = Literal["inner", "left", "cross"]
Cardinality = Literal["one", "many", "cross"]

Dimension = Callable[[Expr], Expr]
Measure = Callable[[Expr], Expr]




from .query_compiler import _compile_query


from .chart import _detect_chart_spec


@frozen(kw_only=True, slots=True)
class QueryExpr:
    model: "SemanticModel"
    dimensions: Tuple[str, ...] = field(factory=tuple)
    measures: Tuple[str, ...] = field(factory=tuple)
    filters: Tuple[Filter, ...] = field(factory=tuple)
    order_by: Tuple[Tuple[str, str], ...] = field(factory=tuple)
    limit: Optional[int] = None
    time_range: Optional[Tuple[str, str]] = None
    time_grain: Optional[TimeGrain] = None

    def with_dimensions(self, *dimensions: str) -> "QueryExpr":
        """
        Return a new QueryExpr with additional dimensions added.

        Args:
            *dimensions: Dimension names to add.
        Returns:
            QueryExpr: A new QueryExpr with the specified dimensions.
        """
        return self.clone(dimensions=self.dimensions + dimensions)

    def with_measures(self, *measures: str) -> "QueryExpr":
        """
        Return a new QueryExpr with additional measures added.

        Args:
            *measures: Measure names to add.
        Returns:
            QueryExpr: A new QueryExpr with the specified measures.
        """
        return self.clone(measures=self.measures + measures)

    def with_filters(
        self, *f: Union[Filter, Dict[str, Any], str, Callable[[Expr], Expr]]
    ) -> "QueryExpr":
        """
        Return a new QueryExpr with additional filters added.

        Args:
            *f: Filters to add (Filter, dict, str, or callable).
        Returns:
            QueryExpr: A new QueryExpr with the specified filters.
        """
        wrapped = tuple(fi if isinstance(fi, Filter) else Filter(filter=fi) for fi in f)
        return self.clone(filters=self.filters + wrapped)

    def sorted(self, *order: Tuple[str, str]) -> "QueryExpr":
        """
        Return a new QueryExpr with additional order by clauses.

        Args:
            *order: Tuples of (field, direction) to order by.
        Returns:
            QueryExpr: A new QueryExpr with the specified ordering.
        """
        return self.clone(order_by=self.order_by + order)

    def top(self, n: int) -> "QueryExpr":
        """
        Return a new QueryExpr with a row limit applied.

        Args:
            n: The maximum number of rows to return.
        Returns:
            QueryExpr: A new QueryExpr with the specified row limit.
        """
        return self.clone(limit=n)

    def grain(self, g: TimeGrain) -> "QueryExpr":
        """
        Return a new QueryExpr with a specified time grain.

        Args:
            g: The time grain to use.
        Returns:
            QueryExpr: A new QueryExpr with the specified time grain.
        """
        return self.clone(time_grain=g)

    def clone(self, **changes) -> "QueryExpr":
        """
        Return a copy of this QueryExpr with the specified changes applied.

        Args:
            **changes: Fields to override in the new QueryExpr.
        Returns:
            QueryExpr: A new QueryExpr with the changes applied.
        """
        return evolve(self, **changes)

    def to_expr(self) -> Expr:
        """
        Compile this QueryExpr into an Ibis expression.

        Returns:
            Expr: The compiled Ibis expression representing the query.
        """
        return _compile_query(self)

    to_ibis = to_expr

    def execute(self, *args, **kwargs):
        """
        Execute the compiled Ibis expression and return the result.

        Args:
            *args: Positional arguments passed to Ibis execute().
            **kwargs: Keyword arguments passed to Ibis execute().
        Returns:
            The result of executing the query.
        """
        return self.to_expr().execute(*args, **kwargs)

    def sql(self) -> str:
        """
        Return the SQL string for the compiled query.

        Returns:
            str: The SQL representation of the query.
        """
        return ibis_mod.to_sql(self.to_expr())

    def maybe_to_expr(self) -> Optional[Expr]:
        """
        Try to compile this QueryExpr to an Ibis expression, returning None if it fails.

        Returns:
            Optional[Expr]: The compiled Ibis expression, or None if compilation fails.
        """
        try:
            return self.to_expr()
        except Exception:
            return None

    def chart(
        self,
        spec: Optional[Dict[str, Any]] = None,
        format: str = "altair",
    ) -> Union["altair.Chart", Dict[str, Any], bytes, str]:
        """
        Create a chart from the query using native Ibis-Altair integration.

        Args:
            spec: Optional Vega-Lite specification for the chart.
                  If not provided, will auto-detect chart type based on query.
                  If partial spec is provided (e.g., only encoding or only mark),
                  missing parts will be auto-detected and merged.
            format: The output format of the chart:
                - "altair" (default): Returns Altair Chart object
                - "interactive": Returns interactive Altair Chart with tooltip
                - "json": Returns Vega-Lite JSON specification
                - "png": Returns PNG image bytes
                - "svg": Returns SVG string

        Returns:
            Chart in the requested format:
                - altair/interactive: Altair Chart object
                - json: Dict containing Vega-Lite specification
                - png: bytes of PNG image
                - svg: str containing SVG markup

        Raises:
            ImportError: If Altair is not installed
            ValueError: If an unsupported format is specified
        """
        try:
            import altair as alt
        except ImportError:
            raise ImportError(
                "Altair is required for chart creation. "
                "Install it with: pip install 'boring-semantic-layer[visualization]'"
            )

        # Always start with auto-detected spec as base
        base_spec = _detect_chart_spec(
            dimensions=list(self.dimensions),
            measures=list(self.measures),
            time_dimension=self.model.time_dimension,
            time_grain=self.time_grain,
        )

        if spec is None:
            spec = base_spec
        else:
            if "mark" not in spec.keys():
                spec["mark"] = base_spec.get("mark", "point")

            if "encoding" not in spec.keys():
                spec["encoding"] = base_spec.get("encoding", {})

            if "transform" not in spec.keys():
                spec["transform"] = base_spec.get("transform", [])

        chart = alt.Chart(self.to_expr(), **spec)

        # Handle different output formats
        if format == "altair":
            return chart
        elif format == "interactive":
            return chart.interactive()
        elif format == "json":
            return chart.to_dict()
        elif format in ["png", "svg"]:
            try:
                import io

                buffer = io.BytesIO()
                chart.save(buffer, format=format)
                return buffer.getvalue()
            except Exception as e:
                raise ImportError(
                    f"{format} export requires additional dependencies: {e}. "
                    "Install with: pip install 'altair[all]' or pip install vl-convert-python"
                )
        else:
            raise ValueError(
                f"Unsupported format: {format}. "
                "Supported formats: 'altair', 'interactive', 'json', 'png', 'svg'"
            )


@frozen(kw_only=True, slots=True)
class SemanticModel:
    """
    Define a semantic model over an Ibis table expression with reusable dimensions and measures.

    Attributes:
        table: Base Ibis table expression.
        dimensions: Mapping of dimension names to callables producing column expressions.
        measures: Mapping of measure names to callables producing aggregate expressions.
        time_dimension: Optional name of the time dimension column.
        smallest_time_grain: Optional smallest time grain for the time dimension.

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
            },
            time_dimension='date',
            smallest_time_grain='TIME_GRAIN_DAY'
        )
    """

    table: Expr = field()
    dimensions: Mapping[str, Dimension] = field(
        converter=lambda d: MappingProxyType(dict(d))
    )
    measures: Mapping[str, Measure] = field(
        converter=lambda m: MappingProxyType(dict(m))
    )
    joins: Mapping[str, Join] = field(
        converter=lambda j: MappingProxyType(dict(j or {})),
        default=MappingProxyType({}),
    )
    primary_key: Optional[str] = field(default=None)
    name: Optional[str] = field(default=None)
    time_dimension: Optional[str] = field(default=None)
    smallest_time_grain: Optional[TimeGrain] = field(default=None)

    def __attrs_post_init__(self):
        # Derive model name if not provided
        if self.name is None:
            try:
                nm = self.table.get_name()
            except Exception:
                nm = None
            object.__setattr__(self, "name", nm)
        # Validate smallest_time_grain
        if (
            self.smallest_time_grain is not None
            and self.smallest_time_grain not in TIME_GRAIN_TRANSFORMATIONS
        ):
            # Error message indicates invalid smallest_time_grain
            valid_grains = ", ".join(TIME_GRAIN_TRANSFORMATIONS.keys())
            raise ValueError(
                f"Invalid smallest_time_grain. Must be one of: {valid_grains}"
            )

    def build_query(self) -> "QueryExpr":
        """
        Create a new QueryExpr for this SemanticModel.

        Returns:
            QueryExpr: A new QueryExpr instance for building queries.
        """
        return QueryExpr(model=self)

    def _validate_time_grain(self, time_grain: Optional[TimeGrain]) -> None:
        """Validate that the requested time grain is not finer than the smallest allowed grain."""
        if time_grain is None or self.smallest_time_grain is None:
            return

        requested_idx = TIME_GRAIN_ORDER.index(time_grain)
        smallest_idx = TIME_GRAIN_ORDER.index(self.smallest_time_grain)

        if requested_idx < smallest_idx:
            raise ValueError(
                f"Requested time grain '{time_grain}' is finer than the smallest allowed grain '{self.smallest_time_grain}'"
            )

    def _transform_time_dimension(
        self, table: Expr, time_grain: Optional[TimeGrain]
    ) -> Tuple[Expr, Dict[str, Dimension]]:
        """Transform the time dimension based on the specified grain."""
        if not self.time_dimension or not time_grain:
            return table, self.dimensions.copy()

        # Create a copy of dimensions
        dimensions = self.dimensions.copy()

        # Get or create the time dimension function
        if self.time_dimension in dimensions:
            time_dim_func = dimensions[self.time_dimension]
        else:
            # Create a default time dimension function that accesses the column directly
            def time_dim_func(t: Expr) -> Expr:
                return getattr(t, self.time_dimension)

            dimensions[self.time_dimension] = time_dim_func

        # Create the transformed dimension function
        transform_func = TIME_GRAIN_TRANSFORMATIONS[time_grain]
        dimensions[self.time_dimension] = lambda t: transform_func(time_dim_func(t))

        return table, dimensions

    def query(
        self,
        dimensions: Optional[List[str]] = None,
        measures: Optional[List[str]] = None,
        filters: Optional[
            List[Union[Dict[str, Any], str, Callable[[Expr], Expr]]]
        ] = None,
        order_by: Optional[List[Tuple[str, str]]] = None,
        limit: Optional[int] = None,
        time_range: Optional[Dict[str, str]] = None,
        time_grain: Optional[TimeGrain] = None,
    ) -> "QueryExpr":
        """
        Build a QueryExpr for this model with the specified query parameters.

        Args:
            dimensions: List of dimension names to include.
            measures: List of measure names to include.
            filters: List of filters (dict, str, callable, or Filter).
            order_by: List of (field, direction) tuples for ordering.
            limit: Maximum number of rows to return.
            time_range: Dict with 'start' and 'end' keys for time filtering.
            time_grain: The time grain to use for the time dimension.
        Returns:
            QueryExpr: The constructed QueryExpr.
        """
        # Validate time grain
        self._validate_time_grain(time_grain)
        # Prepare components, alias 'dimensions' to dimension names
        dimensions_list = list(dimensions) if dimensions else []
        measures_list = list(measures) if measures else []
        # Validate dimensions
        for d in dimensions_list:
            if isinstance(d, str) and "." in d:
                alias, field = d.split(".", 1)
                join = self.joins.get(alias)
                if not join or field not in join.model.dimensions:
                    raise KeyError(f"Unknown dimension: {d}")
            else:
                if d not in self.dimensions:
                    raise KeyError(f"Unknown dimension: {d}")
        # Validate measures
        for m in measures_list:
            if isinstance(m, str) and "." in m:
                alias, field = m.split(".", 1)
                join = self.joins.get(alias)
                if not join or field not in join.model.measures:
                    raise KeyError(f"Unknown measure: {m}")
            else:
                if m not in self.measures:
                    raise KeyError(f"Unknown measure: {m}")
        # Normalize filters to list
        if filters is None:
            filters_list = []
        else:
            filters_list = filters if isinstance(filters, list) else [filters]
        # Validate time_range format
        if time_range is not None:
            if (
                not isinstance(time_range, dict)
                or "start" not in time_range
                or "end" not in time_range
            ):
                raise ValueError(
                    "time_range must be a dictionary with 'start' and 'end' keys"
                )
        # Normalize order_by to list
        order_list = list(order_by) if order_by else []
        # Normalize time_range to tuple
        time_range_tuple = None
        if time_range:
            time_range_tuple = (time_range.get("start"), time_range.get("end"))
        # Early JSON filter validation to catch invalid specs
        # - Simple filters require 'field' and 'operator'; compound filters deferred
        for f in filters_list:
            if not isinstance(f, dict):
                continue
            # Skip compound filters here
            if f.get("operator") in Filter.COMPOUND_OPERATORS and "conditions" in f:
                continue
            # Validate required keys for simple filters
            required = {"field", "operator"}
            missing = required - set(f.keys())
            if missing:
                raise KeyError(f"Missing required keys in filter: {missing}")
            # Validate via Ibis parse to catch invalid operators or field refs
            Filter(filter=f).to_ibis(self.table, self)

        # Build the QueryExpr using fluent interface
        q = self.build_query()
        if dimensions_list:
            q = q.with_dimensions(*dimensions_list)
        if measures_list:
            q = q.with_measures(*measures_list)
        if filters_list:
            q = q.with_filters(*filters_list)
        if order_list:
            q = q.sorted(*order_list)
        if limit is not None:
            q = q.top(limit)
        if time_grain:
            q = q.grain(time_grain)
        if time_range_tuple:
            q = q.clone(time_range=time_range_tuple)

        return q

    def get_time_range(self) -> Dict[str, Any]:
        """Get the available time range for the model's time dimension.

        Returns:
            A dictionary with 'start' and 'end' dates in ISO format, or an error if no time dimension
        """
        if not self.time_dimension:
            return {"error": "Model does not have a time dimension"}

        # Get the original time dimension function
        time_dim_func = self.dimensions[self.time_dimension]

        # Query the min and max dates
        time_range = self.table.aggregate(
            start=time_dim_func(self.table).min(), end=time_dim_func(self.table).max()
        ).execute()

        # Convert to ISO format if not None
        # Access the first (and only) row's values directly
        start_val = time_range["start"].iloc[0]
        end_val = time_range["end"].iloc[0]
        start_date = start_val.isoformat() if start_val is not None else None
        end_date = end_val.isoformat() if end_val is not None else None

        return {"start": start_date, "end": end_date}

    @classmethod
    def from_yaml(
        cls,
        yaml_path: str,
        tables: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, "SemanticModel"]:
        """Load semantic models from a YAML file."""
        from .yaml_loader import from_yaml as _from_yaml
        return _from_yaml(cls, yaml_path, tables)

    @property
    def available_dimensions(self) -> List[str]:
        """
        List all available dimension keys, including joined model dimensions.

        Returns:
            List[str]: The available dimension names.
        """
        keys = list(self.dimensions.keys())
        # Include time dimension if it exists and is not already in dimensions
        if self.time_dimension and self.time_dimension not in keys:
            keys.append(self.time_dimension)
        for alias, join in self.joins.items():
            keys.extend([f"{alias}.{d}" for d in join.model.dimensions.keys()])
        return keys

    @property
    def available_measures(self) -> List[str]:
        """
        List all available measure keys, including joined model measures.

        Returns:
            List[str]: The available measure names.
        """
        keys = list(self.measures.keys())
        for alias, join in self.joins.items():
            keys.extend([f"{alias}.{m}" for m in join.model.measures.keys()])
        return keys

    @property
    def json_definition(self) -> Dict[str, Any]:
        """
        Return a JSON-serializable definition of the model, including name, dimensions, measures, time dimension, and time grain.

        Returns:
            Dict[str, Any]: The model metadata.
        """
        definition = {
            "name": self.name,
            "dimensions": self.available_dimensions,
            "measures": self.available_measures,
        }

        # Add time dimension info if present
        if self.time_dimension:
            definition["time_dimension"] = self.time_dimension

        # Add smallest time grain if present
        if self.smallest_time_grain:
            definition["smallest_time_grain"] = self.smallest_time_grain

        return definition

    @staticmethod
    def _is_additive(expr: Expr) -> bool:
        op = expr.op()
        name = type(op).__name__
        if name not in ("Sum", "Count", "Min", "Max"):
            return False
        if getattr(op, "distinct", False):
            return False
        return True

    def materialize(
        self,
        *,
        time_grain: TimeGrain = "TIME_GRAIN_DAY",
        cutoff: Union[str, datetime.datetime, datetime.date, None] = None,
        dimensions: Optional[List[str]] = None,
        storage: Any = None,
    ) -> "SemanticModel":
        """
        Materialize the model at a specified time grain, optionally filtering by cutoff and restricting dimensions.

        Args:
            time_grain: The time grain to use for materialization.
            cutoff: Optional cutoff date/time for filtering.
            dimensions: Optional list of dimensions to include.
            storage: Optional storage backend for caching.
        Returns:
            SemanticModel: A new materialized SemanticModel.
        Raises:
            RuntimeError: If not using the xorq vendor ibis backend.
        """
        if not IS_XORQ_USED:
            raise RuntimeError("materialize() requires xorq vendor ibis backend")
        mod = self.table.__class__.__module__
        if not mod.startswith("xorq.vendor.ibis"):
            raise RuntimeError(
                f"materialize() requires xorq.vendor.ibis expressions, got module {mod}"
            )
        flat = self.table
        for alias, join in self.joins.items():
            right = join.model.table
            cond = join.on(flat, right)
            flat = flat.join(right, cond, how=join.how)

        if cutoff is not None and self.time_dimension:
            if isinstance(cutoff, str):
                try:
                    cutoff_ts = datetime.datetime.fromisoformat(cutoff)
                except ValueError:
                    cutoff_ts = datetime.datetime.strptime(cutoff, "%Y-%m-%d")
            else:
                cutoff_ts = cutoff
            flat = flat.filter(getattr(flat, self.time_dimension) <= cutoff_ts)

        keys = dimensions if dimensions is not None else list(self.dimensions.keys())

        group_exprs: List[Expr] = []
        for key in keys:
            if key == self.time_dimension:
                col = flat[self.time_dimension]
                transform = TIME_GRAIN_TRANSFORMATIONS[time_grain]
                grouped_col = transform(col).name(key)
            else:
                grouped_col = self.dimensions[key](flat).name(key)
            group_exprs.append(grouped_col)

        agg_kwargs: Dict[str, Expr] = {}
        for name, fn in self.measures.items():
            expr = fn(flat)
            if self._is_additive(expr):
                agg_kwargs[name] = expr.name(name)

        if agg_kwargs:
            cube_expr = flat.group_by(*group_exprs).aggregate(**agg_kwargs)
        else:
            cube_expr = flat
        cube_table = cube_expr.cache(storage=storage)

        new_dimensions = {key: (lambda t, c=key: t[c]) for key in keys}
        new_measures: Dict[str, Measure] = {}
        for name in agg_kwargs:
            new_measures[name] = lambda t, c=name: t[c]
        for name, fn in self.measures.items():
            if name not in agg_kwargs:
                new_measures[name] = fn

        return SemanticModel(
            table=cube_table,
            dimensions=new_dimensions,
            measures=new_measures,
            joins={},
            name=f"{self.name}_cube_{time_grain.lower()}",
            time_dimension=self.time_dimension,
            smallest_time_grain=time_grain,
        )


# MCP functionality - only available if mcp package is installed
try:
    from mcp.server.fastmcp import FastMCP
    from typing import Annotated

    class MCPSemanticModel(FastMCP):
        """
        MCP server specialized for semantic models.

        Provides pre-defined tools for interacting with semantic models:
        - list_models: List all available semantic model names
        - get_model: Get model metadata and schema information
        - get_time_range: Get available time range for time-series data
        - query_model: Execute queries and optionally return visualizations

        Example:
            >>> from boring_semantic_layer import SemanticModel, MCPSemanticModel
            >>>
            >>> # Create semantic models
            >>> flights_sm = SemanticModel(
            ...     name="flights",
            ...     table=flights_table,
            ...     dimensions={...},
            ...     measures={...}
            ... )
            >>>
            >>> # Create MCP server
            >>> mcp_server = MCPSemanticModel(
            ...     models={"flights": flights_sm},
            ...     name="Flight Data Server"
            ... )
            >>>
            >>> # Run server
            >>> mcp_server.run()
        """

        def __init__(
            self,
            models: Dict[str, SemanticModel],
            name: str = "Semantic Layer MCP Server",
            *args,
            **kwargs,
        ):
            """
            Initialize MCP server with semantic models.

            Args:
                models: Dictionary mapping model names to SemanticModel instances
                name: Name of the MCP server
                *args, **kwargs: Additional arguments passed to FastMCP
            """
            super().__init__(name, *args, **kwargs)
            self.models = models
            self._register_tools()

        def _register_tools(self):
            """Register the standard semantic layer tools."""

            @self.tool()
            def list_models() -> List[str]:
                """List all available semantic model names."""
                return list(self.models.keys())

            @self.tool()
            def get_model(model_name: str) -> Dict[str, Any]:
                """Get details about a specific semantic model including available dimensions and measures."""
                if model_name not in self.models:
                    raise ValueError(f"Model {model_name} not found")
                return self.models[model_name].json_definition

            @self.tool()
            def get_time_range(model_name: str) -> Dict[str, Any]:
                """Get the available time range for a model's time dimension.

                Returns:
                    A dictionary with 'start' and 'end' dates in ISO format, or an error if the model has no time dimension
                """
                if model_name not in self.models:
                    raise ValueError(f"Model {model_name} not found")
                return self.models[model_name].get_time_range()

            @self.tool()
            def query_model(
                model_name: str,
                dimensions: Optional[List[str]] = [],
                measures: Optional[List[str]] = [],
                filters: Annotated[
                    Optional[Union[Dict, List[Dict]]],
                    """
                    List of JSON filter objects with the following structure:

                    Simple Filter:
                    {
                        "field": "dimension_name",  # Must be an existing dimension (check model schema first!).
                        "operator": "=",            # See operator list below
                        "value": "single_value"     # For single-value operators (=, !=, >, >=, <, <=, ilike, not ilike, like, not like)
                        # OR for 'in'/'not in' operators only:
                        "values": ["val1", "val2"]  # REQUIRED for 'in' and 'not in' operators
                    }

                    IMPORTANT OPERATOR GUIDELINES:
                    - Equality: Use "=" (preferred), "eq", or "equals" - all work identically
                    - Text matching: Use "ilike" (case-insensitive) instead of "like" for better results
                    - List membership: "in" requires "values" field (array), NOT "value"
                    - Negated list: "not in" requires "values" field (array), NOT "value"
                    - Pattern matching: "ilike" and "not ilike" support wildcards (%, _)
                    - Null checks: "is null" and "is not null" need no value/values field

                    Available operators:
                    - "=" / "eq" / "equals": exact match (use "value")
                    - "!=": not equal (use "value")
                    - ">", ">=", "<", "<=": comparisons (use "value")
                    - "in": value is in list (use "values" array)
                    - "not in": value not in list (use "values" array)
                    - "ilike": case-insensitive pattern match (use "value" with % wildcards)
                    - "not ilike": negated case-insensitive pattern (use "value" with % wildcards)
                    - "like": case-sensitive pattern match (use "value" with % wildcards)
                    - "not like": negated case-sensitive pattern (use "value" with % wildcards)
                    - "is null": field is null (no value/values needed)
                    - "is not null": field is not null (no value/values needed)

                    COMMON MISTAKES TO AVOID:
                    1. Don't use "value" with "in"/"not in" - use "values" array instead
                    2. Don't filter on measures - only filter on dimensions
                    3. Don't use .month(), .year() etc. - use time_grain parameter instead
                    4. For case-insensitive text search, prefer "ilike" over "like"

                    Compound Filter (AND/OR):
                    {
                        "operator": "AND",          # or "OR"
                        "conditions": [             # Non-empty list of other filter objects
                            {
                                "field": "country",
                                "operator": "equals",   # or "=" or "eq" - all equivalent
                                "value": "US"
                            },
                            {
                                "field": "tier",
                                "operator": "in",       # MUST use "values" array for "in"
                                "values": ["gold", "platinum"]  # NOT "value"
                            },
                            {
                                "field": "name",
                                "operator": "ilike",    # Case-insensitive pattern matching
                                "value": "%john%"       # Wildcards supported
                            }
                        ]
                    }

                    Example filters:
                    [
                        {"field": "status", "operator": "in", "values": ["active", "pending"]},
                        {"field": "name", "operator": "ilike", "value": "%smith%"},
                        {"field": "created_date", "operator": ">=", "value": "2024-01-01"},
                        {"field": "email", "operator": "not ilike", "value": "%spam%"}
                    ]
                    Example of a complex nested filter with time ranges:
                    [{
                        "operator": "AND",
                        "conditions": [
                            {
                                "operator": "AND",
                                "conditions": [
                                    {"field": "flight_date", "operator": ">=", "value": "2024-01-01"},
                                    {"field": "flight_date", "operator": "<", "value": "2024-04-01"}
                                ]
                            },
                            {"field": "carrier.country", "operator": "eq", "value": "US"}
                        ]
                    }]
                    """,
                ] = [],
                order_by: Annotated[
                    List[Tuple[str, str]],
                    "The order by clause to apply to the query (list of tuples: [('field', 'asc|desc')]",
                ] = [],
                limit: Annotated[int, "The limit to apply to the query"] = None,
                time_range: Annotated[
                    Optional[Dict[str, str]],
                    """Optional time range filter with format:
                        {
                            "start": "2024-01-01T00:00:00Z",  # ISO 8601 format
                            "end": "2024-12-31T23:59:59Z"     # ISO 8601 format
                        }

                        Using time_range is preferred over using filters for time-based filtering because:
                        1. It automatically applies to the model's primary time dimension
                        2. It ensures proper time zone handling with ISO 8601 format
                        3. It's more concise than creating complex filter conditions
                        4. It works seamlessly with time_grain parameter for time-based aggregations
                    """,
                ] = None,
                time_grain: Annotated[
                    Optional[
                        Literal[
                            "TIME_GRAIN_YEAR",
                            "TIME_GRAIN_QUARTER",
                            "TIME_GRAIN_MONTH",
                            "TIME_GRAIN_WEEK",
                            "TIME_GRAIN_DAY",
                            "TIME_GRAIN_HOUR",
                            "TIME_GRAIN_MINUTE",
                            "TIME_GRAIN_SECOND",
                        ]
                    ],
                    """Time grain for aggregating time-based dimensions.

                    IMPORTANT: Instead of trying to use .month(), .year(), .quarter() etc. in filters,
                    use this time_grain parameter to aggregate by time periods. The system will
                    automatically handle time dimension transformations.

                    Examples:
                    - For monthly data: time_grain="TIME_GRAIN_MONTH"
                    - For yearly data: time_grain="TIME_GRAIN_YEAR"
                    - For daily data: time_grain="TIME_GRAIN_DAY"

                    Then filter using the time_range parameter or regular date filters like:
                    {"field": "date_column", "operator": ">=", "value": "2024-01-01"}
                    """,
                ] = None,
                chart_spec: Annotated[
                    Optional[Union[bool, Dict[str, Any]]],
                    """Controls chart generation:
                    - None/False: Returns {"records": [...]} (default)
                    - True: Returns {"records": [...], "chart": {...}} with auto-detected chart
                    - Dict: Returns {"records": [...], "chart": {...}} with custom Vega-Lite specification
                      Can be partial (e.g., just {"mark": "line"} or {"encoding": {"y": {"scale": {"zero": False}}}}).
                      BSL intelligently merges partial specs with auto-detected defaults.

                    Common chart specifications:
                    - {"mark": "bar"} - Bar chart
                    - {"mark": "line"} - Line chart
                    - {"mark": "point"} - Scatter plot
                    - {"mark": "rect"} - Heatmap
                    - {"title": "My Chart"} - Add title
                    - {"width": 600, "height": 400} - Set size
                    - {"encoding": {"color": {"field": "category"}}} - Color by field
                    - {"encoding": {"y": {"scale": {"zero": False}}}} - Don't start Y-axis at zero

                    BSL auto-detection logic:
                    - Time series (time dimension + measure) → Line chart
                    - Categorical (1 dimension + 1 measure) → Bar chart
                    - Multiple measures → Multi-series chart
                    - Two dimensions → Heatmap
                    """,
                ] = None,
                chart_format: Annotated[
                    Optional[Literal["altair", "interactive", "json", "png", "svg"]],
                    """Chart output format when chart_spec is provided:
                    - "altair": Altair Chart object (default)
                    - "interactive": Interactive Altair Chart with tooltips
                    - "json": Vega-Lite JSON specification
                    - "png": Base64-encoded PNG image {"format": "png", "data": "base64..."} (requires altair[all])
                    - "svg": SVG string {"format": "svg", "data": "svg..."} (requires altair[all])
                    """,
                ] = "json",
            ) -> Dict[str, Any]:
                """Query a semantic model with JSON-based filtering and optional visualization.

                Args:
                    model_name: The name of the model to query.
                    dimensions: The dimensions to group by. Can include time dimensions like "flight_date", "flight_month", "flight_year".
                    measures: The measures to aggregate.
                    filters: List of JSON filter objects (see detailed description above).
                    order_by: The order by clause to apply to the query (list of tuples: [("field", "asc|desc")]).
                    limit: The limit to apply to the query (integer).
                    time_range: Optional time range filter for time dimensions. Preferred over using filters for time-based filtering.
                    time_grain: Optional time grain for time-based dimensions (YEAR, QUARTER, MONTH, WEEK, DAY, HOUR, MINUTE, SECOND).
                    chart_spec: Controls chart generation - None/False for data, True for auto-detected chart, or Dict for custom spec.
                    chart_format: Output format when chart_spec is provided.

                Example queries:
                ```python
                # 1. Get data as records (default)
                query_model(
                    model_name="flights",
                    dimensions=["flight_month", "carrier"],
                    measures=["total_delay", "avg_delay"],
                    time_range={"start": "2024-01-01", "end": "2024-03-31"},
                    time_grain="TIME_GRAIN_MONTH",
                    order_by=[("avg_delay", "desc")],
                    limit=10
                )  # Returns {"records": [...]}

                # 2. Get data with auto-detected chart
                query_model(
                    model_name="flights",
                    dimensions=["date"],
                    measures=["flight_count"],
                    time_grain="TIME_GRAIN_WEEK",
                    chart_spec=True  # Returns {"records": [...], "chart": {...}}
                )

                # 3. Get data with custom chart styling
                query_model(
                    model_name="flights",
                    dimensions=["date", "carrier"],
                    measures=["on_time_rate"],
                    filters=[{"field": "carrier", "operator": "in", "values": ["AA", "UA", "DL"]}],
                    time_grain="TIME_GRAIN_MONTH",
                    chart_spec={
                        "encoding": {"y": {"scale": {"zero": False}}},
                        "title": "Carrier Performance Comparison"
                    },
                    chart_format="interactive"  # Chart format in the response
                )

                # 4. Get data with PNG chart image
                query_model(
                    model_name="flights",
                    dimensions=["carrier"],
                    measures=["flight_count"],
                    chart_spec={"mark": "bar", "title": "Flight Count by Carrier"},
                    chart_format="png"  # Chart will be {"format": "png", "data": "base64..."}
                )
                ```

                Raises:
                    ValueError: If any filter object doesn't match the required structure or model not found
                """
                if not isinstance(order_by, list):
                    raise ValueError("order_by must be a list of tuples")
                for item in order_by:
                    if not (isinstance(item, (list, tuple)) and len(item) == 2):
                        raise ValueError(
                            "Each order_by item must be a tuple of (field, direction)"
                        )
                    field, direction = item
                    if not isinstance(field, str) or direction not in ("asc", "desc"):
                        raise ValueError(
                            "Each order_by tuple must be (field: str, direction: 'asc' or 'desc')"
                        )

                if model_name not in self.models:
                    raise ValueError(f"Model {model_name} not found")

                model = self.models[model_name]

                # Validate time grain if provided
                if time_grain and model.smallest_time_grain:
                    grain_order = [
                        "TIME_GRAIN_SECOND",
                        "TIME_GRAIN_MINUTE",
                        "TIME_GRAIN_HOUR",
                        "TIME_GRAIN_DAY",
                        "TIME_GRAIN_WEEK",
                        "TIME_GRAIN_MONTH",
                        "TIME_GRAIN_QUARTER",
                        "TIME_GRAIN_YEAR",
                    ]
                    if grain_order.index(time_grain) < grain_order.index(
                        model.smallest_time_grain
                    ):
                        raise ValueError(
                            f"Time grain {time_grain} is smaller than model's smallest allowed grain {model.smallest_time_grain}"
                        )

                query = model.query(
                    dimensions=dimensions,
                    measures=measures,
                    filters=filters,
                    order_by=order_by,
                    limit=limit,
                    time_range=time_range,
                    time_grain=time_grain,
                )

                output = {
                    "records": query.execute().to_dict(orient="records"),
                }
                # Check if chart is requested
                if chart_spec is not None:
                    # Handle boolean True for auto-detection
                    spec = None if chart_spec is True else chart_spec

                    # Generate the chart
                    chart = query.chart(spec=spec, format=chart_format)

                    output["chart"] = chart

                return output


except ImportError:
    # MCP not available, this is fine
    pass

# Override Join with external implementation from joins.py (imported at top)
