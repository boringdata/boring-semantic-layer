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
    import plotly.graph_objects as go

try:
    import xorq.vendor.ibis as ibis_mod

    IS_XORQ_USED = True
except ImportError:
    import ibis as ibis_mod

    IS_XORQ_USED = False

# Import from separate modules
from .joins import Join
from .filters import Filter
from .time_grain import TimeGrain, TIME_GRAIN_TRANSFORMATIONS, TIME_GRAIN_ORDER
from .query_compiler import _compile_query
from .chart import (
    _detect_altair_spec,
    _detect_plotly_chart_type,
    _prepare_plotly_data_and_params,
)

Expr = ibis_mod.expr.types.core.Expr
_ = ibis_mod._

# Join strategies
How = Literal["inner", "left", "cross"]
Cardinality = Literal["one", "many", "cross"]


@frozen(kw_only=True, slots=True)
class DimensionSpec:
    expr: Callable[[Expr], Expr]
    description: Optional[str] = None

    def __call__(self, table: Expr) -> Expr:
        return self.expr(table)


@frozen(kw_only=True, slots=True)
class MeasureSpec:
    expr: Callable[[Expr], Expr]
    description: Optional[str] = None

    def __call__(self, table: Expr) -> Expr:
        return self.expr(table)


Dimension = DimensionSpec
Measure = MeasureSpec


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

    def _chart_altair(
        self,
        spec: Optional[Dict[str, Any]] = None,
        format: str = "static",
    ) -> Union["altair.Chart", Dict[str, Any], bytes, str]:
        """
        Private method to create a chart using Altair backend.

        Args:
            spec: Optional Altair-specific specification for the chart.
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
                "Install it with: pip install 'boring-semantic-layer[viz-altair]'"
            )

        # Always start with auto-detected spec as base
        base_spec = _detect_altair_spec(
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
        if format == "static":
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
                "Supported formats: 'static', 'interactive', 'json', 'png', 'svg'"
            )

    def _chart_plotly(
        self,
        spec: Optional[Dict[str, Any]] = None,
        format: str = "interactive",
    ) -> Union["go.Figure", Dict[str, Any], bytes, str]:
        """
        Private method to create a chart using Plotly backend.

        Args:
            spec: Optional Plotly-specific parameters to customize the chart.
                  These are passed directly to the Plotly Express function (e.g., px.bar()).
                  Examples: {"title": "My Chart", "color": "category", "labels": {...}}
                  Special keys: "chart_type", "layout" and "config" are handled separately.
                  - "chart_type": Optional chart type override ("bar", "line", "scatter", "heatmap", "table")
                  - "layout": Applied via fig.update_layout()
                  - "config": Applied via fig.update_layout()
            format: The output format of the chart:
                - "plotly" (default): Returns Plotly Figure object
                - "interactive": Returns interactive Plotly Figure with tooltip
                - "json": Returns Plotly specification dictionary
                - "png": Returns PNG image bytes
                - "svg": Returns SVG string

        Returns:
            Chart in the requested format:
                - plotly/interactive: go.Figure
                - json: Dict containing Plotly specification
                - png: bytes of PNG image
                - svg: str containing SVG markup

        Raises:
            ImportError: If Plotly is not installed
            ValueError: If an unsupported format or chart_type is specified
        """
        try:
            import plotly
            import plotly.graph_objects as go
            import plotly.express as px
        except ImportError:
            raise ImportError(
                "plotly is required for chart creation. "
                "Install it with: pip install 'boring-semantic-layer[viz-plotly]'"
            )
        if format not in ["static", "interactive", "json", "png", "svg"]:
            raise ValueError(
                f"Unsupported format: {format}. "
                "Supported formats: 'static', 'interactive', 'json', 'png', 'svg'"
            )

        # Extract chart_type from spec if provided, otherwise auto-detect
        chart_type = None
        if spec is not None and "chart_type" in spec:
            chart_type = spec["chart_type"]

        if chart_type is not None:
            final_chart_type = chart_type
        else:
            final_chart_type = _detect_plotly_chart_type(
                dimensions=list(self.dimensions),
                measures=list(self.measures),
                time_dimension=self.model.time_dimension,
            )

        # Prepare data and base parameters
        df, base_params = _prepare_plotly_data_and_params(self, final_chart_type)

        # Merge base params with user-provided Plotly Express parameters
        # All spec properties are Plotly Express parameters except chart_type, layout and config
        user_params = {}
        layout_params = {}
        config_params = {}

        if spec is not None:
            for k, v in spec.items():
                if k == "chart_type":
                    pass  # Already handled above
                elif k == "layout":
                    layout_params = v
                elif k == "config":
                    config_params = v
                else:
                    user_params[k] = v

        # Final parameters for Plotly Express - user params override base params
        final_params = {**base_params, **user_params}

        # Create the actual Plotly figure by calling Plotly Express directly
        if final_chart_type == "indicator":
            raise NotImplementedError(
                "Indicator charts are not yet supported for Plotly backend"
            )
        elif final_chart_type == "bar":
            fig = px.bar(**final_params)
            # For multiple measures, set barmode to 'group' to create grouped bars instead of stacked
            if len(list(self.measures)) > 1:
                fig.update_layout(barmode="group")
        elif final_chart_type == "line":
            fig = px.line(**final_params)
        elif final_chart_type == "scatter":
            fig = px.scatter(**final_params)
        elif final_chart_type == "heatmap":
            fig = go.Figure(data=go.Heatmap(**final_params))
        elif final_chart_type == "table":
            # Special case for table - doesn't use Plotly Express
            dimensions = list(self.dimensions)
            measures = list(self.measures)
            columns = user_params.get("columns", dimensions + measures)
            fig = go.Figure(
                data=[
                    go.Table(
                        header=dict(values=columns),
                        cells=dict(
                            values=[df[col] for col in columns if col in df.columns]
                        ),
                    )
                ]
            )
        else:
            # Fallback
            fig = px.scatter(**final_params)

        if layout_params:
            fig.update_layout(**layout_params)
        if config_params:
            fig.update_layout(**config_params)

        # Handle different output formats
        if format == "static":
            return fig
        elif format == "interactive":
            return fig
        elif format == "json":
            return plotly.io.to_json(fig)
        elif format in ["png", "svg"]:
            return fig.to_image(format=format)

    def chart(
        self,
        spec: Optional[Dict[str, Any]] = None,
        backend: str = "altair",
        format: str = "static",
    ) -> Union["altair.Chart", "go.Figure", Dict[str, Any], bytes, str]:
        """
        Create a chart from the query using the specified backend.

        Args:
            spec: Optional chart specification. Format depends on backend:
                  - For vega: Vega-Lite specification
                  - For plotly: Plotly specification
            backend: The charting backend to use:
                - "altair" (default): Use Altair backend
                - "plotly": Use Plotly backend
            format: The output format of the chart:
                - "static" (default): Returns chart object (Chart/Figure)
                - "interactive": Returns interactive chart with tooltip
                - "json": Returns JSON specification
                - "png": Returns PNG image bytes
                - "svg": Returns SVG string

        Returns:
            Chart in the requested format. The exact type depends on backend:
                - vega object/interactive: Altair Chart object
                - plotly object/interactive: Plotly Figure object
                - json: Dict containing specification
                - png: bytes of PNG image
                - svg: str containing SVG markup

        Raises:
            ImportError: If the specified backend is not installed
            ValueError: If an unsupported backend or format is specified
        """
        if backend not in ["altair", "plotly"]:
            raise ValueError(
                f"Unsupported backend: {backend}. Supported backends: 'altair', 'plotly'"
            )

        if format not in ["static", "interactive", "json", "png", "svg"]:
            raise ValueError(
                f"Unsupported format: {format}. "
                "Supported formats: 'static', 'interactive', 'json', 'png', 'svg'"
            )

        if backend == "altair":
            return self._chart_altair(spec=spec, format=format)
        elif backend == "plotly":
            return self._chart_plotly(spec=spec, format=format)


def _convert_dimensions(dimension_dict) -> dict:
    """Convert plain callables to DimensionSpec with no description for backward compatibility."""
    result = {}
    for name, dim in dimension_dict.items():
        if isinstance(dim, DimensionSpec):
            result[name] = dim
        elif callable(dim):
            result[name] = DimensionSpec(expr=dim, description="")
        else:
            raise ValueError(
                f"Invalid dimension specification for {name}: {dim}. Must be a callable or DimensionSpec instance."
            )
    return result


def _convert_measures(measure_dict) -> dict:
    """Convert plain callables to MeasureSpec with no description for backward compatibility."""
    result = {}
    for name, measure in measure_dict.items():
        if isinstance(measure, MeasureSpec):
            result[name] = measure
        elif callable(measure):
            result[name] = MeasureSpec(expr=measure, description="")
        else:
            raise ValueError(
                f"Invalid measure specification for {name}: {measure}. Must be a callable or MeasureSpec instance."
            )
    return result


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
        converter=lambda d: MappingProxyType(_convert_dimensions(d))
    )
    measures: Mapping[str, Measure] = field(
        converter=lambda m: MappingProxyType(_convert_measures(m))
    )
    joins: Mapping[str, Join] = field(
        converter=lambda j: MappingProxyType(dict(j or {})),
        default=MappingProxyType({}),
    )
    description: Optional[str] = field(default=None)
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

    # Fluent builder methods for immutably extending the model
    def with_dimension(self, name: str, fn: Dimension) -> "SemanticModel":
        """
        Return a new SemanticModel with an added dimension.
        """
        dims = dict(self.dimensions)
        dims[name] = fn
        return evolve(self, dimensions=dims)

    def with_measure(self, name: str, fn: Measure) -> "SemanticModel":
        """
        Return a new SemanticModel with an added measure.
        """
        meas = dict(self.measures)
        meas[name] = fn
        return evolve(self, measures=meas)

    def with_join(self, join: Join) -> "SemanticModel":
        """
        Return a new SemanticModel with an added join.
        """
        js = dict(self.joins)
        js[join.alias] = join
        return evolve(self, joins=js)

    def with_primary_key(self, pk: str) -> "SemanticModel":
        """
        Return a new SemanticModel with a primary key set.
        """
        return evolve(self, primary_key=pk)

    def with_time_dimension(
        self, col: str, smallest_time_grain: Optional[TimeGrain] = None
    ) -> "SemanticModel":
        """
        Return a new SemanticModel with time dimension and optional smallest grain.
        """
        if smallest_time_grain is None:
            return evolve(self, time_dimension=col)
        return evolve(
            self,
            time_dimension=col,
            smallest_time_grain=smallest_time_grain,
        )

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
    def available_dimensions(self) -> Mapping[str, Dimension]:
        """
        All available dimension specs, including joined model dimensions.
        """
        dims: Dict[str, Dimension] = dict(self.dimensions)

        # Include time dimension if missing
        if self.time_dimension and self.time_dimension not in dims:
            dims[self.time_dimension] = Dimension(
                expr=lambda t, col=self.time_dimension: getattr(t, col),
                description="",
            )

        # Add joined dimensions
        for alias, join in self.joins.items():
            for dname, dspec in join.model.dimensions.items():
                dims[f"{alias}.{dname}"] = dspec

        return MappingProxyType(dims)

    @property
    def available_measures(self) -> Mapping[str, Measure]:
        """
        All available measure specs, including joined model measures.
        """
        meas: Dict[str, Measure] = dict(self.measures)

        # Add joined measures
        for alias, join in self.joins.items():
            for mname, mspec in join.model.measures.items():
                meas[f"{alias}.{mname}"] = mspec

        return MappingProxyType(meas)

    @property
    def json_definition(self) -> Dict[str, Any]:
        """
        Return a JSON-serializable definition of the model, including name, dimensions, measures, time dimension, and time grain.

        Returns:
            Dict[str, Any]: The model metadata.
        """

        # Convert DimensionSpec and MeasureSpec objects to JSON-serializable format
        dimensions_dict = {
            name: {"description": spec.description}
            for name, spec in self.available_dimensions.items()
        }
        measures_dict = {
            name: {"description": spec.description}
            for name, spec in self.available_measures.items()
        }

        definition = {
            "name": self.name,
            "dimensions": dimensions_dict,
            "measures": measures_dict,
        }

        if self.description:
            definition["description"] = self.description

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

        new_dimensions: Dict[str, Dimension] = {
            key: DimensionSpec(expr=lambda t, c=key: t[c]) for key in keys
        }
        new_measures: Dict[str, Measure] = {}
        for name in agg_kwargs:
            new_measures[name] = MeasureSpec(expr=lambda t, c=name: t[c])
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
