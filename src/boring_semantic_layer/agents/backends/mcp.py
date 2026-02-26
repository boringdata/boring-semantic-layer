import json
import re
import sys
from collections.abc import Mapping
from pathlib import Path
from typing import Annotated, Any

import ibis
from dotenv import load_dotenv
from fastmcp import FastMCP
from pydantic import Field
from pydantic.functional_validators import BeforeValidator

from ...query import _find_time_dimension
from ..utils.chart_handler import generate_chart_with_data
from ..utils.prompts import load_prompt

load_dotenv()


def _get_prompts_dir() -> Path:
    """Get the MCP prompts directory from shared-data or dev location."""
    # First try installed location (shared-data from wheel)
    installed = Path(sys.prefix) / "share" / "bsl" / "prompts" / "query" / "mcp"
    if installed.exists():
        return installed

    # Fall back to development location
    package_dir = Path(__file__).parent.parent.parent.parent.parent
    return package_dir / "docs" / "md" / "prompts" / "query" / "mcp"


PROMPTS_DIR = _get_prompts_dir()

SYSTEM_INSTRUCTIONS = load_prompt(PROMPTS_DIR, "system.md") or "MCP server for semantic models"


def _parse_json_string(v: Any) -> Any:
    if isinstance(v, str):
        try:
            return json.loads(v)
        except (json.JSONDecodeError, ValueError):
            return v
    return v


class MCPSemanticModel(FastMCP):
    def __init__(
        self,
        models: Mapping[str, Any],
        name: str = "Semantic Layer MCP Server",
        instructions: str = SYSTEM_INSTRUCTIONS,
        **kwargs,
    ):
        super().__init__(name=name, instructions=instructions, **kwargs)
        self.models = models
        self._register_tools()

    def _register_tools(self):
        @self.tool(
            name="list_models",
            description=load_prompt(PROMPTS_DIR, "tool-list-models-desc.md"),
        )
        def list_models() -> Mapping[str, str]:
            return {name: f"Semantic model: {name}" for name in self.models}

        @self.tool(
            name="get_model",
            description=load_prompt(PROMPTS_DIR, "tool-get-model-desc.md"),
        )
        def get_model(model_name: str) -> Mapping[str, Any]:
            if model_name not in self.models:
                raise ValueError(f"Model {model_name} not found")

            model = self.models[model_name]

            # Build dimension info with metadata
            dimensions = {}
            for name, dim in model.get_dimensions().items():
                dimensions[name] = {
                    "description": dim.description,
                    "is_time_dimension": dim.is_time_dimension,
                    "smallest_time_grain": dim.smallest_time_grain,
                }

            # Build measure info with metadata
            measures = {}
            for name, meas in model.get_measures().items():
                measures[name] = {"description": meas.description}

            result = {
                "name": model.name or "unnamed",
                "dimensions": dimensions,
                "measures": measures,
                "calculated_measures": list(model.get_calculated_measures().keys()),
            }

            if model.description:
                result["description"] = model.description

            return result

        @self.tool(
            name="get_time_range",
            description=load_prompt(PROMPTS_DIR, "tool-get-time-range-desc.md"),
        )
        def get_time_range(model_name: str) -> Mapping[str, Any]:
            if model_name not in self.models:
                raise ValueError(f"Model {model_name} not found")

            model = self.models[model_name]
            all_dims = list(model.dimensions)
            time_dim_name = _find_time_dimension(model, all_dims)

            if not time_dim_name:
                raise ValueError(f"Model {model_name} has no time dimension")

            # Access column directly from table to avoid Deferred recursion issue
            # time_dim.expr(tbl) returns a Deferred object that causes infinite
            # recursion when passed to tbl.aggregate()
            tbl = model.table
            # For joined models, dimension names have table prefix (e.g., 'flights.flight_date')
            # but the actual column name is just the part after the dot ('flight_date')
            col_name = time_dim_name.split(".")[-1] if "." in time_dim_name else time_dim_name
            time_col = tbl[col_name]
            result = tbl.aggregate(start=time_col.min(), end=time_col.max()).execute()

            return {
                "start": result["start"].iloc[0].isoformat(),
                "end": result["end"].iloc[0].isoformat(),
            }

        @self.tool(
            name="query_model",
            description=load_prompt(PROMPTS_DIR, "tool-query-desc.md"),
        )
        def query_model(
            model_name: str,
            dimensions: Annotated[
                list[str] | None,
                BeforeValidator(_parse_json_string),
                Field(
                    default=None,
                    description=load_prompt(PROMPTS_DIR, "tool-query-param-dimensions.md"),
                ),
            ] = None,
            measures: Annotated[
                list[str] | None,
                BeforeValidator(_parse_json_string),
                Field(
                    default=None,
                    description=load_prompt(PROMPTS_DIR, "tool-query-param-measures.md"),
                ),
            ] = None,
            filters: Annotated[
                list[dict[str, Any]] | None,
                BeforeValidator(_parse_json_string),
                Field(
                    default=None,
                    description=load_prompt(PROMPTS_DIR, "tool-query-param-filters.md"),
                ),
            ] = None,
            order_by: Annotated[
                list[list[str]] | None,
                BeforeValidator(_parse_json_string),
                Field(
                    default=None,
                    description=load_prompt(PROMPTS_DIR, "tool-query-param-order_by.md"),
                    json_schema_extra={"items": {"type": "array", "items": {"type": "string"}}},
                ),
            ] = None,
            limit: Annotated[
                int | None,
                Field(
                    default=None,
                    description=load_prompt(PROMPTS_DIR, "tool-query-param-limit.md"),
                ),
            ] = None,
            time_grain: Annotated[
                str | None,
                Field(
                    default=None,
                    description=load_prompt(PROMPTS_DIR, "tool-query-param-time_grain.md"),
                ),
            ] = None,
            time_range: Annotated[
                dict[str, str] | None,
                BeforeValidator(_parse_json_string),
                Field(
                    default=None,
                    description=load_prompt(PROMPTS_DIR, "tool-query-param-time_range.md"),
                ),
            ] = None,
            get_records: Annotated[
                bool,
                Field(
                    default=True,
                    description=load_prompt(PROMPTS_DIR, "tool-query-param-get_records.md"),
                ),
            ] = True,
            records_limit: Annotated[
                int | None,
                Field(
                    default=None,
                    description=load_prompt(PROMPTS_DIR, "tool-query-param-records_limit.md"),
                ),
            ] = None,
            get_chart: Annotated[
                bool,
                Field(
                    default=True,
                    description=load_prompt(PROMPTS_DIR, "tool-query-param-get_chart.md"),
                ),
            ] = True,
            chart_backend: Annotated[
                str | None,
                Field(
                    default=None,
                    description=load_prompt(PROMPTS_DIR, "tool-query-param-chart_backend.md"),
                ),
            ] = None,
            chart_format: Annotated[
                str | None,
                Field(
                    default=None,
                    description=load_prompt(PROMPTS_DIR, "tool-query-param-chart_format.md"),
                ),
            ] = None,
            chart_spec: Annotated[
                dict[str, Any] | None,
                BeforeValidator(_parse_json_string),
                Field(
                    default=None,
                    description=load_prompt(PROMPTS_DIR, "tool-query-param-chart_spec.md"),
                ),
            ] = None,
        ) -> str:
            if model_name not in self.models:
                raise ValueError(f"Model {model_name} not found")

            model = self.models[model_name]
            query_result = model.query(
                dimensions=dimensions,
                measures=measures,
                filters=filters or [],
                order_by=order_by,
                limit=limit,
                time_grain=time_grain,
                time_range=time_range,
            )

            return generate_chart_with_data(
                query_result,
                get_records=get_records,
                records_limit=records_limit,
                get_chart=get_chart,
                chart_backend=chart_backend,
                chart_format=chart_format,
                chart_spec=chart_spec,
                default_backend="altair",
            )

        @self.tool(
            name="search_dimension_values",
            description=load_prompt(PROMPTS_DIR, "tool-search-dimension-values-desc.md"),
        )
        def search_dimension_values(
            model_name: str,
            dimension_name: str,
            search_term: str | None = None,
            limit: int = 20,
        ) -> dict:
            if model_name not in self.models:
                raise ValueError(f"Model '{model_name}' not found")

            model = self.models[model_name]
            dims = model.get_dimensions()

            if dimension_name not in dims:
                raise ValueError(
                    f"Dimension '{dimension_name}' not found in model '{model_name}'. "
                    f"Available dimensions: {list(dims.keys())}"
                )

            dim = dims[dimension_name]
            tbl = model.table
            col_expr = dim(tbl)

            # Aggregate by value to get frequency counts
            agg = (
                tbl
                .select(col_expr.name("_value"))
                .filter(lambda t: t["_value"].notnull())
                .group_by("_value")
                .aggregate(frequency=lambda t: t.count())
            )

            # Total distinct count (before applying search filter)
            total_distinct = int(agg.count().execute())

            def _to_value_list(df):
                return [
                    {"value": str(row["_value"]), "count": int(row["frequency"])}
                    for _, row in df.iterrows()
                ]

            def _fetch(base_agg, n):
                """Fetch top n+1 rows and return (values_list, is_complete)."""
                df = (
                    base_agg
                    .order_by(ibis.desc("frequency"))
                    .limit(n + 1)
                    .execute()
                )
                complete = len(df) <= n
                return _to_value_list(df.head(n)), complete

            _SEP = r"[\s\-_.,]+"

            # Apply case-insensitive search filter if provided
            if search_term:
                search_normalized = re.sub(_SEP, " ", search_term.lower()).strip()
                filtered_agg = agg.filter(
                    lambda t: (
                        t["_value"].cast("string").lower()
                        .re_replace(_SEP, " ").strip()
                        .contains(search_normalized)
                    )
                )
                values, is_complete = _fetch(filtered_agg, limit)

                # Fallback: if search returned nothing, show top values as reference
                if not values:
                    fallback_values, fallback_complete = _fetch(agg, limit)
                    return {
                        "total_distinct": total_distinct,
                        "is_complete": fallback_complete,
                        "values": [],
                        "fallback_top_values": fallback_values,
                        "note": (
                            f"No matches found for '{search_term}'. "
                            "Showing top values for reference — use one of these exact spellings."
                        ),
                    }
            else:
                values, is_complete = _fetch(agg, limit)

            return {
                "total_distinct": total_distinct,
                "is_complete": is_complete,
                "values": values,
            }


def create_mcp_server(
    models: Mapping[str, Any],
    name: str = "Semantic Layer MCP Server",
) -> MCPSemanticModel:
    return MCPSemanticModel(models=models, name=name)
