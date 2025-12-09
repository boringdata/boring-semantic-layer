import json
from collections.abc import Mapping
from pathlib import Path
from typing import Annotated, Any

from dotenv import load_dotenv
from fastmcp import FastMCP
from pydantic import Field
from pydantic.functional_validators import BeforeValidator

from ...query import _find_time_dimension
from ..utils.chart_handler import generate_chart_with_data
from ..utils.prompts import load_prompt

load_dotenv()

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent.parent
PROMPTS_DIR = _PROJECT_ROOT / "docs" / "md" / "prompts" / "query" / "mcp"

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

            time_dim = model.get_dimensions()[time_dim_name]
            tbl = model.table
            time_col = time_dim.expr(tbl)
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


def create_mcp_server(
    models: Mapping[str, Any],
    name: str = "Semantic Layer MCP Server",
) -> MCPSemanticModel:
    return MCPSemanticModel(models=models, name=name)
