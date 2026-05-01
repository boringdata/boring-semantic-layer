import json
import re
import sys
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Annotated, Any

from dotenv import load_dotenv
from fastmcp import Context, FastMCP
from fastmcp.exceptions import ToolError
from mcp.types import Annotations, ToolAnnotations
from pydantic import Field
from pydantic.functional_validators import BeforeValidator

from ...query import _find_time_dimension as find_time_dimension
from ...skills import SkillMetadata
from ...yaml import SemanticModelBundle
from ..utils.chart_handler import generate_chart_with_data
from ..utils.prompts import load_prompt
from ._skill_mcp import (
    WRITE_ANNOTATIONS,
    build_domain_context,
    register_all_skill_resources,
    register_skill_resources,
    resolve_add_skill_target,
    validate_skill_name,
    write_skill,
)

load_dotenv()

_find_time_dimension = find_time_dimension  # backwards-compat alias


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

# Shared annotations — all tools are read-only, idempotent, and operate on a closed model set
READONLY_ANNOTATIONS = ToolAnnotations(
    readOnlyHint=True,
    destructiveHint=False,
    idempotentHint=True,
    openWorldHint=False,
)
_READONLY_ANNOTATIONS = READONLY_ANNOTATIONS  # backwards-compat alias


def _parse_json_string(v: Any) -> Any:
    if isinstance(v, str):
        try:
            return json.loads(v)
        except (json.JSONDecodeError, ValueError):
            return v
    return v


def _build_model_info(model: Any) -> dict[str, Any]:
    """Build model metadata dict from a semantic model (shared by tool and resource)."""
    dimensions = {}
    for name, dim in model.get_dimensions().items():
        dimensions[name] = {
            "description": dim.description,
            "is_time_dimension": dim.is_time_dimension,
            "smallest_time_grain": dim.smallest_time_grain,
        }

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


def _get_time_range_data(model: Any, model_name: str) -> dict[str, str]:
    """Get time range for a model. Returns dict with start/end as strings.

    Uses isoformat() for datetime-like values (dates, timestamps) and str()
    for scalar types like int (e.g. tax_year, fiscal_year).
    """
    all_dims = list(model.dimensions)
    time_dim_name = find_time_dimension(model, all_dims)

    if not time_dim_name:
        raise ToolError(f"Model {model_name} has no time dimension")

    tbl = model.table
    col_name = time_dim_name.split(".")[-1] if "." in time_dim_name else time_dim_name
    time_col = tbl[col_name]
    result = tbl.aggregate(start=time_col.min(), end=time_col.max()).execute()

    start_val = result["start"].iloc[0]
    end_val = result["end"].iloc[0]
    return {
        "start": start_val.isoformat() if hasattr(start_val, "isoformat") else str(start_val),
        "end": end_val.isoformat() if hasattr(end_val, "isoformat") else str(end_val),
    }


@dataclass
class ModelSelection:
    """Elicitation response type for model selection."""

    model_name: str


async def resolve_model(
    models: Mapping[str, Any],
    model_name: str,
    ctx: Context | None,
) -> Any:
    """Resolve a model by name, using elicitation if available and model not found."""
    if model_name in models:
        return models[model_name]

    # Try elicitation to let the user pick the correct model
    if ctx:
        available = list(models.keys())
        try:
            response = await ctx.elicit(
                f"Model '{model_name}' not found. "
                f"Available models: {', '.join(available)}. "
                f"Which model did you mean?",
                response_type=ModelSelection,
            )
            if response.action == "accept" and response.data:
                picked = response.data.model_name
                if picked in models:
                    await ctx.info(f"Resolved to model '{picked}'")
                    return models[picked]
        except Exception:
            pass  # Client doesn't support elicitation — fall through to error

    raise ToolError(f"Model '{model_name}' not found. Available models: {list(models.keys())}")


_resolve_model = resolve_model  # backwards-compat alias


class MCPSemanticModel(FastMCP):
    def __init__(
        self,
        models: Mapping[str, Any] | SemanticModelBundle,
        name: str = "Semantic Layer MCP Server",
        instructions: str = SYSTEM_INSTRUCTIONS,
        code_mode: bool = False,
        include_domain_context_tool: bool = True,
        include_add_skill_tool: bool = True,
        **kwargs,
    ):
        transforms = kwargs.pop("transforms", [])
        if code_mode:
            transforms = [*transforms, *_build_code_mode_transforms()]
        super().__init__(name=name, instructions=instructions, transforms=transforms, **kwargs)

        # Extract skills metadata if a bundle was passed; otherwise treat as
        # a plain mapping for backward compatibility.
        if isinstance(models, SemanticModelBundle):
            self.models = models
            self._parent_skills: list[SkillMetadata] = list(models.parent_skills)
            self._model_skills: dict[str, list[SkillMetadata]] = {
                k: list(v) for k, v in models.model_skills.items()
            }
            self._parent_skills_dir: Path | None = models.parent_skills_dir
            self._model_skills_dirs: dict[str, Path] = dict(models.model_skills_dirs)
        else:
            self.models = models
            self._parent_skills = []
            self._model_skills = {}
            self._parent_skills_dir = None
            self._model_skills_dirs = {}

        # "Has skills" means at least one skill was discovered OR at least one
        # skills_dir is configured (so add_skill can write into an empty dir).
        self._has_skills = (
            bool(self._parent_skills)
            or any(self._model_skills.values())
            or self._parent_skills_dir is not None
            or bool(self._model_skills_dirs)
        )

        self._register_tools()
        self._register_resources()
        self._register_prompts()

        if self._has_skills:
            register_all_skill_resources(self, self._parent_skills, self._model_skills)
            if include_domain_context_tool:
                self._register_domain_context_tool()
            if include_add_skill_tool:
                self._register_add_skill_tool()

    def _register_tools(self):
        @self.tool(
            name="list_models",
            description=load_prompt(PROMPTS_DIR, "tool-list-models-desc.md"),
            tags={"discovery"},
            annotations=READONLY_ANNOTATIONS,
        )
        def list_models() -> Mapping[str, str]:
            return {name: f"Semantic model: {name}" for name in self.models}

        @self.tool(
            name="get_model",
            description=load_prompt(PROMPTS_DIR, "tool-get-model-desc.md"),
            tags={"discovery", "metadata"},
            annotations=READONLY_ANNOTATIONS,
        )
        async def get_model(
            model_name: str,
            ctx: Context | None = None,
        ) -> Mapping[str, Any]:
            model = await resolve_model(self.models, model_name, ctx)
            return _build_model_info(model)

        @self.tool(
            name="get_time_range",
            description=load_prompt(PROMPTS_DIR, "tool-get-time-range-desc.md"),
            tags={"metadata"},
            annotations=READONLY_ANNOTATIONS,
        )
        async def get_time_range(
            model_name: str,
            ctx: Context | None = None,
        ) -> Mapping[str, Any]:
            model = await resolve_model(self.models, model_name, ctx)
            return _get_time_range_data(model, model_name)

        @self.tool(
            name="query_model",
            description=load_prompt(PROMPTS_DIR, "tool-query-desc.md"),
            tags={"query"},
            annotations=READONLY_ANNOTATIONS,
        )
        async def query_model(
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
            time_grains: Annotated[
                dict[str, str] | None,
                BeforeValidator(_parse_json_string),
                Field(
                    default=None,
                    description='Per-dimension time grains as a dict mapping dimension names to grain values (e.g., {"order_date": "month", "ship_date": "quarter"}). Cannot be used with time_grain.',
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
            ctx: Context | None = None,
        ) -> str:
            model = await resolve_model(self.models, model_name, ctx)

            if ctx:
                await ctx.info(
                    f"Querying model '{model_name}': dims={dimensions}, measures={measures}"
                )
                await ctx.report_progress(progress=10, total=100)

            query_result = model.query(
                dimensions=dimensions,
                measures=measures,
                filters=filters or [],
                order_by=order_by,
                limit=limit,
                time_grain=time_grain,
                time_grains=time_grains,
                time_range=time_range,
            )

            if ctx:
                await ctx.report_progress(progress=50, total=100)

            result = generate_chart_with_data(
                query_result,
                get_records=get_records,
                records_limit=records_limit,
                get_chart=get_chart,
                chart_backend=chart_backend,
                chart_format=chart_format,
                chart_spec=chart_spec,
                default_backend="altair",
            )

            if ctx:
                await ctx.report_progress(progress=90, total=100)

                # Store query in session state for follow-up tools
                await ctx.set_state(
                    "last_query",
                    {
                        "model_name": model_name,
                        "dimensions": dimensions,
                        "measures": measures,
                        "filters": filters,
                        "order_by": order_by,
                        "limit": limit,
                        "time_grain": time_grain,
                        "time_range": time_range,
                    },
                )
                await ctx.set_state("last_result", result)

                await ctx.report_progress(progress=100, total=100)

            return result

        @self.tool(
            name="search_dimension_values",
            description=load_prompt(PROMPTS_DIR, "tool-search-dimension-values-desc.md"),
            tags={"discovery", "query"},
            annotations=READONLY_ANNOTATIONS,
        )
        async def search_dimension_values(
            model_name: str,
            dimension_name: str,
            search_term: str | None = None,
            limit: int = 20,
            ctx: Context | None = None,
        ) -> dict:
            model = await resolve_model(self.models, model_name, ctx)

            if ctx:
                await ctx.info(
                    f"Searching '{dimension_name}' in '{model_name}' for '{search_term}'"
                )

            dims = model.get_dimensions()

            if dimension_name not in dims:
                # Try elicitation to let the user pick the correct dimension
                resolved = False
                if ctx:
                    try:

                        @dataclass
                        class DimensionSelection:
                            dimension_name: str

                        response = await ctx.elicit(
                            f"Dimension '{dimension_name}' not found in model "
                            f"'{model_name}'. Available dimensions: "
                            f"{', '.join(dims.keys())}. Which dimension did you mean?",
                            response_type=DimensionSelection,
                        )
                        if response.action == "accept" and response.data:
                            picked = response.data.dimension_name
                            if picked in dims:
                                dimension_name = picked
                                resolved = True
                                await ctx.info(f"Resolved to dimension '{picked}'")
                    except Exception:
                        pass  # Client doesn't support elicitation

                if not resolved:
                    raise ToolError(
                        f"Dimension '{dimension_name}' not found in model '{model_name}'. "
                        f"Available dimensions: {list(dims.keys())}"
                    )

            tbl = model.table
            # Access column directly — dim(tbl) returns a Deferred on joined
            # models which causes infinite recursion in .select()
            col_name = dimension_name.split(".")[-1] if "." in dimension_name else dimension_name
            col_expr = tbl[col_name]

            # Aggregate by value to get frequency counts
            agg = (
                tbl.select(col_expr.name("_value"))
                .filter(lambda t: t["_value"].notnull())
                .group_by("_value")
                .aggregate(frequency=lambda t: t.count())
            )

            # Total distinct count (before applying search filter)
            total_distinct = int(agg.count().execute())

            def _json_safe(val):
                """Preserve JSON-serializable types; str() everything else."""
                if val is None or isinstance(val, (bool, int, float, str)):
                    return val
                return str(val)

            def _fetch(base_agg, n):
                """Fetch top n+1 rows and return (values_list, is_complete).

                Executes via pyarrow, then sorts there — ibis
                order_by().limit() can hit infinite recursion on joined models.
                """
                tbl_pa = base_agg.to_pyarrow()
                sorted_pa = tbl_pa.sort_by([("frequency", "descending")])
                top = sorted_pa.slice(0, n + 1)
                complete = top.num_rows <= n
                result = top.slice(0, n)
                values = [
                    {
                        "value": _json_safe(result.column("_value")[i].as_py()),
                        "count": int(result.column("frequency")[i].as_py()),
                    }
                    for i in range(result.num_rows)
                ]
                return values, complete

            _SEP = r"[\s\-_.,]+"

            # Apply case-insensitive search filter if provided
            if search_term:
                search_normalized = re.sub(_SEP, " ", search_term.lower()).strip()
                filtered_agg = agg.filter(
                    lambda t: (
                        t["_value"]
                        .cast("string")
                        .lower()
                        .re_replace(_SEP, " ")
                        .strip()
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

        @self.tool(
            name="summarize_results",
            description=(
                "Generate a natural-language summary of the last query results "
                "using the connected LLM. Requires a prior query_model call in "
                "the same session."
            ),
            tags={"query", "analysis"},
            annotations=READONLY_ANNOTATIONS,
        )
        async def summarize_results(
            question: Annotated[
                str | None,
                Field(
                    default=None,
                    description="Optional question to focus the summary on",
                ),
            ] = None,
            ctx: Context | None = None,
        ) -> str:
            if not ctx:
                raise ToolError("summarize_results requires an MCP client with Context support")

            last_result = await ctx.get_state("last_result")
            last_query = await ctx.get_state("last_query")

            if not last_result:
                raise ToolError(
                    "No previous query results found in this session. Run query_model first."
                )

            prompt_parts = [
                "Analyze the following semantic layer query results and provide "
                "a concise, insightful summary.",
            ]
            if last_query:
                prompt_parts.append(
                    f"\nQuery context: model={last_query.get('model_name')}, "
                    f"dimensions={last_query.get('dimensions')}, "
                    f"measures={last_query.get('measures')}"
                )
            if question:
                prompt_parts.append(f"\nFocus on answering: {question}")
            prompt_parts.append(f"\nResults:\n{last_result}")

            try:
                response = await ctx.sample(
                    "\n".join(prompt_parts),
                    system_prompt=(
                        "You are a data analyst summarizing query results. "
                        "Be concise and highlight key insights, trends, and outliers."
                    ),
                    max_tokens=1024,
                )
                return response.text
            except Exception:
                return "\n".join(prompt_parts)

    def _register_resources(self):
        @self.resource(
            uri="semantic://models",
            name="Available Models",
            description="List all available semantic models",
            mime_type="application/json",
            annotations=Annotations(audience=["assistant"], priority=1.0),
        )
        def list_models_resource() -> str:
            models_list = {}
            for model_name in self.models:
                model = self.models[model_name]
                info = {"name": model_name}
                if model.description:
                    info["description"] = model.description
                models_list[model_name] = info
            return json.dumps(models_list, indent=2)

        @self.resource(
            uri="semantic://models/{model_name}",
            name="Model Schema",
            description="Get schema with dimensions and measures for a specific model",
            mime_type="application/json",
            annotations=Annotations(audience=["assistant"], priority=0.8),
        )
        def get_model_resource(model_name: str) -> str:
            if model_name not in self.models:
                raise ToolError(f"Model {model_name} not found")

            return json.dumps(_build_model_info(self.models[model_name]), indent=2)

        @self.resource(
            uri="semantic://models/{model_name}/time-range",
            name="Model Time Range",
            description="Get date bounds for time-series models",
            mime_type="application/json",
            annotations=Annotations(audience=["assistant"], priority=0.5),
        )
        def get_time_range_resource(model_name: str) -> str:
            if model_name not in self.models:
                raise ToolError(f"Model {model_name} not found")

            model = self.models[model_name]
            all_dims = list(model.dimensions)
            time_dim_name = find_time_dimension(model, all_dims)

            if not time_dim_name:
                return json.dumps({"error": f"Model {model_name} has no time dimension"})

            tbl = model.table
            col_name = time_dim_name.split(".")[-1] if "." in time_dim_name else time_dim_name
            time_col = tbl[col_name]
            result = tbl.aggregate(start=time_col.min(), end=time_col.max()).execute()

            start_val = result["start"].iloc[0]
            end_val = result["end"].iloc[0]
            return json.dumps(
                {
                    "model": model_name,
                    "start": start_val.isoformat()
                    if hasattr(start_val, "isoformat")
                    else str(start_val),
                    "end": end_val.isoformat() if hasattr(end_val, "isoformat") else str(end_val),
                }
            )

    def _register_domain_context_tool(self):
        @self.tool(
            name="get_domain_context",
            description=(
                load_prompt(PROMPTS_DIR, "tool-get-domain-context-desc.md")
                or "Aggregated Level-1 metadata for every available skill plus a model summary."
            ),
            tags={"discovery"},
            annotations=READONLY_ANNOTATIONS,
        )
        def get_domain_context() -> Mapping[str, Any]:
            parent_dirs: list[Path] = (
                [self._parent_skills_dir] if self._parent_skills_dir is not None else []
            )
            return build_domain_context(parent_dirs, self._model_skills_dirs, self.models)

    def _register_add_skill_tool(self):
        @self.tool(
            name="add_skill",
            description=(
                load_prompt(PROMPTS_DIR, "tool-add-skill-desc.md")
                or "Create a new skill by writing SKILL.md to the configured skills_dir."
            ),
            tags={"metadata"},
            annotations=WRITE_ANNOTATIONS,
        )
        def add_skill(
            name: Annotated[
                str,
                Field(
                    description=(
                        "Skill name in kebab-case (lowercase letters, digits, hyphens; "
                        "must start with a letter). Becomes the directory name."
                    ),
                ),
            ],
            description: Annotated[
                str,
                Field(
                    description=(
                        "Trigger phrase for the skill. Goes into the SKILL.md frontmatter; "
                        "should describe when an agent should invoke it."
                    ),
                ),
            ],
            steps: Annotated[
                str,
                Field(
                    description=(
                        "Markdown body of the SKILL.md (everything below the frontmatter). "
                        "Should be runnable instructions, not a description of the skill."
                    ),
                ),
            ],
            model_name: Annotated[
                str | None,
                Field(
                    default=None,
                    description=(
                        "Optional model to scope this skill to. Falls back to the parent "
                        "skills_dir if the named model has no skills_dir."
                    ),
                ),
            ] = None,
            reference: Annotated[
                str | None,
                Field(
                    default=None,
                    description="Optional REFERENCE.md content (longer reference docs).",
                ),
            ] = None,
        ) -> Mapping[str, Any]:
            validate_skill_name(name)
            target_dir, scope = resolve_add_skill_target(
                name,
                model_name,
                self._parent_skills_dir,
                self._model_skills_dirs,
            )
            skill_dir = write_skill(target_dir, name, description, steps, reference)

            new_skill = SkillMetadata(
                name=name,
                description=description.strip(),
                dir_path=skill_dir,
                scope=scope,
            )
            register_skill_resources(self, new_skill)

            uri_prefix = f"skill://{name}/" if scope == "parent" else f"skill://{scope}/{name}/"
            return {
                "status": "created",
                "name": name,
                "scope": scope,
                "dir_path": str(skill_dir),
                "uri_prefix": uri_prefix,
            }

    def _register_prompts(self):
        @self.prompt(
            name="query_guide",
            description="Comprehensive query syntax reference for the semantic layer",
        )
        def query_guide() -> str:
            return load_prompt(PROMPTS_DIR, "tool-query.md") or "Query guide not available."

        @self.prompt(
            name="model_exploration_guide",
            description="How to explore model metadata (dimensions, measures, time ranges)",
        )
        def model_exploration_guide() -> str:
            return (
                load_prompt(PROMPTS_DIR, "tool-get-model.md")
                or "Model exploration guide not available."
            )

        @self.prompt(
            name="getting_started",
            description="Semantic layer overview and usage guidelines",
        )
        def getting_started() -> str:
            return load_prompt(PROMPTS_DIR, "system.md") or "Getting started guide not available."


def _build_code_mode_transforms() -> list:
    """Build CodeMode transforms. Requires fastmcp[code-mode] >= 3.1.0."""
    try:
        from fastmcp.experimental.transforms.code_mode import (
            CodeMode,
            GetSchemas,
            GetTags,
            Search,
        )
    except ImportError as e:
        raise ImportError(
            "CodeMode requires the 'code-mode' extra. "
            "Install with: pip install 'fastmcp[code-mode]>=3.1.0' "
            "or: pip install 'boring-semantic-layer[mcp-code-mode]'"
        ) from e

    return [
        CodeMode(
            discovery_tools=[GetTags(), Search(), GetSchemas()],
        )
    ]


def create_mcp_server(
    models: Mapping[str, Any],
    name: str = "Semantic Layer MCP Server",
    code_mode: bool = False,
) -> MCPSemanticModel:
    return MCPSemanticModel(models=models, name=name, code_mode=code_mode)
