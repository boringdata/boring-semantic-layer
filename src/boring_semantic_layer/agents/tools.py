"""Generic tool definitions for BSL agents.

Stores tool definitions in OpenAI JSON Schema format (the de facto standard).
LangChain's bind_tools() accepts this format directly.

References:
- OpenAI Function Calling: https://platform.openai.com/docs/guides/function-calling
- LangChain convert_to_openai_tool: https://python.langchain.com/api_reference/core/utils/langchain_core.utils.function_calling.convert_to_openai_tool.html
"""

from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

import ibis

from boring_semantic_layer import from_yaml
from boring_semantic_layer.agents.utils.chart_handler import generate_chart_with_data
from boring_semantic_layer.agents.utils.prompts import load_prompt
from boring_semantic_layer.utils import safe_eval


def _get_md_dir() -> Path:
    """Get the directory containing markdown documentation files.

    In development mode (editable install), prefer source docs for live editing.
    In production, use installed package data.
    """
    import sys

    # Check for source docs first (development mode - editable install)
    project_root = Path(__file__).resolve().parent.parent.parent.parent
    source_dir = project_root / "docs" / "md"
    if source_dir.exists():
        return source_dir

    # Fall back to installed package data
    installed_dir = Path(sys.prefix) / "share" / "bsl"
    if installed_dir.exists():
        return installed_dir

    return source_dir  # Return source path even if not found for better error messages


_MD_DIR = _get_md_dir()
_PROMPT_DIR = _MD_DIR / "prompts" / "query" / "langchain"

# Build documentation topics list for get_documentation tool
_index_path = _MD_DIR / "index.json"
_doc_index = json.loads(_index_path.read_text())
_topics = _doc_index.get("topics", {})
_topic_list = ", ".join(f'"{t}"' for t in _topics)

# Tool definitions in OpenAI JSON Schema format (the de facto standard).
# Can be used directly with:
# - OpenAI: client.chat.completions.create(tools=TOOL_DEFINITIONS)
# - LangChain: llm.bind_tools(TOOL_DEFINITIONS)
TOOL_DEFINITIONS: list[dict] = [
    {
        "type": "function",
        "function": {
            "name": "list_models",
            "description": "List all available semantic models by name. Returns model names only - use get_model(name) to see dimensions and measures for a specific model.",
            "parameters": {
                "type": "object",
                "properties": {},
                "required": [],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_model",
            "description": "Get detailed schema for a specific model. Returns all dimensions and measures with descriptions. ALWAYS call this before querying a model to know exactly which fields are available.",
            "parameters": {
                "type": "object",
                "properties": {
                    "model_name": {
                        "type": "string",
                        "description": "Name of the model to inspect (from list_models output)",
                    },
                },
                "required": ["model_name"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "query_model",
            "description": load_prompt(_PROMPT_DIR, "tool-query-model.md"),
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": load_prompt(_PROMPT_DIR, "param-query-model-query.md"),
                    },
                    "get_records": {
                        "type": "boolean",
                        "description": load_prompt(_PROMPT_DIR, "param-query-model-get_records.md"),
                        "default": True,
                    },
                    "records_limit": {
                        "type": "integer",
                        "description": load_prompt(
                            _PROMPT_DIR, "param-query-model-records_limit.md"
                        ),
                    },
                    "records_displayed_limit": {
                        "type": "integer",
                        "description": load_prompt(
                            _PROMPT_DIR, "param-query-model-records_displayed_limit.md"
                        ),
                        "default": 10,
                    },
                    "get_chart": {
                        "type": "boolean",
                        "description": load_prompt(_PROMPT_DIR, "param-query-model-get_chart.md"),
                        "default": True,
                    },
                    "chart_backend": {
                        "type": "string",
                        "description": load_prompt(
                            _PROMPT_DIR, "param-query-model-chart_backend.md"
                        ),
                    },
                    "chart_format": {
                        "type": "string",
                        "description": load_prompt(
                            _PROMPT_DIR, "param-query-model-chart_format.md"
                        ),
                    },
                    "chart_spec": {
                        "type": "object",
                        "description": load_prompt(_PROMPT_DIR, "param-query-model-chart_spec.md"),
                    },
                },
                "required": ["query"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_documentation",
            "description": f"Retrieve detailed documentation on BSL topics. Available topics: {_topic_list}",
            "parameters": {
                "type": "object",
                "properties": {
                    "topic": {
                        "type": "string",
                        "description": "The documentation topic to retrieve",
                    },
                },
                "required": ["topic"],
            },
        },
    },
]

# System prompt for BSL agents
SYSTEM_PROMPT: str = load_prompt(_PROMPT_DIR, "system.md")


class BSLTools:
    """BSL tools for LLM function calling.

    Provides tool definitions and handlers that work with any LLM provider:
    - OpenAI: client.chat.completions.create(tools=bsl.tools)
    - LangChain: llm.bind_tools(bsl.tools)

    Example:
        bsl = BSLTools(model_path=Path("flights.yml"), profile="dev")
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[...],
            tools=bsl.tools,
        )
        result = bsl.execute(tool_name, tool_args)
    """

    # Static tool definitions (OpenAI JSON Schema format)
    tools: list[dict] = TOOL_DEFINITIONS
    system_prompt: str = SYSTEM_PROMPT

    def __init__(
        self,
        model_path: Path,
        profile: str | None = None,
        profile_file: Path | str | None = None,
        chart_backend: str = "plotext",
    ):
        """Initialize BSL tools.

        Args:
            model_path: Path to YAML model definition file
            profile: Profile name from profile file
            profile_file: Path to profiles YAML file
            chart_backend: Chart backend ("plotext", "altair", "plotly")
        """
        self.model_path = model_path
        self.profile = profile
        self.profile_file = profile_file
        self.chart_backend = chart_backend
        self._error_callback: Callable[[str], None] | None = None

        self.models = from_yaml(
            str(model_path),
            profile=profile,
            profile_path=str(profile_file) if profile_file else None,
        )

    def execute(self, name: str, arguments: dict[str, Any]) -> str:
        """Execute a tool by name."""
        handlers = {
            "list_models": lambda: self._list_models(),
            "get_model": lambda: self._get_model(**arguments),
            "query_model": lambda: self._query_model(**arguments),
            "get_documentation": lambda: self._get_documentation(**arguments),
        }
        return handlers.get(name, lambda: f"Unknown tool: {name}")()

    def _list_models(self) -> str:
        """Return list of model names with brief descriptions."""
        return json.dumps(
            {name: m.description or f"Semantic model: {name}" for name, m in self.models.items()},
            indent=2,
        )

    def _get_model(self, model_name: str) -> str:
        """Return detailed schema for a specific model."""
        if model_name not in self.models:
            available = ", ".join(self.models.keys())
            return f"❌ Model '{model_name}' not found. Available models: {available}"

        model = self.models[model_name]

        # Build dimension info with metadata
        dimensions = {}
        for name, dim in model.get_dimensions().items():
            dim_info = {}
            if dim.description:
                dim_info["description"] = dim.description
            if dim.is_time_dimension:
                dim_info["is_time_dimension"] = True
            if dim.smallest_time_grain:
                dim_info["smallest_time_grain"] = dim.smallest_time_grain
            dimensions[name] = dim_info if dim_info else "dimension"

        # Build measure info with metadata
        measures = {}
        for name, meas in model.get_measures().items():
            measures[name] = meas.description if meas.description else "measure"

        result = {
            "name": model_name,
            "dimensions": dimensions,
            "measures": measures,
        }

        if model.description:
            result["description"] = model.description

        # Include calculated measures if any
        calc_measures = list(model.get_calculated_measures().keys())
        if calc_measures:
            result["calculated_measures"] = calc_measures

        return json.dumps(result, indent=2)

    def _extract_model_name(self, query: str) -> str | None:
        """Extract model name from query string (e.g., 'flights.group_by(...)' -> 'flights')."""
        for model_name in self.models:
            if query.strip().startswith(model_name + ".") or query.strip().startswith(
                model_name + "("
            ):
                return model_name
        return None

    def _query_model(
        self,
        query: str,
        get_records: bool = True,
        records_limit: int | None = None,
        records_displayed_limit: int | None = 10,
        get_chart: bool = True,
        chart_backend: str | None = None,
        chart_format: str | None = None,
        chart_spec: dict | None = None,
    ) -> str:
        from ibis import _
        from returns.result import Failure, Success

        # Extract model name for error context
        model_name = self._extract_model_name(query)

        try:
            result = safe_eval(query, context={**self.models, "ibis": ibis, "_": _})
            if isinstance(result, Failure):
                raise result.failure()
            query_result = result.unwrap() if isinstance(result, Success) else result

            return generate_chart_with_data(
                query_result,
                get_records=get_records,
                records_limit=records_limit,
                records_displayed_limit=records_displayed_limit,
                get_chart=get_chart,
                chart_backend=chart_backend,
                chart_format=chart_format,
                chart_spec=chart_spec,
                default_backend=self.chart_backend or "altair",
                return_json=False,  # CLI mode: show table in terminal
                error_callback=self._error_callback,
            )
        except Exception as e:
            error_str = str(e)
            # Truncate error to avoid context overflow (Ibis repr can be huge)
            max_error_len = 300
            if len(error_str) > max_error_len:
                # Try to extract just the key error message
                # Look for common patterns like "has no attribute 'xxx'"
                import re

                attr_match = re.search(r"'[^']+' object has no attribute '[^']+'", error_str)
                type_match = re.search(r"is not coercible to", error_str)
                if attr_match:
                    error_str = attr_match.group(0)
                elif type_match:
                    # Extract the type error part
                    error_str = error_str[:max_error_len] + "..."
                else:
                    error_str = error_str[:max_error_len] + "..."

            # Build concise error message for LLM (no traceback to save tokens)
            error_msg = f"❌ Query Error: {error_str}"
            # Add guidance for common errors
            if "truth value" in error_str.lower() and "ibis" in error_str.lower():
                error_msg += "\n\n⚠️ Don't use Python's `in` operator with Ibis columns. Use `.isin()` instead:\n  WRONG: t.col in ['a', 'b']\n  CORRECT: t.col.isin(['a', 'b'])"
            elif "has no attribute" in error_str or "AttributeError" in error_str:
                if model_name:
                    schema = self._get_model(model_name)
                    error_msg += f"\n\n📋 Available fields for '{model_name}':\n{schema}"
                else:
                    error_msg += "\n\n⚠️ This usually means you used a field/method that doesn't exist. Call get_model(model_name) to see the exact dimensions and measures available."
            if self._error_callback:
                self._error_callback(f"❌ Query Error: {error_str}")
            return error_msg

    def _get_documentation(self, topic: str) -> str:
        if topic in _topics:
            topic_info = _topics[topic]
            source_path = topic_info.get("source") if isinstance(topic_info, dict) else topic_info
            doc_content = load_prompt(_MD_DIR, source_path)
            return doc_content or f"❌ File not found: {source_path}"
        return f"❌ Unknown topic '{topic}'. Available topics: {', '.join(_topics.keys())}"

    def get_callable_tools(self) -> list:
        """Get LangChain-compatible callable tools with full descriptions.

        Returns tools that can be used with LangGraph's create_react_agent
        or any framework that needs callable tools (not just JSON schemas).

        The tools use descriptions from TOOL_DEFINITIONS to properly guide the LLM.

        Example:
            agent = create_react_agent(llm, bsl.get_callable_tools())
        """
        from langchain_core.tools import StructuredTool
        from pydantic import BaseModel, Field

        # Get descriptions from TOOL_DEFINITIONS
        tool_descs = {t["function"]["name"]: t["function"] for t in TOOL_DEFINITIONS}

        # list_models tool
        list_models_tool = StructuredTool.from_function(
            func=lambda: self.execute("list_models", {}),
            name="list_models",
            description=tool_descs["list_models"]["description"],
        )

        # get_model tool
        class GetModelArgs(BaseModel):
            model_name: str = Field(
                description=tool_descs["get_model"]["parameters"]["properties"]["model_name"][
                    "description"
                ]
            )

        get_model_tool = StructuredTool.from_function(
            func=lambda model_name: self.execute("get_model", {"model_name": model_name}),
            name="get_model",
            description=tool_descs["get_model"]["description"],
            args_schema=GetModelArgs,
        )

        # query_model tool with proper schema
        class QueryModelArgs(BaseModel):
            query: str = Field(
                description=tool_descs["query_model"]["parameters"]["properties"]["query"][
                    "description"
                ]
            )
            get_records: bool = Field(
                default=True,
                description=tool_descs["query_model"]["parameters"]["properties"]["get_records"][
                    "description"
                ],
            )
            records_limit: int | None = Field(
                default=None,
                description=tool_descs["query_model"]["parameters"]["properties"]["records_limit"][
                    "description"
                ],
            )
            records_displayed_limit: int | None = Field(
                default=10,
                description=tool_descs["query_model"]["parameters"]["properties"][
                    "records_displayed_limit"
                ]["description"],
            )
            get_chart: bool = Field(
                default=True,
                description=tool_descs["query_model"]["parameters"]["properties"]["get_chart"][
                    "description"
                ],
            )
            chart_backend: str | None = Field(
                default=None,
                description=tool_descs["query_model"]["parameters"]["properties"]["chart_backend"][
                    "description"
                ],
            )
            chart_format: str | None = Field(
                default=None,
                description=tool_descs["query_model"]["parameters"]["properties"]["chart_format"][
                    "description"
                ],
            )
            chart_spec: dict | None = Field(
                default=None,
                description=tool_descs["query_model"]["parameters"]["properties"]["chart_spec"][
                    "description"
                ],
            )

        def _query_model(
            query: str,
            get_records: bool = True,
            records_limit: int | None = None,
            records_displayed_limit: int | None = 10,
            get_chart: bool = True,
            chart_backend: str | None = None,
            chart_format: str | None = None,
            chart_spec: dict | None = None,
        ) -> str:
            args: dict[str, Any] = {"query": query}
            args["get_records"] = get_records
            if records_limit is not None:
                args["records_limit"] = records_limit
            if records_displayed_limit is not None:
                args["records_displayed_limit"] = records_displayed_limit
            args["get_chart"] = get_chart
            if chart_backend:
                args["chart_backend"] = chart_backend
            if chart_format:
                args["chart_format"] = chart_format
            if chart_spec:
                args["chart_spec"] = chart_spec
            return self.execute("query_model", args)

        query_model_tool = StructuredTool.from_function(
            func=_query_model,
            name="query_model",
            description=tool_descs["query_model"]["description"],
            args_schema=QueryModelArgs,
        )

        # get_documentation tool
        class GetDocumentationArgs(BaseModel):
            topic: str = Field(
                description=tool_descs["get_documentation"]["parameters"]["properties"]["topic"][
                    "description"
                ]
            )

        get_documentation_tool = StructuredTool.from_function(
            func=lambda topic: self.execute("get_documentation", {"topic": topic}),
            name="get_documentation",
            description=tool_descs["get_documentation"]["description"],
            args_schema=GetDocumentationArgs,
        )

        return [list_models_tool, get_model_tool, query_model_tool, get_documentation_tool]
