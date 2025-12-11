"""Generic tool definitions for BSL agents (OpenAI JSON Schema format)."""

from __future__ import annotations

import json
import sys
from collections.abc import Callable
from functools import cache
from pathlib import Path
from typing import Any

import ibis
from langchain_core.tools import ToolException

from boring_semantic_layer import from_yaml
from boring_semantic_layer.agents.utils.chart_handler import generate_chart_with_data
from boring_semantic_layer.agents.utils.prompts import load_prompt
from boring_semantic_layer.utils import safe_eval


@cache
def _get_md_dir() -> Path:
    """Get the directory containing markdown documentation files.

    In development mode (editable install), prefer source docs for live editing.
    In production, use installed package data.
    """
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


@cache
def _get_prompt_dir() -> Path:
    return _get_md_dir() / "prompts" / "query" / "langchain"


@cache
def _get_topics() -> tuple[dict, str]:
    """Load documentation topics and formatted list."""
    topics = json.loads((_get_md_dir() / "index.json").read_text()).get("topics", {})
    return topics, ", ".join(f'"{t}"' for t in topics)


def _tool(
    name: str, description: str, properties: dict | None = None, required: list | None = None
) -> dict:
    """Helper to create tool definition."""
    return {
        "type": "function",
        "function": {
            "name": name,
            "description": description,
            "parameters": {
                "type": "object",
                "properties": properties or {},
                "required": required or [],
            },
        },
    }


@cache
def _build_tool_definitions() -> list[dict]:
    """Build tool definitions (cached)."""
    p = _get_prompt_dir()
    _, topic_list = _get_topics()
    return [
        _tool("list_models", load_prompt(p, "tool-list-models.md")),
        _tool(
            "get_model",
            "Get detailed schema for a specific model. Returns all dimensions and measures with descriptions. ALWAYS call this before querying a model to know exactly which fields are available.",
            {
                "model_name": {
                    "type": "string",
                    "description": "Name of the model to inspect (from list_models output)",
                }
            },
            ["model_name"],
        ),
        _tool(
            "query_model",
            load_prompt(p, "tool-query-model.md"),
            {
                "query": {
                    "type": "string",
                    "description": load_prompt(p, "param-query-model-query.md"),
                },
                "chart_spec": {
                    "type": "object",
                    "description": load_prompt(p, "param-query-model-chart_spec.md"),
                },
            },
            ["query"],
        ),
        _tool(
            "get_documentation",
            f"Retrieve detailed documentation on BSL topics. Available topics: {topic_list}",
            {"topic": {"type": "string", "description": "The documentation topic to retrieve"}},
            ["topic"],
        ),
    ]


@cache
def _get_system_prompt() -> str:
    return load_prompt(_get_prompt_dir(), "system.md")


# Backward-compatible exports (lazy via __getattr__)
def __getattr__(name: str):
    """Lazy module-level access for backward compatibility."""
    if name == "TOOL_DEFINITIONS":
        return _build_tool_definitions()
    if name == "SYSTEM_PROMPT":
        return _get_system_prompt()
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


class BSLTools:
    """BSL tools for LLM function calling (OpenAI/LangChain compatible)."""

    tools = property(lambda self: _build_tool_definitions())
    system_prompt = property(lambda self: _get_system_prompt())

    def __init__(
        self,
        model_path: Path,
        profile: str | None = None,
        profile_file: Path | str | None = None,
        chart_backend: str = "plotext",
    ):
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
            "list_models": self._list_models,
            "get_model": lambda: self._get_model(**arguments),
            "query_model": lambda: self._query_model(**arguments),
            "get_documentation": lambda: self._get_documentation(**arguments),
        }
        handler = handlers.get(name)
        return handler() if handler else f"Unknown tool: {name}"

    def get_callable_tools(self) -> list:
        """Get LangChain-compatible callable tools for create_agent.

        Converts the JSON Schema tool definitions to actual callable functions
        that LangGraph's create_agent can use. Tools raise ToolException on errors,
        which are handled by LangChain and returned to the LLM as error messages.
        """
        from langchain_core.tools import StructuredTool

        callable_tools = []

        # list_models - no args
        callable_tools.append(
            StructuredTool.from_function(
                func=self._list_models,
                name="list_models",
                description=self.tools[0]["function"]["description"],
                handle_tool_error=True,
            )
        )

        # get_model - model_name arg
        callable_tools.append(
            StructuredTool.from_function(
                func=self._get_model,
                name="get_model",
                description=self.tools[1]["function"]["description"],
                handle_tool_error=True,
            )
        )

        # query_model - query arg, optional chart_spec
        callable_tools.append(
            StructuredTool.from_function(
                func=self._query_model,
                name="query_model",
                description=self.tools[2]["function"]["description"],
                handle_tool_error=True,
            )
        )

        # get_documentation - topic arg
        callable_tools.append(
            StructuredTool.from_function(
                func=self._get_documentation,
                name="get_documentation",
                description=self.tools[3]["function"]["description"],
                handle_tool_error=True,
            )
        )

        return callable_tools

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
            raise ToolException(f"Model '{model_name}' not found. Available models: {available}")

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
                import re

                attr_match = re.search(r"'[^']+' object has no attribute '[^']+'", error_str)
                type_match = re.search(r"is not coercible to", error_str)
                if attr_match:
                    error_str = attr_match.group(0)
                elif type_match:
                    error_str = error_str[:max_error_len] + "..."
                else:
                    error_str = error_str[:max_error_len] + "..."

            # Build error message with guidance for common errors
            error_msg = f"Query Error: {error_str}"
            if "truth value" in error_str.lower() and "ibis" in error_str.lower():
                error_msg += "\n\nTip: Don't use Python's `in` operator with Ibis columns. Use `.isin()` instead:\n  WRONG: t.col in ['a', 'b']\n  CORRECT: t.col.isin(['a', 'b'])"
            elif "has no attribute" in error_str or "AttributeError" in error_str:
                if model_name:
                    schema = self._get_model(model_name)
                    error_msg += f"\n\nAvailable fields for '{model_name}':\n{schema}"
                else:
                    error_msg += "\n\nTip: This usually means you used a field/method that doesn't exist. Call get_model(model_name) to see the exact dimensions and measures available."

            # Always add guidance to check documentation
            error_msg += '\n\n**IMPORTANT**: Call `get_documentation("query-methods")` or any relevant topic to learn the correct syntax before retrying.'

            if self._error_callback:
                self._error_callback(error_msg)
            raise ToolException(error_msg) from e

    def _get_documentation(self, topic: str, max_chars: int = 2000) -> str:
        """Get documentation, truncated to save context tokens."""
        topics, _ = _get_topics()
        if topic in topics:
            topic_info = topics[topic]
            source_path = topic_info.get("source") if isinstance(topic_info, dict) else topic_info
            doc_content = load_prompt(_get_md_dir(), source_path)
            if not doc_content:
                raise ToolException(f"Documentation file not found: {source_path}")
            # Truncate to save context - LLM should internalize key points
            if len(doc_content) > max_chars:
                doc_content = (
                    doc_content[:max_chars] + "\n\n[...truncated - key syntax shown above]"
                )
            return doc_content
        raise ToolException(
            f"Unknown topic '{topic}'. Available topics: {', '.join(topics.keys())}"
        )
