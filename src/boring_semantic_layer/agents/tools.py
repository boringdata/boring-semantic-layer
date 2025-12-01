"""Generic tool definitions for BSL agents.

Stores tool definitions in OpenAI JSON Schema format (the de facto standard).
LangChain's bind_tools() accepts this format directly.

References:
- OpenAI Function Calling: https://platform.openai.com/docs/guides/function-calling
- LangChain convert_to_openai_tool: https://python.langchain.com/api_reference/core/utils/langchain_core.utils.function_calling.convert_to_openai_tool.html
"""

from __future__ import annotations

import json
import traceback
from collections.abc import Callable
from pathlib import Path
from typing import Any

import ibis

from boring_semantic_layer import from_yaml
from boring_semantic_layer.agents.utils.chart_handler import generate_chart_with_data
from boring_semantic_layer.agents.utils.prompts import load_prompt
from boring_semantic_layer.utils import safe_eval


def _get_md_dir() -> Path:
    """Get the directory containing markdown documentation files."""
    import sys

    installed_dir = Path(sys.prefix) / "share" / "bsl"
    if installed_dir.exists():
        return installed_dir

    project_root = Path(__file__).resolve().parent.parent.parent.parent
    return project_root / "docs" / "md"


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
            "description": load_prompt(_PROMPT_DIR, "tool-list-models.md"),
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
            "name": "query_model",
            "description": load_prompt(_PROMPT_DIR, "tool-query-model.md"),
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": load_prompt(_PROMPT_DIR, "param-query-model-query.md"),
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
            "query_model": lambda: self._query_model(**arguments),
            "get_documentation": lambda: self._get_documentation(**arguments),
        }
        return handlers.get(name, lambda: f"Unknown tool: {name}")()

    def _list_models(self) -> str:
        return json.dumps(
            {
                name: {
                    "dimensions": list(m.dimensions),
                    "measures": list(m.measures),
                    **({"description": m.description} if m.description else {}),
                }
                for name, m in self.models.items()
            },
            indent=2,
        )

    def _query_model(self, query: str, chart_spec: dict | None = None) -> str:
        from ibis import _
        from returns.result import Failure, Success

        try:
            result = safe_eval(query, context={**self.models, "ibis": ibis, "_": _})
            if isinstance(result, Failure):
                raise result.failure()
            query_result = result.unwrap() if isinstance(result, Success) else result

            is_cli_mode = self.chart_backend == "plotext"
            if chart_spec is None and is_cli_mode:
                chart_spec = {
                    "backend": "plotext",
                    "format": "static",
                    "show_chart": True,
                    "show_table": True,
                }

            return generate_chart_with_data(
                query_result,
                chart_spec,
                default_backend=self.chart_backend,
                return_json=not is_cli_mode,
                error_callback=self._error_callback,
            )
        except Exception as e:
            error_detail = traceback.format_exc()
            error_msg = f"❌ Query Error: {e}\n{error_detail}"
            if self._error_callback:
                self._error_callback(error_msg)
            return f"❌ Error: {e}"

    def _get_documentation(self, topic: str) -> str:
        if topic in _topics:
            topic_info = _topics[topic]
            source_path = topic_info.get("source") if isinstance(topic_info, dict) else topic_info
            doc_content = load_prompt(_MD_DIR, source_path)
            return doc_content or f"❌ File not found: {source_path}"
        return f"❌ Unknown topic '{topic}'. Available topics: {', '.join(_topics.keys())}"
