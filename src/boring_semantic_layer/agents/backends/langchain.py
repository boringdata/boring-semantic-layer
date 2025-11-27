import json
import traceback
from collections.abc import Callable
from pathlib import Path

import ibis
from dotenv import load_dotenv
from langchain.chat_models import init_chat_model
from langchain.tools import tool
from pydantic import BaseModel, Field
from returns.result import Failure, Success

from boring_semantic_layer import from_yaml
from boring_semantic_layer.agents.utils.chart_handler import generate_chart_with_data
from boring_semantic_layer.agents.utils.prompts import load_prompt
from boring_semantic_layer.utils import safe_eval

load_dotenv()

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent.parent
_MD_DIR = _PROJECT_ROOT / "docs" / "md"
_PROMPT_DIR = _MD_DIR / "prompts" / "query" / "langchain"

SYSTEM_PROMPT = load_prompt(_PROMPT_DIR, "system.md")


def process_query(
    llm,
    user_input: str,
    conversation_history: list,
    tools: list,
    on_tool_call: Callable[[str, dict], None] | None = None,
) -> tuple[str, str]:
    messages = (
        [
            {"role": "system", "content": SYSTEM_PROMPT},
        ]
        + conversation_history
        + [{"role": "user", "content": user_input}]
    )

    llm_with_tools = llm.bind_tools(tools)

    max_iterations = 5
    all_tool_outputs = []

    for _ in range(max_iterations):
        # Get LLM response with tool calling
        response = llm_with_tools.invoke(messages)

        # Check if the LLM wants to call a tool
        if not response.tool_calls:
            # No more tool calls, return final response
            tool_output = "\n\n".join(all_tool_outputs) if all_tool_outputs else ""
            return tool_output, response.content

        messages.append(response)

        tool_map = {t.name: t for t in tools}
        for tool_call in response.tool_calls:
            function_name = tool_call["name"]
            function_args = tool_call["args"]

            # Notify frontend about tool call
            if on_tool_call:
                on_tool_call(function_name, function_args)

            if function_name in tool_map:
                tool_func = tool_map[function_name]
                tool_output = tool_func.invoke(function_args)

                # Only add query_model results to display outputs
                if function_name == "query_model":
                    all_tool_outputs.append(tool_output)
            else:
                tool_output = f"Unknown function: {function_name}"
                all_tool_outputs.append(tool_output)

            # Add tool result to messages
            messages.append(
                {
                    "role": "tool",
                    "content": tool_output,
                    "tool_call_id": tool_call["id"],
                }
            )

    tool_output = "\n\n".join(all_tool_outputs) if all_tool_outputs else ""
    return tool_output, "Reached maximum function call iterations."


class LangChainAgent:
    def __init__(
        self,
        model_path: Path,
        llm_model: str = "gpt-4",
        chart_backend: str = "plotext",
        profile: str | None = None,
        profile_file: Path | str | None = None,
    ):
        self.model_path = model_path
        self.llm_model = llm_model
        self.chart_backend = chart_backend
        self.profile = profile
        self.profile_file = profile_file
        self.conversation_history = []
        self._error_callback: Callable[[str], None] | None = None

        self.models = from_yaml(
            str(model_path),
            profile=profile,
            profile_path=str(profile_file) if profile_file else None,
        )

        @tool(description=load_prompt(_PROMPT_DIR, "tool-list-models.md"))
        def list_models() -> str:
            result = {}
            for model_name, model in self.models.items():
                model_info = {
                    "dimensions": list(model.dimensions),
                    "measures": list(model.measures),
                }
                if model.description:
                    model_info["description"] = model.description
                result[model_name] = model_info
            return json.dumps(result, indent=2)

        _index_path = _MD_DIR / "index.json"
        _doc_index = json.loads(_index_path.read_text())

        # Build description from index
        topics = _doc_index.get("topics", {})
        topic_list = ", ".join(f'"{t}"' for t in topics)
        _doc_description = (
            f"Retrieve detailed documentation on BSL topics. Available topics: {topic_list}"
        )

        @tool(description=_doc_description)
        def get_documentation(topic: str) -> str:
            if topic in topics:
                topic_info = topics[topic]
                source_path = (
                    topic_info.get("source") if isinstance(topic_info, dict) else topic_info
                )
                doc_content = load_prompt(_MD_DIR, source_path)
                return doc_content or f"❌ File not found: {source_path}"
            return f"❌ Unknown topic '{topic}'. Available topics: {', '.join(topics.keys())}"

        class QueryModelArgs(BaseModel):
            query: str = Field(description=load_prompt(_PROMPT_DIR, "param-query-model-query.md"))
            chart_spec: dict | None = Field(
                default=None,
                description=load_prompt(_PROMPT_DIR, "param-query-model-chart_spec.md"),
            )

        @tool(
            description=load_prompt(_PROMPT_DIR, "tool-query-model.md"), args_schema=QueryModelArgs
        )
        def query_model(**kwargs) -> str:
            try:
                # safe_eval returns Result type - unwrap it here
                result = safe_eval(kwargs["query"], context={**self.models, "ibis": ibis})
                if isinstance(result, Failure):
                    raise result.failure()
                query_result = result.unwrap() if isinstance(result, Success) else result
                chart_spec = kwargs.get("chart_spec")

                # Determine if we're in CLI mode (plotext backend)
                is_cli_mode = self.chart_backend == "plotext"

                # If no chart_spec provided and we're in CLI mode, add a default plotext spec
                if chart_spec is None and is_cli_mode:
                    chart_spec = {"backend": "plotext", "format": "static"}

                return generate_chart_with_data(
                    query_result,
                    chart_spec,
                    default_backend=self.chart_backend,
                    return_json=not is_cli_mode,  # Return JSON for web, render for CLI
                    error_callback=self._error_callback,
                )
            except Exception as e:
                error_detail = traceback.format_exc()
                error_msg = f"❌ Query Error: {str(e)}\n{error_detail}"
                if self._error_callback:
                    self._error_callback(error_msg)
                else:
                    print(f"\n{error_msg}")
                return f"❌ Error: {str(e)}"

        self.tools = [list_models, query_model, get_documentation]

        # Create the LLM
        self.llm = init_chat_model(llm_model, temperature=0)

    def query(
        self,
        user_input: str,
        on_tool_call: Callable[[str, dict], None] | None = None,
        on_error: Callable[[str], None] | None = None,
    ) -> tuple[str, str]:
        # Store error callback for use by query_model tool
        self._error_callback = on_error

        tool_output, agent_response = process_query(
            self.llm, user_input, self.conversation_history, self.tools, on_tool_call
        )

        self.conversation_history.append({"role": "user", "content": user_input})
        self.conversation_history.append({"role": "assistant", "content": agent_response})

        if len(self.conversation_history) > 20:
            self.conversation_history = self.conversation_history[-20:]

        # Clear the callback after query is done
        self._error_callback = None

        return tool_output, agent_response

    def reset_history(self):
        self.conversation_history = []
