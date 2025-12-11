"""LangGraph backend for BSL.

Uses LangGraph create_agent with selective middleware (no filesystem/subagent tools).
"""

import json
from collections.abc import Callable
from pathlib import Path

from dotenv import load_dotenv
from langchain.agents import create_agent
from langchain.agents.middleware import (
    ClearToolUsesEdit,
    ContextEditingMiddleware,
    TodoListMiddleware,
)
from langchain.agents.middleware.summarization import SummarizationMiddleware
from langchain.chat_models import init_chat_model
from langchain_core.messages import HumanMessage

from boring_semantic_layer.agents.tools import BSLTools

load_dotenv()


class LangGraphBackend(BSLTools):
    """LangGraph backend wrapping BSLTools.

    Uses LangGraph create_agent with selective middleware:
    - TodoListMiddleware for planning (write_todos)
    - SummarizationMiddleware for long conversations
    - AnthropicPromptCachingMiddleware for efficiency (Anthropic models only)
    - NO FilesystemMiddleware (no ls, read_file, etc.)
    - NO SubAgentMiddleware (no task tool)
    """

    def __init__(
        self,
        model_path: Path,
        llm_model: str = "anthropic:claude-opus-4-20250514",
        profile: str | None = None,
        profile_file: Path | str | None = None,
        chart_backend: str = "plotext",
    ):
        super().__init__(
            model_path=model_path,
            profile=profile,
            profile_file=profile_file,
            chart_backend=chart_backend,
        )
        self.llm_model = llm_model
        self.llm = init_chat_model(llm_model, temperature=0)
        self.conversation_history: list = []

        # Build middleware list
        middleware = [
            TodoListMiddleware(),  # Planning with write_todos
            ContextEditingMiddleware(
                edits=[
                    # Clear get_documentation immediately after use (keep=0)
                    ClearToolUsesEdit(
                        trigger=2000,
                        keep=0,
                        exclude_tools=["query_model", "list_models", "get_model"],
                    ),
                    # Keep only last get_model result
                    ClearToolUsesEdit(
                        trigger=3500,
                        keep=1,
                        exclude_tools=["query_model", "list_models", "get_documentation"],
                    ),
                ]
            ),
            SummarizationMiddleware(
                model=self.llm,
                trigger=("tokens", 6000),  # Summarize earlier to reduce context
                keep=("messages", 3),
            ),
        ]

        # Add Anthropic prompt caching middleware only for Anthropic models
        if llm_model.startswith("anthropic:") or "claude" in llm_model.lower():
            from langchain_anthropic.middleware import AnthropicPromptCachingMiddleware

            middleware.append(AnthropicPromptCachingMiddleware(unsupported_model_behavior="ignore"))

        # Create agent with selective middleware (no filesystem/subagent tools)
        self.agent = create_agent(
            self.llm,
            tools=self.get_callable_tools(),
            system_prompt=self.system_prompt,
            middleware=middleware,
        )

    def query(
        self,
        user_input: str,
        on_tool_call: Callable[[str, dict, dict | None], None] | None = None,
        on_error: Callable[[str], None] | None = None,
        on_thinking: Callable[[str], None] | None = None,
        on_token_usage: Callable[[dict], None] | None = None,
        on_tool_result: Callable[[str, str, str | None, str | None], None] | None = None,
    ) -> tuple[str, str]:
        """Run a LangGraph query with planning capabilities.

        Args:
            user_input: The user's query
            on_tool_call: Callback for tool calls (name, args, tokens)
            on_error: Callback for errors
            on_thinking: Callback for thinking text
            on_token_usage: Callback for token usage
            on_tool_result: Callback for tool results (tool_call_id, status, error, content)

        Returns:
            tuple of (tool_outputs, final_response)
        """
        self._error_callback = on_error

        # Build messages with history
        messages = list(self.conversation_history)
        messages.append(HumanMessage(content=user_input))

        all_tool_outputs = []
        final_response = ""

        # Stream through the agent execution
        for chunk in self.agent.stream(
            {"messages": messages},
            stream_mode="updates",
        ):
            # Handle model node output (LLM responses)
            if "model" in chunk:
                model_messages = chunk["model"].get("messages", [])
                for msg in model_messages:
                    has_tool_calls = hasattr(msg, "tool_calls") and msg.tool_calls
                    content = getattr(msg, "content", None)

                    # Extract token usage from this LLM call if available
                    call_tokens = None
                    if hasattr(msg, "usage_metadata") and msg.usage_metadata:
                        call_tokens = {
                            "input_tokens": msg.usage_metadata.get("input_tokens", 0),
                            "output_tokens": msg.usage_metadata.get("output_tokens", 0),
                        }

                    # Content can be a string or a list (for Claude's mixed content)
                    thinking_text = ""
                    if isinstance(content, str) and content.strip():
                        thinking_text = content.strip()
                    elif isinstance(content, list):
                        # Claude returns list of content blocks
                        text_parts = [
                            block.get("text", "")
                            for block in content
                            if isinstance(block, dict) and block.get("type") == "text"
                        ]
                        thinking_text = " ".join(text_parts).strip()

                    if thinking_text:
                        if has_tool_calls:
                            # This is thinking before tool execution
                            if on_thinking:
                                on_thinking(thinking_text)
                        else:
                            # This is the final response
                            final_response = thinking_text

                    # Handle tool calls - pass token info for this LLM call
                    if has_tool_calls:
                        for tool_call in msg.tool_calls:
                            if on_tool_call:
                                on_tool_call(tool_call["name"], tool_call["args"], call_tokens)

            # Handle tools node output (tool results)
            if "tools" in chunk:
                tool_messages = chunk["tools"].get("messages", [])
                for msg in tool_messages:
                    tool_name = getattr(msg, "name", None)
                    tool_call_id = getattr(msg, "tool_call_id", None)
                    content = getattr(msg, "content", "")

                    if tool_name == "query_model" and content:
                        all_tool_outputs.append(content)

                    # Report tool result status with full content
                    if on_tool_result and tool_call_id:
                        # Use ToolMessage.status from LangChain (set when ToolException is raised)
                        tool_status = getattr(msg, "status", None)
                        is_error = tool_status == "error"
                        error_msg = None

                        if is_error:
                            # Error message is in content when ToolException was raised
                            error_msg = content if isinstance(content, str) else str(content)
                        elif isinstance(content, str):
                            # Check JSON for chart_error (non-fatal warning from chart generation)
                            try:
                                parsed = json.loads(content)
                                if isinstance(parsed, dict) and "chart_error" in parsed:
                                    is_error = True
                                    error_msg = f"Chart error: {parsed['chart_error']}"
                            except (json.JSONDecodeError, TypeError):
                                pass

                        status = "error" if is_error else "success"
                        on_tool_result(tool_call_id, status, error_msg, content)

        # Update conversation history
        self._update_history(user_input, final_response)
        self._error_callback = None

        tool_output = "\n\n".join(all_tool_outputs) if all_tool_outputs else ""
        return tool_output, final_response

    def _update_history(self, user_input: str, response: str):
        """Maintain conversation history."""
        self.conversation_history.append(HumanMessage(content=user_input))
        if response:
            from langchain_core.messages import AIMessage

            self.conversation_history.append(AIMessage(content=response))
        # Keep history bounded
        if len(self.conversation_history) > 20:
            self.conversation_history = self.conversation_history[-20:]

    def reset_history(self):
        """Clear conversation history."""
        self.conversation_history = []
