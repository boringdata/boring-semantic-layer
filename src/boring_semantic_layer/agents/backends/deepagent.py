"""DeepAgent backend for BSL.

Uses deepagents library for advanced planning and task decomposition.
DeepAgents automatically handles multi-step planning, making it better
at discovering values before filtering.
"""

import warnings
from collections.abc import Callable
from pathlib import Path

# Suppress deprecation warnings from deepagents SummarizationMiddleware
# These are internal to the deepagents library and will be fixed upstream
warnings.filterwarnings(
    "ignore",
    message="max_tokens_before_summary is deprecated",
    category=DeprecationWarning,
    module="deepagents.graph",
)
warnings.filterwarnings(
    "ignore",
    message="messages_to_keep is deprecated",
    category=DeprecationWarning,
    module="deepagents.graph",
)

from deepagents import create_deep_agent  # noqa: E402
from dotenv import load_dotenv  # noqa: E402
from langchain.chat_models import init_chat_model  # noqa: E402
from langchain_core.messages import HumanMessage  # noqa: E402

from boring_semantic_layer.agents.tools import BSLTools  # noqa: E402

load_dotenv()


class DeepAgentBackend(BSLTools):
    """DeepAgent backend wrapping BSLTools.

    Uses create_deep_agent for advanced planning:
    - Built-in task decomposition (write_todos)
    - Better multi-step reasoning
    - Automatic planning before execution
    """

    def __init__(
        self,
        model_path: Path,
        llm_model: str = "anthropic:claude-sonnet-4-20250514",
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

        # Create the deep agent with BSL tools
        # DeepAgent adds planning capabilities on top
        self.agent = create_deep_agent(
            model=self.llm,
            tools=self.get_callable_tools(),
            system_prompt=self.system_prompt,
        )

    def query(
        self,
        user_input: str,
        on_tool_call: Callable[[str, dict], None] | None = None,
        on_error: Callable[[str], None] | None = None,
        on_thinking: Callable[[str], None] | None = None,
    ) -> tuple[str, str]:
        """Run a DeepAgent query with planning capabilities.

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

                    # Handle tool calls
                    if has_tool_calls:
                        for tool_call in msg.tool_calls:
                            if on_tool_call:
                                on_tool_call(tool_call["name"], tool_call["args"])

            # Handle tools node output (tool results)
            if "tools" in chunk:
                tool_messages = chunk["tools"].get("messages", [])
                for msg in tool_messages:
                    if (
                        hasattr(msg, "name")
                        and msg.name == "query_model"
                        and hasattr(msg, "content")
                    ):
                        all_tool_outputs.append(msg.content)

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
