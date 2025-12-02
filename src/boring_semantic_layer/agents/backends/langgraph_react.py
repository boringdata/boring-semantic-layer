"""LangGraph ReAct agent for BSL.

Uses langgraph.prebuilt.create_react_agent for a proper ReAct loop
with automatic Thought -> Action -> Observation handling.
"""

from collections.abc import Callable
from pathlib import Path

from dotenv import load_dotenv
from langchain.chat_models import init_chat_model
from langchain_core.messages import HumanMessage, SystemMessage
from langgraph.prebuilt import create_react_agent

from boring_semantic_layer.agents.tools import BSLTools

load_dotenv()


class LangGraphReActAgent(BSLTools):
    """LangGraph ReAct agent wrapping BSLTools.

    Uses create_react_agent for proper ReAct behavior:
    - Automatic Thought -> Action -> Observation loop
    - Streaming support
    - Multi-model support (OpenAI, Anthropic, Google)
    """

    def __init__(
        self,
        model_path: Path,
        llm_model: str = "gpt-4",
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

        # Create the ReAct agent with callable tools from BSLTools
        self.agent = create_react_agent(
            self.llm,
            self.get_callable_tools(),  # Uses TOOL_DEFINITIONS descriptions
        )

    def query(
        self,
        user_input: str,
        on_tool_call: Callable[[str, dict], None] | None = None,
        on_error: Callable[[str], None] | None = None,
        on_thinking: Callable[[str], None] | None = None,
    ) -> tuple[str, str]:
        """Run a ReAct query with streaming callbacks.

        Returns:
            tuple of (tool_outputs, final_response)
        """
        self._error_callback = on_error

        # Build messages with system prompt and history
        messages = [SystemMessage(content=self.system_prompt)]
        messages.extend(self.conversation_history)
        messages.append(HumanMessage(content=user_input))

        all_tool_outputs = []
        final_response = ""

        # Stream through the agent execution
        for chunk in self.agent.stream(
            {"messages": messages},
            stream_mode="updates",
        ):
            # Handle agent node output (LLM responses)
            if "agent" in chunk:
                agent_messages = chunk["agent"].get("messages", [])
                for msg in agent_messages:
                    # Check for thinking/reasoning content before tool calls
                    if hasattr(msg, "content") and msg.content:
                        if hasattr(msg, "tool_calls") and msg.tool_calls:
                            # This is thinking before tool execution
                            if on_thinking:
                                on_thinking(msg.content)
                        else:
                            # This is the final response
                            final_response = msg.content

                    # Handle tool calls
                    if hasattr(msg, "tool_calls") and msg.tool_calls:
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
