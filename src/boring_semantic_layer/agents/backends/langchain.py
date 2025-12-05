"""LangChain-based chat agent for BSL."""

from collections.abc import Callable
from pathlib import Path

from dotenv import load_dotenv
from langchain.chat_models import init_chat_model

from boring_semantic_layer.agents.tools import BSLTools

load_dotenv()


class LangChainAgent(BSLTools):
    """LangChain chat agent wrapping BSLTools.

    Adds conversation history and LLM chat loop on top of BSLTools.
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
        self.conversation_history: list[dict] = []

    def query(
        self,
        user_input: str,
        on_tool_call: Callable[[str, dict], None] | None = None,
        on_error: Callable[[str], None] | None = None,
        on_thinking: Callable[[str], None] | None = None,
    ) -> tuple[str, str]:
        """Run a chat query with tool calling loop."""
        self._error_callback = on_error

        messages = (
            [{"role": "system", "content": self.system_prompt}]
            + self.conversation_history
            + [{"role": "user", "content": user_input}]
        )

        llm_with_tools = self.llm.bind_tools(self.tools)
        all_tool_outputs = []

        for _ in range(5):
            response = llm_with_tools.invoke(messages)

            # Display LLM's reasoning/thinking text if present (before tool calls)
            if response.content and response.tool_calls and on_thinking:
                on_thinking(response.content)

            if not response.tool_calls:
                tool_output = "\n\n".join(all_tool_outputs) if all_tool_outputs else ""
                self._update_history(user_input, response.content)
                self._error_callback = None
                return tool_output, response.content

            messages.append(response)

            for tool_call in response.tool_calls:
                name = tool_call["name"]
                args = tool_call["args"]

                if on_tool_call:
                    on_tool_call(name, args)

                result = self.execute(name, args)

                if name == "query_model":
                    all_tool_outputs.append(result)

                messages.append(
                    {
                        "role": "tool",
                        "content": result,
                        "tool_call_id": tool_call["id"],
                    }
                )

        tool_output = "\n\n".join(all_tool_outputs) if all_tool_outputs else ""
        self._update_history(user_input, "Reached maximum function call iterations.")
        self._error_callback = None
        return tool_output, "Reached maximum function call iterations."

    def _update_history(self, user_input: str, response: str):
        self.conversation_history.append({"role": "user", "content": user_input})
        self.conversation_history.append({"role": "assistant", "content": response})
        if len(self.conversation_history) > 20:
            self.conversation_history = self.conversation_history[-20:]

    def reset_history(self):
        self.conversation_history = []
