"""OpenAI Assistants API agent for BSL.

Uses OpenAI's native Assistants API with built-in tool handling,
thread management, and streaming support.
"""

import json
from collections.abc import Callable
from pathlib import Path

from dotenv import load_dotenv
from openai import OpenAI

from boring_semantic_layer.agents.tools import BSLTools

load_dotenv()


class OpenAIAssistantAgent(BSLTools):
    """OpenAI Assistants API agent wrapping BSLTools.

    Features:
    - Native OpenAI tool handling
    - Automatic thread management
    - Streaming support with run steps
    - Built-in retry and error handling
    """

    def __init__(
        self,
        model_path: Path,
        llm_model: str = "gpt-4o",
        profile: str | None = None,
        profile_file: Path | str | None = None,
        chart_backend: str = "plotext",
        assistant_id: str | None = None,
    ):
        """Initialize OpenAI Assistant agent.

        Args:
            model_path: Path to YAML model definition
            llm_model: OpenAI model to use (gpt-4o, gpt-4-turbo, etc.)
            profile: Database profile name
            profile_file: Path to profiles YAML
            chart_backend: Chart backend for visualizations
            assistant_id: Existing assistant ID to reuse (optional)
        """
        super().__init__(
            model_path=model_path,
            profile=profile,
            profile_file=profile_file,
            chart_backend=chart_backend,
        )
        self.llm_model = llm_model
        self.client = OpenAI()

        # Convert tools to OpenAI Assistants format
        self._openai_tools = self._convert_tools()

        # Create or reuse assistant
        if assistant_id:
            self.assistant = self.client.beta.assistants.retrieve(assistant_id)
        else:
            self.assistant = self._create_assistant()

        # Create a thread for this session
        self.thread = self.client.beta.threads.create()

    def _convert_tools(self) -> list[dict]:
        """Convert BSL tools to OpenAI Assistants format."""
        # OpenAI Assistants uses same format as function calling
        return [{"type": "function", "function": tool["function"]} for tool in self.tools]

    def _create_assistant(self):
        """Create a new OpenAI Assistant with BSL tools."""
        return self.client.beta.assistants.create(
            name="BSL Query Assistant",
            instructions=self.system_prompt,
            model=self.llm_model,
            tools=self._openai_tools,
        )

    def query(
        self,
        user_input: str,
        on_tool_call: Callable[[str, dict], None] | None = None,
        on_error: Callable[[str], None] | None = None,
        on_thinking: Callable[[str], None] | None = None,
    ) -> tuple[str, str]:
        """Run a query using OpenAI Assistants API.

        Returns:
            tuple of (tool_outputs, final_response)
        """
        self._error_callback = on_error
        all_tool_outputs = []

        # Add message to thread
        self.client.beta.threads.messages.create(
            thread_id=self.thread.id,
            role="user",
            content=user_input,
        )

        # Create and poll the run
        run = self.client.beta.threads.runs.create(
            thread_id=self.thread.id,
            assistant_id=self.assistant.id,
        )

        # Process the run until completion
        while run.status in ["queued", "in_progress", "requires_action"]:
            if run.status == "requires_action":
                # Handle tool calls
                tool_outputs = []
                required_action = run.required_action

                if required_action and required_action.type == "submit_tool_outputs":
                    for tool_call in required_action.submit_tool_outputs.tool_calls:
                        name = tool_call.function.name
                        args = json.loads(tool_call.function.arguments)

                        if on_tool_call:
                            on_tool_call(name, args)

                        # Execute the tool
                        result = self.execute(name, args)

                        if name == "query_model":
                            all_tool_outputs.append(result)

                        tool_outputs.append({"tool_call_id": tool_call.id, "output": result})

                    # Submit tool outputs
                    run = self.client.beta.threads.runs.submit_tool_outputs(
                        thread_id=self.thread.id,
                        run_id=run.id,
                        tool_outputs=tool_outputs,
                    )
            else:
                # Poll for updates
                import time

                time.sleep(0.5)
                run = self.client.beta.threads.runs.retrieve(
                    thread_id=self.thread.id,
                    run_id=run.id,
                )

                # Check for thinking/reasoning in run steps
                if on_thinking:
                    steps = self.client.beta.threads.runs.steps.list(
                        thread_id=self.thread.id,
                        run_id=run.id,
                    )
                    for step in steps.data:
                        if step.type == "message_creation" and step.status == "in_progress":
                            # Could extract partial thinking here if streaming
                            pass

        # Handle completion or failure
        if run.status == "completed":
            # Get the assistant's response
            messages = self.client.beta.threads.messages.list(
                thread_id=self.thread.id,
                limit=1,
                order="desc",
            )

            final_response = ""
            if messages.data:
                msg = messages.data[0]
                if msg.role == "assistant" and msg.content:
                    for content_block in msg.content:
                        if content_block.type == "text":
                            final_response = content_block.text.value
                            break

            self._error_callback = None
            tool_output = "\n\n".join(all_tool_outputs) if all_tool_outputs else ""
            return tool_output, final_response

        elif run.status == "failed":
            error_msg = f"Run failed: {run.last_error}"
            if on_error:
                on_error(error_msg)
            self._error_callback = None
            return "", error_msg

        else:
            self._error_callback = None
            return "", f"Run ended with status: {run.status}"

    def query_stream(
        self,
        user_input: str,
        on_tool_call: Callable[[str, dict], None] | None = None,
        on_error: Callable[[str], None] | None = None,
        on_thinking: Callable[[str], None] | None = None,
        on_token: Callable[[str], None] | None = None,
    ) -> tuple[str, str]:
        """Run a streaming query using OpenAI Assistants API.

        Args:
            on_token: Callback for each streamed token

        Returns:
            tuple of (tool_outputs, final_response)
        """
        self._error_callback = on_error
        all_tool_outputs = []
        final_response = ""

        # Add message to thread
        self.client.beta.threads.messages.create(
            thread_id=self.thread.id,
            role="user",
            content=user_input,
        )

        # Create streaming run
        with self.client.beta.threads.runs.stream(
            thread_id=self.thread.id,
            assistant_id=self.assistant.id,
        ) as stream:
            for event in stream:
                # Handle text delta (streaming response)
                if event.event == "thread.message.delta":
                    delta = event.data.delta
                    if delta.content:
                        for content_block in delta.content:
                            if content_block.type == "text":
                                text = content_block.text.value
                                final_response += text
                                if on_token:
                                    on_token(text)

                # Handle tool calls
                elif event.event == "thread.run.requires_action":
                    run = event.data
                    if run.required_action and run.required_action.type == "submit_tool_outputs":
                        tool_outputs = []

                        for tool_call in run.required_action.submit_tool_outputs.tool_calls:
                            name = tool_call.function.name
                            args = json.loads(tool_call.function.arguments)

                            if on_tool_call:
                                on_tool_call(name, args)

                            result = self.execute(name, args)

                            if name == "query_model":
                                all_tool_outputs.append(result)

                            tool_outputs.append({"tool_call_id": tool_call.id, "output": result})

                        # Submit and continue streaming
                        stream.submit_tool_outputs(tool_outputs=tool_outputs)

                # Handle run step creation (thinking)
                elif event.event == "thread.run.step.created":
                    step = event.data
                    if step.type == "message_creation" and on_thinking:
                        # New message step started - could indicate thinking
                        pass

        self._error_callback = None
        tool_output = "\n\n".join(all_tool_outputs) if all_tool_outputs else ""
        return tool_output, final_response

    def reset_history(self):
        """Create a new thread (clears conversation history)."""
        self.thread = self.client.beta.threads.create()

    def cleanup(self):
        """Delete the assistant and thread."""
        try:
            self.client.beta.threads.delete(self.thread.id)
            self.client.beta.assistants.delete(self.assistant.id)
        except Exception:
            pass  # Ignore cleanup errors
