"""LangChain-powered Slack integration for Boring Semantic Layer.

This module provides a Slack bot that uses LangChain with OpenAI function calling
to answer data questions using BSL semantic models.
"""

import contextlib
import json
import logging
import os
from pathlib import Path
from typing import Any

from boring_semantic_layer.agents.tools import BSLTools

logger = logging.getLogger(__name__)

# Global state for tools instance (needed for LangChain @tool decorator)
_TOOLS: BSLTools | None = None


class LangChainSlackBot:
    """Slack bot for querying BSL semantic models via LangChain agent."""

    def __init__(
        self,
        semantic_model_path: str | Path,
        slack_bot_token: str | None = None,
        slack_app_token: str | None = None,
        openai_api_key: str | None = None,
        llm_model: str = "gpt-4o-mini",
        chart_backend: str = "plotly",
    ):
        """Initialize the LangChain Slack BSL bot.

        Args:
            semantic_model_path: Path to the YAML semantic model definition
            slack_bot_token: Slack Bot User OAuth Token (xoxb-...).
                           Falls back to SLACK_BOT_TOKEN env var.
            slack_app_token: Slack App-Level Token for Socket Mode (xapp-...).
                           Falls back to SLACK_APP_TOKEN env var.
            openai_api_key: OpenAI API key.
                          Falls back to OPENAI_API_KEY env var.
            llm_model: OpenAI model to use (default: gpt-4o-mini)
            chart_backend: Chart backend to use (default: plotly for image generation)
        """
        try:
            from langchain.tools import tool
            from langchain_openai import ChatOpenAI
            from slack_bolt import App
            from slack_bolt.adapter.socket_mode import SocketModeHandler
        except ImportError as e:
            raise ImportError(
                "Slack and LangChain dependencies not found. "
                "Install with: pip install boring-semantic-layer[slack]"
            ) from e

        self.semantic_model_path = Path(semantic_model_path)
        if not self.semantic_model_path.exists():
            raise FileNotFoundError(f"Semantic model not found: {semantic_model_path}")

        # Get tokens from args or environment
        self.slack_bot_token = slack_bot_token or os.environ.get("SLACK_BOT_TOKEN")
        self.slack_app_token = slack_app_token or os.environ.get("SLACK_APP_TOKEN")
        self.openai_api_key = openai_api_key or os.environ.get("OPENAI_API_KEY")

        if not self.slack_bot_token:
            raise ValueError("SLACK_BOT_TOKEN not provided or set in environment")
        if not self.slack_app_token:
            raise ValueError("SLACK_APP_TOKEN not provided or set in environment")
        if not self.openai_api_key:
            raise ValueError("OPENAI_API_KEY not provided or set in environment")

        # Set OpenAI API key
        os.environ["OPENAI_API_KEY"] = self.openai_api_key

        # Initialize Slack app
        self.app = App(token=self.slack_bot_token)
        self.socket_handler = SocketModeHandler(self.app, self.slack_app_token)

        # Initialize BSL tools
        global _TOOLS
        _TOOLS = BSLTools(self.semantic_model_path, chart_backend)
        self.tools_instance = _TOOLS

        # Instance variables for chart state
        self.last_chart_path: str | None = None
        self.last_chart_request: bool = False

        # Store reference to self for use in tool functions
        bot_instance = self

        # Define LangChain tools
        @tool
        def list_models() -> str:
            """List all available semantic models with their dimensions and measures."""
            if _TOOLS is None:
                return "Error: No models loaded."
            return _TOOLS.list_models()

        @tool
        def query_model(query: str, show_chart: bool = True, chart_spec: dict | None = None) -> str:
            """Execute a semantic model query and optionally generate a chart.

            Args:
                query: Query string (e.g., 'model.group_by("dim").aggregate("measure")')
                show_chart: Whether to generate a chart visualization (default: True)
                chart_spec: Optional chart specification dict

            Returns:
                Query results as formatted text. If show_chart=True, the chart will be
                uploaded to Slack separately.
            """
            if _TOOLS is None:
                return "Error: No models loaded."

            # Store whether we want a chart for later retrieval (in bot instance)
            bot_instance.last_chart_request = show_chart

            # Execute query and potentially generate chart file
            if show_chart:
                try:
                    summary_text, chart_path = _TOOLS.query_model_with_chart_file(query, chart_spec)
                    # Store chart path for upload (in bot instance)
                    bot_instance.last_chart_path = chart_path
                    return summary_text
                except Exception as e:
                    return str(e)
            else:
                # Just return data without chart
                return _TOOLS.query_model(
                    query, show_chart=False, show_table=False, chart_spec=chart_spec, limit=5
                )

        self.list_models_tool = list_models
        self.query_model_tool = query_model

        # Create the LLM
        try:
            self.llm = ChatOpenAI(model=llm_model, temperature=0)
        except Exception as e:
            raise RuntimeError(f"Error creating LLM: {e}") from e

        # Register event handlers
        self._register_handlers()

        logger.info(
            "LangChainSlackBot initialized with model %s (%s)", llm_model, semantic_model_path
        )

    def _register_handlers(self):
        """Register Slack event handlers."""

        @self.app.event("app_mention")
        def handle_mention(event: dict[str, Any], say: Any, logger: Any):
            """Handle when the bot is mentioned in a channel."""
            try:
                # Reset chart state
                self.last_chart_path = None
                self.last_chart_request = False

                # Extract the query (remove bot mention)
                text = event.get("text", "")
                # Remove bot mention from text
                query = text.split(">", 1)[-1].strip() if ">" in text else text

                if not query:
                    say("👋 Hi! Ask me a question about your data!")
                    return

                # Show thinking indicator
                say(f"🤔 Analyzing: _{query}_")

                # Process query with LangChain agent
                response_text = self._process_query(query)

                # Post results back to Slack
                say(f"📊 *Results:*\n\n{response_text}")

                # If a chart was generated, upload it
                if self.last_chart_path and self.last_chart_request:
                    try:
                        import os

                        channel_id = event.get("channel")
                        self.app.client.files_upload_v2(
                            channel=channel_id,
                            file=self.last_chart_path,
                            title="Query Results Chart",
                            initial_comment="📈 Here's your chart visualization:",
                        )

                        # Clean up temp file
                        with contextlib.suppress(Exception):
                            os.unlink(self.last_chart_path)
                    except Exception as chart_error:
                        logger.error("Error uploading chart: %s", chart_error, exc_info=True)
                        say(f"⚠️ Chart was generated but upload failed: {chart_error!s}")

            except Exception as e:
                logger.error("Error handling mention: %s", e, exc_info=True)
                say(f"❌ Sorry, I encountered an error: {e!s}")

        @self.app.event("message")
        def handle_message(event: dict[str, Any], say: Any, logger: Any):
            """Handle direct messages to the bot."""
            # Only respond to DMs (not channel messages)
            if event.get("channel_type") == "im":
                query = event.get("text", "")
                if query:
                    handle_mention(event, say, logger)

    def _create_system_message(self) -> str:
        """Create the system message for the LangChain agent."""
        # Load the BSL Query Expert skill
        skill_path = Path(__file__).parent / "claude-code" / "bsl-query-expert" / "SKILL.md"
        try:
            bsl_guide = skill_path.read_text()
        except FileNotFoundError:
            # Fallback to basic instructions
            bsl_guide = """
## Basic Query Syntax

All BSL queries follow this pattern:
```python
model_name.group_by(<dimensions>).aggregate(<measures>)
```

### Key Rules:
1. group_by() only accepts dimension names as strings
2. Use .with_dimensions() for time transformations
3. Always quote dimension and measure names
4. Use ibis.desc("column") for descending order
5. Use .limit(N) to limit results
"""

        return f"""You are a helpful data analytics assistant for Slack, specialized in querying semantic models using the Boring Semantic Layer.

Your role is to:
1. Understand natural language questions about data
2. Translate them into semantic model queries
3. Use the available tools to execute queries
4. Present results clearly in Slack-friendly format

Available Tools:
1. list_models() - Lists all available models, dimensions, and measures
2. query_model(query) - Executes a query and returns formatted results

When responding to questions:
1. Only call list_models() if you truly don't know what dimensions/measures are available
2. For most questions, directly construct and execute the appropriate query
3. Keep responses concise and Slack-friendly (use markdown formatting)
4. Summarize results in a clear, actionable way
5. If the data is large, provide a brief summary rather than dumping all rows

Important formatting for Slack:
- Use *bold* for emphasis
- Use `code` for dimension/measure names
- Use bullet points for lists
- Keep it concise and scannable

---

{bsl_guide}"""

    def _process_query(self, user_query: str) -> str:
        """Process a user query using the LangChain agent.

        Args:
            user_query: The user's question

        Returns:
            Formatted response text
        """
        # Create system message and user message
        messages = [
            {"role": "system", "content": self._create_system_message()},
            {"role": "user", "content": user_query},
        ]

        # Define function schemas for OpenAI
        functions = [
            {
                "name": "list_models",
                "description": "Lists all available semantic models with their dimensions and measures",
                "parameters": {
                    "type": "object",
                    "properties": {},
                },
            },
            {
                "name": "query_model",
                "description": "Executes a semantic model query and optionally generates a chart visualization. Query format: model_name.group_by('dim').aggregate('measure')",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "query": {
                            "type": "string",
                            "description": "The query to execute",
                        },
                        "show_chart": {
                            "type": "boolean",
                            "description": "Whether to generate a chart visualization (default: true). Charts are uploaded as images to Slack.",
                        },
                        "chart_spec": {
                            "type": "object",
                            "description": 'Optional chart specification (e.g., {"chart_type": "bar"})',
                        },
                    },
                    "required": ["query"],
                },
            },
        ]

        # Allow multiple rounds of function calling
        max_iterations = 5
        all_outputs = []

        for _ in range(max_iterations):
            # Get LLM response with function calling
            response = self.llm.invoke(messages, functions=functions, function_call="auto")

            # Check if the LLM wants to call a function
            if not response.additional_kwargs.get("function_call"):
                # No more function calls, return final response
                return response.content

            # Execute the function
            function_call = response.additional_kwargs["function_call"]
            function_name = function_call["name"]
            function_args = json.loads(function_call["arguments"])

            if function_name == "list_models":
                tool_output = self.list_models_tool.invoke({})
            elif function_name == "query_model":
                tool_output = self.query_model_tool.invoke(function_args)
            else:
                tool_output = f"Unknown function: {function_name}"

            all_outputs.append(tool_output)

            # Add function call and result to messages for next iteration
            messages.append(
                {
                    "role": "assistant",
                    "content": response.content or "",
                    "function_call": function_call,
                }
            )
            messages.append({"role": "function", "name": function_name, "content": tool_output})

        # If we hit max iterations, return what we have
        return "Processed query but reached iteration limit. Results:\n\n" + "\n\n".join(
            all_outputs
        )

    def start(self):
        """Start the Slack bot with Socket Mode."""
        logger.info("🚀 Starting LangChain Slack Bot...")
        logger.info("Semantic model: %s", self.semantic_model_path)
        logger.info("Bot is running and listening for messages!")
        print("\n✨ LangChain Slack Bot is running!")
        print(f"📄 Using semantic model: {self.semantic_model_path}")
        print("🤖 Powered by LangChain + OpenAI")
        print("💬 Mention the bot in Slack to ask questions about your data")
        print("Press Ctrl+C to stop\n")

        try:
            self.socket_handler.start()
        except KeyboardInterrupt:
            logger.info("Bot stopped by user")
            print("\n👋 Bot stopped")


def start_langchain_slack_bot(
    semantic_model_path: str | Path,
    slack_bot_token: str | None = None,
    slack_app_token: str | None = None,
    openai_api_key: str | None = None,
    llm_model: str = "gpt-4o-mini",
    chart_backend: str = "plotly",
):
    """Start the LangChain-powered Slack bot with chart visualization support.

    Installation:
        pip install boring-semantic-layer[slack]

    This installs Slack, LangChain, and Plotly dependencies for chart generation.

    Args:
        semantic_model_path: Path to the YAML semantic model definition
        slack_bot_token: Slack Bot User OAuth Token (xoxb-...)
        slack_app_token: Slack App-Level Token for Socket Mode (xapp-...)
        openai_api_key: OpenAI API key
        llm_model: OpenAI model to use (default: gpt-4o-mini)
        chart_backend: Chart backend to use (default: plotly for Slack image uploads)

    Environment variables:
        SLACK_BOT_TOKEN: Slack Bot User OAuth Token (if not provided as arg)
        SLACK_APP_TOKEN: Slack App-Level Token (if not provided as arg)
        OPENAI_API_KEY: OpenAI API key (if not provided as arg)
    """
    bot = LangChainSlackBot(
        semantic_model_path=semantic_model_path,
        slack_bot_token=slack_bot_token,
        slack_app_token=slack_app_token,
        openai_api_key=openai_api_key,
        llm_model=llm_model,
        chart_backend=chart_backend,
    )
    bot.start()
