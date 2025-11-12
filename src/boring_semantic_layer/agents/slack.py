"""Slack integration for Boring Semantic Layer.

This module provides a Slack bot that uses Claude's code execution API
to answer data questions using BSL semantic models.
"""

import logging
import os
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


class SlackBSLBot:
    """Slack bot for querying BSL semantic models via Claude code execution."""

    def __init__(
        self,
        semantic_model_path: str | Path,
        slack_bot_token: str | None = None,
        slack_app_token: str | None = None,
        anthropic_api_key: str | None = None,
    ):
        """Initialize the Slack BSL bot.

        Args:
            semantic_model_path: Path to the YAML semantic model definition
            slack_bot_token: Slack Bot User OAuth Token (xoxb-...).
                           Falls back to SLACK_BOT_TOKEN env var.
            slack_app_token: Slack App-Level Token for Socket Mode (xapp-...).
                           Falls back to SLACK_APP_TOKEN env var.
            anthropic_api_key: Anthropic API key.
                             Falls back to ANTHROPIC_API_KEY env var.
        """
        try:
            from anthropic import Anthropic
            from slack_bolt import App
            from slack_bolt.adapter.socket_mode import SocketModeHandler
        except ImportError as e:
            raise ImportError(
                "Slack and Anthropic dependencies not found. "
                "Install with: pip install 'boring-semantic-layer[slack]'"
            ) from e

        self.semantic_model_path = Path(semantic_model_path)
        if not self.semantic_model_path.exists():
            raise FileNotFoundError(f"Semantic model not found: {semantic_model_path}")

        # Load semantic model YAML
        self.semantic_model_yaml = self.semantic_model_path.read_text()

        # Get tokens from args or environment
        self.slack_bot_token = slack_bot_token or os.environ.get("SLACK_BOT_TOKEN")
        self.slack_app_token = slack_app_token or os.environ.get("SLACK_APP_TOKEN")
        self.anthropic_api_key = anthropic_api_key or os.environ.get("ANTHROPIC_API_KEY")

        if not self.slack_bot_token:
            raise ValueError("SLACK_BOT_TOKEN not provided or set in environment")
        if not self.slack_app_token:
            raise ValueError("SLACK_APP_TOKEN not provided or set in environment")
        if not self.anthropic_api_key:
            raise ValueError("ANTHROPIC_API_KEY not provided or set in environment")

        # Initialize Slack app
        self.app = App(token=self.slack_bot_token)
        self.socket_handler = SocketModeHandler(self.app, self.slack_app_token)

        # Initialize Anthropic client
        self.anthropic_client = Anthropic(api_key=self.anthropic_api_key)

        # Register event handlers
        self._register_handlers()

        logger.info("SlackBSLBot initialized with semantic model: %s", semantic_model_path)

    def _register_handlers(self):
        """Register Slack event handlers."""

        @self.app.event("app_mention")
        def handle_mention(event: dict[str, Any], say: Any, logger: Any):
            """Handle when the bot is mentioned in a channel."""
            try:
                # Extract the query (remove bot mention)
                user_id = event.get("user", "")
                text = event.get("text", "")
                # Remove bot mention from text
                query = text.split(">", 1)[-1].strip() if ">" in text else text

                if not query:
                    say("👋 Hi! Ask me a question about your data!")
                    return

                # Show thinking indicator
                say(f"🤔 Analyzing your question: _{query}_")

                # Build prompt for Claude with code execution
                prompt = self._build_claude_prompt(query)

                # Call Claude API with code execution
                response = self.anthropic_client.messages.create(
                    model="claude-sonnet-4-5-20250929",
                    max_tokens=4096,
                    tools=[{"type": "code_execution_20250825", "name": "code_execution"}],
                    betas=["code-execution-2025-08-25"],
                    messages=[{"role": "user", "content": prompt}],
                )

                # Extract response text
                result_text = self._extract_response(response)

                # Post results back to Slack
                say(f"📊 *Results:*\n\n{result_text}")

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

    def _build_claude_prompt(self, user_query: str) -> str:
        """Build the prompt for Claude with the semantic model and user query.

        Args:
            user_query: The user's question about the data

        Returns:
            Formatted prompt for Claude
        """
        return f"""You are a data analysis assistant with access to a semantic layer.

The user has defined semantic models using Boring Semantic Layer (BSL), a Python library
built on Ibis that provides a semantic layer over data sources.

**Semantic Model Definition (YAML):**
```yaml
{self.semantic_model_yaml}
```

**User Question:**
{user_query}

**Instructions:**
1. Install boring-semantic-layer:
   ```bash
   pip install boring-semantic-layer
   ```

2. Load the semantic model from the YAML above using BSL's YAML loader

3. Generate sample data or connect to an appropriate data source
   (use DuckDB in-memory with sample data for testing)

4. Parse the user's question and determine what dimensions, measures, and filters to use

5. Execute the query using BSL's semantic table API

6. Format the results clearly for a Slack message (use markdown formatting)

**Important:**
- Use BSL's semantic API (not raw SQL)
- Return results in a clear, formatted way suitable for Slack
- If you need to create sample data, make it realistic and relevant
- Include brief explanation of what the query returns
- Keep responses concise but informative

**Example BSL code structure:**
```python
import ibis
import yaml
from boring_semantic_layer import load_semantic_model_from_yaml

# Parse YAML
config = yaml.safe_load(yaml_str)

# Create sample data with ibis
con = ibis.duckdb.connect(":memory:")
# ... create tables ...

# Load semantic models
models = load_semantic_model_from_yaml(config, connection=con)

# Query using semantic API
result = models["model_name"].group_by("dimension").aggregate("measure")
df = result.execute()
print(df)
```

Now answer the user's question!
"""

    def _extract_response(self, response: Any) -> str:
        """Extract text from Claude's response.

        Args:
            response: The response from Claude API

        Returns:
            Extracted text content
        """
        result_parts = []

        for block in response.content:
            if hasattr(block, "text"):
                result_parts.append(block.text)
            elif hasattr(block, "type") and block.type == "tool_use":
                # Include code execution results if present
                if hasattr(block, "output"):
                    result_parts.append(f"```\n{block.output}\n```")

        return "\n\n".join(result_parts) if result_parts else "No response generated"

    def start(self):
        """Start the Slack bot with Socket Mode."""
        logger.info("🚀 Starting BSL Slack Bot...")
        logger.info("Semantic model: %s", self.semantic_model_path)
        logger.info("Bot is running and listening for messages!")
        print("\n✨ BSL Slack Bot is running!")
        print(f"📄 Using semantic model: {self.semantic_model_path}")
        print("💬 Mention the bot in Slack to ask questions about your data")
        print("Press Ctrl+C to stop\n")

        try:
            self.socket_handler.start()
        except KeyboardInterrupt:
            logger.info("Bot stopped by user")
            print("\n👋 Bot stopped")


def start_slack_bot(
    semantic_model_path: str | Path,
    slack_bot_token: str | None = None,
    slack_app_token: str | None = None,
    anthropic_api_key: str | None = None,
):
    """Start the Slack bot.

    Args:
        semantic_model_path: Path to the YAML semantic model definition
        slack_bot_token: Slack Bot User OAuth Token (xoxb-...)
        slack_app_token: Slack App-Level Token for Socket Mode (xapp-...)
        anthropic_api_key: Anthropic API key

    Environment variables:
        SLACK_BOT_TOKEN: Slack Bot User OAuth Token (if not provided as arg)
        SLACK_APP_TOKEN: Slack App-Level Token (if not provided as arg)
        ANTHROPIC_API_KEY: Anthropic API key (if not provided as arg)
    """
    bot = SlackBSLBot(
        semantic_model_path=semantic_model_path,
        slack_bot_token=slack_bot_token,
        slack_app_token=slack_app_token,
        anthropic_api_key=anthropic_api_key,
    )
    bot.start()
