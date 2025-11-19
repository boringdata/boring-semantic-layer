import os
import tempfile
from pathlib import Path
from typing import Any

from boring_semantic_layer.agents.backends.langchain import LangChainAgent


class BSLSlackBot:
    def __init__(
        self,
        semantic_model_path: str | Path,
        slack_bot_token: str | None = None,
        slack_app_token: str | None = None,
        llm_model: str = "gpt-4o-mini",
        chart_backend: str = "plotly",
        profile: str | None = None,
        profile_file: str | Path | None = None,
    ):
        try:
            from slack_bolt import App
            from slack_bolt.adapter.socket_mode import SocketModeHandler
        except ImportError as e:
            raise ImportError("Install with: pip install boring-semantic-layer[slack]") from e

        model_path = Path(semantic_model_path)
        if not model_path.exists():
            raise FileNotFoundError(f"Semantic model not found: {semantic_model_path}")

        bot_token = slack_bot_token or os.environ.get("SLACK_BOT_TOKEN")
        app_token = slack_app_token or os.environ.get("SLACK_APP_TOKEN")

        if not bot_token:
            raise ValueError("SLACK_BOT_TOKEN required")
        if not app_token:
            raise ValueError("SLACK_APP_TOKEN required")

        self.app = App(token=bot_token)
        self.socket_handler = SocketModeHandler(self.app, app_token)
        self.chart_backend = chart_backend
        self.agent = LangChainAgent(
            model_path=model_path,
            llm_model=llm_model,
            chart_backend=chart_backend,
            profile=profile,
            profile_file=profile_file,
        )
        self._register_handlers()

    def _make_chart(self, query_str: str) -> str | None:
        import ibis

        from boring_semantic_layer.chart import chart

        try:
            result = eval(query_str, {**self.agent.models, "ibis": ibis})
            fd, path = tempfile.mkstemp(suffix=".png")
            os.close(fd)
            chart(result, backend=self.chart_backend, format="file", filename=path)
            return path
        except Exception:
            return None

    def _register_handlers(self):
        @self.app.event("app_mention")
        def handle_mention(event: dict[str, Any], say: Any, logger: Any):
            try:
                text = event.get("text", "")
                query = text.split(">", 1)[-1].strip() if ">" in text else text

                if not query:
                    say("👋 Hi! Ask me a question about your data!")
                    return

                say(f"🤔 Analyzing: _{query}_")

                chart_path = None

                def on_tool_call(tool_name: str, args: dict):
                    nonlocal chart_path
                    if tool_name == "query_model" and args.get("show_chart"):
                        chart_path = self._make_chart(args["query"])

                tool_output, agent_response = self.agent.query(query, on_tool_call=on_tool_call)

                response_parts = []
                if agent_response:
                    response_parts.append(agent_response)
                if tool_output:
                    response_parts.append(f"\n```\n{tool_output}\n```")

                say(f"📊 *Results:*\n\n{'\n'.join(response_parts)}")

                if chart_path:
                    try:
                        self.app.client.files_upload_v2(
                            channel=event["channel"],
                            file=chart_path,
                            title="Chart",
                        )
                    finally:
                        os.unlink(chart_path)

            except Exception as e:
                logger.error("Error: %s", e, exc_info=True)
                say(f"❌ Error: {e!s}")

        @self.app.event("message")
        def handle_dm(event: dict[str, Any], say: Any, logger: Any):
            if event.get("channel_type") == "im" and event.get("text"):
                handle_mention(event, say, logger)

    def start(self):
        print("\n✨ Slack bot starting...")
        print("💬 Mention the bot to ask questions")
        print("Press Ctrl+C to stop\n")
        try:
            self.socket_handler.start()
        except KeyboardInterrupt:
            print("\n👋 Stopped")


def start_slack_bot(
    semantic_model_path: str | Path,
    slack_bot_token: str | None = None,
    slack_app_token: str | None = None,
    llm_model: str = "gpt-4o-mini",
    chart_backend: str = "plotly",
    profile: str | None = None,
    profile_file: str | Path | None = None,
):
    BSLSlackBot(
        semantic_model_path=semantic_model_path,
        slack_bot_token=slack_bot_token,
        slack_app_token=slack_app_token,
        llm_model=llm_model,
        chart_backend=chart_backend,
        profile=profile,
        profile_file=profile_file,
    ).start()
