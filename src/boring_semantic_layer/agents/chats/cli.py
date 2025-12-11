"""Rich CLI Frontend for BSL Agents."""

import json
import logging
from pathlib import Path

from dotenv import load_dotenv
from rich.console import Console
from rich.panel import Panel
from rich.status import Status

# Disable httpx logging
logging.getLogger("httpx").setLevel(logging.WARNING)

console = Console()


def display_tool_call(
    function_name: str,
    function_args: dict,
    status: Status | None = None,
    tokens: dict | None = None,
):
    """Display a tool call in grey, aichat-style format.

    Args:
        function_name: Name of the tool being called
        function_args: Arguments passed to the tool
        status: Optional Status spinner to stop before tool execution
        tokens: Optional dict with 'input_tokens' and 'output_tokens' for this call
    """
    # Stop spinner before tool output
    if status:
        status.stop()

    # Format token info if available
    token_info = ""
    if tokens:
        input_t = tokens.get("input_tokens", 0)
        output_t = tokens.get("output_tokens", 0)
        if input_t or output_t:
            token_info = f" ({input_t} in / {output_t} out)"

    if function_name == "query_model" and "query" in function_args:
        query = function_args["query"]
        # Format query nicely - preserve line breaks for multiline queries
        if "\n" in query:
            # Multiline query - display with proper formatting
            console.print(f"Call bsl query_bsl{token_info}", style="dim")
            # Add slight indent to query lines
            for line in query.split("\n"):
                console.print(f"  {line}", style="dim")
        else:
            # Single line query
            console.print(f"Call bsl query_bsl {query}{token_info}", style="dim")

        # Show non-default parameters on separate line if present
        extra_params = {}
        if function_args.get("limit", 10) != 10:
            extra_params["limit"] = function_args["limit"]
        if function_args.get("chart_spec"):
            extra_params["chart_spec"] = function_args["chart_spec"]
        if extra_params:
            console.print(f"  params: {json.dumps(extra_params)}", style="dim")
    elif function_name == "list_models":
        console.print(f"Call bsl list_models{token_info}", style="dim")
    elif function_name == "get_model":
        model_name = function_args.get("model_name", "?")
        console.print(f"Call bsl get_model {model_name}{token_info}", style="dim")
    elif function_name == "get_documentation":
        topic = function_args.get("topic", "?")
        console.print(f"Call bsl get_documentation {topic}{token_info}", style="dim")
    else:
        console.print(f"Call bsl {function_name}{token_info}", style="dim")


def display_error(error_msg: str, status: Status | None = None):
    """Display an error message and stop the spinner.

    Args:
        error_msg: Error message to display
        status: Optional Status spinner to stop before displaying error
    """
    # Stop spinner before error output
    if status:
        status.stop()

    console.print(error_msg, style="red")


def display_thinking(thinking_text: str, status: Status | None = None):
    """Display the LLM's reasoning/thinking text in grey.

    Args:
        thinking_text: The LLM's reasoning text before tool calls
        status: Optional Status spinner to stop before displaying
    """
    # Stop spinner before thinking output
    if status:
        status.stop()

    # Display thinking text in dim/grey style
    console.print(f"\n{thinking_text}", style="dim italic")


def display_token_usage(usage: dict, status: Status | None = None):
    """Display token usage statistics in dim style.

    Args:
        usage: Dict with input_tokens, output_tokens, total_tokens, optional 'estimated' flag
        status: Optional Status spinner to stop before displaying
    """
    # Don't stop status here - let it be stopped by the caller after query completes

    input_tokens = usage.get("input_tokens", 0)
    output_tokens = usage.get("output_tokens", 0)
    total_tokens = usage.get("total_tokens", 0)
    is_estimated = usage.get("estimated", False)

    if is_estimated:
        # Streaming mode - only input tokens available (estimated)
        console.print(
            f"\n[tokens: ~{input_tokens} in (estimated)]",
            style="dim",
        )
    else:
        console.print(
            f"\n[tokens: {input_tokens} in / {output_tokens} out = {total_tokens} total]",
            style="dim",
        )


def start_chat(
    model_path: Path,
    llm_model: str = "anthropic:claude-opus-4-20250514",
    chart_backend: str = "plotext",
    initial_query: str | None = None,
    profile: str | None = None,
    profile_file: Path | None = None,
    env_path: Path | str | None = None,
    auto_exit: bool = False,
):
    """Start an interactive chat session with rich formatting."""
    # Load environment variables
    load_dotenv(dotenv_path=env_path)

    # Initialize agent
    try:
        with Status("[dim]Loading semantic models...", console=console):
            from boring_semantic_layer.agents.backends import LangGraphBackend

            agent = LangGraphBackend(
                model_path=model_path,
                llm_model=llm_model,
                chart_backend=chart_backend,
                profile=profile,
                profile_file=profile_file,
            )

        console.print("✅ Models loaded successfully\n", style="green")
    except Exception as e:
        console.print(f"❌ Error loading models: {e}", style="bold red")
        return

    status_msg = f"[dim]Calling {llm_model}...[/dim]"

    # Beta notice
    console.print(
        Panel.fit(
            "[bold]⚠️  BSL Chat is in BETA[/bold]\n"
            "If you encounter any problem, don't hesitate to open an issue:\n"
            "[link=https://github.com/boringdata/boring-semantic-layer/issues]https://github.com/boringdata/boring-semantic-layer/issues[/link]\n"
            "Thanks a lot for helping us! 🙏",
            border_style="yellow",
        )
    )
    console.print()

    # Welcome message
    console.print(
        Panel.fit(
            f"[bold cyan]Boring Semantic Layer - Chat Interface[/bold cyan]\n\n"
            f"Model: {llm_model}\n"
            f"Charts: Enabled ({chart_backend})\n\n"
            f"Type your questions in natural language!\n"
            f"Commands: [dim]quit, exit, q[/dim]",
            border_style="cyan",
        )
    )
    console.print("\n[dim]Example: What data is available? | Show me sales by category[/dim]\n")

    # Interactive loop
    first_iteration = True
    while True:
        try:
            # Handle initial query on first iteration
            if first_iteration and initial_query:
                user_input = initial_query
                console.print(f"[bold blue]bsl>>[/bold blue] {user_input}")
                first_iteration = False
            else:
                first_iteration = False
                user_input = console.input("\n[bold blue]bsl>>[/bold blue] ").strip()
                if not user_input:
                    continue
                if user_input.lower() in ("quit", "exit", "q"):
                    break

            # Process the query with loading spinner
            status = Status(status_msg, console=console)
            status.start()
            try:
                # Pass status to callbacks so they can stop the spinner
                # Use default argument to bind loop variable
                _, agent_response = agent.query(
                    user_input,
                    on_tool_call=lambda fn, args, tokens, s=status: display_tool_call(
                        fn, args, s, tokens
                    ),
                    on_error=lambda msg, s=status: display_error(msg, s),
                    on_thinking=lambda text, s=status: display_thinking(text, s),
                    on_token_usage=lambda usage, s=status: display_token_usage(usage, s),
                )
            finally:
                status.stop()

            # Display the agent's summary (if meaningful)
            if agent_response and agent_response.strip():
                console.print(f"\n💬 {agent_response}")

            # Exit after initial query if auto_exit is enabled
            if auto_exit and initial_query and user_input == initial_query:
                return

        except KeyboardInterrupt:
            break
        except Exception as e:
            console.print(f"\n❌ Error: {e}", style="bold red")

    console.print("\n👋 Goodbye!", style="bold green")
