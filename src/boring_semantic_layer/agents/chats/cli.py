"""
Rich CLI Frontend for BSL Agents

This module provides a beautiful terminal interface for BSL agents
using Rich for formatting, loading spinners, and styled output.

Supports multiple backends:
- langchain: Simple tool-calling loop (default)
- langgraph: Full ReAct agent with LangGraph
- openai: OpenAI Assistants API
"""

import json
import logging
from pathlib import Path
from typing import Literal

from dotenv import load_dotenv
from rich.console import Console
from rich.panel import Panel
from rich.status import Status

# Disable httpx and openai logging
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("openai").setLevel(logging.WARNING)

console = Console()


def display_tool_call(function_name: str, function_args: dict, status: Status | None = None):
    """Display a tool call in grey, aichat-style format.

    Args:
        function_name: Name of the tool being called
        function_args: Arguments passed to the tool
        status: Optional Status spinner to stop before tool execution
    """
    # Stop spinner before tool output
    if status:
        status.stop()

    if function_name == "query_model" and "query" in function_args:
        query = function_args["query"]
        # Format query nicely - preserve line breaks for multiline queries
        if "\n" in query:
            # Multiline query - display with proper formatting
            console.print("Call bsl query_bsl", style="dim")
            # Add slight indent to query lines
            for line in query.split("\n"):
                console.print(f"  {line}", style="dim")
        else:
            # Single line query
            console.print(f"Call bsl query_bsl {query}", style="dim")

        # Show non-default parameters on separate line if present
        extra_params = {}
        if function_args.get("limit", 10) != 10:
            extra_params["limit"] = function_args["limit"]
        if function_args.get("chart_spec"):
            extra_params["chart_spec"] = function_args["chart_spec"]
        if extra_params:
            console.print(f"  params: {json.dumps(extra_params)}", style="dim")
    elif function_name == "list_models":
        console.print("Call bsl list_models", style="dim")
    elif function_name == "get_model":
        model_name = function_args.get("model_name", "?")
        console.print(f"Call bsl get_model {model_name}", style="dim")


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


def start_chat(
    model_path: Path,
    llm_model: str = "gpt-4",
    chart_backend: str = "plotext",
    initial_query: str | None = None,
    profile: str | None = None,
    profile_file: Path | None = None,
    env_path: Path | str | None = None,
    backend: Literal["langchain", "langgraph", "openai", "deepagent"] = "langgraph",
):
    """
    Start an interactive chat session with rich formatting.

    Args:
        model_path: Path to YAML semantic model definition
        llm_model: LLM model to use. Supports OpenAI (gpt-4), Anthropic (claude-*),
                  Google (gemini-*). Auto-detects based on available API keys.
                  (default: gpt-4, or auto-selected)
        chart_backend: Chart backend to use (default: plotext)
        initial_query: Optional query to run immediately (exits after response if provided)
        profile: Optional profile name to use for database connection
        profile_file: Optional path to profiles.yml file
        env_path: Optional path to a .env file to load credentials from
        backend: Agent backend to use (langchain, langgraph, or openai)
    """
    # Load environment variables
    load_dotenv(dotenv_path=env_path)

    import os

    # Auto-detect model based on available API keys if not explicitly set
    if llm_model == "gpt-4":  # Default value, check if we should auto-select
        if os.getenv("OPENAI_API_KEY"):
            llm_model = "gpt-4"  # Keep default
        elif os.getenv("ANTHROPIC_API_KEY"):
            llm_model = "claude-3-5-sonnet-20241022"
            console.print("ℹ️  Auto-selected Claude (ANTHROPIC_API_KEY found)", style="dim")
        elif os.getenv("GOOGLE_API_KEY"):
            llm_model = "gemini-1.5-pro"
            console.print("ℹ️  Auto-selected Gemini (GOOGLE_API_KEY found)", style="dim")
        else:
            console.print("\n❌ Error: No API key found.", style="bold red")
            console.print("Please set one of the following environment variables:")
            console.print("  - OPENAI_API_KEY (for GPT-4)")
            console.print("  - ANTHROPIC_API_KEY (for Claude)")
            console.print("  - GOOGLE_API_KEY (for Gemini)")
            console.print("\nExample .env file:")
            console.print("  OPENAI_API_KEY=sk-your-key-here", style="dim")
            console.print("  # or", style="dim")
            console.print("  ANTHROPIC_API_KEY=sk-ant-your-key-here", style="dim")
            console.print("  # or", style="dim")
            console.print("  GOOGLE_API_KEY=your-key-here", style="dim")
            return

    # Initialize agent based on selected backend
    try:
        with Status(f"[dim]Loading semantic models ({backend} backend)...", console=console):
            if backend == "langchain":
                from boring_semantic_layer.agents.backends.langchain import LangChainAgent

                agent = LangChainAgent(
                    model_path=model_path,
                    llm_model=llm_model,
                    chart_backend=chart_backend,
                    profile=profile,
                    profile_file=profile_file,
                )
            elif backend == "langgraph":
                from boring_semantic_layer.agents.backends.langgraph_react import (
                    LangGraphReActAgent,
                )

                agent = LangGraphReActAgent(
                    model_path=model_path,
                    llm_model=llm_model,
                    chart_backend=chart_backend,
                    profile=profile,
                    profile_file=profile_file,
                )
            elif backend == "openai":
                from boring_semantic_layer.agents.backends.openai_assistant import (
                    OpenAIAssistantAgent,
                )

                agent = OpenAIAssistantAgent(
                    model_path=model_path,
                    llm_model=llm_model,
                    chart_backend=chart_backend,
                    profile=profile,
                    profile_file=profile_file,
                )
            elif backend == "deepagent":
                from boring_semantic_layer.agents.backends.deepagent import (
                    DeepAgentBackend,
                )

                agent = DeepAgentBackend(
                    model_path=model_path,
                    llm_model=llm_model,
                    chart_backend=chart_backend,
                    profile=profile,
                    profile_file=profile_file,
                )
            else:
                console.print(f"❌ Unknown backend: {backend}", style="bold red")
                return

        backend_names = {
            "langchain": "LangChain",
            "langgraph": "LangGraph ReAct",
            "openai": "OpenAI Assistants",
            "deepagent": "DeepAgents (Planning)",
        }
        console.print(
            f"✅ Models loaded successfully ({backend_names[backend]} backend)\n",
            style="green",
        )
    except Exception as e:
        console.print(f"❌ Error loading models: {e}", style="bold red")
        return

    # Create status message based on model
    status_msg = f"[dim]Calling {llm_model}...[/dim]"

    # Handle initial query (non-interactive mode)
    if initial_query:
        console.print(f"[bold blue]bsl>>[/bold blue] {initial_query}")

        status = Status(status_msg, console=console)
        status.start()
        try:
            # Pass status to callbacks so they can stop the spinner
            _, agent_response = agent.query(
                initial_query,
                on_tool_call=lambda fn, args: display_tool_call(fn, args, status),
                on_error=lambda msg: display_error(msg, status),
                on_thinking=lambda text: display_thinking(text, status),
            )
        finally:
            status.stop()

        # Display the agent's summary (if meaningful)
        if agent_response and agent_response.strip():
            console.print(f"\n💬 {agent_response}")

        return  # Exit after processing initial query

    # Print welcome message for interactive mode
    console.print(
        Panel.fit(
            f"[bold cyan]Boring Semantic Layer - Chat Interface[/bold cyan]\n\n"
            f"Model: {llm_model}\n"
            f"Backend: {backend_names[backend]}\n"
            f"Charts: Enabled ({chart_backend})\n\n"
            f"Type your questions in natural language!\n"
            f"Commands: [dim]quit, exit, q[/dim]",
            border_style="cyan",
        )
    )

    console.print("\n[dim]Example questions:[/dim]")
    console.print("  [dim]• What data is available?[/dim]")
    console.print("  [dim]• Show me flight counts by carrier[/dim]")
    console.print("  [dim]• Number of flights per month[/dim]\n")

    # Interactive loop
    while True:
        try:
            # Get user input
            user_input = console.input("\n[bold blue]bsl>>[/bold blue] ").strip()

            if not user_input:
                continue

            if user_input.lower() in ["quit", "exit", "q"]:
                console.print("\n👋 Goodbye!", style="bold green")
                break

            # Process the query with loading spinner
            status = Status(status_msg, console=console)
            status.start()
            try:
                # Pass status to callbacks so they can stop the spinner
                # Use default argument to bind loop variable
                _, agent_response = agent.query(
                    user_input,
                    on_tool_call=lambda fn, args, s=status: display_tool_call(fn, args, s),
                    on_error=lambda msg, s=status: display_error(msg, s),
                    on_thinking=lambda text, s=status: display_thinking(text, s),
                )
            finally:
                status.stop()

            # Display the agent's summary (if meaningful)
            if agent_response and agent_response.strip():
                console.print(f"\n💬 {agent_response}")

        except KeyboardInterrupt:
            console.print("\n\n👋 Goodbye!", style="bold green")
            break
        except Exception as e:
            console.print(f"\n❌ Error: {e}", style="bold red")
            console.print("Please try again.\n", style="dim")
