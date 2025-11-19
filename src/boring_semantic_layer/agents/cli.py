"""Command-line interface for Boring Semantic Layer - v2 with generic backend support."""

import argparse
import logging
import sys
from pathlib import Path

from dotenv import load_dotenv


def setup_logging(verbose: bool = False):
    """Set up logging configuration."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )


def cmd_chat(args):
    """Start an interactive chat session with the semantic model."""
    from boring_semantic_layer.agents.chats.cli import start_chat

    # Get model from args (no validation - let LangChain handle it)
    llm_model = args.model if hasattr(args, "model") and args.model else "gpt-4"
    initial_query = " ".join(args.query) if hasattr(args, "query") and args.query else None
    profile = args.profile if hasattr(args, "profile") else None
    profile_file = args.profile_file if hasattr(args, "profile_file") else None
    env_path = getattr(args, "env_path", None)

    # Auto-select profile if not specified and only one exists
    if not profile and profile_file:
        try:
            import yaml

            with open(profile_file) as f:
                profiles_data = yaml.safe_load(f)
                if profiles_data and "profiles" in profiles_data:
                    available_profiles = list(profiles_data["profiles"].keys())
                    if len(available_profiles) == 1:
                        profile = available_profiles[0]
                        print(f"ℹ️  Auto-selected profile: {profile}")
                    elif len(available_profiles) > 1:
                        print(f"⚠️  Multiple profiles available: {', '.join(available_profiles)}")
                        print("   Use --profile to select one")
        except Exception:
            # If we can't read the profiles file, just continue without auto-selection
            pass

    start_chat(
        model_path=args.sm,
        llm_model=llm_model,
        chart_backend=args.chart_backend,
        initial_query=initial_query,
        profile=profile,
        profile_file=profile_file,
        env_path=env_path,
    )


def cmd_render(args):
    """Render markdown files with BSL queries."""
    from boring_semantic_layer.chart.md_renderer import cmd_render as render_func

    success = render_func(
        md_path=args.input,
        output=args.output,
        format=args.format,
        images_dir=args.images_dir,
        watch=args.watch,
    )

    if not success:
        sys.exit(1)


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        prog="bsl",
        description="Boring Semantic Layer - CLI tools and integrations",
    )

    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Enable verbose logging",
    )
    parser.add_argument(
        "--env-path",
        type=Path,
        help="Path to a .env file for loading credentials before running the command",
    )

    # Create subparsers for different commands
    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    # Chat command
    chat_parser = subparsers.add_parser("chat", help="Start interactive chat session")
    chat_parser.add_argument(
        "--sm",
        type=Path,
        required=True,
        help="Path to semantic model definition (YAML file)",
    )
    chat_parser.add_argument(
        "--chart-backend",
        choices=["plotext", "altair", "plotly"],
        default="plotext",
        help="Chart backend for visualizations (default: plotext)",
    )
    chat_parser.add_argument(
        "--model",
        default="gpt-4",
        help="LLM model to use. Supported: OpenAI (gpt-4, gpt-4o, gpt-3.5-turbo), Anthropic (claude-3-5-sonnet-20241022), Google (gemini-1.5-pro, gemini-1.5-flash). Auto-detects based on API keys. (default: gpt-4 or auto-selected)",
    )
    chat_parser.add_argument(
        "--profile",
        "-p",
        help="Profile name to use for database connection (e.g., 'my_flights_db')",
    )
    chat_parser.add_argument(
        "--profile-file",
        type=Path,
        help="Path to profiles.yml file (default: looks for profiles.yml in current directory and examples/)",
    )
    chat_parser.add_argument(
        "--list-fields",
        "-l",
        action="store_true",
        help="List available dimensions and measures before starting chat",
    )
    chat_parser.add_argument(
        "query",
        nargs="*",
        help="Optional initial query to run",
    )
    chat_parser.set_defaults(func=cmd_chat)

    # Render command
    render_parser = subparsers.add_parser(
        "render",
        help="Render markdown files with BSL queries to HTML or markdown with images",
    )
    render_parser.add_argument(
        "input",
        type=Path,
        help="Path to input markdown file",
    )
    render_parser.add_argument(
        "-o",
        "--output",
        type=Path,
        help="Path to output file (default: input file with .html or _rendered.md suffix)",
    )
    render_parser.add_argument(
        "-f",
        "--format",
        choices=["html", "markdown"],
        default="html",
        help="Output format (default: html)",
    )
    render_parser.add_argument(
        "--images-dir",
        type=Path,
        help="Directory for exported images (markdown format only, default: <output>_images)",
    )
    render_parser.add_argument(
        "-w",
        "--watch",
        action="store_true",
        help="Watch for file changes and auto-regenerate output",
    )
    render_parser.set_defaults(func=cmd_render)

    args = parser.parse_args()

    # Setup logging
    setup_logging(args.verbose)

    # Load env vars if requested
    if args.env_path:
        load_dotenv(dotenv_path=args.env_path)

    # Check if a command was provided
    if not hasattr(args, "func"):
        parser.print_help()
        sys.exit(1)

    # Execute command
    try:
        args.func(args)
    except KeyboardInterrupt:
        print("\n👋 Interrupted by user")
        sys.exit(0)
    except Exception as e:
        print(f"❌ Error: {e}", file=sys.stderr)
        if args.verbose:
            raise
        sys.exit(1)


if __name__ == "__main__":
    main()
