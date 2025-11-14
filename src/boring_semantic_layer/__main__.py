"""Command-line interface for Boring Semantic Layer - v2 with generic backend support."""

import argparse
import logging
import sys
from pathlib import Path


def setup_logging(verbose: bool = False):
    """Set up logging configuration."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )


def cmd_chat(args):
    """Start an interactive chat session with the semantic model."""
    backend = args.backend

    if backend == "aichat":
        from boring_semantic_layer.agents.aichat import start_aichat_session

        start_aichat_session(
            model_path=args.sm,
            backend=args.chart_backend,
            list_fields=args.list_fields,
            aichat_args=args.backend_args,
        )

    elif backend == "langchain":
        from boring_semantic_layer.agents.langchain_agent import (
            start_langchain_agent,
        )

        # Extract langchain-specific args
        llm_model = "gpt-4"
        openai_api_key = None
        for i, arg in enumerate(args.backend_args):
            if arg == "--model" and i + 1 < len(args.backend_args):
                llm_model = args.backend_args[i + 1]
            elif arg == "--openai-api-key" and i + 1 < len(args.backend_args):
                openai_api_key = args.backend_args[i + 1]

        start_langchain_agent(
            model_path=args.sm,
            llm_model=llm_model,
            openai_api_key=openai_api_key,
            chart_backend=args.chart_backend,
        )

    else:
        print(f"❌ Error: Unknown backend: {backend}", file=sys.stderr)
        print("\nSupported backends: aichat, langchain")
        sys.exit(1)


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

    # Create subparsers for different commands
    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    # Chat command - with subcommands for each backend
    chat_parser = subparsers.add_parser("chat", help="Start interactive chat session")
    chat_subparsers = chat_parser.add_subparsers(dest="backend", help="Chat backend to use")

    # Aichat backend
    aichat_parser = chat_subparsers.add_parser("aichat", help="Use aichat backend")
    aichat_parser.add_argument(
        "--sm",
        type=Path,
        required=True,
        help="Path to semantic model definition (YAML file)",
    )
    aichat_parser.add_argument(
        "--chart-backend",
        choices=["plotext", "altair", "plotly"],
        default="plotext",
        help="Chart backend for visualizations (default: plotext)",
    )
    aichat_parser.add_argument(
        "--list-fields",
        "-l",
        action="store_true",
        help="List available dimensions and measures before starting chat",
    )
    aichat_parser.add_argument(
        "backend_args",
        nargs="*",
        help="Additional arguments to pass to aichat",
    )
    aichat_parser.set_defaults(func=cmd_chat)

    # Langchain backend
    langchain_parser = chat_subparsers.add_parser("langchain", help="Use langchain backend")
    langchain_parser.add_argument(
        "--sm",
        type=Path,
        required=True,
        help="Path to semantic model definition (YAML file)",
    )
    langchain_parser.add_argument(
        "--chart-backend",
        choices=["plotext", "altair", "plotly"],
        default="plotext",
        help="Chart backend for visualizations (default: plotext)",
    )
    langchain_parser.add_argument(
        "--list-fields",
        "-l",
        action="store_true",
        help="List available dimensions and measures before starting chat",
    )
    langchain_parser.add_argument(
        "backend_args",
        nargs="*",
        help="Additional arguments to pass to langchain",
    )
    langchain_parser.set_defaults(func=cmd_chat)

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
