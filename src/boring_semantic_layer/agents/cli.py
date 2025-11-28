"""Command-line interface for Boring Semantic Layer - v2 with generic backend support."""

import argparse
import logging
import shutil
import sys
from pathlib import Path

from dotenv import load_dotenv

# Tool configurations - how each tool stores skills
TOOL_CONFIGS = {
    "claude-code": {
        "target_pattern": ".claude/skills/{skill_name}/SKILL.md",
        "description": "Claude Code skills",
    },
    "cursor": {
        "target_pattern": ".cursor/rules/{skill_name}.mdc",
        "description": "Cursor rules",
    },
    "codex": {
        "target_pattern": ".codex/{skill_name}.codex",
        "description": "Codex instructions",
    },
}


def _get_skills_dir() -> Path:
    """Get the directory containing skill files (bundled with the package).

    Looks in multiple locations:
    1. Installed shared-data location (sys.prefix/share/bsl/skills)
    2. Development location (docs/md/skills relative to package)
    """
    # First try installed location (shared-data from wheel)
    installed_skills_dir = Path(sys.prefix) / "share" / "bsl" / "skills"
    if installed_skills_dir.exists():
        return installed_skills_dir

    # Fall back to development location
    package_dir = Path(__file__).parent.parent.parent.parent
    skills_dir = package_dir / "docs" / "md" / "skills"
    return skills_dir


def _discover_skills_for_tool(tool: str) -> list[dict]:
    """Discover all skills available for a tool by scanning the directory."""
    skills_dir = _get_skills_dir()
    tool_dir = skills_dir / tool

    if not tool_dir.exists():
        return []

    skills = []
    config = TOOL_CONFIGS.get(tool, {})

    if tool == "claude-code":
        # Claude Code: each subdirectory is a skill with SKILL.md
        for skill_dir in tool_dir.iterdir():
            if skill_dir.is_dir():
                skill_file = skill_dir / "SKILL.md"
                if skill_file.exists():
                    skill_name = skill_dir.name
                    target = config["target_pattern"].format(skill_name=skill_name)
                    skills.append(
                        {
                            "name": skill_name,
                            "source": skill_file,
                            "target": target,
                        }
                    )
    elif tool == "cursor":
        # Cursor: each .cursorrules or .mdc file is a skill
        for skill_file in tool_dir.glob("*.cursorrules"):
            skill_name = skill_file.stem
            target = config["target_pattern"].format(skill_name=skill_name)
            skills.append(
                {
                    "name": skill_name,
                    "source": skill_file,
                    "target": target,
                }
            )
        for skill_file in tool_dir.glob("*.mdc"):
            skill_name = skill_file.stem
            target = config["target_pattern"].format(skill_name=skill_name)
            skills.append(
                {
                    "name": skill_name,
                    "source": skill_file,
                    "target": target,
                }
            )
    elif tool == "codex":
        # Codex: each .codex file is a skill
        for skill_file in tool_dir.glob("*.codex"):
            skill_name = skill_file.stem
            target = config["target_pattern"].format(skill_name=skill_name)
            skills.append(
                {
                    "name": skill_name,
                    "source": skill_file,
                    "target": target,
                }
            )

    return skills


def cmd_skill_list(args):
    """List available skills."""
    print("Available BSL skills:\n")
    for tool, config in TOOL_CONFIGS.items():
        skills = _discover_skills_for_tool(tool)
        if skills:
            print(f"  {tool} ({config['description']}):")
            for skill in skills:
                print(f"    ✓ {skill['name']}")
                print(f"      → {skill['target']}")
        else:
            print(f"  {tool}: (no skills found)")
    print("\nUse 'bsl skill show <tool>' to preview skills")
    print("Use 'bsl skill install <tool>' to install all skills for a tool")


def cmd_skill_show(args):
    """Show the content of skill files for a tool."""
    tool = args.tool
    if tool not in TOOL_CONFIGS:
        print(f"❌ Unknown tool: {tool}")
        print(f"   Available tools: {', '.join(TOOL_CONFIGS.keys())}")
        return

    skills = _discover_skills_for_tool(tool)
    if not skills:
        print(f"❌ No skills found for {tool}")
        return

    for i, skill in enumerate(skills):
        if i > 0:
            print("\n" + "=" * 60 + "\n")
        print(f"# Skill: {skill['name']}")
        print(f"# Source: {skill['source']}")
        print(f"# Target: {skill['target']}")
        print("-" * 60)
        print(skill["source"].read_text())


def cmd_skill_install(args):
    """Install all skill files for a specific tool."""
    tool = args.tool
    if tool not in TOOL_CONFIGS:
        print(f"❌ Unknown tool: {tool}")
        print(f"   Available tools: {', '.join(TOOL_CONFIGS.keys())}")
        return

    skills = _discover_skills_for_tool(tool)
    if not skills:
        print(f"❌ No skills found for {tool}")
        return

    installed = 0
    skipped = 0

    # Install skill files
    for skill in skills:
        target_path = Path.cwd() / skill["target"]

        # Check if target exists
        if target_path.exists() and not args.force:
            print(f"⚠️  Skipped {skill['name']} (already exists: {target_path})")
            skipped += 1
            continue

        # Create parent directories if needed
        target_path.parent.mkdir(parents=True, exist_ok=True)

        # Copy the file
        shutil.copy2(skill["source"], target_path)
        print(f"✅ Installed {skill['name']}")
        print(f"   → {target_path}")
        installed += 1

    print(f"\n📦 Installed {installed} skill(s)" + (f", skipped {skipped}" if skipped else ""))
    if skipped:
        print("   Use --force to overwrite existing files")


def setup_logging(verbose: bool = False):
    """Set up logging configuration."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )


def cmd_chat(args):
    """Start an interactive chat session with the semantic model."""
    import os

    from boring_semantic_layer.agents.chats.cli import start_chat

    # Load .env file early if specified, so env vars are available
    env_path = getattr(args, "env_path", None)
    if env_path:
        load_dotenv(dotenv_path=env_path)
    else:
        # Try to load from current directory or parent directories
        load_dotenv()

    # Get model_path from args or BSL_MODEL_PATH env var
    model_path = args.sm if hasattr(args, "sm") and args.sm else None
    if not model_path:
        model_path_str = os.environ.get("BSL_MODEL_PATH")
        if model_path_str:
            model_path = Path(model_path_str)
        else:
            print("❌ Error: No semantic model file specified.")
            print("   Use --sm <path> or set BSL_MODEL_PATH environment variable")
            return

    # Get LLM from args (no validation - let LangChain handle it)
    llm_model = args.llm if hasattr(args, "llm") and args.llm else "gpt-4"
    initial_query = " ".join(args.query) if hasattr(args, "query") and args.query else None
    profile = args.profile if hasattr(args, "profile") else None

    # Get profile_file from args or BSL_PROFILE_FILE env var
    profile_file = (
        args.profile_file if hasattr(args, "profile_file") and args.profile_file else None
    )
    if not profile_file:
        profile_file_str = os.environ.get("BSL_PROFILE_FILE")
        if profile_file_str:
            profile_file = Path(profile_file_str)

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
        model_path=model_path,
        llm_model=llm_model,
        chart_backend=args.chart_backend,
        initial_query=initial_query,
        profile=profile,
        profile_file=profile_file,
        env_path=env_path,  # Pass through for start_chat's internal use
    )


# TODO: Add tests for cmd_render before re-enabling
# def cmd_render(args):
#     """Render markdown files with BSL queries."""
#     from boring_semantic_layer.chart.md_renderer import cmd_render as render_func
#
#     success = render_func(
#         md_path=args.input,
#         output=args.output,
#         format=args.format,
#         images_dir=args.images_dir,
#         watch=args.watch,
#     )
#
#     if not success:
#         sys.exit(1)


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
        help="Path to semantic model definition (YAML file). Can also be set via BSL_MODEL_PATH environment variable.",
    )
    chat_parser.add_argument(
        "--chart-backend",
        choices=["plotext", "altair", "plotly"],
        default="plotext",
        help="Chart backend for visualizations (default: plotext)",
    )
    chat_parser.add_argument(
        "--llm",
        default="gpt-4",
        help="LLM model to use (e.g., gpt-4o, claude-3-5-sonnet-20241022, gemini-1.5-pro)",
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
        "query",
        nargs="*",
        help="Optional initial query to run",
    )
    chat_parser.set_defaults(func=cmd_chat)

    # TODO: Add tests for render command before re-enabling
    # # Render command
    # render_parser = subparsers.add_parser(
    #     "render",
    #     help="Render markdown files with BSL queries to HTML or markdown with images",
    # )
    # render_parser.add_argument(
    #     "input",
    #     type=Path,
    #     help="Path to input markdown file",
    # )
    # render_parser.add_argument(
    #     "-o",
    #     "--output",
    #     type=Path,
    #     help="Path to output file (default: input file with .html or _rendered.md suffix)",
    # )
    # render_parser.add_argument(
    #     "-f",
    #     "--format",
    #     choices=["html", "markdown"],
    #     default="html",
    #     help="Output format (default: html)",
    # )
    # render_parser.add_argument(
    #     "--images-dir",
    #     type=Path,
    #     help="Directory for exported images (markdown format only, default: <output>_images)",
    # )
    # render_parser.add_argument(
    #     "-w",
    #     "--watch",
    #     action="store_true",
    #     help="Watch for file changes and auto-regenerate output",
    # )
    # render_parser.set_defaults(func=cmd_render)

    # Skill command with subcommands
    skill_parser = subparsers.add_parser(
        "skill",
        help="Manage BSL skills for AI coding assistants",
    )
    skill_subparsers = skill_parser.add_subparsers(dest="skill_command", help="Skill command")

    # skill list
    skill_list_parser = skill_subparsers.add_parser("list", help="List available skills")
    skill_list_parser.set_defaults(func=cmd_skill_list)

    # skill show <tool>
    skill_show_parser = skill_subparsers.add_parser("show", help="Show skill content")
    skill_show_parser.add_argument(
        "tool",
        choices=list(TOOL_CONFIGS.keys()),
        help="Tool to show skills for",
    )
    skill_show_parser.set_defaults(func=cmd_skill_show)

    # skill install <tool>
    skill_install_parser = skill_subparsers.add_parser(
        "install", help="Install all skills for a tool"
    )
    skill_install_parser.add_argument(
        "tool",
        choices=list(TOOL_CONFIGS.keys()),
        help="Tool to install skills for",
    )
    skill_install_parser.add_argument(
        "-f",
        "--force",
        action="store_true",
        help="Overwrite existing file",
    )
    skill_install_parser.set_defaults(func=cmd_skill_install)

    args = parser.parse_args()

    # Setup logging
    setup_logging(args.verbose)

    # Load env vars if requested
    if args.env_path:
        load_dotenv(dotenv_path=args.env_path)

    # Check if a command was provided
    if not hasattr(args, "func"):
        # Handle 'bsl skill' without subcommand
        if args.command == "skill":
            skill_parser.print_help()
        else:
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
