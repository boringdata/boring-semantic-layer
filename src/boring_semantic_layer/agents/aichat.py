"""AIChat session management for BSL."""

import json
import shutil
import subprocess
import sys
from pathlib import Path


def start_aichat_session(
    model_path: Path,
    backend: str = "plotext",
    list_fields: bool = False,
    aichat_args: list[str] | None = None,
):
    """
    Start an interactive aichat session with a semantic model.

    Args:
        model_path: Path to semantic model (YAML or Python file)
        backend: Chart backend to use (plotext, altair, plotly)
        list_fields: Whether to list available fields before starting chat
        aichat_args: Additional arguments to pass to aichat command
    """
    if aichat_args is None:
        aichat_args = []
    # Check if aichat is installed
    if not shutil.which("aichat"):
        print("❌ Error: aichat is not installed", file=sys.stderr)
        print("\nInstall aichat:")
        print("  brew install aichat  # macOS")
        print("  cargo install aichat  # Using Rust")
        print("\nOr visit: https://github.com/sigoden/aichat")
        sys.exit(1)

    # Check if aichat LLM provider is configured
    try:
        result = subprocess.run(
            ["aichat", "--info"],
            capture_output=True,
            text=True,
            check=False,
        )

        # Check if there's a model configured
        has_model = False
        for line in result.stdout.splitlines():
            stripped = line.strip()
            # Line format: "model                   openai:gpt-4.1"
            if stripped.startswith("model") and stripped != "model:":
                # Check if there's a value after "model"
                parts = stripped.split(maxsplit=1)
                if len(parts) > 1 and parts[1] and parts[1] != "null":
                    has_model = True
                    break

        if not has_model:
            print("❌ Error: aichat LLM provider not configured", file=sys.stderr)
            print("\nYou need to configure an LLM provider before using BSL chat.")
            print("\nQuick setup:")
            print("  aichat  # Run aichat, it will guide you through setup")
            print("\nOr configure manually:")
            print("  export OPENAI_API_KEY=sk-...")
            print("  export ANTHROPIC_API_KEY=sk-ant-...")
            print("\nSee: https://github.com/sigoden/aichat#configuration")
            sys.exit(1)
    except Exception:
        # If we can't check, continue anyway - aichat will handle it
        pass

    # Resolve model path
    model_path = model_path.resolve()
    if not model_path.exists():
        print(f"❌ Error: Model file not found: {model_path}", file=sys.stderr)
        sys.exit(1)

    print(f"🚀 Starting aichat session with model: {model_path}")
    print(f"📊 Chart backend: {backend}")
    print()

    # Get aichat functions directory
    functions_dir = _get_aichat_functions_dir()

    # Create agent directory directly in aichat functions
    agent_dir = functions_dir / "agents" / "bsl"
    agent_dir.parent.mkdir(parents=True, exist_ok=True)

    # Remove existing agent directory if present
    if agent_dir.exists():
        shutil.rmtree(agent_dir)

    agent_dir.mkdir()

    # Generate agent files
    _create_agent_files(agent_dir, model_path, backend)

    # Create bin executable for the agent
    bin_dir = functions_dir / "bin"
    bin_dir.mkdir(exist_ok=True)
    bin_script = bin_dir / "bsl"

    # Remove existing bin script if present
    if bin_script.exists():
        bin_script.unlink()

    # Create executable that calls Argcfile
    bin_content = f"""#!/usr/bin/env bash
set -e

# Get the functions directory
FUNCTIONS_DIR="{functions_dir}"

# Call Argcfile to run the agent
cd "$FUNCTIONS_DIR"
exec bash Argcfile.sh run@agent bsl "$1" "$2"
"""
    bin_script.write_text(bin_content)
    bin_script.chmod(0o755)

    try:
        # List fields if requested
        if list_fields:
            print("📋 Available dimensions and measures:")
            print("=" * 70)
            _list_model_fields(model_path)
            print("=" * 70)
            print()

        # Start aichat with the agent and any additional args
        print("💬 Starting aichat... (Press Ctrl+C to exit)")
        print()

        cmd = ["aichat", "--agent", "bsl"] + aichat_args
        subprocess.run(cmd, check=True)

    finally:
        # Clean up agent directory and bin script
        if agent_dir.exists():
            shutil.rmtree(agent_dir)
        if bin_script.exists():
            bin_script.unlink()


def _get_aichat_functions_dir() -> Path:
    """Get the aichat functions directory from aichat --info."""
    try:
        result = subprocess.run(
            ["aichat", "--info"],
            capture_output=True,
            text=True,
            check=True,
        )

        for line in result.stdout.splitlines():
            if "functions_dir" in line:
                # Parse line like: "functions_dir  /path/to/functions"
                parts = line.split(maxsplit=1)
                if len(parts) == 2:
                    return Path(parts[1].strip())

        # Fallback to default location
        if sys.platform == "darwin":
            return Path.home() / "Library" / "Application Support" / "aichat" / "functions"
        else:
            return Path.home() / ".local" / "share" / "aichat" / "functions"

    except subprocess.CalledProcessError:
        print("⚠️  Warning: Could not get aichat info, using default functions directory")
        if sys.platform == "darwin":
            return Path.home() / "Library" / "Application Support" / "aichat" / "functions"
        else:
            return Path.home() / ".local" / "share" / "aichat" / "functions"


def _create_agent_files(agent_dir: Path, model_path: Path, backend: str):
    """Create agent configuration and tool files."""
    # Get current Python interpreter (from virtual env if active)
    python_exe = sys.executable

    # Load agent prompt from shared markdown file
    prompt_file = Path(__file__).parent.parent / "BSL_AGENT_GUIDE.md"
    agent_instructions = prompt_file.read_text()

    # Add context about the specific model
    agent_instructions += (
        f"\n\n## Current Model\n\nModel file: `{model_path.name}`\nFull path: `{model_path}`"
    )

    # Indent instructions for YAML format (2 spaces per line)
    indented_instructions = "\n".join("  " + line for line in agent_instructions.split("\n"))

    # Create index.yaml
    index_yaml = f"""name: bsl
description: BSL agent for querying {model_path.name}
version: 0.1.0
interpreter: {python_exe}

instructions: |
{indented_instructions}

conversation_starters:
  - What dimensions and measures are available?
  - Show me the data
  - What can I query?
"""

    (agent_dir / "index.yaml").write_text(index_yaml)

    # Create tools.py with model path and backend embedded
    tools_py = f'''"""BSL tools for temporary aichat agent."""

from pathlib import Path

# Import shared BSL tools
from boring_semantic_layer.agents.tools import initialize_tools, get_tools

# Initialize tools with the model path and backend
MODEL_PATH = Path("{model_path}")
BACKEND = "{backend}"

# Initialize the shared tools instance
initialize_tools(MODEL_PATH, BACKEND)


def list_bsl():
    """List all available dimensions and measures from the semantic model."""
    tools = get_tools()
    return tools.list_models()


def query_bsl(query: str, show_chart: bool = None):
    """
    Execute a BSL query and optionally display results with a chart.

    Args:
        query: BSL query string (e.g., 'model.group_by("dim").aggregate("measure")')
        show_chart: Whether to display a chart (True/False). If None, only returns data table.
    """
    tools = get_tools()
    return tools.query_model(query, show_chart)
'''

    (agent_dir / "tools.py").write_text(tools_py)

    # Generate functions.json
    functions_json = [
        {
            "name": "list_bsl",
            "description": "List all available dimensions and measures from the semantic model",
            "parameters": {"type": "object", "properties": {}},
            "agent": True,
        },
        {
            "name": "query_bsl",
            "description": "Execute a BSL query and optionally display results with a chart",
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "BSL query string",
                    },
                    "show_chart": {
                        "type": "boolean",
                        "description": "Whether to display a chart",
                    },
                },
                "required": ["query"],
            },
            "agent": True,
        },
    ]

    (agent_dir / "functions.json").write_text(json.dumps(functions_json, indent=2))


def _list_model_fields(model_path: Path):
    """List available dimensions and measures from the model."""
    try:
        from boring_semantic_layer import from_yaml

        models = from_yaml(str(model_path))

        for model_name, model in models.items():
            print(f"📊 Model: {model_name}")
            print(f"   Dimensions: {', '.join(model.dimensions)}")
            print(f"   Measures: {', '.join(model.measures)}")
            print()

    except Exception as e:
        print(f"⚠️  Could not load model: {e}")
