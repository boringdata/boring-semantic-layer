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

    # Get current Python interpreter (from virtual env if active)
    python_exe = sys.executable

    # Create bin executable for the agent
    bin_dir = functions_dir / "bin"
    bin_dir.mkdir(exist_ok=True)
    bin_script = bin_dir / "bsl"

    # Remove existing bin script if present
    if bin_script.exists():
        bin_script.unlink()

    # Create executable that calls Python tools directly
    # Note: We need to properly handle the agent directory path in the bin script
    bin_content = f"""#!/usr/bin/env bash
set -e

# Python executable and agent directory
PYTHON_EXE="{python_exe}"
AGENT_DIR="{agent_dir}"

# Call Python with the agent's tools module
"$PYTHON_EXE" -c '
import sys
import json
sys.path.insert(0, "{agent_dir}")

from tools import list_bsl, query_bsl

# Get function name and arguments
func_name = sys.argv[1]
args_json = sys.argv[2] if len(sys.argv) > 2 else "{{}}"

# Parse arguments
args = json.loads(args_json)

# Call the appropriate function and print result
if func_name == "list_bsl":
    print(list_bsl())
elif func_name == "query_bsl":
    print(query_bsl(**args))
else:
    print(f"Error: Unknown function {{func_name}}")
    sys.exit(1)
' "$1" "$2"
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

    # Load agent prompt from BSL query expert skill
    skill_file = Path(__file__).parent / "claude-code" / "bsl-query-expert" / "SKILL.md"
    agent_instructions = skill_file.read_text()

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


def query_bsl(query: str, show_chart: bool = None, chart_spec: dict = None):
    """
    Execute a BSL query and optionally display results with a chart.

    Args:
        query: BSL query string (e.g., 'model.group_by("dim").aggregate("measure")')
        show_chart: Whether to display a chart (True/False). If None, only returns data table.
        chart_spec: Optional chart specification dict (e.g., {{"chart_type": "bar"}}).
    """
    tools = get_tools()
    return tools.query_model(query, show_chart, chart_spec)
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
                    "chart_spec": {
                        "type": "object",
                        "description": 'Optional chart specification (e.g., {"chart_type": "bar"})',
                    },
                },
                "required": ["query"],
            },
            "agent": True,
        },
    ]

    (agent_dir / "functions.json").write_text(json.dumps(functions_json, indent=2))

    # Create Argcfile.sh for function dispatch (placed in functions_dir root)
    functions_dir = agent_dir.parent.parent
    argcfile_path = functions_dir / "Argcfile.sh"

    argcfile_content = f'''#!/usr/bin/env bash
# Argcfile for BSL agent function calls

# Function to run agent tools
run@agent() {{
    local agent_name="$1"
    local func_name="$2"
    local args_json="${{3:-{{}}}}"

    # Get the agent directory
    local agent_dir="{functions_dir}/agents/$agent_name"

    # Call Python with the agent's tools module
    "{python_exe}" -c '
import sys
import json
sys.path.insert(0, "'"$agent_dir"'")

from tools import list_bsl, query_bsl

# Get function name and arguments
func_name = "'"$func_name"'"
args_json = "'"$args_json"'"

# Parse arguments
args = json.loads(args_json) if args_json and args_json != "{{}}" else {{}}

# Call the appropriate function and print result
if func_name == "list_bsl":
    print(list_bsl())
elif func_name == "query_bsl":
    print(query_bsl(**args))
else:
    print(f"Error: Unknown function {{func_name}}")
    sys.exit(1)
'
}}
'''

    argcfile_path.write_text(argcfile_content)
    argcfile_path.chmod(0o755)


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
