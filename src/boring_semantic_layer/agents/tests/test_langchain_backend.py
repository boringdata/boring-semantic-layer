"""Unit tests for LangChain backend."""

import json
import subprocess
from pathlib import Path
from unittest.mock import Mock, patch

import pytest
from returns.result import Failure, Success

# Skip all tests if langchain is not installed
pytest.importorskip("langchain")


@pytest.fixture
def mock_models():
    """Mock semantic models."""
    mock_model = Mock()
    mock_model.dimensions = {"carrier", "origin", "destination"}
    mock_model.measures = {"flight_count", "avg_distance"}
    mock_model.description = "Test description"
    return {"flights": mock_model}


@pytest.fixture
def temp_model_file(tmp_path):
    """Create a temporary semantic model YAML file."""
    model_content = """
models:
  flights:
    db_table: flights_table
    dimensions:
      - name: carrier
        expr: carrier_code
      - name: origin
        expr: origin_airport
    measures:
      - name: flight_count
        expr: count(*)
"""
    model_file = tmp_path / "test_model.yml"
    model_file.write_text(model_content)
    return model_file


class TestLangChainAgent:
    """Tests for LangChainAgent class."""

    @patch("boring_semantic_layer.agents.backends.langchain.from_yaml")
    @patch("boring_semantic_layer.agents.backends.langchain.init_chat_model")
    @patch("boring_semantic_layer.agents.backends.langchain.load_prompt")
    def test_agent_initialization(self, mock_load_prompt, mock_init_chat, mock_from_yaml, tmp_path):
        """Test agent initialization."""
        from boring_semantic_layer.agents.backends.langchain import LangChainAgent

        # Setup mocks
        mock_from_yaml.return_value = {"flights": Mock()}
        mock_init_chat.return_value = Mock()
        mock_load_prompt.return_value = "Test prompt"

        model_file = tmp_path / "test.yml"
        model_file.write_text("test")

        # Create agent
        agent = LangChainAgent(
            model_path=model_file,
            llm_model="gpt-4",
            chart_backend="plotext",
        )

        # Assertions
        assert agent.llm_model == "gpt-4"
        assert agent.chart_backend == "plotext"
        assert len(agent.tools) == 3  # list_models, query_model, and get_documentation
        assert agent.conversation_history == []
        mock_from_yaml.assert_called_once()
        mock_init_chat.assert_called_once_with("gpt-4", temperature=0)

    @patch("boring_semantic_layer.agents.backends.langchain.from_yaml")
    @patch("boring_semantic_layer.agents.backends.langchain.init_chat_model")
    @patch("boring_semantic_layer.agents.backends.langchain.load_prompt")
    def test_list_models_tool(
        self,
        mock_load_prompt,
        mock_init_chat,
        mock_from_yaml,
        tmp_path,
        mock_models,
    ):
        """Test list_models tool returns correct JSON."""
        from boring_semantic_layer.agents.backends.langchain import LangChainAgent

        mock_from_yaml.return_value = mock_models
        mock_init_chat.return_value = Mock()
        mock_load_prompt.return_value = "Test prompt"

        model_file = tmp_path / "test.yml"
        model_file.write_text("test")

        agent = LangChainAgent(model_path=model_file)

        # Get list_models tool
        list_models_tool = agent.tools[0]
        result = list_models_tool.func()

        # Parse JSON result
        result_dict = json.loads(result)
        assert "flights" in result_dict
        assert "dimensions" in result_dict["flights"]
        assert "measures" in result_dict["flights"]

    @patch("boring_semantic_layer.agents.backends.langchain.from_yaml")
    @patch("boring_semantic_layer.agents.backends.langchain.init_chat_model")
    @patch("boring_semantic_layer.agents.backends.langchain.load_prompt")
    def test_query_model_tool_success(
        self, mock_load_prompt, mock_init_chat, mock_from_yaml, tmp_path, mock_models
    ):
        """Test query_model tool executes successfully."""
        from boring_semantic_layer.agents.backends.langchain import LangChainAgent

        mock_from_yaml.return_value = mock_models
        mock_init_chat.return_value = Mock()
        mock_load_prompt.return_value = "Test prompt"

        model_file = tmp_path / "test.yml"
        model_file.write_text("test")

        agent = LangChainAgent(model_path=model_file)

        # Get query_model tool
        query_model_tool = agent.tools[1]

        # Mock safe_eval and generate_chart_with_data
        with (
            patch("boring_semantic_layer.agents.backends.langchain.safe_eval") as mock_eval,
            patch(
                "boring_semantic_layer.agents.backends.langchain.generate_chart_with_data"
            ) as mock_chart,
        ):
            mock_result = Mock()
            mock_eval.return_value = mock_result
            mock_chart.return_value = '{"status": "success", "data": []}'

            result = query_model_tool.func(query="flights.aggregate('flight_count')")

            assert "success" in result or '"status"' in result
            mock_eval.assert_called_once()
            mock_chart.assert_called_once()

    @patch("boring_semantic_layer.agents.backends.langchain.from_yaml")
    @patch("boring_semantic_layer.agents.backends.langchain.init_chat_model")
    @patch("boring_semantic_layer.agents.backends.langchain.load_prompt")
    def test_query_model_tool_error(
        self, mock_load_prompt, mock_init_chat, mock_from_yaml, tmp_path, mock_models
    ):
        """Test query_model tool handles errors."""
        from boring_semantic_layer.agents.backends.langchain import LangChainAgent

        mock_from_yaml.return_value = mock_models
        mock_init_chat.return_value = Mock()
        mock_load_prompt.return_value = "Test prompt"

        model_file = tmp_path / "test.yml"
        model_file.write_text("test")

        agent = LangChainAgent(model_path=model_file)

        # Get query_model tool
        query_model_tool = agent.tools[1]

        # Mock safe_eval to raise error
        with patch("boring_semantic_layer.agents.backends.langchain.safe_eval") as mock_eval:
            mock_eval.side_effect = ValueError("Invalid query")

            result = query_model_tool.func(query="invalid query")

            assert "❌" in result
            assert "Invalid query" in result

    @patch("boring_semantic_layer.agents.backends.langchain.from_yaml")
    @patch("boring_semantic_layer.agents.backends.langchain.init_chat_model")
    @patch("boring_semantic_layer.agents.backends.langchain.load_prompt")
    def test_query_model_tool_with_failure_result(
        self, mock_load_prompt, mock_init_chat, mock_from_yaml, tmp_path, mock_models
    ):
        """Test query_model tool handles Result.Failure from safe_eval."""
        from boring_semantic_layer.agents.backends.langchain import LangChainAgent

        mock_from_yaml.return_value = mock_models
        mock_init_chat.return_value = Mock()
        mock_load_prompt.return_value = "Test prompt"

        model_file = tmp_path / "test.yml"
        model_file.write_text("test")

        agent = LangChainAgent(model_path=model_file)

        # Get query_model tool
        query_model_tool = agent.tools[1]

        # Mock safe_eval to return an actual Failure object (not an exception!)
        with patch("boring_semantic_layer.agents.backends.langchain.safe_eval") as mock_eval:
            # Return a real Failure object from the returns library
            mock_eval.return_value = Failure(ValueError("Query execution failed"))

            result = query_model_tool.func(query="invalid query")

            # Should return an error message
            assert "❌" in result
            assert len(result) > 20  # More than just the prefix
            mock_eval.assert_called_once()

    @patch("boring_semantic_layer.agents.backends.langchain.from_yaml")
    @patch("boring_semantic_layer.agents.backends.langchain.init_chat_model")
    @patch("boring_semantic_layer.agents.backends.langchain.load_prompt")
    def test_query_model_tool_with_success_result(
        self, mock_load_prompt, mock_init_chat, mock_from_yaml, tmp_path, mock_models
    ):
        """Test query_model tool properly unwraps Result.Success from safe_eval."""
        from boring_semantic_layer.agents.backends.langchain import LangChainAgent

        mock_from_yaml.return_value = mock_models
        mock_init_chat.return_value = Mock()
        mock_load_prompt.return_value = "Test prompt"

        model_file = tmp_path / "test.yml"
        model_file.write_text("test")

        agent = LangChainAgent(model_path=model_file, chart_backend="plotext")

        # Get query_model tool
        query_model_tool = agent.tools[1]

        # Create a mock query result with execute() and chart() methods
        mock_query_result = Mock()
        mock_df = Mock()
        mock_df.to_json.return_value = '{"data": []}'
        mock_df.__len__ = Mock(return_value=5)  # Mock len() for result_df
        mock_query_result.execute.return_value = mock_df
        mock_query_result.chart.return_value = None  # chart() renders to terminal

        # Mock safe_eval to return an actual Success object
        with patch("boring_semantic_layer.agents.backends.langchain.safe_eval") as mock_eval:
            # Return a real Success object from the returns library
            mock_eval.return_value = Success(mock_query_result)

            result = query_model_tool.func(query="flights.aggregate('flight_count')")

            # Should successfully unwrap and call generate_chart_with_data
            assert "error" not in result.lower()
            assert "successfully" in result.lower()
            mock_eval.assert_called_once()
            mock_query_result.execute.assert_called_once()
            # With plotext backend and no chart_spec, chart() should be called
            mock_query_result.chart.assert_called_once()

    @patch("boring_semantic_layer.agents.backends.langchain.from_yaml")
    @patch("boring_semantic_layer.agents.backends.langchain.init_chat_model")
    @patch("boring_semantic_layer.agents.backends.langchain.load_prompt")
    @patch("boring_semantic_layer.agents.backends.langchain.process_query")
    def test_query_method(
        self, mock_process_query, mock_load_prompt, mock_init_chat, mock_from_yaml, tmp_path
    ):
        """Test query method updates conversation history."""
        from boring_semantic_layer.agents.backends.langchain import LangChainAgent

        mock_from_yaml.return_value = {"flights": Mock()}
        mock_init_chat.return_value = Mock()
        mock_load_prompt.return_value = "Test prompt"
        mock_process_query.return_value = ("tool output", "agent response")

        model_file = tmp_path / "test.yml"
        model_file.write_text("test")

        agent = LangChainAgent(model_path=model_file)
        tool_output, agent_response = agent.query("Show me flights")

        assert tool_output == "tool output"
        assert agent_response == "agent response"
        assert len(agent.conversation_history) == 2
        assert agent.conversation_history[0]["role"] == "user"
        assert agent.conversation_history[1]["role"] == "assistant"

    @patch("boring_semantic_layer.agents.backends.langchain.from_yaml")
    @patch("boring_semantic_layer.agents.backends.langchain.init_chat_model")
    @patch("boring_semantic_layer.agents.backends.langchain.load_prompt")
    @patch("boring_semantic_layer.agents.backends.langchain.process_query")
    def test_conversation_history_limit(
        self, mock_process_query, mock_load_prompt, mock_init_chat, mock_from_yaml, tmp_path
    ):
        """Test conversation history is limited to 20 messages."""
        from boring_semantic_layer.agents.backends.langchain import LangChainAgent

        mock_from_yaml.return_value = {"flights": Mock()}
        mock_init_chat.return_value = Mock()
        mock_load_prompt.return_value = "Test prompt"
        mock_process_query.return_value = ("output", "response")

        model_file = tmp_path / "test.yml"
        model_file.write_text("test")

        agent = LangChainAgent(model_path=model_file)

        # Add 15 exchanges (30 messages)
        for i in range(15):
            agent.query(f"Query {i}")

        # History should be limited to 20 messages (10 exchanges)
        assert len(agent.conversation_history) == 20

    @patch("boring_semantic_layer.agents.backends.langchain.from_yaml")
    @patch("boring_semantic_layer.agents.backends.langchain.init_chat_model")
    @patch("boring_semantic_layer.agents.backends.langchain.load_prompt")
    def test_reset_history(self, mock_load_prompt, mock_init_chat, mock_from_yaml, tmp_path):
        """Test reset_history clears conversation history."""
        from boring_semantic_layer.agents.backends.langchain import LangChainAgent

        mock_from_yaml.return_value = {"flights": Mock()}
        mock_init_chat.return_value = Mock()
        mock_load_prompt.return_value = "Test prompt"

        model_file = tmp_path / "test.yml"
        model_file.write_text("test")

        agent = LangChainAgent(model_path=model_file)
        agent.conversation_history = [{"role": "user", "content": "test"}]

        agent.reset_history()

        assert agent.conversation_history == []


class TestProcessQuery:
    """Tests for process_query function."""

    def test_process_query_no_tool_calls(self):
        """Test process_query when LLM doesn't call tools."""
        from boring_semantic_layer.agents.backends.langchain import process_query

        mock_llm = Mock()
        mock_response = Mock()
        mock_response.tool_calls = []
        mock_response.content = "Here's your answer"
        mock_llm.bind_tools.return_value.invoke.return_value = mock_response

        tool_output, agent_response = process_query(
            mock_llm,
            "What is the weather?",
            [],
            [],
        )

        assert tool_output == ""
        assert agent_response == "Here's your answer"

    def test_process_query_with_tool_calls(self):
        """Test process_query when LLM calls tools."""
        from boring_semantic_layer.agents.backends.langchain import process_query

        mock_llm = Mock()

        # First response with tool call
        mock_response1 = Mock()
        mock_response1.tool_calls = [{"name": "test_tool", "args": {}, "id": "123"}]

        # Second response without tool calls (final response)
        mock_response2 = Mock()
        mock_response2.tool_calls = []
        mock_response2.content = "Final answer"

        mock_llm.bind_tools.return_value.invoke.side_effect = [mock_response1, mock_response2]

        # Create mock tool
        mock_tool = Mock()
        mock_tool.name = "test_tool"
        mock_tool.invoke.return_value = "Tool result"

        tool_output, agent_response = process_query(
            mock_llm,
            "Test query",
            [],
            [mock_tool],
        )

        assert agent_response == "Final answer"
        mock_tool.invoke.assert_called_once()

    def test_process_query_max_iterations(self):
        """Test process_query stops after max iterations."""
        from boring_semantic_layer.agents.backends.langchain import process_query

        mock_llm = Mock()

        # Always return tool calls
        mock_response = Mock()
        mock_response.tool_calls = [{"name": "test_tool", "args": {}, "id": "123"}]
        mock_llm.bind_tools.return_value.invoke.return_value = mock_response

        mock_tool = Mock()
        mock_tool.name = "test_tool"
        mock_tool.invoke.return_value = "Tool result"

        tool_output, agent_response = process_query(
            mock_llm,
            "Test query",
            [],
            [mock_tool],
        )

        assert "maximum function call iterations" in agent_response.lower()
        # Should be called 5 times (max_iterations)
        assert mock_tool.invoke.call_count == 5

    def test_process_query_with_on_tool_call_callback(self):
        """Test process_query calls on_tool_call callback."""
        from boring_semantic_layer.agents.backends.langchain import process_query

        mock_llm = Mock()

        # First response with tool call
        mock_response1 = Mock()
        mock_response1.tool_calls = [{"name": "test_tool", "args": {"param": "value"}, "id": "123"}]

        # Second response without tool calls (final response)
        mock_response2 = Mock()
        mock_response2.tool_calls = []
        mock_response2.content = "Final answer"

        mock_llm.bind_tools.return_value.invoke.side_effect = [mock_response1, mock_response2]

        # Create mock tool
        mock_tool = Mock()
        mock_tool.name = "test_tool"
        mock_tool.invoke.return_value = "Tool result"

        # Create callback to track calls
        callback_calls = []

        def on_tool_call(fn_name, fn_args):
            callback_calls.append((fn_name, fn_args))

        tool_output, agent_response = process_query(
            mock_llm,
            "Test query",
            [],
            [mock_tool],
            on_tool_call=on_tool_call,
        )

        # Callback should be called with tool name and args
        assert len(callback_calls) == 1
        assert callback_calls[0] == ("test_tool", {"param": "value"})
        assert agent_response == "Final answer"


class TestGetDocumentation:
    """Tests for get_documentation tool."""

    @patch("boring_semantic_layer.agents.backends.langchain.from_yaml")
    @patch("boring_semantic_layer.agents.backends.langchain.init_chat_model")
    @patch("boring_semantic_layer.agents.backends.langchain.load_prompt")
    def test_get_documentation_returns_content(
        self, mock_load_prompt, mock_init_chat, mock_from_yaml, tmp_path
    ):
        """Test get_documentation returns documentation content."""
        from boring_semantic_layer.agents.backends.langchain import LangChainAgent

        mock_from_yaml.return_value = {"flights": Mock()}
        mock_init_chat.return_value = Mock()
        # Make load_prompt return different values based on the path
        mock_load_prompt.side_effect = lambda dir, file: f"Content for {file}"

        model_file = tmp_path / "test.yml"
        model_file.write_text("test")

        agent = LangChainAgent(model_path=model_file)

        # Get get_documentation tool (it's the 3rd tool)
        get_doc_tool = agent.tools[2]
        assert get_doc_tool.name == "get_documentation"

    @patch("boring_semantic_layer.agents.backends.langchain.from_yaml")
    @patch("boring_semantic_layer.agents.backends.langchain.init_chat_model")
    @patch("boring_semantic_layer.agents.backends.langchain.load_prompt")
    def test_get_documentation_unknown_topic(
        self, mock_load_prompt, mock_init_chat, mock_from_yaml, tmp_path
    ):
        """Test get_documentation returns error for unknown topic."""
        from boring_semantic_layer.agents.backends.langchain import LangChainAgent

        mock_from_yaml.return_value = {"flights": Mock()}
        mock_init_chat.return_value = Mock()
        mock_load_prompt.return_value = "Test prompt"

        model_file = tmp_path / "test.yml"
        model_file.write_text("test")

        agent = LangChainAgent(model_path=model_file)

        # Get get_documentation tool
        get_doc_tool = agent.tools[2]
        result = get_doc_tool.func(topic="unknown-topic-xyz")

        assert "❌" in result
        assert "Unknown topic" in result
        assert "unknown-topic-xyz" in result

    @patch("boring_semantic_layer.agents.backends.langchain.from_yaml")
    @patch("boring_semantic_layer.agents.backends.langchain.init_chat_model")
    @patch("boring_semantic_layer.agents.backends.langchain.load_prompt")
    def test_get_documentation_tool_description_lists_topics(
        self, mock_load_prompt, mock_init_chat, mock_from_yaml, tmp_path
    ):
        """Test get_documentation tool description lists available topics."""
        from boring_semantic_layer.agents.backends.langchain import LangChainAgent

        mock_from_yaml.return_value = {"flights": Mock()}
        mock_init_chat.return_value = Mock()
        mock_load_prompt.return_value = "Test prompt"

        model_file = tmp_path / "test.yml"
        model_file.write_text("test")

        agent = LangChainAgent(model_path=model_file)

        # Get get_documentation tool
        get_doc_tool = agent.tools[2]

        # Description should list topics from index.json
        assert "getting-started" in get_doc_tool.description
        assert "semantic-table" in get_doc_tool.description


class TestGetMdDir:
    """Tests for _get_md_dir function."""

    def test_get_md_dir_returns_path(self):
        """Test that _get_md_dir returns a Path."""
        from boring_semantic_layer.agents.backends.langchain import _get_md_dir

        result = _get_md_dir()
        assert isinstance(result, Path)

    def test_get_md_dir_contains_expected_files(self):
        """Test that the md dir contains expected files."""
        from boring_semantic_layer.agents.backends.langchain import _get_md_dir

        md_dir = _get_md_dir()
        # Should contain index.json
        assert (md_dir / "index.json").exists()
        # Should contain prompts directory
        assert (md_dir / "prompts").exists()
        # Should contain doc directory
        assert (md_dir / "doc").exists()


@pytest.mark.slow
class TestGetDocumentationIntegration:
    """Integration tests for get_documentation tool when installed as a package."""

    def test_get_documentation_works_when_installed(self, tmp_path):
        """Test that get_documentation can find docs when BSL is installed as a package."""
        project_dir = tmp_path / "test-project"
        project_dir.mkdir()

        # Get the BSL source directory
        bsl_source = Path(__file__).parent.parent.parent.parent.parent

        # Initialize a uv project
        result = subprocess.run(
            ["uv", "init"],
            cwd=project_dir,
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0, f"uv init failed: {result.stderr}"

        # Add BSL with agent extras from local path
        result = subprocess.run(
            ["uv", "add", f"boring-semantic-layer[agent] @ {bsl_source}"],
            cwd=project_dir,
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0, f"uv add failed: {result.stderr}"

        # Create a test script that uses get_documentation
        test_script = project_dir / "test_docs.py"
        test_script.write_text("""
import sys
from pathlib import Path

# Test _get_md_dir finds docs
from boring_semantic_layer.agents.backends.langchain import _get_md_dir

md_dir = _get_md_dir()
print(f"MD_DIR: {md_dir}")

# Check that required files exist
assert md_dir.exists(), f"MD dir does not exist: {md_dir}"
assert (md_dir / "index.json").exists(), f"index.json not found in {md_dir}"
assert (md_dir / "prompts").exists(), f"prompts dir not found in {md_dir}"
assert (md_dir / "doc").exists(), f"doc dir not found in {md_dir}"

# Check index.json can be loaded
import json
index_data = json.loads((md_dir / "index.json").read_text())
assert "topics" in index_data, "No topics in index.json"
assert "getting-started" in index_data["topics"], "getting-started not in topics"

# Check a doc file exists
getting_started_source = index_data["topics"]["getting-started"]["source"]
doc_path = md_dir / getting_started_source
assert doc_path.exists(), f"Doc file not found: {doc_path}"

print("SUCCESS: All documentation files found!")
""")

        # Run the test script
        result = subprocess.run(
            ["uv", "run", "python", "test_docs.py"],
            cwd=project_dir,
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0, f"Test script failed: {result.stderr}\n{result.stdout}"
        assert "SUCCESS" in result.stdout, f"Test didn't succeed: {result.stdout}"

    def test_bsl_chat_with_mocked_llm(self, tmp_path):
        """Test bsl chat command with a mocked LLM in a fresh project."""
        project_dir = tmp_path / "test-chat-project"
        project_dir.mkdir()

        # Get the BSL source directory
        bsl_source = Path(__file__).parent.parent.parent.parent.parent

        # Initialize a uv project
        result = subprocess.run(
            ["uv", "init"],
            cwd=project_dir,
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0, f"uv init failed: {result.stderr}"

        # Add BSL with agent and examples extras from local path
        result = subprocess.run(
            ["uv", "add", f"boring-semantic-layer[agent,examples] @ {bsl_source}"],
            cwd=project_dir,
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0, f"uv add failed: {result.stderr}"

        # Create a simple semantic model
        model_file = project_dir / "model.yml"
        model_file.write_text("""\
items:
  description: "Simple items table"
  table: items
  dimensions:
    category: _.category
  measures:
    item_count: _.count()
    total_value: _.value.sum()
""")

        # Create test data parquet file
        import duckdb

        conn = duckdb.connect()
        conn.execute("""
            CREATE TABLE items (category VARCHAR, value INTEGER);
            INSERT INTO items VALUES ('electronics', 100), ('electronics', 200), ('books', 50);
            COPY items TO 'items.parquet' (FORMAT PARQUET);
        """)
        # Move file to project dir
        import shutil

        shutil.move("items.parquet", project_dir / "items.parquet")

        # Create a profiles.yml with DuckDB in-memory
        profiles_file = project_dir / "profiles.yml"
        profiles_file.write_text("""\
test_db:
  type: duckdb
  database: ":memory:"
  tables:
    items: "items.parquet"
""")

        # Create a test script that tests all tools individually
        test_script = project_dir / "test_chat.py"
        test_script.write_text("""
import sys
from unittest.mock import Mock, patch
from pathlib import Path

# Patch init_chat_model before importing LangChainAgent
with patch("boring_semantic_layer.agents.backends.langchain.init_chat_model") as mock_init:
    mock_llm = Mock()
    mock_init.return_value = mock_llm

    from boring_semantic_layer.agents.backends.langchain import LangChainAgent

    agent = LangChainAgent(
        model_path=Path("model.yml"),
        llm_model="gpt-4",
        chart_backend="plotext",
        profile="test_db",
        profile_file=Path("profiles.yml"),
    )

    # Verify we have all 3 tools
    tool_names = [t.name for t in agent.tools]
    print(f"Available tools: {tool_names}")
    assert "list_models" in tool_names, f"Missing list_models, got: {tool_names}"
    assert "query_model" in tool_names, f"Missing query_model, got: {tool_names}"
    assert "get_documentation" in tool_names, f"Missing get_documentation, got: {tool_names}"

    # Test 1: list_models tool
    print("\\n=== Testing list_models tool ===")
    list_models_tool = next(t for t in agent.tools if t.name == "list_models")
    result = list_models_tool.func()
    print(f"list_models result: {result[:200]}...")
    assert "items" in result, f"items not in list_models result: {result}"
    assert "category" in result, f"category not in list_models result: {result}"
    assert "item_count" in result, f"item_count not in list_models result: {result}"
    print("✓ list_models tool works!")

    # Test 2: get_documentation tool
    print("\\n=== Testing get_documentation tool ===")
    get_doc_tool = next(t for t in agent.tools if t.name == "get_documentation")
    result = get_doc_tool.func(topic="getting-started")
    print(f"get_documentation result length: {len(result)} chars")
    assert len(result) > 100, f"Documentation too short: {result[:100]}"
    assert "BSL" in result or "semantic" in result.lower(), f"Unexpected doc content: {result[:200]}"
    print("✓ get_documentation tool works!")

    # Test 3: query_model tool with chart rendering
    print("\\n=== Testing query_model tool ===")
    query_model_tool = next(t for t in agent.tools if t.name == "query_model")

    # Capture stdout to check for chart output (plotext prints directly to stdout)
    import io
    import sys as sys_module
    captured_output = io.StringIO()
    old_stdout = sys_module.stdout
    sys_module.stdout = captured_output

    result = query_model_tool.func(query="items.group_by('category').aggregate('item_count')")

    sys_module.stdout = old_stdout
    chart_output = captured_output.getvalue()

    print(f"query_model result: {result}")
    print(f"Chart output captured ({len(chart_output)} chars)")

    # Should contain success message
    assert "success" in result.lower() or "rows" in result.lower(), \\
        f"Unexpected query result: {result}"
    print("✓ query_model tool works!")

    # Test 4: Verify chart is rendered (plotext prints ASCII chart to stdout)
    print("\\n=== Testing chart rendering ===")
    # plotext uses characters like ─, │, ┼, █, ▄, etc for charts
    has_chart_chars = any(c in chart_output for c in ["─", "│", "┼", "█", "▄", "▀", "┌", "┐", "└", "┘"])
    print(f"Chart characters found: {has_chart_chars}")
    if chart_output:
        print(f"Chart output preview:\\n{chart_output[:500]}...")

    # With plotext backend, we expect ASCII chart to be printed to stdout
    assert has_chart_chars, f"Expected ASCII chart in stdout, got: {chart_output[:200] if chart_output else '(empty)'}"
    # Also verify the chart contains our data labels
    assert "electronics" in chart_output or "books" in chart_output, \\
        f"Expected category labels in chart: {chart_output}"
    print("✓ Chart rendering works!")

    print("\\n=== SUCCESS: All tools work correctly! ===")
""")

        # Run the test script
        result = subprocess.run(
            ["uv", "run", "python", "test_chat.py"],
            cwd=project_dir,
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0, f"Test script failed: {result.stderr}\n{result.stdout}"
        assert "SUCCESS" in result.stdout, f"Test didn't succeed: {result.stdout}"


class TestErrorCallback:
    """Tests for error callback mechanism."""

    @patch("boring_semantic_layer.agents.backends.langchain.from_yaml")
    @patch("boring_semantic_layer.agents.backends.langchain.init_chat_model")
    @patch("boring_semantic_layer.agents.backends.langchain.load_prompt")
    def test_query_with_error_callback_on_failure(
        self, mock_load_prompt, mock_init_chat, mock_from_yaml, tmp_path, mock_models
    ):
        """Test that error callback is called when query fails."""
        from boring_semantic_layer.agents.backends.langchain import LangChainAgent

        mock_from_yaml.return_value = mock_models
        mock_llm = Mock()
        mock_init_chat.return_value = mock_llm
        mock_load_prompt.return_value = "Test prompt"

        model_file = tmp_path / "test.yml"
        model_file.write_text("test")

        agent = LangChainAgent(model_path=model_file, chart_backend="plotext")

        # Track error callback calls
        error_messages = []

        def on_error(msg):
            error_messages.append(msg)

        # Mock LLM to call query_model tool
        mock_response1 = Mock()
        mock_response1.tool_calls = [
            {"name": "query_model", "args": {"query": "invalid.query()"}, "id": "123"}
        ]

        mock_response2 = Mock()
        mock_response2.tool_calls = []
        mock_response2.content = "Query failed"

        mock_llm.bind_tools.return_value.invoke.side_effect = [mock_response1, mock_response2]

        # Mock safe_eval to return Failure
        with patch("boring_semantic_layer.agents.backends.langchain.safe_eval") as mock_eval:
            mock_eval.return_value = Failure(ValueError("Invalid query syntax"))

            tool_output, agent_response = agent.query(
                "Run invalid query",
                on_error=on_error,
            )

            # Error callback should have been called
            assert len(error_messages) == 1
            assert "❌ Query Error" in error_messages[0]
            # The error message should contain something meaningful (either the error or traceback)
            assert len(error_messages[0]) > 20  # More than just the prefix

    @patch("boring_semantic_layer.agents.backends.langchain.from_yaml")
    @patch("boring_semantic_layer.agents.backends.langchain.init_chat_model")
    @patch("boring_semantic_layer.agents.backends.langchain.load_prompt")
    def test_query_with_error_callback_on_execution_error(
        self, mock_load_prompt, mock_init_chat, mock_from_yaml, tmp_path, mock_models
    ):
        """Test that error callback is called when query execution fails."""
        from boring_semantic_layer.agents.backends.langchain import LangChainAgent

        mock_from_yaml.return_value = mock_models
        mock_llm = Mock()
        mock_init_chat.return_value = mock_llm
        mock_load_prompt.return_value = "Test prompt"

        model_file = tmp_path / "test.yml"
        model_file.write_text("test")

        agent = LangChainAgent(model_path=model_file, chart_backend="plotext")

        # Track error callback calls
        error_messages = []

        def on_error(msg):
            error_messages.append(msg)

        # Mock LLM to call query_model tool
        mock_response1 = Mock()
        mock_response1.tool_calls = [
            {"name": "query_model", "args": {"query": "flights.query()"}, "id": "123"}
        ]

        mock_response2 = Mock()
        mock_response2.tool_calls = []
        mock_response2.content = "Query failed"

        mock_llm.bind_tools.return_value.invoke.side_effect = [mock_response1, mock_response2]

        # Mock safe_eval to return Success, but execute() raises error
        with patch("boring_semantic_layer.agents.backends.langchain.safe_eval") as mock_eval:
            mock_query_result = Mock()
            mock_query_result.execute.side_effect = RuntimeError("Database connection failed")
            mock_eval.return_value = Success(mock_query_result)

            tool_output, agent_response = agent.query(
                "Run query",
                on_error=on_error,
            )

            # Error callback should have been called
            assert len(error_messages) == 1
            assert "❌ Query Execution Error" in error_messages[0]
            assert "Database connection failed" in error_messages[0]

    @patch("boring_semantic_layer.agents.backends.langchain.from_yaml")
    @patch("boring_semantic_layer.agents.backends.langchain.init_chat_model")
    @patch("boring_semantic_layer.agents.backends.langchain.load_prompt")
    def test_query_clears_error_callback_after_execution(
        self, mock_load_prompt, mock_init_chat, mock_from_yaml, tmp_path
    ):
        """Test that error callback is cleared after query execution."""
        from boring_semantic_layer.agents.backends.langchain import LangChainAgent

        mock_from_yaml.return_value = {"flights": Mock()}
        mock_llm = Mock()
        mock_init_chat.return_value = mock_llm
        mock_load_prompt.return_value = "Test prompt"

        model_file = tmp_path / "test.yml"
        model_file.write_text("test")

        agent = LangChainAgent(model_path=model_file)

        # Mock LLM to return without tool calls
        mock_response = Mock()
        mock_response.tool_calls = []
        mock_response.content = "Response"

        mock_llm.bind_tools.return_value.invoke.return_value = mock_response

        def on_error(msg):
            pass

        # Run query with error callback
        agent.query("Test query", on_error=on_error)

        # Error callback should be cleared
        assert agent._error_callback is None
