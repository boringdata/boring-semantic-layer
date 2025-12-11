"""Unit tests for LangGraph backend."""

from unittest.mock import Mock, patch

import pytest

# Skip entire module if langchain is not installed (optional dependency)
pytest.importorskip("langchain", reason="langchain not installed")


@pytest.fixture
def mock_models():
    """Mock semantic models."""
    mock_model = Mock()
    mock_model.dimensions = {"category", "region"}
    mock_model.measures = {"count", "total"}
    mock_model.description = "Test model"
    return {"test": mock_model}


class TestLangGraphBackendInit:
    """Tests for LangGraphBackend initialization."""

    @patch("boring_semantic_layer.agents.backends.langgraph.create_agent")
    @patch("boring_semantic_layer.agents.backends.langgraph.init_chat_model")
    @patch("boring_semantic_layer.agents.tools.from_yaml")
    def test_init_creates_langgraph_agent(
        self, mock_from_yaml, mock_init_chat, mock_create_agent, tmp_path, mock_models
    ):
        """Test that initialization creates the LangGraph agent."""
        from boring_semantic_layer.agents.backends.langgraph import LangGraphBackend

        mock_from_yaml.return_value = mock_models
        mock_llm = Mock()
        mock_init_chat.return_value = mock_llm

        model_file = tmp_path / "test.yml"
        model_file.write_text("test")

        agent = LangGraphBackend(model_path=model_file, llm_model="claude-3-sonnet")

        assert agent.llm_model == "claude-3-sonnet"
        mock_init_chat.assert_called_once_with("claude-3-sonnet", temperature=0)
        mock_create_agent.assert_called_once()

    @patch("boring_semantic_layer.agents.backends.langgraph.create_agent")
    @patch("boring_semantic_layer.agents.backends.langgraph.init_chat_model")
    @patch("boring_semantic_layer.agents.tools.from_yaml")
    def test_init_default_model(
        self, mock_from_yaml, mock_init_chat, mock_create_agent, tmp_path, mock_models
    ):
        """Test that default model is Claude."""
        from boring_semantic_layer.agents.backends.langgraph import LangGraphBackend

        mock_from_yaml.return_value = mock_models
        mock_init_chat.return_value = Mock()

        model_file = tmp_path / "test.yml"
        model_file.write_text("test")

        agent = LangGraphBackend(model_path=model_file)

        assert "claude" in agent.llm_model.lower() or "anthropic" in agent.llm_model.lower()

    @patch("boring_semantic_layer.agents.backends.langgraph.create_agent")
    @patch("boring_semantic_layer.agents.backends.langgraph.init_chat_model")
    @patch("boring_semantic_layer.agents.tools.from_yaml")
    def test_init_with_profile(
        self, mock_from_yaml, mock_init_chat, mock_create_agent, tmp_path, mock_models
    ):
        """Test initialization with profile settings."""
        from boring_semantic_layer.agents.backends.langgraph import LangGraphBackend

        mock_from_yaml.return_value = mock_models
        mock_init_chat.return_value = Mock()

        model_file = tmp_path / "test.yml"
        model_file.write_text("test")
        profile_file = tmp_path / "profiles.yml"
        profile_file.write_text("test")

        agent = LangGraphBackend(
            model_path=model_file,
            profile="prod",
            profile_file=profile_file,
            chart_backend="plotly",
        )

        assert agent.profile == "prod"
        assert agent.chart_backend == "plotly"

    @patch("boring_semantic_layer.agents.backends.langgraph.create_agent")
    @patch("boring_semantic_layer.agents.backends.langgraph.init_chat_model")
    @patch("boring_semantic_layer.agents.tools.from_yaml")
    def test_init_empty_history(
        self, mock_from_yaml, mock_init_chat, mock_create_agent, tmp_path, mock_models
    ):
        """Test that conversation history starts empty."""
        from boring_semantic_layer.agents.backends.langgraph import LangGraphBackend

        mock_from_yaml.return_value = mock_models
        mock_init_chat.return_value = Mock()

        model_file = tmp_path / "test.yml"
        model_file.write_text("test")

        agent = LangGraphBackend(model_path=model_file)

        assert agent.conversation_history == []


class TestLangGraphBackendQuery:
    """Tests for LangGraphBackend.query() method."""

    @patch("boring_semantic_layer.agents.backends.langgraph.create_agent")
    @patch("boring_semantic_layer.agents.backends.langgraph.init_chat_model")
    @patch("boring_semantic_layer.agents.tools.from_yaml")
    def test_query_returns_tuple(
        self, mock_from_yaml, mock_init_chat, mock_create_agent, tmp_path, mock_models
    ):
        """Test that query returns tuple of (tool_outputs, response)."""
        from boring_semantic_layer.agents.backends.langgraph import LangGraphBackend

        mock_from_yaml.return_value = mock_models
        mock_init_chat.return_value = Mock()

        mock_agent = Mock()
        mock_agent.stream.return_value = iter([])
        mock_create_agent.return_value = mock_agent

        model_file = tmp_path / "test.yml"
        model_file.write_text("test")

        agent = LangGraphBackend(model_path=model_file)
        result = agent.query("test query")

        assert isinstance(result, tuple)
        assert len(result) == 2

    @patch("boring_semantic_layer.agents.backends.langgraph.create_agent")
    @patch("boring_semantic_layer.agents.backends.langgraph.init_chat_model")
    @patch("boring_semantic_layer.agents.tools.from_yaml")
    def test_query_calls_on_tool_call_callback(
        self, mock_from_yaml, mock_init_chat, mock_create_agent, tmp_path, mock_models
    ):
        """Test that on_tool_call callback is invoked for tool calls."""
        from boring_semantic_layer.agents.backends.langgraph import LangGraphBackend

        mock_from_yaml.return_value = mock_models
        mock_init_chat.return_value = Mock()

        mock_msg = Mock()
        mock_msg.tool_calls = [{"name": "query_model", "args": {"query": "test"}}]
        mock_msg.content = ""
        mock_msg.usage_metadata = None

        mock_agent = Mock()
        mock_agent.stream.return_value = iter([{"model": {"messages": [mock_msg]}}])
        mock_create_agent.return_value = mock_agent

        model_file = tmp_path / "test.yml"
        model_file.write_text("test")

        agent = LangGraphBackend(model_path=model_file)

        tool_calls = []
        agent.query("test", on_tool_call=lambda name, args, tokens: tool_calls.append((name, args)))

        assert len(tool_calls) == 1
        assert tool_calls[0][0] == "query_model"

    @patch("boring_semantic_layer.agents.backends.langgraph.create_agent")
    @patch("boring_semantic_layer.agents.backends.langgraph.init_chat_model")
    @patch("boring_semantic_layer.agents.tools.from_yaml")
    def test_query_collects_tool_outputs(
        self, mock_from_yaml, mock_init_chat, mock_create_agent, tmp_path, mock_models
    ):
        """Test that tool outputs are collected from query_model calls."""
        from boring_semantic_layer.agents.backends.langgraph import LangGraphBackend

        mock_from_yaml.return_value = mock_models
        mock_init_chat.return_value = Mock()

        # Mock tool message
        mock_tool_msg = Mock()
        mock_tool_msg.name = "query_model"
        mock_tool_msg.tool_call_id = "test_id"
        mock_tool_msg.content = '{"records": [{"a": 1}]}'

        mock_agent = Mock()
        mock_agent.stream.return_value = iter([{"tools": {"messages": [mock_tool_msg]}}])
        mock_create_agent.return_value = mock_agent

        model_file = tmp_path / "test.yml"
        model_file.write_text("test")

        agent = LangGraphBackend(model_path=model_file)
        tool_output, _ = agent.query("test")

        assert "records" in tool_output


class TestLangGraphBackendHistory:
    """Tests for conversation history management."""

    @patch("boring_semantic_layer.agents.backends.langgraph.create_agent")
    @patch("boring_semantic_layer.agents.backends.langgraph.init_chat_model")
    @patch("boring_semantic_layer.agents.tools.from_yaml")
    def test_reset_history(
        self, mock_from_yaml, mock_init_chat, mock_create_agent, tmp_path, mock_models
    ):
        """Test that reset_history clears conversation history."""
        from boring_semantic_layer.agents.backends.langgraph import LangGraphBackend

        mock_from_yaml.return_value = mock_models
        mock_init_chat.return_value = Mock()
        mock_agent = Mock()
        mock_agent.stream.return_value = iter([])
        mock_create_agent.return_value = mock_agent

        model_file = tmp_path / "test.yml"
        model_file.write_text("test")

        agent = LangGraphBackend(model_path=model_file)

        # Add some history
        agent.query("first query")
        assert len(agent.conversation_history) > 0

        # Reset
        agent.reset_history()
        assert agent.conversation_history == []

    @patch("boring_semantic_layer.agents.backends.langgraph.create_agent")
    @patch("boring_semantic_layer.agents.backends.langgraph.init_chat_model")
    @patch("boring_semantic_layer.agents.tools.from_yaml")
    def test_history_bounded_at_20(
        self, mock_from_yaml, mock_init_chat, mock_create_agent, tmp_path, mock_models
    ):
        """Test that history is bounded at 20 messages."""
        from boring_semantic_layer.agents.backends.langgraph import LangGraphBackend

        mock_from_yaml.return_value = mock_models
        mock_init_chat.return_value = Mock()

        mock_msg = Mock()
        mock_msg.tool_calls = []
        mock_msg.content = "Response"
        mock_msg.usage_metadata = None

        mock_agent = Mock()
        mock_agent.stream.return_value = iter([{"model": {"messages": [mock_msg]}}])
        mock_create_agent.return_value = mock_agent

        model_file = tmp_path / "test.yml"
        model_file.write_text("test")

        agent = LangGraphBackend(model_path=model_file)

        # Make many queries
        for i in range(25):
            mock_agent.stream.return_value = iter([{"model": {"messages": [mock_msg]}}])
            agent.query(f"query {i}")

        # History should be bounded
        assert len(agent.conversation_history) <= 20
