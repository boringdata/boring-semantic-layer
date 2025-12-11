"""Unit tests for BSLTools."""

import json
from pathlib import Path
from unittest.mock import Mock, patch

import pytest
from returns.result import Failure, Success


@pytest.fixture
def mock_models():
    """Mock semantic models."""
    mock_model = Mock()
    mock_model.dimensions = {"carrier", "origin", "destination"}
    mock_model.measures = {"flight_count", "avg_distance"}
    mock_model.description = "Test flight data"
    return {"flights": mock_model}


class TestToolDefinitions:
    """Tests for TOOL_DEFINITIONS constant."""

    def test_tool_definitions_is_list(self):
        from boring_semantic_layer.agents.tools import TOOL_DEFINITIONS

        assert isinstance(TOOL_DEFINITIONS, list)
        assert len(TOOL_DEFINITIONS) == 4  # list_models, get_model, query_model, get_documentation

    def test_tool_definitions_openai_format(self):
        from boring_semantic_layer.agents.tools import TOOL_DEFINITIONS

        for tool in TOOL_DEFINITIONS:
            assert tool["type"] == "function"
            assert "function" in tool
            assert "name" in tool["function"]
            assert "description" in tool["function"]
            assert "parameters" in tool["function"]

    def test_list_models_tool_definition(self):
        from boring_semantic_layer.agents.tools import TOOL_DEFINITIONS

        list_models = next(t for t in TOOL_DEFINITIONS if t["function"]["name"] == "list_models")
        assert list_models["function"]["parameters"]["required"] == []

    def test_query_model_tool_definition(self):
        from boring_semantic_layer.agents.tools import TOOL_DEFINITIONS

        query_model = next(t for t in TOOL_DEFINITIONS if t["function"]["name"] == "query_model")
        params = query_model["function"]["parameters"]
        assert "query" in params["properties"]
        assert "chart_spec" in params["properties"]
        assert params["required"] == ["query"]

    def test_get_documentation_tool_definition(self):
        from boring_semantic_layer.agents.tools import TOOL_DEFINITIONS

        get_doc = next(t for t in TOOL_DEFINITIONS if t["function"]["name"] == "get_documentation")
        params = get_doc["function"]["parameters"]
        assert "topic" in params["properties"]
        assert params["required"] == ["topic"]
        # Should list available topics in description
        assert "getting-started" in get_doc["function"]["description"]


class TestSystemPrompt:
    """Tests for SYSTEM_PROMPT constant."""

    def test_system_prompt_exists(self):
        from boring_semantic_layer.agents.tools import SYSTEM_PROMPT

        assert isinstance(SYSTEM_PROMPT, str)
        assert len(SYSTEM_PROMPT) > 100

    def test_system_prompt_mentions_bsl(self):
        from boring_semantic_layer.agents.tools import SYSTEM_PROMPT

        assert "BSL" in SYSTEM_PROMPT or "semantic" in SYSTEM_PROMPT.lower()


class TestBSLToolsInit:
    """Tests for BSLTools initialization."""

    @patch("boring_semantic_layer.agents.tools.from_yaml")
    def test_init_basic(self, mock_from_yaml, tmp_path, mock_models):
        from boring_semantic_layer.agents.tools import BSLTools

        mock_from_yaml.return_value = mock_models
        model_file = tmp_path / "test.yml"
        model_file.write_text("test")

        bsl = BSLTools(model_path=model_file)

        assert bsl.model_path == model_file
        assert bsl.profile is None
        assert bsl.chart_backend == "plotext"
        mock_from_yaml.assert_called_once()

    @patch("boring_semantic_layer.agents.tools.from_yaml")
    def test_init_with_profile(self, mock_from_yaml, tmp_path, mock_models):
        from boring_semantic_layer.agents.tools import BSLTools

        mock_from_yaml.return_value = mock_models
        model_file = tmp_path / "test.yml"
        model_file.write_text("test")
        profile_file = tmp_path / "profiles.yml"
        profile_file.write_text("test")

        bsl = BSLTools(
            model_path=model_file,
            profile="dev",
            profile_file=profile_file,
            chart_backend="altair",
        )

        assert bsl.profile == "dev"
        assert bsl.profile_file == profile_file
        assert bsl.chart_backend == "altair"

    @patch("boring_semantic_layer.agents.tools.from_yaml")
    def test_tools_and_system_prompt_accessible(self, mock_from_yaml, tmp_path, mock_models):
        from boring_semantic_layer.agents.tools import SYSTEM_PROMPT, TOOL_DEFINITIONS, BSLTools

        mock_from_yaml.return_value = mock_models
        model_file = tmp_path / "test.yml"
        model_file.write_text("test")

        bsl = BSLTools(model_path=model_file)

        assert bsl.tools == TOOL_DEFINITIONS
        assert bsl.system_prompt == SYSTEM_PROMPT


class TestBSLToolsExecute:
    """Tests for BSLTools.execute() method."""

    @patch("boring_semantic_layer.agents.tools.from_yaml")
    def test_execute_unknown_tool(self, mock_from_yaml, tmp_path, mock_models):
        from boring_semantic_layer.agents.tools import BSLTools

        mock_from_yaml.return_value = mock_models
        model_file = tmp_path / "test.yml"
        model_file.write_text("test")

        bsl = BSLTools(model_path=model_file)
        result = bsl.execute("unknown_tool", {})

        assert "Unknown tool" in result

    @patch("boring_semantic_layer.agents.tools.from_yaml")
    def test_execute_list_models(self, mock_from_yaml, tmp_path, mock_models):
        from boring_semantic_layer.agents.tools import BSLTools

        mock_from_yaml.return_value = mock_models
        model_file = tmp_path / "test.yml"
        model_file.write_text("test")

        bsl = BSLTools(model_path=model_file)
        result = bsl.execute("list_models", {})

        result_dict = json.loads(result)
        assert "flights" in result_dict
        # list_models now returns {model_name: description} format
        assert result_dict["flights"] == "Test flight data"

    @patch("boring_semantic_layer.agents.tools.from_yaml")
    def test_execute_get_documentation_valid_topic(self, mock_from_yaml, tmp_path, mock_models):
        from boring_semantic_layer.agents.tools import BSLTools

        mock_from_yaml.return_value = mock_models
        model_file = tmp_path / "test.yml"
        model_file.write_text("test")

        bsl = BSLTools(model_path=model_file)
        result = bsl.execute("get_documentation", {"topic": "getting-started"})

        assert len(result) > 100
        assert "❌" not in result

    @patch("boring_semantic_layer.agents.tools.from_yaml")
    def test_execute_get_documentation_invalid_topic(self, mock_from_yaml, tmp_path, mock_models):
        from langchain_core.tools import ToolException

        from boring_semantic_layer.agents.tools import BSLTools

        mock_from_yaml.return_value = mock_models
        model_file = tmp_path / "test.yml"
        model_file.write_text("test")

        bsl = BSLTools(model_path=model_file)

        with pytest.raises(ToolException) as exc_info:
            bsl._get_documentation(topic="nonexistent-topic")

        assert "Unknown topic" in str(exc_info.value)


class TestBSLToolsQueryModel:
    """Tests for BSLTools query_model execution."""

    @patch("boring_semantic_layer.agents.tools.from_yaml")
    def test_query_model_success(self, mock_from_yaml, tmp_path, mock_models):
        from boring_semantic_layer.agents.tools import BSLTools

        mock_from_yaml.return_value = mock_models
        model_file = tmp_path / "test.yml"
        model_file.write_text("test")

        bsl = BSLTools(model_path=model_file)

        with (
            patch("boring_semantic_layer.agents.tools.safe_eval") as mock_eval,
            patch("boring_semantic_layer.agents.tools.generate_chart_with_data") as mock_chart,
        ):
            mock_eval.return_value = Mock()
            mock_chart.return_value = '{"status": "success"}'

            result = bsl.execute("query_model", {"query": "flights.aggregate('count')"})

            assert "success" in result
            mock_eval.assert_called_once()
            mock_chart.assert_called_once()

    @patch("boring_semantic_layer.agents.tools.from_yaml")
    def test_query_model_with_chart_spec(self, mock_from_yaml, tmp_path, mock_models):
        from boring_semantic_layer.agents.tools import BSLTools

        mock_from_yaml.return_value = mock_models
        model_file = tmp_path / "test.yml"
        model_file.write_text("test")

        bsl = BSLTools(model_path=model_file, chart_backend="altair")

        with (
            patch("boring_semantic_layer.agents.tools.safe_eval") as mock_eval,
            patch("boring_semantic_layer.agents.tools.generate_chart_with_data") as mock_chart,
        ):
            mock_eval.return_value = Mock()
            mock_chart.return_value = '{"chart": "data"}'

            chart_spec = {"type": "bar", "x": "category"}
            bsl.execute("query_model", {"query": "test", "chart_spec": chart_spec})

            mock_chart.assert_called_once()
            call_args = mock_chart.call_args
            assert call_args.kwargs["default_backend"] == "altair"

    @patch("boring_semantic_layer.agents.tools.from_yaml")
    def test_query_model_handles_exception(self, mock_from_yaml, tmp_path, mock_models):
        from langchain_core.tools import ToolException

        from boring_semantic_layer.agents.tools import BSLTools

        mock_from_yaml.return_value = mock_models
        model_file = tmp_path / "test.yml"
        model_file.write_text("test")

        bsl = BSLTools(model_path=model_file)

        with patch("boring_semantic_layer.agents.tools.safe_eval") as mock_eval:
            mock_eval.side_effect = ValueError("Invalid query")

            with pytest.raises(ToolException) as exc_info:
                bsl._query_model(query="bad query")

            assert "Invalid query" in str(exc_info.value)

    @patch("boring_semantic_layer.agents.tools.from_yaml")
    def test_query_model_handles_failure_result(self, mock_from_yaml, tmp_path, mock_models):
        from langchain_core.tools import ToolException

        from boring_semantic_layer.agents.tools import BSLTools

        mock_from_yaml.return_value = mock_models
        model_file = tmp_path / "test.yml"
        model_file.write_text("test")

        bsl = BSLTools(model_path=model_file)

        with patch("boring_semantic_layer.agents.tools.safe_eval") as mock_eval:
            mock_eval.return_value = Failure(ValueError("Query failed"))

            with pytest.raises(ToolException) as exc_info:
                bsl._query_model(query="test")

            assert "Query failed" in str(exc_info.value)

    @patch("boring_semantic_layer.agents.tools.from_yaml")
    @patch("boring_semantic_layer.agents.tools.generate_chart_with_data")
    def test_query_model_unwraps_success_result(
        self, mock_chart, mock_from_yaml, tmp_path, mock_models
    ):
        from boring_semantic_layer.agents.tools import BSLTools

        mock_from_yaml.return_value = mock_models
        mock_chart.return_value = '{"success": true}'
        model_file = tmp_path / "test.yml"
        model_file.write_text("test")

        bsl = BSLTools(model_path=model_file)

        mock_query_result = Mock()

        with patch("boring_semantic_layer.agents.tools.safe_eval") as mock_eval:
            mock_eval.return_value = Success(mock_query_result)

            bsl._query_model(query="test")

            # Verify Success result was unwrapped and passed to chart generator
            mock_chart.assert_called_once()
            assert mock_chart.call_args[0][0] is mock_query_result

    @patch("boring_semantic_layer.agents.tools.from_yaml")
    def test_query_model_raises_tool_exception_on_error(
        self, mock_from_yaml, tmp_path, mock_models
    ):
        from langchain_core.tools import ToolException

        from boring_semantic_layer.agents.tools import BSLTools

        mock_from_yaml.return_value = mock_models
        model_file = tmp_path / "test.yml"
        model_file.write_text("test")

        bsl = BSLTools(model_path=model_file)

        error_messages = []
        bsl._error_callback = lambda msg: error_messages.append(msg)

        with patch("boring_semantic_layer.agents.tools.safe_eval") as mock_eval:
            mock_eval.side_effect = ValueError("Test error")

            with pytest.raises(ToolException) as exc_info:
                bsl._query_model(query="test")

            assert "Query Error" in str(exc_info.value)
            assert len(error_messages) == 1
            assert "Query Error" in error_messages[0]


class TestBSLToolsListModels:
    """Tests for list_models output format."""

    @patch("boring_semantic_layer.agents.tools.from_yaml")
    def test_list_models_json_format(self, mock_from_yaml, tmp_path, mock_models):
        from boring_semantic_layer.agents.tools import BSLTools

        mock_from_yaml.return_value = mock_models
        model_file = tmp_path / "test.yml"
        model_file.write_text("test")

        bsl = BSLTools(model_path=model_file)
        result = bsl.execute("list_models", {})

        # Should be valid JSON
        data = json.loads(result)
        assert isinstance(data, dict)

    @patch("boring_semantic_layer.agents.tools.from_yaml")
    def test_list_models_includes_description(self, mock_from_yaml, tmp_path, mock_models):
        from boring_semantic_layer.agents.tools import BSLTools

        mock_from_yaml.return_value = mock_models
        model_file = tmp_path / "test.yml"
        model_file.write_text("test")

        bsl = BSLTools(model_path=model_file)
        result = bsl.execute("list_models", {})

        data = json.loads(result)
        # list_models returns {model_name: description} format
        assert data["flights"] == "Test flight data"

    @patch("boring_semantic_layer.agents.tools.from_yaml")
    def test_list_models_without_description(self, mock_from_yaml, tmp_path):
        from boring_semantic_layer.agents.tools import BSLTools

        mock_model = Mock()
        mock_model.dimensions = {"col1"}
        mock_model.measures = {"count"}
        mock_model.description = None  # No description

        mock_from_yaml.return_value = {"test": mock_model}
        model_file = tmp_path / "test.yml"
        model_file.write_text("test")

        bsl = BSLTools(model_path=model_file)
        result = bsl.execute("list_models", {})

        data = json.loads(result)
        # When no description, falls back to default "Semantic model: {name}"
        assert data["test"] == "Semantic model: test"


class TestGetMdDir:
    """Tests for _get_md_dir function."""

    def test_get_md_dir_returns_path(self):
        from boring_semantic_layer.agents.tools import _get_md_dir

        result = _get_md_dir()
        assert isinstance(result, Path)

    def test_get_md_dir_contains_required_files(self):
        from boring_semantic_layer.agents.tools import _get_md_dir

        md_dir = _get_md_dir()
        assert (md_dir / "index.json").exists()
        assert (md_dir / "prompts").exists()
        assert (md_dir / "doc").exists()


class TestQueryModelExplicitParams:
    """Tests for query_model with explicit chart/record parameters."""

    @patch("boring_semantic_layer.agents.tools.from_yaml")
    def test_query_model_get_records_param(self, mock_from_yaml, tmp_path, mock_models):
        """Test that get_records param is passed to generate_chart_with_data."""
        from boring_semantic_layer.agents.tools import BSLTools

        mock_from_yaml.return_value = mock_models
        model_file = tmp_path / "test.yml"
        model_file.write_text("test")

        bsl = BSLTools(model_path=model_file)

        with (
            patch("boring_semantic_layer.agents.tools.safe_eval") as mock_eval,
            patch("boring_semantic_layer.agents.tools.generate_chart_with_data") as mock_chart,
        ):
            mock_eval.return_value = Mock()
            mock_chart.return_value = '{"data": []}'

            bsl.execute("query_model", {"query": "test", "get_records": False})

            mock_chart.assert_called_once()
            call_kwargs = mock_chart.call_args.kwargs
            assert call_kwargs["get_records"] is False

    @patch("boring_semantic_layer.agents.tools.from_yaml")
    def test_query_model_records_limit_param(self, mock_from_yaml, tmp_path, mock_models):
        """Test that records_limit param is passed to generate_chart_with_data."""
        from boring_semantic_layer.agents.tools import BSLTools

        mock_from_yaml.return_value = mock_models
        model_file = tmp_path / "test.yml"
        model_file.write_text("test")

        bsl = BSLTools(model_path=model_file)

        with (
            patch("boring_semantic_layer.agents.tools.safe_eval") as mock_eval,
            patch("boring_semantic_layer.agents.tools.generate_chart_with_data") as mock_chart,
        ):
            mock_eval.return_value = Mock()
            mock_chart.return_value = '{"data": []}'

            bsl.execute("query_model", {"query": "test", "records_limit": 50})

            mock_chart.assert_called_once()
            call_kwargs = mock_chart.call_args.kwargs
            assert call_kwargs["records_limit"] == 50

    @patch("boring_semantic_layer.agents.tools.from_yaml")
    def test_query_model_get_chart_false(self, mock_from_yaml, tmp_path, mock_models):
        """Test that get_chart=False disables chart generation."""
        from boring_semantic_layer.agents.tools import BSLTools

        mock_from_yaml.return_value = mock_models
        model_file = tmp_path / "test.yml"
        model_file.write_text("test")

        bsl = BSLTools(model_path=model_file)

        with (
            patch("boring_semantic_layer.agents.tools.safe_eval") as mock_eval,
            patch("boring_semantic_layer.agents.tools.generate_chart_with_data") as mock_chart,
        ):
            mock_eval.return_value = Mock()
            mock_chart.return_value = '{"data": []}'

            bsl.execute("query_model", {"query": "test", "get_chart": False})

            mock_chart.assert_called_once()
            call_kwargs = mock_chart.call_args.kwargs
            assert call_kwargs["get_chart"] is False

    @patch("boring_semantic_layer.agents.tools.from_yaml")
    def test_query_model_chart_backend_override(self, mock_from_yaml, tmp_path, mock_models):
        """Test that chart_backend param overrides default backend."""
        from boring_semantic_layer.agents.tools import BSLTools

        mock_from_yaml.return_value = mock_models
        model_file = tmp_path / "test.yml"
        model_file.write_text("test")

        # Initialize with plotext as default
        bsl = BSLTools(model_path=model_file, chart_backend="plotext")

        with (
            patch("boring_semantic_layer.agents.tools.safe_eval") as mock_eval,
            patch("boring_semantic_layer.agents.tools.generate_chart_with_data") as mock_chart,
        ):
            mock_eval.return_value = Mock()
            mock_chart.return_value = '{"chart": "data"}'

            # Override with altair at query time
            bsl.execute("query_model", {"query": "test", "chart_backend": "altair"})

            mock_chart.assert_called_once()
            call_kwargs = mock_chart.call_args.kwargs
            assert call_kwargs["chart_backend"] == "altair"

    @patch("boring_semantic_layer.agents.tools.from_yaml")
    def test_query_model_chart_format_param(self, mock_from_yaml, tmp_path, mock_models):
        """Test that chart_format param is passed to generate_chart_with_data."""
        from boring_semantic_layer.agents.tools import BSLTools

        mock_from_yaml.return_value = mock_models
        model_file = tmp_path / "test.yml"
        model_file.write_text("test")

        bsl = BSLTools(model_path=model_file)

        with (
            patch("boring_semantic_layer.agents.tools.safe_eval") as mock_eval,
            patch("boring_semantic_layer.agents.tools.generate_chart_with_data") as mock_chart,
        ):
            mock_eval.return_value = Mock()
            mock_chart.return_value = '{"chart": "data"}'

            bsl.execute("query_model", {"query": "test", "chart_format": "json"})

            mock_chart.assert_called_once()
            call_kwargs = mock_chart.call_args.kwargs
            assert call_kwargs["chart_format"] == "json"

    @patch("boring_semantic_layer.agents.tools.from_yaml")
    def test_query_model_all_params_combined(self, mock_from_yaml, tmp_path, mock_models):
        """Test query_model with all explicit params together."""
        from boring_semantic_layer.agents.tools import BSLTools

        mock_from_yaml.return_value = mock_models
        model_file = tmp_path / "test.yml"
        model_file.write_text("test")

        bsl = BSLTools(model_path=model_file)

        with (
            patch("boring_semantic_layer.agents.tools.safe_eval") as mock_eval,
            patch("boring_semantic_layer.agents.tools.generate_chart_with_data") as mock_chart,
        ):
            mock_eval.return_value = Mock()
            mock_chart.return_value = '{"full": "response"}'

            bsl.execute(
                "query_model",
                {
                    "query": "flights.aggregate('count')",
                    "get_records": True,
                    "records_limit": 25,
                    "get_chart": True,
                    "chart_backend": "plotly",
                    "chart_format": "json",
                    "chart_spec": {"chart_type": "bar"},
                },
            )

            mock_chart.assert_called_once()
            call_kwargs = mock_chart.call_args.kwargs
            assert call_kwargs["get_records"] is True
            assert call_kwargs["records_limit"] == 25
            assert call_kwargs["get_chart"] is True
            assert call_kwargs["chart_backend"] == "plotly"
            assert call_kwargs["chart_format"] == "json"
            assert call_kwargs["chart_spec"] == {"chart_type": "bar"}

    @patch("boring_semantic_layer.agents.tools.from_yaml")
    def test_query_model_default_params_use_instance_backend(
        self, mock_from_yaml, tmp_path, mock_models
    ):
        """Test that default backend comes from BSLTools instance."""
        from boring_semantic_layer.agents.tools import BSLTools

        mock_from_yaml.return_value = mock_models
        model_file = tmp_path / "test.yml"
        model_file.write_text("test")

        bsl = BSLTools(model_path=model_file, chart_backend="altair")

        with (
            patch("boring_semantic_layer.agents.tools.safe_eval") as mock_eval,
            patch("boring_semantic_layer.agents.tools.generate_chart_with_data") as mock_chart,
        ):
            mock_eval.return_value = Mock()
            mock_chart.return_value = '{"chart": "data"}'

            # No chart_backend in params
            bsl.execute("query_model", {"query": "test"})

            mock_chart.assert_called_once()
            call_kwargs = mock_chart.call_args.kwargs
            assert call_kwargs["default_backend"] == "altair"
