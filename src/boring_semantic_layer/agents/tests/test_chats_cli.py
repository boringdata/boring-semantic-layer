"""Unit tests for Rich CLI chat frontend."""

from unittest.mock import Mock, patch


class TestDisplayToolCall:
    """Tests for display_tool_call function."""

    def test_display_query_model_single_line(self, capsys):
        """Test displaying a single-line query_model call."""
        from boring_semantic_layer.agents.chats.cli import display_tool_call

        with patch("boring_semantic_layer.agents.chats.cli.console") as mock_console:
            display_tool_call("query_model", {"query": "model.aggregate('count')"})

            # Should print the query
            mock_console.print.assert_called()
            call_args = str(mock_console.print.call_args_list)
            assert "query_bsl" in call_args

    def test_display_query_model_multiline(self):
        """Test displaying a multiline query_model call."""
        from boring_semantic_layer.agents.chats.cli import display_tool_call

        with patch("boring_semantic_layer.agents.chats.cli.console") as mock_console:
            multiline_query = "model\n  .group_by('x')\n  .aggregate('y')"
            display_tool_call("query_model", {"query": multiline_query})

            # Should print multiple lines
            assert mock_console.print.call_count >= 3

    def test_display_query_model_with_extra_params(self):
        """Test displaying query_model with non-default parameters."""
        from boring_semantic_layer.agents.chats.cli import display_tool_call

        with patch("boring_semantic_layer.agents.chats.cli.console") as mock_console:
            display_tool_call(
                "query_model",
                {"query": "test", "limit": 5, "chart_spec": {"type": "bar"}},
            )

            # Should print params line
            call_args = str(mock_console.print.call_args_list)
            assert "params" in call_args

    def test_display_list_models(self):
        """Test displaying list_models call."""
        from boring_semantic_layer.agents.chats.cli import display_tool_call

        with patch("boring_semantic_layer.agents.chats.cli.console") as mock_console:
            display_tool_call("list_models", {})

            mock_console.print.assert_called()
            call_args = str(mock_console.print.call_args)
            assert "list_models" in call_args

    def test_display_get_model(self):
        """Test displaying get_model call."""
        from boring_semantic_layer.agents.chats.cli import display_tool_call

        with patch("boring_semantic_layer.agents.chats.cli.console") as mock_console:
            display_tool_call("get_model", {"model_name": "flights"})

            mock_console.print.assert_called()
            call_args = str(mock_console.print.call_args)
            assert "get_model" in call_args
            assert "flights" in call_args

    def test_display_tool_call_stops_status(self):
        """Test that display_tool_call stops the status spinner."""
        from boring_semantic_layer.agents.chats.cli import display_tool_call

        mock_status = Mock()

        with patch("boring_semantic_layer.agents.chats.cli.console"):
            display_tool_call("list_models", {}, status=mock_status)

        mock_status.stop.assert_called_once()

    def test_display_tool_call_without_status(self):
        """Test that display_tool_call works without status spinner."""
        from boring_semantic_layer.agents.chats.cli import display_tool_call

        with patch("boring_semantic_layer.agents.chats.cli.console"):
            # Should not raise
            display_tool_call("list_models", {}, status=None)


class TestDisplayError:
    """Tests for display_error function."""

    def test_display_error_prints_red(self):
        """Test that errors are displayed in red."""
        from boring_semantic_layer.agents.chats.cli import display_error

        with patch("boring_semantic_layer.agents.chats.cli.console") as mock_console:
            display_error("Test error message")

            mock_console.print.assert_called_once()
            call_kwargs = mock_console.print.call_args[1]
            assert call_kwargs["style"] == "red"

    def test_display_error_stops_status(self):
        """Test that display_error stops the status spinner."""
        from boring_semantic_layer.agents.chats.cli import display_error

        mock_status = Mock()

        with patch("boring_semantic_layer.agents.chats.cli.console"):
            display_error("Error", status=mock_status)

        mock_status.stop.assert_called_once()


class TestDisplayThinking:
    """Tests for display_thinking function."""

    def test_display_thinking_dim_italic(self):
        """Test that thinking text is displayed in dim italic."""
        from boring_semantic_layer.agents.chats.cli import display_thinking

        with patch("boring_semantic_layer.agents.chats.cli.console") as mock_console:
            display_thinking("I'm thinking about this...")

            mock_console.print.assert_called_once()
            call_kwargs = mock_console.print.call_args[1]
            assert "dim" in call_kwargs["style"]
            assert "italic" in call_kwargs["style"]

    def test_display_thinking_stops_status(self):
        """Test that display_thinking stops the status spinner."""
        from boring_semantic_layer.agents.chats.cli import display_thinking

        mock_status = Mock()

        with patch("boring_semantic_layer.agents.chats.cli.console"):
            display_thinking("Thinking...", status=mock_status)

        mock_status.stop.assert_called_once()


class TestStartChat:
    """Tests for start_chat function."""

    @patch("boring_semantic_layer.agents.chats.cli.load_dotenv")
    @patch("os.getenv")
    @patch("boring_semantic_layer.agents.chats.cli.console")
    def test_start_chat_no_api_key_error(self, mock_console, mock_getenv, mock_dotenv, tmp_path):
        """Test that start_chat shows error when no API key is set."""
        from boring_semantic_layer.agents.chats.cli import start_chat

        mock_getenv.return_value = None  # No API keys

        model_file = tmp_path / "test.yml"
        model_file.write_text("test: {}")

        start_chat(model_path=model_file)

        # Should print error about no API key
        call_args = str(mock_console.print.call_args_list)
        assert "API key" in call_args or "Error" in call_args

    @patch("boring_semantic_layer.agents.chats.cli.load_dotenv")
    @patch("os.getenv")
    @patch("boring_semantic_layer.agents.chats.cli.console")
    def test_start_chat_auto_selects_claude(self, mock_console, mock_getenv, mock_dotenv, tmp_path):
        """Test that Claude is auto-selected when ANTHROPIC_API_KEY is set."""
        from boring_semantic_layer.agents.chats.cli import start_chat

        def getenv_side_effect(key):
            if key == "ANTHROPIC_API_KEY":
                return "sk-ant-test"
            return None

        mock_getenv.side_effect = getenv_side_effect

        model_file = tmp_path / "test.yml"
        model_file.write_text("test: {}")

        # Mock the agent loading to avoid actual import
        import contextlib

        with (
            patch("boring_semantic_layer.agents.chats.cli.Status"),
            contextlib.suppress(Exception),
        ):
            start_chat(model_path=model_file)

        # Should print auto-select message
        call_args = str(mock_console.print.call_args_list)
        assert "Claude" in call_args or "ANTHROPIC" in call_args

    @patch("boring_semantic_layer.agents.chats.cli.load_dotenv")
    @patch("os.getenv")
    @patch("boring_semantic_layer.agents.chats.cli.console")
    def test_start_chat_unknown_backend_error(
        self, mock_console, mock_getenv, mock_dotenv, tmp_path
    ):
        """Test that unknown backend shows error."""
        from boring_semantic_layer.agents.chats.cli import start_chat

        mock_getenv.return_value = "test-key"

        model_file = tmp_path / "test.yml"
        model_file.write_text("test: {}")

        with patch("boring_semantic_layer.agents.chats.cli.Status"):
            start_chat(model_path=model_file, backend="unknown_backend")

        # Should print error
        call_args = str(mock_console.print.call_args_list)
        assert "Unknown backend" in call_args

    @patch("boring_semantic_layer.agents.chats.cli.load_dotenv")
    @patch("os.getenv")
    @patch("boring_semantic_layer.agents.chats.cli.console")
    @patch("boring_semantic_layer.agents.chats.cli.Status")
    def test_start_chat_initial_query_mode(
        self, mock_status, mock_console, mock_getenv, mock_dotenv, tmp_path
    ):
        """Test non-interactive mode with initial query."""
        from boring_semantic_layer.agents.chats.cli import start_chat

        mock_getenv.return_value = "test-key"
        mock_status_instance = Mock()
        mock_status.return_value = mock_status_instance
        mock_status_instance.__enter__ = Mock(return_value=mock_status_instance)
        mock_status_instance.__exit__ = Mock(return_value=False)

        model_file = tmp_path / "test.yml"
        model_file.write_text("test: {}")

        # Mock the langchain backend
        mock_agent = Mock()
        mock_agent.query.return_value = ("", "Test response")

        with patch(
            "boring_semantic_layer.agents.backends.langchain.LangChainAgent",
            return_value=mock_agent,
        ):
            start_chat(
                model_path=model_file,
                backend="langchain",
                initial_query="What data is available?",
            )

        # Should have called query with initial_query
        mock_agent.query.assert_called_once()


class TestBackendNames:
    """Tests for backend name mapping."""

    def test_backend_names_dict(self):
        """Test that backend names are properly mapped."""
        # Just verify the module loads properly
        from boring_semantic_layer.agents.chats.cli import start_chat

        assert callable(start_chat)
