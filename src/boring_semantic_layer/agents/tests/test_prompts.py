"""Unit tests for prompts utility module."""


class TestLoadPrompt:
    """Tests for load_prompt function."""

    def test_load_existing_file(self, tmp_path):
        """Test loading a file that exists."""
        from boring_semantic_layer.agents.utils.prompts import load_prompt

        # Create a test file
        test_file = tmp_path / "test_prompt.md"
        test_file.write_text("# Test Prompt\n\nThis is a test prompt.")

        result = load_prompt(tmp_path, "test_prompt.md")

        assert result == "# Test Prompt\n\nThis is a test prompt."

    def test_load_nonexistent_file(self, tmp_path):
        """Test loading a file that doesn't exist returns empty string."""
        from boring_semantic_layer.agents.utils.prompts import load_prompt

        result = load_prompt(tmp_path, "nonexistent.md")

        assert result == ""

    def test_load_strips_whitespace(self, tmp_path):
        """Test that loaded content is stripped of leading/trailing whitespace."""
        from boring_semantic_layer.agents.utils.prompts import load_prompt

        test_file = tmp_path / "whitespace.md"
        test_file.write_text("\n\n  Content with whitespace  \n\n")

        result = load_prompt(tmp_path, "whitespace.md")

        assert result == "Content with whitespace"

    def test_load_empty_file(self, tmp_path):
        """Test loading an empty file returns empty string."""
        from boring_semantic_layer.agents.utils.prompts import load_prompt

        test_file = tmp_path / "empty.md"
        test_file.write_text("")

        result = load_prompt(tmp_path, "empty.md")

        assert result == ""

    def test_load_whitespace_only_file(self, tmp_path):
        """Test loading a file with only whitespace returns empty string."""
        from boring_semantic_layer.agents.utils.prompts import load_prompt

        test_file = tmp_path / "whitespace_only.md"
        test_file.write_text("   \n\n   ")

        result = load_prompt(tmp_path, "whitespace_only.md")

        assert result == ""

    def test_load_with_unicode(self, tmp_path):
        """Test loading a file with unicode characters."""
        from boring_semantic_layer.agents.utils.prompts import load_prompt

        test_file = tmp_path / "unicode.md"
        test_file.write_text("# Test\n\nUnicode: \u2713 \u2717 \U0001f4ca")

        result = load_prompt(tmp_path, "unicode.md")

        assert "\u2713" in result
        assert "\U0001f4ca" in result

    def test_load_nested_path(self, tmp_path):
        """Test loading from nested directory structure."""
        from boring_semantic_layer.agents.utils.prompts import load_prompt

        nested_dir = tmp_path / "subdir"
        nested_dir.mkdir()
        test_file = nested_dir / "nested.md"
        test_file.write_text("Nested content")

        result = load_prompt(nested_dir, "nested.md")

        assert result == "Nested content"

    def test_load_preserves_internal_whitespace(self, tmp_path):
        """Test that internal whitespace is preserved."""
        from boring_semantic_layer.agents.utils.prompts import load_prompt

        test_file = tmp_path / "internal.md"
        test_file.write_text("Line 1\n\nLine 2\n\n\nLine 3")

        result = load_prompt(tmp_path, "internal.md")

        assert result == "Line 1\n\nLine 2\n\n\nLine 3"
