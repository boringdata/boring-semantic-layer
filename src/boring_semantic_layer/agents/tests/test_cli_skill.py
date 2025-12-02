"""Tests for CLI skill commands."""

import re
import subprocess
from argparse import Namespace
from pathlib import Path

import pytest

from boring_semantic_layer.agents.cli import (
    TOOL_CONFIGS,
    _discover_skills_for_tool,
    _get_skills_dir,
    cmd_skill_install,
    cmd_skill_list,
    cmd_skill_show,
)


class TestGetSkillsDir:
    """Tests for _get_skills_dir function."""

    def test_returns_path(self):
        """Test that _get_skills_dir returns a Path."""
        result = _get_skills_dir()
        assert isinstance(result, Path)

    def test_skills_dir_exists(self):
        """Test that the skills directory exists."""
        result = _get_skills_dir()
        assert result.exists(), f"Skills directory does not exist: {result}"

    def test_skills_dir_contains_expected_subdirs(self):
        """Test that skills dir contains expected tool subdirectories."""
        skills_dir = _get_skills_dir()
        assert (skills_dir / "claude-code").exists()
        assert (skills_dir / "cursor").exists()
        assert (skills_dir / "codex").exists()


class TestDiscoverSkillsForTool:
    """Tests for _discover_skills_for_tool function."""

    def test_returns_empty_for_unknown_tool(self):
        """Test that unknown tool returns empty list."""
        result = _discover_skills_for_tool("unknown-tool")
        assert result == []

    def test_returns_skills_for_known_tools(self):
        """Test that known tools return skill lists."""
        for tool in TOOL_CONFIGS:
            result = _discover_skills_for_tool(tool)
            assert isinstance(result, list)
            assert len(result) > 0, f"No skills found for {tool}"

    def test_skills_have_required_keys(self):
        """Test that each skill has required keys."""
        required_keys = {"name", "source", "target"}
        for tool in TOOL_CONFIGS:
            skills = _discover_skills_for_tool(tool)
            for skill in skills:
                assert required_keys.issubset(skill.keys()), f"Skill missing keys: {skill}"

    def test_source_files_exist(self):
        """Test that all skill source files exist."""
        for tool in TOOL_CONFIGS:
            skills = _discover_skills_for_tool(tool)
            for skill in skills:
                assert skill["source"].exists(), f"Skill file missing: {skill['source']}"


class TestCmdSkillList:
    """Tests for cmd_skill_list function."""

    def test_lists_all_tools(self, capsys):
        """Test that skill list shows all configured tools."""
        args = Namespace()
        cmd_skill_list(args)

        captured = capsys.readouterr()
        for tool in TOOL_CONFIGS:
            assert tool in captured.out

    def test_shows_checkmarks_for_existing_skills(self, capsys):
        """Test that existing skills show checkmarks."""
        args = Namespace()
        cmd_skill_list(args)

        captured = capsys.readouterr()
        # All skills should exist and show checkmarks
        assert "✓" in captured.out


class TestCmdSkillShow:
    """Tests for cmd_skill_show function."""

    def test_shows_skill_content(self, capsys):
        """Test that skill content is displayed."""
        args = Namespace(tool="claude-code")
        cmd_skill_show(args)

        captured = capsys.readouterr()
        # Should contain skill content markers
        assert "BSL Query Expert" in captured.out
        assert "group_by" in captured.out

    def test_shows_header_info(self, capsys):
        """Test that header info is displayed."""
        args = Namespace(tool="cursor")
        cmd_skill_show(args)

        captured = capsys.readouterr()
        assert "Skill:" in captured.out
        assert "Source:" in captured.out
        assert "Target:" in captured.out

    def test_unknown_tool_shows_error(self, capsys):
        """Test that unknown tool shows error message."""
        args = Namespace(tool="unknown")
        cmd_skill_show(args)

        captured = capsys.readouterr()
        assert "Unknown tool" in captured.out


class TestCmdSkillInstall:
    """Tests for cmd_skill_install function."""

    def test_installs_skill_to_target(self, tmp_path, capsys, monkeypatch):
        """Test that skill is installed to target location."""
        # Change to temp directory
        monkeypatch.chdir(tmp_path)

        args = Namespace(tool="claude-code", force=False)
        cmd_skill_install(args)

        captured = capsys.readouterr()
        assert "✅ Installed" in captured.out

        # Verify file was created
        target_path = tmp_path / ".claude" / "skills" / "bsl-query-expert" / "SKILL.md"
        assert target_path.exists()

        # Verify content
        content = target_path.read_text()
        assert "BSL Query Expert" in content

    def test_creates_parent_directories(self, tmp_path, monkeypatch):
        """Test that parent directories are created."""
        monkeypatch.chdir(tmp_path)

        args = Namespace(tool="claude-code", force=False)
        cmd_skill_install(args)

        # Should have created nested directories
        assert (tmp_path / ".claude" / "skills" / "bsl-query-expert").exists()

    def test_warns_if_file_exists(self, tmp_path, capsys, monkeypatch):
        """Test that existing file triggers warning."""
        monkeypatch.chdir(tmp_path)

        # Create target file first
        target_path = tmp_path / ".cursor" / "rules" / "bsl-query-expert.mdc"
        target_path.parent.mkdir(parents=True, exist_ok=True)
        target_path.write_text("existing content")

        args = Namespace(tool="cursor", force=False)
        cmd_skill_install(args)

        captured = capsys.readouterr()
        assert "already exists" in captured.out
        assert "--force" in captured.out

        # Original content should be preserved
        assert target_path.read_text() == "existing content"

    def test_force_overwrites_existing(self, tmp_path, capsys, monkeypatch):
        """Test that --force overwrites existing file."""
        monkeypatch.chdir(tmp_path)

        # Create target file first
        target_path = tmp_path / ".cursor" / "rules" / "bsl-query-expert.mdc"
        target_path.parent.mkdir(parents=True, exist_ok=True)
        target_path.write_text("existing content")

        args = Namespace(tool="cursor", force=True)
        cmd_skill_install(args)

        captured = capsys.readouterr()
        assert "✅ Installed" in captured.out

        # Content should be replaced
        assert "BSL Query Expert" in target_path.read_text()

    def test_unknown_tool_shows_error(self, tmp_path, capsys, monkeypatch):
        """Test that unknown tool shows error."""
        monkeypatch.chdir(tmp_path)

        args = Namespace(tool="unknown", force=False)
        cmd_skill_install(args)

        captured = capsys.readouterr()
        assert "Unknown tool" in captured.out

    def test_installs_cursor_skill(self, tmp_path, capsys, monkeypatch):
        """Test installing cursor skill."""
        monkeypatch.chdir(tmp_path)

        args = Namespace(tool="cursor", force=False)
        cmd_skill_install(args)

        target_path = tmp_path / ".cursor" / "rules" / "bsl-query-expert.mdc"
        assert target_path.exists()
        assert "BSL Query Expert" in target_path.read_text()

    def test_installs_codex_skill(self, tmp_path, capsys, monkeypatch):
        """Test installing codex skill."""
        monkeypatch.chdir(tmp_path)

        args = Namespace(tool="codex", force=False)
        cmd_skill_install(args)

        target_path = tmp_path / ".codex" / "bsl-query-expert.codex"
        assert target_path.exists()
        assert "BSL Query Expert" in target_path.read_text()


class TestToolConfigs:
    """Tests for TOOL_CONFIGS constant."""

    def test_all_tools_have_required_keys(self):
        """Test that all tool configs have required keys."""
        required_keys = {"target_pattern", "description"}
        for tool, config in TOOL_CONFIGS.items():
            assert required_keys.issubset(config.keys()), f"{tool} missing keys"

    def test_expected_tools_configured(self):
        """Test that expected tools are configured."""
        expected_tools = {"claude-code", "cursor", "codex"}
        assert expected_tools == set(TOOL_CONFIGS.keys())


class TestSkillDocURLs:
    """Tests that SKILL files reference documentation URLs correctly."""

    def test_skill_additional_info_has_urls(self):
        """Test that skills contain GitHub URLs for documentation."""
        skills_dir = _get_skills_dir()
        skill_file = skills_dir / "claude-code" / "bsl-query-expert" / "SKILL.md"
        skill_content = skill_file.read_text()

        # Should contain GitHub URLs
        assert "https://github.com/boringdata/boring-semantic-layer" in skill_content
        assert "## Additional Information" in skill_content
        assert "URL:" in skill_content

    def test_skill_urls_point_to_valid_docs(self):
        """Test that URLs in skills point to expected doc locations."""
        skills_dir = _get_skills_dir()
        skill_file = skills_dir / "claude-code" / "bsl-query-expert" / "SKILL.md"
        skill_content = skill_file.read_text()

        # Extract URLs from skill file
        url_pattern = (
            r"URL: (https://github\.com/boringdata/boring-semantic-layer/blob/main/docs/md/[^\s]+)"
        )
        urls = re.findall(url_pattern, skill_content)

        assert len(urls) > 0, "No documentation URLs found in skill file"

        # Verify URLs have expected format
        for url in urls:
            assert url.endswith(".md"), f"URL should point to markdown file: {url}"


@pytest.mark.slow
class TestSkillInstallIntegration:
    """Integration tests that create a real project and install skills."""

    def test_install_skills_in_new_project(self, tmp_path):
        """Test installing skills in a fresh uv project with BSL installed from local path."""
        project_dir = tmp_path / "test-project"
        project_dir.mkdir()

        # Get the BSL source directory (parent of src/boring_semantic_layer)
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

        # Test skill list
        result = subprocess.run(
            ["uv", "run", "bsl", "skill", "list"],
            cwd=project_dir,
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0, f"bsl skill list failed: {result.stderr}"
        assert "claude-code" in result.stdout
        assert "cursor" in result.stdout
        assert "codex" in result.stdout

        # Install claude-code skills
        result = subprocess.run(
            ["uv", "run", "bsl", "skill", "install", "claude-code"],
            cwd=project_dir,
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0, f"bsl skill install claude-code failed: {result.stderr}"
        assert "Installed" in result.stdout

        # Verify skill files were created
        skill_file = project_dir / ".claude" / "skills" / "bsl-query-expert" / "SKILL.md"
        assert skill_file.exists(), "Claude Code skill file not created"

        skill_content = skill_file.read_text()
        assert "BSL Query Expert" in skill_content
        assert "semantic models" in skill_content.lower()  # Verify skill has relevant content

        # Install cursor skills
        result = subprocess.run(
            ["uv", "run", "bsl", "skill", "install", "cursor"],
            cwd=project_dir,
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0, f"bsl skill install cursor failed: {result.stderr}"

        cursor_file = project_dir / ".cursor" / "rules" / "bsl-query-expert.mdc"
        assert cursor_file.exists(), "Cursor skill file not created"

    def test_skill_install_idempotent(self, tmp_path):
        """Test that running skill install twice doesn't fail."""
        project_dir = tmp_path / "test-project-2"
        project_dir.mkdir()

        bsl_source = Path(__file__).parent.parent.parent.parent.parent

        # Initialize and add BSL
        subprocess.run(["uv", "init"], cwd=project_dir, capture_output=True)
        subprocess.run(
            ["uv", "add", f"boring-semantic-layer[agent] @ {bsl_source}"],
            cwd=project_dir,
            capture_output=True,
        )

        # Install twice
        result1 = subprocess.run(
            ["uv", "run", "bsl", "skill", "install", "claude-code"],
            cwd=project_dir,
            capture_output=True,
            text=True,
        )
        assert result1.returncode == 0

        result2 = subprocess.run(
            ["uv", "run", "bsl", "skill", "install", "claude-code"],
            cwd=project_dir,
            capture_output=True,
            text=True,
        )
        assert result2.returncode == 0
        assert "already exists" in result2.stdout or "Skipped" in result2.stdout
