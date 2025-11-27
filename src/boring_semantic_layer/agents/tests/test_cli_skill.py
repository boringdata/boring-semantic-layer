"""Tests for CLI skill commands."""

import json
import re
from argparse import Namespace
from pathlib import Path

from boring_semantic_layer.agents.cli import (
    TOOL_CONFIGS,
    _discover_skills_for_tool,
    _get_doc_files_from_index,
    _get_md_dir,
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


class TestGetDocFilesFromIndex:
    """Tests for _get_doc_files_from_index function."""

    def test_returns_list(self):
        """Test that _get_doc_files_from_index returns a list."""
        result = _get_doc_files_from_index()
        assert isinstance(result, list)

    def test_doc_files_exist(self):
        """Test that all doc files from index exist."""
        doc_files = _get_doc_files_from_index()
        for doc_file in doc_files:
            assert doc_file["source"].exists(), f"Doc file missing: {doc_file['source']}"

    def test_doc_files_have_required_keys(self):
        """Test that each doc file has required keys."""
        required_keys = {"topic_id", "source", "relative_path"}
        doc_files = _get_doc_files_from_index()
        for doc_file in doc_files:
            assert required_keys.issubset(doc_file.keys()), f"Doc file missing keys: {doc_file}"

    def test_index_json_exists(self):
        """Test that index.json exists."""
        md_dir = _get_md_dir()
        index_path = md_dir / "index.json"
        assert index_path.exists(), f"index.json not found at {index_path}"


class TestSkillDocPathsMatch:
    """Tests that SKILL files reference documentation paths that exist after install."""

    def test_skill_additional_info_paths_match_index(self):
        """Test that paths in SKILL files match those in index.json."""
        md_dir = _get_md_dir()
        index_path = md_dir / "index.json"
        index = json.loads(index_path.read_text())
        topics = index.get("topics", {})

        # Read the generated SKILL file
        skills_dir = _get_skills_dir()
        skill_file = skills_dir / "claude-code" / "bsl-query-expert" / "SKILL.md"
        skill_content = skill_file.read_text()

        # Extract paths from SKILL file (format: `docs/path/to/file.md`)
        path_pattern = r"Path: `(docs/[^`]+)`"
        additional_info_section = skill_content.split("## Additional Information")[-1]
        skill_paths = re.findall(path_pattern, additional_info_section)

        # Verify each path corresponds to an entry in index.json
        for skill_path in skill_paths:
            # Extract the relative path (remove docs/ prefix)
            relative_path = skill_path.replace("docs/", "")

            # Find topic with matching source
            found = False
            for _topic_id, topic_info in topics.items():
                if topic_info.get("source") == relative_path:
                    found = True
                    break

            assert found, f"Path '{skill_path}' in SKILL file not found in index.json"

        # Verify all index topics have paths in skill file
        for topic_id, topic_info in topics.items():
            source = topic_info.get("source", "")
            expected_path = f"docs/{source}"
            assert expected_path in skill_content, (
                f"Topic '{topic_id}' with path '{expected_path}' not found in SKILL file"
            )

    def test_install_creates_docs_matching_skill_paths(self, tmp_path, monkeypatch):
        """Test that installed docs match paths referenced in SKILL file."""
        monkeypatch.chdir(tmp_path)

        # Install skills and docs
        args = Namespace(tool="claude-code", force=False)
        cmd_skill_install(args)

        # Read installed SKILL file
        skill_dir = tmp_path / ".claude" / "skills" / "bsl-query-expert"
        skill_file = skill_dir / "SKILL.md"
        skill_content = skill_file.read_text()

        # Extract paths from Additional Information section
        additional_info = skill_content.split("## Additional Information")[-1]
        path_pattern = r"Path: `(docs/[^`]+)`"
        referenced_paths = re.findall(path_pattern, additional_info)

        # Verify each referenced path exists relative to skill folder
        for path in referenced_paths:
            installed_doc = skill_dir / path
            assert installed_doc.exists(), (
                f"SKILL file references path '{path}' but file not installed at {installed_doc}"
            )

    def test_all_index_topics_have_source_files(self):
        """Test that all topics in index.json have valid source files."""
        md_dir = _get_md_dir()
        index_path = md_dir / "index.json"
        index = json.loads(index_path.read_text())
        topics = index.get("topics", {})

        for topic_id, topic_info in topics.items():
            source_path = topic_info.get("source", "")
            assert source_path, f"Topic '{topic_id}' missing 'source' field"

            full_path = md_dir / source_path
            assert full_path.exists(), (
                f"Topic '{topic_id}' references non-existent file: {full_path}"
            )

    def test_install_creates_all_docs_from_index(self, tmp_path, monkeypatch):
        """Test that install creates all doc files listed in index.json in each skill folder."""
        monkeypatch.chdir(tmp_path)

        # Install skills and docs
        args = Namespace(tool="claude-code", force=False)
        cmd_skill_install(args)

        # Get expected docs from index
        md_dir = _get_md_dir()
        index_path = md_dir / "index.json"
        index = json.loads(index_path.read_text())
        topics = index.get("topics", {})

        # Check docs in first skill folder
        skill_dir = tmp_path / ".claude" / "skills" / "bsl-query-expert"
        docs_base = skill_dir / "docs"

        for topic_id, topic_info in topics.items():
            source_path = topic_info.get("source", "")
            if source_path:
                installed_doc = docs_base / source_path
                assert installed_doc.exists(), (
                    f"Doc file for topic '{topic_id}' not installed: {installed_doc}"
                )

    def test_installed_docs_content_matches_source(self, tmp_path, monkeypatch):
        """Test that installed doc content matches source files."""
        monkeypatch.chdir(tmp_path)

        # Install skills and docs
        args = Namespace(tool="claude-code", force=False)
        cmd_skill_install(args)

        # Compare content of installed docs with source
        doc_files = _get_doc_files_from_index()
        skill_dir = tmp_path / ".claude" / "skills" / "bsl-query-expert"
        docs_base = skill_dir / "docs"

        for doc_file in doc_files:
            source_content = doc_file["source"].read_text()
            installed_path = docs_base / doc_file["relative_path"]
            installed_content = installed_path.read_text()

            assert source_content == installed_content, (
                f"Content mismatch for {doc_file['relative_path']}"
            )
