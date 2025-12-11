#!/usr/bin/env python3
"""
Skills Builder - Generate IDE/AI assistant skills from modular prompt files.

This script generates skill files for different AI coding assistants:
- Claude Code (SKILL.md)
- Codex (.codex files)
- Cursor (.cursorrules or similar)

All generated from the same source prompts to avoid duplication.

Usage:
    python skills_builder.py
    python skills_builder.py --check  # Verify skills are up to date
"""

import argparse
import json
import re
import sys
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))


class SkillBuilder:
    """Build skills for AI coding assistants from prompts."""

    def __init__(self, docs_dir: Path | None = None):
        """
        Initialize the skill builder.

        Args:
            docs_dir: Path to docs directory. If None, uses parent of this script.
        """
        if docs_dir is None:
            # Script is in docs/md/, so we stay in md/
            docs_dir = Path(__file__).parent
        self.docs_dir = docs_dir
        self.prompts_dir = docs_dir / "prompts"
        self.skills_dir = docs_dir / "skills"
        self._index = None

    def read_prompt(self, category: str, filename: str) -> str:
        """Read a prompt file from the prompts directory."""
        return (self.prompts_dir / category / filename).read_text()

    def read_index(self) -> dict:
        """Read the index.json file containing documentation topics."""
        if self._index is None:
            index_path = self.docs_dir / "index.json"
            self._index = json.loads(index_path.read_text())
        return self._index

    def build_additional_info_for_skill(self, tool: str = "claude-code") -> str:
        """Build the Additional Information section for CLI skills (no get_documentation tool).

        Args:
            tool: The target tool (claude-code, cursor, codex) - determines docs path
        """
        index = self.read_index()
        topics = index.get("topics", {})

        # Base URL for documentation on GitHub
        docs_base_url = "https://github.com/boringdata/boring-semantic-layer/blob/main/docs/md"

        lines = [
            "## Additional Information",
            "",
            "**Available documentation:**",
            "",
        ]
        for topic_id, topic_info in topics.items():
            title = topic_info.get("title", topic_id)
            desc = topic_info.get("description", "")
            source = topic_info.get("source", "")
            # Create GitHub URL for the doc
            doc_url = f"{docs_base_url}/{source}" if source else ""
            lines.append(f"- **{title}**: {desc}")
            if doc_url:
                lines.append(f"  - URL: {doc_url}")

        return "\n".join(lines)

    def transform_prompt_for_skill(self, content: str, tool: str = "claude-code") -> str:
        """Transform a LangChain prompt for use as a CLI skill.

        Replaces the 'Additional Resources' section that references get_documentation()
        with a static version built from index.json with GitHub URLs.

        Args:
            content: The prompt content to transform
            tool: The target tool (claude-code, cursor, codex) - determines docs path
        """
        # Pattern to match the Additional Resources section (including any variation)
        pattern = r"## Additional (Resources|Information).*"
        replacement = self.build_additional_info_for_skill(tool)

        # Replace the section (DOTALL makes . match newlines)
        return re.sub(pattern, replacement, content, flags=re.DOTALL)

    def build_query_expert_claude_code(self) -> str:
        """Build Claude Code bsl-query-expert SKILL.md content."""
        # Read the LangChain system prompt and transform it for skills
        content = self.read_prompt("query/langchain", "system.md")
        content = self.transform_prompt_for_skill(content, tool="claude-code")

        # Include tool-query-model.md content since skills can't call get_documentation()
        tool_query_content = self.read_prompt("query/langchain", "tool-query-model.md")
        content += "\n## Query Syntax Reference\n\n" + tool_query_content

        frontmatter = """---
name: bsl-query-expert
description: Query BSL semantic models with group_by, aggregate, filter, and visualizations. Use for data analysis from existing semantic tables.
---

"""
        return frontmatter + content

    def build_model_builder_claude_code(self) -> str:
        """Build Claude Code bsl-model-builder SKILL.md content."""
        content = self.read_prompt("build", "system.md")
        content = self.transform_prompt_for_skill(content, tool="claude-code")
        frontmatter = """---
name: bsl-model-builder
description: Build BSL semantic models with dimensions, measures, joins, and YAML config. Use for creating/modifying data models.
---

"""
        return frontmatter + content

    def build_query_expert_codex(self) -> str:
        """Build Codex bsl-query-expert skill content."""
        # Read the LangChain system prompt and transform it for skills
        content = self.read_prompt("query/langchain", "system.md")
        content = self.transform_prompt_for_skill(content, tool="codex")

        # Include tool-query-model.md content since skills can't call get_documentation()
        tool_query_content = self.read_prompt("query/langchain", "tool-query-model.md")
        content += "\n## Query Syntax Reference\n\n" + tool_query_content

        header = """# BSL Query Expert - Codex Skill

This skill helps with querying Boring Semantic Layer (BSL) models.

"""
        return header + content

    def build_model_builder_codex(self) -> str:
        """Build Codex bsl-model-builder skill content."""
        content = self.read_prompt("build", "system.md")
        content = self.transform_prompt_for_skill(content, tool="codex")
        header = """# BSL Model Builder - Codex Skill

This skill helps with building Boring Semantic Layer (BSL) semantic models.

"""
        return header + content

    def build_query_expert_cursor(self) -> str:
        """Build Cursor bsl-query-expert skill content (.mdc format with frontmatter)."""
        # Read the LangChain system prompt and transform it for skills
        content = self.read_prompt("query/langchain", "system.md")
        content = self.transform_prompt_for_skill(content, tool="cursor")

        # Include tool-query-model.md content since skills can't call get_documentation()
        tool_query_content = self.read_prompt("query/langchain", "tool-query-model.md")
        content += "\n## Query Syntax Reference\n\n" + tool_query_content

        frontmatter = """---
description: Query BSL semantic models with group_by, aggregate, filter, and visualizations
globs:
alwaysApply: false
---

"""
        return frontmatter + content

    def build_model_builder_cursor(self) -> str:
        """Build Cursor bsl-model-builder skill content (.mdc format with frontmatter)."""
        content = self.read_prompt("build", "system.md")
        content = self.transform_prompt_for_skill(content, tool="cursor")
        frontmatter = """---
description: Build BSL semantic models with dimensions, measures, joins, and YAML config
globs:
alwaysApply: false
---

"""
        return frontmatter + content

    def ensure_skills_dir(self):
        """Create the skills directory structure."""
        self.skills_dir.mkdir(exist_ok=True)
        (self.skills_dir / "claude-code").mkdir(exist_ok=True)
        (self.skills_dir / "codex").mkdir(exist_ok=True)
        (self.skills_dir / "cursor").mkdir(exist_ok=True)

    def _get_all_skills(self) -> dict[str, dict]:
        """Get all skill configurations."""
        return {
            # Claude Code skills
            "claude-code/bsl-query-expert": {
                "path": self.skills_dir / "claude-code" / "bsl-query-expert" / "SKILL.md",
                "content": self.build_query_expert_claude_code(),
            },
            "claude-code/bsl-model-builder": {
                "path": self.skills_dir / "claude-code" / "bsl-model-builder" / "SKILL.md",
                "content": self.build_model_builder_claude_code(),
            },
            # Codex skills
            "codex/bsl-query-expert": {
                "path": self.skills_dir / "codex" / "bsl-query-expert.codex",
                "content": self.build_query_expert_codex(),
            },
            "codex/bsl-model-builder": {
                "path": self.skills_dir / "codex" / "bsl-model-builder.codex",
                "content": self.build_model_builder_codex(),
            },
            # Cursor skills (.mdc format with frontmatter)
            "cursor/bsl-query-expert": {
                "path": self.skills_dir / "cursor" / "bsl-query-expert.mdc",
                "content": self.build_query_expert_cursor(),
            },
            "cursor/bsl-model-builder": {
                "path": self.skills_dir / "cursor" / "bsl-model-builder.mdc",
                "content": self.build_model_builder_cursor(),
            },
        }

    def build_all(self, dry_run: bool = False) -> dict[str, Path]:
        """Build all skill files."""
        self.ensure_skills_dir()

        skills = self._get_all_skills()

        results = {}
        for name, skill in skills.items():
            path = skill["path"]
            content = skill["content"]

            path.parent.mkdir(parents=True, exist_ok=True)

            if dry_run:
                print(f"Would write: {path}")
            else:
                path.write_text(content)
                print(f"✓ Generated {path}")

            results[name] = path

        return results

    def check_up_to_date(self) -> bool:
        """Check if skills are up to date. Returns True if all up to date."""
        skills = self._get_all_skills()

        all_up_to_date = True
        for _name, skill in skills.items():
            path = skill["path"]
            expected_content = skill["content"]

            if not path.exists():
                print(f"✗ Missing: {path}")
                all_up_to_date = False
                continue

            actual_content = path.read_text()
            if actual_content != expected_content:
                print(f"✗ Out of date: {path}")
                all_up_to_date = False
            else:
                print(f"✓ Up to date: {path}")

        return all_up_to_date


def main():
    """Main entry point for the skill builder CLI."""
    parser = argparse.ArgumentParser(description="Build AI assistant skills from prompt files")
    parser.add_argument(
        "--check", action="store_true", help="Check if skills are up to date (don't regenerate)"
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="Show what would be done without writing files"
    )
    args = parser.parse_args()

    builder = SkillBuilder()

    if args.check:
        print("Checking if skills are up to date...")
        print()
        if builder.check_up_to_date():
            print("\n✓ All skills are up to date!")
            return 0
        else:
            print("\n✗ Some skills are out of date. Run without --check to regenerate.")
            return 1
    else:
        print("Generating skills from prompt files...")
        print()
        builder.build_all(dry_run=args.dry_run)
        print("\n✓ Done!")
        return 0


if __name__ == "__main__":
    sys.exit(main())
