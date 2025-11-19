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
        self.prompts_dir = docs_dir / "prompts" / "query" / "langchain"
        self.skills_dir = docs_dir / "skills"

    def read_prompt(self, filename: str) -> str:
        """Read a prompt file from the prompts directory."""
        return (self.prompts_dir / filename).read_text()

    def build_claude_code_skill(self) -> str:
        """Build Claude Code SKILL.md content."""
        content = self.read_prompt("system.md")
        frontmatter = """---
name: bsl-query-expert
description: Query BSL semantic models with group_by, aggregate, filter, and visualizations. Use for data analysis from existing semantic tables.
---

"""
        return frontmatter + content

    def build_codex_skill(self) -> str:
        """Build Codex skill content."""
        content = self.read_prompt("system.md")
        header = """# BSL Query Expert - Codex Skill

This skill helps with querying Boring Semantic Layer (BSL) models.

"""
        return header + content

    def build_cursor_skill(self) -> str:
        """Build Cursor skill content."""
        content = self.read_prompt("system.md")
        header = """# BSL Query Expert - Cursor Skill

Guide for querying Boring Semantic Layer (BSL) semantic models.

"""
        return header + content

    def ensure_skills_dir(self):
        """Create the skills directory structure."""
        self.skills_dir.mkdir(exist_ok=True)
        (self.skills_dir / "claude-code").mkdir(exist_ok=True)
        (self.skills_dir / "codex").mkdir(exist_ok=True)
        (self.skills_dir / "cursor").mkdir(exist_ok=True)

    def build_all(self, dry_run: bool = False) -> dict[str, Path]:
        """Build all skill files."""
        self.ensure_skills_dir()

        skills = {
            "claude-code": {
                "path": self.skills_dir / "claude-code" / "bsl-query-expert" / "SKILL.md",
                "content": self.build_claude_code_skill(),
            },
            "codex": {
                "path": self.skills_dir / "codex" / "bsl-query-expert.codex",
                "content": self.build_codex_skill(),
            },
            "cursor": {
                "path": self.skills_dir / "cursor" / "bsl-query-expert.cursorrules",
                "content": self.build_cursor_skill(),
            },
        }

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
        skills = {
            "claude-code": {
                "path": self.skills_dir / "claude-code" / "bsl-query-expert" / "SKILL.md",
                "content": self.build_claude_code_skill(),
            },
            "codex": {
                "path": self.skills_dir / "codex" / "bsl-query-expert.codex",
                "content": self.build_codex_skill(),
            },
            "cursor": {
                "path": self.skills_dir / "cursor" / "bsl-query-expert.cursorrules",
                "content": self.build_cursor_skill(),
            },
        }

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
