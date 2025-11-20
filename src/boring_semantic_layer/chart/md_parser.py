"""Markdown parsing utilities for BSL query extraction."""

import re
from pathlib import Path


class MarkdownParser:
    """Parse markdown content to extract BSL queries and resolve includes."""

    STANDARD_LANGUAGES = frozenset(
        {
            "python",
            "sql",
            "bash",
            "javascript",
            "typescript",
            "js",
            "ts",
            "yaml",
            "yml",
            "json",
            "toml",
            "text",
            "sh",
        }
    )

    @classmethod
    def extract_queries(
        cls, content: str, include_hidden: bool = False
    ) -> tuple[str, dict[str, str]]:
        """Extract BSL query blocks from markdown content."""
        queries = {}
        modified = content

        if include_hidden:
            modified = cls._extract_hidden_queries(modified, queries)

        modified = cls._extract_visible_queries(modified, queries)
        return modified, queries

    @classmethod
    def _extract_hidden_queries(cls, content: str, queries: dict[str, str]) -> str:
        """Extract queries from HTML comment blocks."""
        pattern = r"<!--\s*\n```(\w+)\n(.*?)\n```\s*\n-->"

        def extract(match):
            name, code = match.group(1), match.group(2).strip()
            if name.lower() not in cls.STANDARD_LANGUAGES:
                queries[name] = code
            return ""

        return re.sub(pattern, extract, content, flags=re.DOTALL)

    @classmethod
    def _extract_visible_queries(cls, content: str, queries: dict[str, str]) -> str:
        """Extract queries from visible code blocks."""
        pattern = r"```(\w+)\n(.*?)\n```"

        def extract(match):
            name, code = match.group(1), match.group(2).strip()
            if name.lower() not in cls.STANDARD_LANGUAGES:
                queries[name] = code
            return match.group(0)

        return re.sub(pattern, extract, content, flags=re.DOTALL)

    @classmethod
    def find_component_types(cls, content: str) -> dict[str, str]:
        """Find component type annotations (bslquery, altairchart, etc)."""
        component_types = {}
        patterns = {
            "altairchart": r'<altairchart[^>]+code-block="(\w+)"',
            "bslquery": r'<bslquery[^>]+code-block="(\w+)"',
            "regularoutput": r'<regularoutput[^>]+code-block="(\w+)"',
            "collapsedcodeblock": r'<collapsedcodeblock[^>]+code-block="(\w+)"',
        }

        for comp_type, pattern in patterns.items():
            for match in re.finditer(pattern, content):
                block_name = match.group(1)
                if block_name not in component_types:
                    component_types[block_name] = comp_type

        return component_types

    @classmethod
    def resolve_file_includes(cls, content: str, base_dir: Path) -> tuple[str, dict[str, str]]:
        """Resolve <yamlcontent path="..."> file includes."""
        files = {}
        pattern = r'<yamlcontent\s+path="([^"]+)"(?:\s*/)?></yamlcontent>'

        def extract(match):
            file_path = match.group(1).strip()
            full_path = base_dir / file_path
            if not full_path.exists():
                return f"<!-- Error: File not found: {file_path} -->"
            files[file_path] = full_path.read_text()
            return match.group(0)

        modified = re.sub(pattern, extract, content)
        return modified, files
