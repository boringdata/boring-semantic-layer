#!/usr/bin/env python3
"""Documentation builder for BSL - generates JSON data from markdown files."""

import json
import os
import subprocess
import sys
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from boring_semantic_layer.chart.md_converter import CustomJSONEncoder
from boring_semantic_layer.chart.md_executor import QueryExecutor
from boring_semantic_layer.chart.md_parser import MarkdownParser


class DocBuilder:
    """Build documentation JSON from markdown files with BSL queries."""

    def __init__(self, docs_dir: Path | None = None):
        """Initialize documentation builder with directory paths."""
        if docs_dir is None:
            docs_dir = Path(__file__).parent.parent
        self.docs_dir = docs_dir
        self.content_dir = docs_dir / "md" / "doc"
        self.output_dir = docs_dir / "web" / "public" / "bsl-data"
        self.pages_file = docs_dir / "web" / "public" / "pages.json"

    def process_markdown_file(self, md_path: Path) -> bool:
        """Process a markdown file and generate JSON output."""
        print(f"Processing {md_path.name}...")

        content = md_path.read_text()
        content, files = MarkdownParser.resolve_file_includes(content, md_path.parent)
        modified_md, queries = MarkdownParser.extract_queries(content, include_hidden=True)
        component_types = MarkdownParser.find_component_types(content)

        if not queries:
            print(f"  No BSL queries found in {md_path.name}")
            return self._save_output(md_path, modified_md, {}, files)

        print(f"  Found {len(queries)} queries: {list(queries.keys())}")
        return self._execute_and_save(md_path, modified_md, queries, component_types, files)

    def _execute_and_save(
        self,
        md_path: Path,
        modified_md: str,
        queries: dict[str, str],
        component_types: dict[str, str],
        files: dict[str, str],
    ) -> bool:
        """Execute queries and save results to JSON."""
        results = {}
        has_errors = False
        executor = QueryExecutor(capture_output=True)

        # Change to content directory for relative paths
        original_cwd = os.getcwd()
        os.chdir(md_path.parent)

        try:
            for query_name, query_code in queries.items():
                print(f"  Executing query: {query_name}")
                is_chart_only = component_types.get(query_name) == "altairchart"
                result = executor.execute(query_code, is_chart_only=is_chart_only)

                if query_name in component_types:
                    results[query_name] = result
                else:
                    print("    (executed for context, no output component)")

                if "error" in result:
                    has_errors = True
                    print(f"  ❌ ERROR in query '{query_name}': {result['error']}")
                    if "traceback" in result:
                        print(f"  Traceback:\n{result['traceback']}")

            return self._save_output(md_path, modified_md, results, files, has_errors)

        finally:
            os.chdir(original_cwd)

    def _save_output(
        self,
        md_path: Path,
        markdown: str,
        queries: dict,
        files: dict,
        has_errors: bool = False,
    ) -> bool:
        """Save processed markdown and query results to JSON file."""
        output_file = self.output_dir / f"{md_path.stem}.json"
        output_data = {"markdown": markdown, "queries": queries, "files": files}
        output_file.write_text(json.dumps(output_data, indent=2, cls=CustomJSONEncoder) + "\n")

        if has_errors:
            print(f"  ⚠️  Saved to {output_file} (with errors)")
            return False

        print(f"  ✅ Saved to {output_file}")
        return True

    def build(self) -> bool:
        """Build all documentation files and generate pages index."""
        print("Building Documentation")
        print("=" * 60)

        self.output_dir.mkdir(parents=True, exist_ok=True)

        md_files = list(self.content_dir.glob("*.md"))
        if not md_files:
            print(f"No markdown files found in {self.content_dir}")
            return False

        print(f"Found {len(md_files)} markdown files\n")

        failed_files = [f.name for f in md_files if not self.process_markdown_file(f)]

        # Generate pages index
        pages = sorted([f.stem for f in md_files])
        self.pages_file.write_text(json.dumps(pages, indent=2) + "\n")

        if failed_files:
            print(f"\n❌ Documentation build completed with ERRORS in {len(failed_files)} file(s):")
            for filename in failed_files:
                print(f"  - {filename}")
            return False

        print(f"\n✅ Documentation build complete! Generated {len(pages)} pages.")
        return True


def main():
    """Build documentation and validate internal links."""
    builder = DocBuilder()
    if not builder.build():
        return 1

    # Validate links if script exists
    print("\n🔗 Validating internal links...")
    print("=" * 60)
    validate_script = Path(__file__).parent / "validate_links.py"

    if not validate_script.exists():
        print("⚠️  Link validation script not found, skipping validation")
        return 0

    result = subprocess.run([sys.executable, str(validate_script)], capture_output=False)
    if result.returncode != 0:
        print("\n❌ Link validation failed!")
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
