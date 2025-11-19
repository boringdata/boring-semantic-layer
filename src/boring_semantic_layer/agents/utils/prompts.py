"""Utilities for loading prompts from markdown files."""

from pathlib import Path


def load_prompt(prompts_dir: Path, filename: str) -> str:
    """Load a prompt from a markdown file.

    Args:
        prompts_dir: Directory containing prompt files
        filename: Name of the markdown file to load

    Returns:
        Content of the file, or empty string if file doesn't exist
    """
    prompt_path = prompts_dir / filename
    if prompt_path.exists():
        return prompt_path.read_text(encoding="utf-8").strip()
    return ""
