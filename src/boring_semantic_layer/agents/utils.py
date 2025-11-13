"""Shared utilities for BSL agent integrations."""

from pathlib import Path


def load_bsl_agent_guide() -> str:
    """
    Load the shared BSL Agent Guide markdown.

    This guide is used by all agent integrations (AiChat, LangChain, MCP)
    to ensure consistent behavior and documentation.

    Returns:
        The complete BSL Agent Guide as a string.

    Raises:
        FileNotFoundError: If the guide file is missing.
    """
    guide_path = Path(__file__).parent / "BSL_AGENT_GUIDE.md"
    if not guide_path.exists():
        raise FileNotFoundError(
            f"BSL Agent Guide not found at {guide_path}. "
            "This file is required for agent functionality."
        )
    return guide_path.read_text()


def extract_guide_section(guide: str, section_title: str) -> str:
    """
    Extract a specific section from the BSL Agent Guide.

    Args:
        guide: The full BSL Agent Guide text
        section_title: The section header to extract (e.g., "### For MCP")

    Returns:
        The extracted section text, or empty string if not found.
    """
    section_start = guide.find(section_title)
    if section_start == -1:
        return ""

    # Find the next section header (## or ###) or document divider (---)
    section_end = guide.find("\n## ", section_start + 1)
    if section_end == -1:
        section_end = guide.find("\n### ", section_start + len(section_title))
    if section_end == -1:
        section_end = guide.find("\n---", section_start + 1)

    if section_end == -1:
        return guide[section_start:]
    return guide[section_start:section_end]
