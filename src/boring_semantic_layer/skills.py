"""Skill discovery and metadata for MCP-served domain context.

A "skill" is a directory containing a SKILL.md file with YAML frontmatter
(name + description) and arbitrary additional files. BSL discovers skills
eagerly via :func:`discover_skills`, parsing only the frontmatter at load
time (Level 1, ~100 tokens per skill). Bodies and other files are read
lazily by the MCP server when a client requests them.

The module deliberately depends only on stdlib + ``pyyaml`` (already a
hard dependency) so that loading a YAML config with ``skills_dir:`` does
not pull in optional MCP/agent extras.
"""

from __future__ import annotations

import warnings
from dataclasses import dataclass, field
from pathlib import Path

import yaml

_FRONTMATTER_DELIM = "---"

_MIME_TYPES: dict[str, str] = {
    ".md": "text/markdown",
    ".pdf": "application/pdf",
    ".csv": "text/csv",
    ".json": "application/json",
    ".py": "text/x-python",
    ".sh": "text/x-shellscript",
    ".yaml": "text/yaml",
    ".yml": "text/yaml",
    ".txt": "text/plain",
    ".html": "text/html",
    ".png": "image/png",
    ".jpg": "image/jpeg",
    ".jpeg": "image/jpeg",
    ".gif": "image/gif",
    ".svg": "image/svg+xml",
}

_TEXT_MIME_PREFIXES = ("text/", "application/json", "application/xml", "image/svg+xml")


@dataclass(frozen=True)
class SkillMetadata:
    """Level 1 skill metadata extracted at load time.

    Attributes:
        name: Skill identifier from SKILL.md frontmatter (kebab-case).
        description: Human-readable trigger phrase from frontmatter.
        dir_path: Absolute path to the skill directory.
        scope: ``"parent"`` for server-wide skills, otherwise the model name.
        extra_frontmatter: Any non-name/description frontmatter fields,
            stored opaquely for downstream consumers.
    """

    name: str
    description: str
    dir_path: Path
    scope: str = "parent"
    extra_frontmatter: dict = field(default_factory=dict)


def parse_frontmatter(skill_md_path: Path) -> dict | None:
    """Parse the YAML frontmatter at the top of a SKILL.md file.

    Returns the parsed mapping, or ``None`` if the file is missing,
    has no frontmatter delimiters, or the frontmatter YAML is malformed.
    Does not read past the closing ``---`` delimiter.
    """
    if not skill_md_path.is_file():
        return None

    try:
        with skill_md_path.open("r", encoding="utf-8") as fh:
            first_line = fh.readline()
            if first_line.strip() != _FRONTMATTER_DELIM:
                return None
            body_lines: list[str] = []
            for line in fh:
                if line.strip() == _FRONTMATTER_DELIM:
                    break
                body_lines.append(line)
            else:
                return None
    except OSError as exc:
        warnings.warn(f"Could not read {skill_md_path}: {exc}", stacklevel=2)
        return None

    try:
        parsed = yaml.safe_load("".join(body_lines))
    except yaml.YAMLError as exc:
        warnings.warn(f"Malformed YAML frontmatter in {skill_md_path}: {exc}", stacklevel=2)
        return None

    if not isinstance(parsed, dict):
        return None
    return parsed


def discover_skills(
    skills_dir: Path,
    *,
    scope: str = "parent",
    exclude_dirs: set[Path] | None = None,
) -> list[SkillMetadata]:
    """Scan ``skills_dir`` for immediate subdirectories containing ``SKILL.md``.

    Only the YAML frontmatter is read. Bodies and other files are not opened.

    Args:
        skills_dir: Directory to scan. Missing directories warn and return ``[]``.
        scope: ``"parent"`` for top-level skills_dir, or the owning model name.
        exclude_dirs: Absolute resolved paths to skip — used by the loader to
            avoid double-registering when a per-model ``skills_dir`` lives
            inside the parent ``skills_dir``.

    Returns:
        List of :class:`SkillMetadata`, one per valid skill subdirectory.
        Skills with missing or malformed frontmatter are skipped with a warning.
    """
    if not skills_dir.is_dir():
        warnings.warn(
            f"skills_dir does not exist or is not a directory: {skills_dir}",
            stacklevel=2,
        )
        return []

    excluded = {p.resolve() for p in (exclude_dirs or set())}
    discovered: list[SkillMetadata] = []

    for child in sorted(skills_dir.iterdir()):
        if not child.is_dir():
            continue
        if child.resolve() in excluded:
            continue

        skill_md = child / "SKILL.md"
        frontmatter = parse_frontmatter(skill_md)
        if frontmatter is None:
            continue

        name = frontmatter.get("name")
        description = frontmatter.get("description")
        if not isinstance(name, str) or not name.strip():
            warnings.warn(
                f"Skill at {child} is missing required 'name' in frontmatter; skipping",
                stacklevel=2,
            )
            continue
        if not isinstance(description, str) or not description.strip():
            warnings.warn(
                f"Skill '{name}' at {child} is missing required 'description'; skipping",
                stacklevel=2,
            )
            continue

        extra = {k: v for k, v in frontmatter.items() if k not in {"name", "description"}}
        discovered.append(
            SkillMetadata(
                name=name.strip(),
                description=description.strip(),
                dir_path=child.resolve(),
                scope=scope,
                extra_frontmatter=extra,
            )
        )

    return discovered


def infer_mime_type(path: Path) -> str:
    """Return the MIME type for a skill resource file based on extension."""
    if path.name == "_manifest":
        return "application/json"
    return _MIME_TYPES.get(path.suffix.lower(), "application/octet-stream")


def is_text_mime(mime_type: str) -> bool:
    """True if a MIME type should be served as text rather than base64-encoded bytes."""
    return mime_type.startswith(_TEXT_MIME_PREFIXES)


def list_skill_files(skill: SkillMetadata) -> list[Path]:
    """Return all files inside a skill directory, recursively, in sorted order.

    Excludes directories themselves; the result contains only file paths
    relative to ``skill.dir_path``. Hidden files (dotfiles) are skipped.
    """
    files: list[Path] = []
    for path in sorted(skill.dir_path.rglob("*")):
        if not path.is_file():
            continue
        if any(part.startswith(".") for part in path.relative_to(skill.dir_path).parts):
            continue
        files.append(path.relative_to(skill.dir_path))
    return files


def build_manifest(skill: SkillMetadata) -> dict:
    """Build the JSON manifest describing every file in a skill directory.

    See doc Section 2.4 for the schema. Used to back ``skill://<name>/_manifest``
    resources.
    """
    files = []
    for rel_path in list_skill_files(skill):
        files.append(
            {
                "path": rel_path.as_posix(),
                "mime_type": infer_mime_type(rel_path),
            }
        )
    return {
        "skill_name": skill.name,
        "scope": skill.scope,
        "files": files,
    }
