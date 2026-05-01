"""Helpers for registering skill resources and tools on an MCP server.

Kept separate from ``mcp.py`` to avoid bloating that file as the surface grows.
All public entry points take an :class:`MCPSemanticModel`-like instance and a
list of :class:`SkillMetadata` and register resources / tools on it.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

from fastmcp.exceptions import ToolError
from mcp.types import Annotations, ToolAnnotations

from ...skills import (
    SkillMetadata,
    build_manifest,
    discover_skills,
    infer_mime_type,
    is_text_mime,
    list_skill_files,
)

WRITE_ANNOTATIONS = ToolAnnotations(
    readOnlyHint=False,
    destructiveHint=False,
    idempotentHint=False,
    openWorldHint=False,
)

_SKILL_NAME_RE = re.compile(r"^[a-z][a-z0-9-]*$")


def _skill_uri_prefix(skill: SkillMetadata) -> str:
    if skill.scope == "parent":
        return f"skill://{skill.name}/"
    return f"skill://{skill.scope}/{skill.name}/"


def _file_uri(skill: SkillMetadata, rel_path: Path) -> str:
    """Compose a URI for one file inside a skill.

    FastMCP URI templates only support single-segment params, so we register
    one static URI per file. Slashes inside ``rel_path`` are URL-encoded so
    the URI stays single-segment relative to the prefix.
    """
    encoded = rel_path.as_posix().replace("/", "__")
    return _skill_uri_prefix(skill) + encoded


def _manifest_uri(skill: SkillMetadata) -> str:
    return _skill_uri_prefix(skill) + "_manifest"


def _make_text_reader(file_path: Path):
    def read_text() -> str:
        return file_path.read_text(encoding="utf-8")

    return read_text


def _make_binary_reader(file_path: Path):
    def read_binary() -> bytes:
        return file_path.read_bytes()

    return read_binary


def _make_manifest_reader(skill: SkillMetadata):
    def read_manifest() -> str:
        return json.dumps(build_manifest(skill), indent=2)

    return read_manifest


def register_skill_resources(server, skill: SkillMetadata) -> None:
    """Register every file under ``skill`` as an MCP resource on ``server``.

    Each file becomes a single-segment URI under ``skill_uri_prefix(skill)``.
    Text-MIME files are served via ``str``; binary files via ``bytes`` so
    FastMCP encodes them as ``BlobResourceContents``.
    """
    for rel_path in list_skill_files(skill):
        uri = _file_uri(skill, rel_path)
        mime = infer_mime_type(rel_path)
        full_path = skill.dir_path / rel_path

        if is_text_mime(mime):
            server.resource(
                uri=uri,
                name=f"{skill.name}/{rel_path.as_posix()}",
                description=f"Skill file ({skill.scope})",
                mime_type=mime,
                annotations=Annotations(audience=["assistant"], priority=0.4),
            )(_make_text_reader(full_path))
        else:
            server.resource(
                uri=uri,
                name=f"{skill.name}/{rel_path.as_posix()}",
                description=f"Skill binary asset ({skill.scope})",
                mime_type=mime,
                annotations=Annotations(audience=["assistant"], priority=0.3),
            )(_make_binary_reader(full_path))

    # Manifest
    server.resource(
        uri=_manifest_uri(skill),
        name=f"{skill.name}/_manifest",
        description=f"File manifest for skill '{skill.name}' ({skill.scope})",
        mime_type="application/json",
        annotations=Annotations(audience=["assistant"], priority=0.5),
    )(_make_manifest_reader(skill))


def register_all_skill_resources(
    server,
    parent_skills: list[SkillMetadata],
    model_skills: dict[str, list[SkillMetadata]],
) -> None:
    """Register resources for every parent and per-model skill."""
    for skill in parent_skills:
        register_skill_resources(server, skill)
    for skills in model_skills.values():
        for skill in skills:
            register_skill_resources(server, skill)


def _skill_to_dict(skill: SkillMetadata) -> dict[str, Any]:
    """Public-facing dict for a skill — Level 1 metadata only, no body content."""
    files = [p.as_posix() for p in list_skill_files(skill)]
    return {
        "name": skill.name,
        "description": skill.description,
        "files": files,
        "uri_prefix": _skill_uri_prefix(skill),
    }


def build_domain_context(
    parent_skills_dirs: list[Path],
    model_skills_dirs: dict[str, Path],
    models: Any,
) -> dict[str, Any]:
    """Re-scan all skills_dir paths and return aggregated Level 1 metadata.

    Re-scanning on each call ensures skills added at runtime via ``add_skill``
    are immediately visible.
    """
    # Re-discover everything from disk so add_skill mutations show up.
    fresh_model_skills: dict[str, list[SkillMetadata]] = {
        model_name: discover_skills(path, scope=model_name)
        for model_name, path in model_skills_dirs.items()
    }
    excluded = set(model_skills_dirs.values())
    fresh_parent_skills: list[SkillMetadata] = []
    for d in parent_skills_dirs:
        fresh_parent_skills.extend(discover_skills(d, scope="parent", exclude_dirs=excluded))

    return {
        "parent_skills": [_skill_to_dict(s) for s in fresh_parent_skills],
        "model_skills": {
            model_name: [_skill_to_dict(s) for s in skills]
            for model_name, skills in fresh_model_skills.items()
        },
        "models": [
            {
                "name": name,
                "description": getattr(models[name], "description", None),
            }
            for name in models
        ],
    }


def validate_skill_name(name: str) -> None:
    """Raise :class:`ToolError` if ``name`` isn't kebab-case."""
    if not _SKILL_NAME_RE.match(name):
        raise ToolError(
            f"Invalid skill name '{name}'. Must be kebab-case: lowercase letters, "
            "digits, hyphens; must start with a letter."
        )


def resolve_add_skill_target(
    name: str,
    model_name: str | None,
    parent_skills_dir: Path | None,
    model_skills_dirs: dict[str, Path],
) -> tuple[Path, str]:
    """Decide where ``add_skill`` should write a new skill.

    Returns ``(target_skills_dir, scope)``.

    Falls back to the parent ``skills_dir`` if ``model_name`` is given but
    that model has no ``skills_dir``. Raises :class:`ToolError` if neither
    a model-level nor parent-level skills_dir is configured (which should
    not happen since the tool wouldn't be registered, but handle defensively).
    """
    if model_name and model_name in model_skills_dirs:
        return model_skills_dirs[model_name], model_name
    if parent_skills_dir is not None:
        return parent_skills_dir, "parent"
    raise ToolError(
        "Cannot add skill: no skills_dir is configured at the parent or "
        f"model level (model_name={model_name!r})."
    )


def write_skill(
    target_dir: Path,
    name: str,
    description: str,
    steps: str,
    reference: str | None = None,
) -> Path:
    """Write a new skill to disk. Returns the created skill directory.

    Refuses to overwrite an existing skill directory.
    """
    skill_dir = target_dir / name
    if skill_dir.exists():
        raise ToolError(f"Skill directory already exists: {skill_dir}. Pick a different name.")

    skill_dir.mkdir(parents=True, exist_ok=False)

    body = steps if steps.endswith("\n") else steps + "\n"
    skill_md_content = f"---\nname: {name}\ndescription: {description.strip()}\n---\n{body}"
    (skill_dir / "SKILL.md").write_text(skill_md_content, encoding="utf-8")

    if reference is not None:
        ref_body = reference if reference.endswith("\n") else reference + "\n"
        (skill_dir / "REFERENCE.md").write_text(ref_body, encoding="utf-8")

    return skill_dir


__all__ = [
    "WRITE_ANNOTATIONS",
    "build_domain_context",
    "register_all_skill_resources",
    "register_skill_resources",
    "resolve_add_skill_target",
    "validate_skill_name",
    "write_skill",
]
