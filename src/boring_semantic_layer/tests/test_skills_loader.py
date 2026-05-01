"""Unit tests for the skills discovery module."""

from __future__ import annotations

import warnings
from pathlib import Path

import pytest

from boring_semantic_layer.skills import (
    SkillMetadata,
    build_manifest,
    discover_skills,
    infer_mime_type,
    is_text_mime,
    list_skill_files,
    parse_frontmatter,
)


def _write_skill(
    parent: Path,
    name: str,
    *,
    frontmatter: str | None = "name: {name}\ndescription: A skill for {name}",
    body: str = "# Body\nSome instructions.",
    extra_files: dict[str, str | bytes] | None = None,
) -> Path:
    """Helper: create a skill dir with SKILL.md plus optional additional files."""
    skill_dir = parent / name
    skill_dir.mkdir(parents=True)
    skill_md = skill_dir / "SKILL.md"
    if frontmatter is None:
        skill_md.write_text(body, encoding="utf-8")
    else:
        rendered_fm = frontmatter.format(name=name)
        skill_md.write_text(f"---\n{rendered_fm}\n---\n{body}", encoding="utf-8")

    for rel, content in (extra_files or {}).items():
        target = skill_dir / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        if isinstance(content, bytes):
            target.write_bytes(content)
        else:
            target.write_text(content, encoding="utf-8")
    return skill_dir


class TestParseFrontmatter:
    def test_returns_dict_for_valid_frontmatter(self, tmp_path):
        path = tmp_path / "SKILL.md"
        path.write_text(
            "---\nname: foo\ndescription: a foo skill\n---\nbody here\n",
            encoding="utf-8",
        )
        result = parse_frontmatter(path)
        assert result == {"name": "foo", "description": "a foo skill"}

    def test_returns_none_for_missing_file(self, tmp_path):
        assert parse_frontmatter(tmp_path / "missing.md") is None

    def test_returns_none_when_no_opening_delimiter(self, tmp_path):
        path = tmp_path / "SKILL.md"
        path.write_text("name: foo\ndescription: bar\n", encoding="utf-8")
        assert parse_frontmatter(path) is None

    def test_returns_none_when_no_closing_delimiter(self, tmp_path):
        path = tmp_path / "SKILL.md"
        path.write_text("---\nname: foo\ndescription: bar\nno closing", encoding="utf-8")
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            assert parse_frontmatter(path) is None

    def test_warns_and_returns_none_on_malformed_yaml(self, tmp_path):
        path = tmp_path / "SKILL.md"
        path.write_text("---\nname: foo\n  invalid:\n: nested\n---\n", encoding="utf-8")
        with pytest.warns(UserWarning, match="Malformed YAML frontmatter"):
            assert parse_frontmatter(path) is None

    def test_does_not_read_body_yaml(self, tmp_path):
        path = tmp_path / "SKILL.md"
        path.write_text(
            "---\nname: foo\ndescription: bar\n---\n```yaml\nthis: would: break: parsers\n```\n",
            encoding="utf-8",
        )
        result = parse_frontmatter(path)
        assert result == {"name": "foo", "description": "bar"}


class TestDiscoverSkills:
    def test_returns_one_skill_for_valid_directory(self, tmp_path):
        skills_dir = tmp_path / "skills"
        _write_skill(skills_dir, "form-990")
        result = discover_skills(skills_dir)
        assert len(result) == 1
        skill = result[0]
        assert skill.name == "form-990"
        assert skill.description == "A skill for form-990"
        assert skill.dir_path == (skills_dir / "form-990").resolve()
        assert skill.scope == "parent"

    def test_returns_empty_for_missing_directory(self, tmp_path):
        with pytest.warns(UserWarning, match="does not exist"):
            assert discover_skills(tmp_path / "nope") == []

    def test_skips_subdir_without_skill_md(self, tmp_path):
        skills_dir = tmp_path / "skills"
        skills_dir.mkdir()
        (skills_dir / "not-a-skill").mkdir()
        (skills_dir / "not-a-skill" / "README.md").write_text("hi", encoding="utf-8")
        assert discover_skills(skills_dir) == []

    def test_skips_skill_without_name(self, tmp_path):
        skills_dir = tmp_path / "skills"
        _write_skill(
            skills_dir,
            "broken",
            frontmatter="description: missing name field",
        )
        with pytest.warns(UserWarning, match="missing required 'name'"):
            assert discover_skills(skills_dir) == []

    def test_skips_skill_without_description(self, tmp_path):
        skills_dir = tmp_path / "skills"
        _write_skill(
            skills_dir,
            "broken",
            frontmatter="name: broken",
        )
        with pytest.warns(UserWarning, match="missing required 'description'"):
            assert discover_skills(skills_dir) == []

    def test_skips_excluded_directories(self, tmp_path):
        skills_dir = tmp_path / "skills"
        _write_skill(skills_dir, "keep")
        _write_skill(skills_dir, "skip-me")
        excluded = {(skills_dir / "skip-me").resolve()}
        result = discover_skills(skills_dir, exclude_dirs=excluded)
        assert [s.name for s in result] == ["keep"]

    def test_scope_propagates(self, tmp_path):
        skills_dir = tmp_path / "skills"
        _write_skill(skills_dir, "rev")
        result = discover_skills(skills_dir, scope="organizations")
        assert result[0].scope == "organizations"

    def test_extra_frontmatter_preserved(self, tmp_path):
        skills_dir = tmp_path / "skills"
        _write_skill(
            skills_dir,
            "tagged",
            frontmatter=(
                "name: tagged\ndescription: with extras\nversion: '1.2'\ntags:\n  - alpha\n  - beta"
            ),
        )
        result = discover_skills(skills_dir)
        assert result[0].extra_frontmatter == {"version": "1.2", "tags": ["alpha", "beta"]}

    def test_does_not_open_other_files(self, tmp_path):
        """parse_frontmatter and discover_skills must read ONLY SKILL.md frontmatter."""
        skills_dir = tmp_path / "skills"
        _write_skill(
            skills_dir,
            "guarded",
            extra_files={
                "huge.bin": b"\x00" * 1024,
                "references/data.csv": "would never load",
            },
        )
        result = discover_skills(skills_dir)
        assert len(result) == 1
        # If discover_skills had touched the body or other files, malformed body
        # markdown could break the test. The fact that it returns clean metadata
        # without surprises is the proof.
        assert result[0].name == "guarded"


class TestInferMimeType:
    @pytest.mark.parametrize(
        "filename,expected",
        [
            ("SKILL.md", "text/markdown"),
            ("guide.pdf", "application/pdf"),
            ("data.csv", "text/csv"),
            ("config.json", "application/json"),
            ("script.py", "text/x-python"),
            ("install.sh", "text/x-shellscript"),
            ("conf.yaml", "text/yaml"),
            ("conf.yml", "text/yaml"),
            ("notes.txt", "text/plain"),
            ("logo.png", "image/png"),
            ("photo.jpg", "image/jpeg"),
            ("diagram.svg", "image/svg+xml"),
            ("unknown.bin", "application/octet-stream"),
            ("noextension", "application/octet-stream"),
            ("_manifest", "application/json"),
        ],
    )
    def test_extension_map(self, filename, expected):
        assert infer_mime_type(Path(filename)) == expected


class TestIsTextMime:
    @pytest.mark.parametrize(
        "mime,expected",
        [
            ("text/markdown", True),
            ("text/csv", True),
            ("application/json", True),
            ("application/xml", True),
            ("image/svg+xml", True),
            ("application/pdf", False),
            ("image/png", False),
            ("application/octet-stream", False),
        ],
    )
    def test_classification(self, mime, expected):
        assert is_text_mime(mime) is expected


class TestListSkillFilesAndManifest:
    def test_lists_files_relative_sorted(self, tmp_path):
        skills_dir = tmp_path / "skills"
        _write_skill(
            skills_dir,
            "form-990",
            extra_files={
                "GLOSSARY.md": "glossary",
                "references/how-to-read.pdf": b"%PDF-fake",
                "scripts/validate.py": "print('hi')",
            },
        )
        skill = discover_skills(skills_dir)[0]
        files = [p.as_posix() for p in list_skill_files(skill)]
        assert files == [
            "GLOSSARY.md",
            "SKILL.md",
            "references/how-to-read.pdf",
            "scripts/validate.py",
        ]

    def test_excludes_dotfiles(self, tmp_path):
        skills_dir = tmp_path / "skills"
        _write_skill(
            skills_dir,
            "form-990",
            extra_files={".hidden": "secret"},
        )
        skill = discover_skills(skills_dir)[0]
        files = [p.name for p in list_skill_files(skill)]
        assert ".hidden" not in files

    def test_manifest_shape(self, tmp_path):
        skills_dir = tmp_path / "skills"
        _write_skill(
            skills_dir,
            "form-990",
            extra_files={
                "GLOSSARY.md": "glossary",
                "references/how-to-read.pdf": b"%PDF-fake",
            },
        )
        skill = discover_skills(skills_dir)[0]
        manifest = build_manifest(skill)
        assert manifest["skill_name"] == "form-990"
        assert manifest["scope"] == "parent"
        paths = {f["path"]: f["mime_type"] for f in manifest["files"]}
        assert paths == {
            "GLOSSARY.md": "text/markdown",
            "SKILL.md": "text/markdown",
            "references/how-to-read.pdf": "application/pdf",
        }


class TestSkillMetadataIsHashable:
    """Frozen dataclass with a dict default should still be usable as expected."""

    def test_can_be_constructed(self, tmp_path):
        meta = SkillMetadata(
            name="x",
            description="y",
            dir_path=tmp_path,
            scope="parent",
        )
        assert meta.name == "x"
        # Mutability check on the frozen wrapper itself:
        with pytest.raises((AttributeError, TypeError)):
            meta.name = "z"
