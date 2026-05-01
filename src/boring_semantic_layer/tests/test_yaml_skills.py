"""Integration tests for skills_dir in YAML loading.

Verifies that:
- Loading a YAML with no skills_dir keys returns a bundle that behaves
  identically to the dict it used to be (backward compatible).
- Top-level ``skills_dir`` populates ``parent_skills``.
- Per-model ``skills_dir`` populates ``model_skills``.
- Overlapping parent/model skills_dir paths don't double-register.
- Relative paths resolve against the YAML file's parent dir, not CWD.
- Missing directories warn but don't error.
"""

from __future__ import annotations

from pathlib import Path

import ibis
import pytest

from boring_semantic_layer import SemanticModelBundle, SemanticTable, from_yaml


@pytest.fixture
def duckdb_conn():
    return ibis.duckdb.connect()


@pytest.fixture
def sample_tables(duckdb_conn):
    flights_data = {
        "carrier": ["AA", "UA"],
        "origin": ["JFK", "LAX"],
    }
    flights_tbl = duckdb_conn.create_table("flights_skills_test", flights_data)
    return {"flights_tbl": flights_tbl}


def _write_skill(skill_dir: Path, name: str, description: str) -> None:
    skill_dir.mkdir(parents=True)
    (skill_dir / "SKILL.md").write_text(
        f"---\nname: {name}\ndescription: {description}\n---\n# Body\n",
        encoding="utf-8",
    )


def _basic_yaml() -> str:
    return (
        "flights:\n"
        "  table: flights_tbl\n"
        "  dimensions:\n"
        "    origin: _.origin\n"
        "  measures:\n"
        "    flight_count: _.count()\n"
    )


class TestBundleBackwardCompat:
    """A YAML with no skills_dir must behave exactly like the old dict return."""

    def test_returns_bundle_that_acts_like_dict(self, sample_tables, tmp_path):
        yaml_path = tmp_path / "config.yml"
        yaml_path.write_text(_basic_yaml(), encoding="utf-8")

        bundle = from_yaml(str(yaml_path), tables=sample_tables)

        assert isinstance(bundle, SemanticModelBundle)
        assert "flights" in bundle
        assert isinstance(bundle["flights"], SemanticTable)
        assert list(bundle) == ["flights"]
        assert len(bundle) == 1
        assert dict(bundle.items()) == {"flights": bundle["flights"]}

    def test_empty_skills_when_not_configured(self, sample_tables, tmp_path):
        yaml_path = tmp_path / "config.yml"
        yaml_path.write_text(_basic_yaml(), encoding="utf-8")

        bundle = from_yaml(str(yaml_path), tables=sample_tables)
        assert bundle.parent_skills == []
        assert bundle.model_skills == {}
        assert bundle.has_skills is False


class TestParentSkillsDir:
    def test_top_level_skills_dir_populates_parent_skills(self, sample_tables, tmp_path):
        skills_dir = tmp_path / "skills"
        _write_skill(skills_dir / "form-990", "form-990", "Form 990 guidance")
        _write_skill(skills_dir / "grants", "grants", "Grant network glossary")

        yaml_path = tmp_path / "config.yml"
        yaml_path.write_text(
            "skills_dir: ./skills\n" + _basic_yaml(),
            encoding="utf-8",
        )

        bundle = from_yaml(str(yaml_path), tables=sample_tables)
        names = sorted(s.name for s in bundle.parent_skills)
        assert names == ["form-990", "grants"]
        assert bundle.has_skills is True

    def test_relative_path_anchors_to_yaml_dir_not_cwd(self, sample_tables, tmp_path, monkeypatch):
        skills_dir = tmp_path / "skills"
        _write_skill(skills_dir / "form-990", "form-990", "Form 990 guidance")

        yaml_path = tmp_path / "config.yml"
        yaml_path.write_text(
            "skills_dir: ./skills\n" + _basic_yaml(),
            encoding="utf-8",
        )

        # Run from a totally unrelated CWD to prove relative resolution
        # uses the YAML file's parent dir.
        monkeypatch.chdir(tmp_path.parent)
        bundle = from_yaml(str(yaml_path), tables=sample_tables)
        assert [s.name for s in bundle.parent_skills] == ["form-990"]

    def test_missing_skills_dir_warns_but_succeeds(self, sample_tables, tmp_path):
        yaml_path = tmp_path / "config.yml"
        yaml_path.write_text(
            "skills_dir: ./does-not-exist\n" + _basic_yaml(),
            encoding="utf-8",
        )

        with pytest.warns(UserWarning, match="does not exist"):
            bundle = from_yaml(str(yaml_path), tables=sample_tables)
        assert bundle.parent_skills == []
        assert bundle["flights"] is not None  # model still loaded

    def test_absolute_path_supported(self, sample_tables, tmp_path):
        skills_dir = tmp_path / "abs_skills"
        _write_skill(skills_dir / "form-990", "form-990", "abs")

        yaml_path = tmp_path / "config.yml"
        yaml_path.write_text(
            f"skills_dir: {skills_dir}\n" + _basic_yaml(),
            encoding="utf-8",
        )

        bundle = from_yaml(str(yaml_path), tables=sample_tables)
        assert [s.name for s in bundle.parent_skills] == ["form-990"]


class TestModelSkillsDir:
    def test_per_model_skills_dir_scoped_to_model(self, sample_tables, tmp_path):
        skills_dir = tmp_path / "skills" / "flights"
        _write_skill(skills_dir / "rev-analysis", "rev-analysis", "Revenue analysis")

        yaml_path = tmp_path / "config.yml"
        yaml_path.write_text(
            "flights:\n"
            "  table: flights_tbl\n"
            "  skills_dir: ./skills/flights\n"
            "  dimensions:\n"
            "    origin: _.origin\n"
            "  measures:\n"
            "    flight_count: _.count()\n",
            encoding="utf-8",
        )

        bundle = from_yaml(str(yaml_path), tables=sample_tables)
        assert bundle.parent_skills == []
        assert "flights" in bundle.model_skills
        assert [s.name for s in bundle.model_skills["flights"]] == ["rev-analysis"]
        assert bundle.model_skills["flights"][0].scope == "flights"

    def test_model_skills_dir_does_not_become_a_dimension(self, sample_tables, tmp_path):
        """skills_dir must be stripped from the model config before parsing."""
        skills_dir = tmp_path / "skills"
        _write_skill(skills_dir / "demo", "demo", "Demo skill")

        yaml_path = tmp_path / "config.yml"
        yaml_path.write_text(
            "flights:\n"
            "  table: flights_tbl\n"
            "  skills_dir: ./skills\n"
            "  dimensions:\n"
            "    origin: _.origin\n"
            "  measures:\n"
            "    flight_count: _.count()\n",
            encoding="utf-8",
        )

        bundle = from_yaml(str(yaml_path), tables=sample_tables)
        flights = bundle["flights"]
        assert "skills_dir" not in flights.dimensions
        assert "skills_dir" not in flights.measures


class TestOverlapExclusion:
    def test_model_skills_dir_excluded_from_parent_scan(self, sample_tables, tmp_path):
        # parent_skills_dir = ./skills. Model skills_dir = ./skills/flights.
        # The 'flights' subdirectory under parent contains a SKILL.md too;
        # it must NOT show up in parent_skills.
        skills_root = tmp_path / "skills"
        _write_skill(skills_root / "form-990", "form-990", "fm")
        _write_skill(skills_root / "flights", "flights-skill", "model-scoped")

        yaml_path = tmp_path / "config.yml"
        yaml_path.write_text(
            "skills_dir: ./skills\n"
            "flights:\n"
            "  table: flights_tbl\n"
            "  skills_dir: ./skills/flights\n"
            "  dimensions:\n"
            "    origin: _.origin\n"
            "  measures:\n"
            "    flight_count: _.count()\n",
            encoding="utf-8",
        )

        bundle = from_yaml(str(yaml_path), tables=sample_tables)
        # Parent skills should contain ONLY form-990 — not the flights subdir
        parent_names = sorted(s.name for s in bundle.parent_skills)
        assert parent_names == ["form-990"]

        # The model-scoped scan operates on the directory itself — that means
        # discover_skills(./skills/flights) looks for SKILL.md inside immediate
        # subdirectories of ./skills/flights, which has none in this test.
        # So model_skills["flights"] should be empty.
        assert bundle.model_skills.get("flights", []) == []

    def test_distinct_dirs_register_independently(self, sample_tables, tmp_path):
        parent_dir = tmp_path / "parent_skills"
        model_dir = tmp_path / "model_skills"
        _write_skill(parent_dir / "shared", "shared", "shared context")
        _write_skill(model_dir / "specific", "specific", "model context")

        yaml_path = tmp_path / "config.yml"
        yaml_path.write_text(
            f"skills_dir: {parent_dir}\n"
            "flights:\n"
            "  table: flights_tbl\n"
            f"  skills_dir: {model_dir}\n"
            "  dimensions:\n"
            "    origin: _.origin\n"
            "  measures:\n"
            "    flight_count: _.count()\n",
            encoding="utf-8",
        )

        bundle = from_yaml(str(yaml_path), tables=sample_tables)
        assert [s.name for s in bundle.parent_skills] == ["shared"]
        assert [s.name for s in bundle.model_skills["flights"]] == ["specific"]


class TestNoSkillsRegression:
    def test_existing_yaml_without_skills_dir_unchanged(self, sample_tables, tmp_path):
        """A YAML that didn't know about skills_dir must produce identical
        models. Verify nothing about the underlying SemanticTable changed."""
        yaml_path = tmp_path / "config.yml"
        yaml_path.write_text(
            "flights:\n"
            "  table: flights_tbl\n"
            "  description: Flight data\n"
            "  dimensions:\n"
            "    origin: _.origin\n"
            "    carrier: _.carrier\n"
            "  measures:\n"
            "    flight_count: _.count()\n",
            encoding="utf-8",
        )

        bundle = from_yaml(str(yaml_path), tables=sample_tables)
        flights = bundle["flights"]
        assert flights.name == "flights"
        assert set(flights.dimensions) == {"origin", "carrier"}
        assert "flight_count" in flights.measures
        # Run an actual query to confirm nothing is broken downstream
        result = flights.group_by("origin").aggregate("flight_count").execute()
        assert len(result) == 2
