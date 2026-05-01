"""Integration tests for skill resources and tools on MCPSemanticModel.

Verifies:
- Bundle without skills → no skill resources, no skill tools registered.
- Bundle with skills → resources at the right URIs, get_domain_context shape.
- add_skill writes a SKILL.md and re-registers resources at runtime.
- Constructor flags suppress get_domain_context / add_skill registration.
- Binary skill assets are served as base64 BlobResourceContents.
- Per-model and parent skills are namespaced correctly.
"""

from __future__ import annotations

import base64
import json
from pathlib import Path

import ibis
import pandas as pd
import pytest
from fastmcp import Client
from fastmcp.exceptions import ToolError

from boring_semantic_layer import MCPSemanticModel, from_yaml


@pytest.fixture(scope="module")
def con():
    return ibis.duckdb.connect(":memory:")


@pytest.fixture(scope="module")
def sample_table(con):
    df = pd.DataFrame({"origin": ["JFK", "LAX"], "carrier": ["AA", "UA"]})
    con.create_table("flights_skills", df, overwrite=True)
    return con.table("flights_skills")


def _write_skill(skill_dir: Path, name: str, description: str, body: str = "body\n") -> None:
    skill_dir.mkdir(parents=True, exist_ok=True)
    (skill_dir / "SKILL.md").write_text(
        f"---\nname: {name}\ndescription: {description}\n---\n{body}",
        encoding="utf-8",
    )


def _write_yaml(path: Path, contents: str) -> None:
    path.write_text(contents, encoding="utf-8")


# ---------------------------------------------------------------------------
# No-skills baseline
# ---------------------------------------------------------------------------


class TestNoSkillsBaseline:
    @pytest.mark.asyncio
    async def test_no_skill_tools_registered(self, sample_table, tmp_path):
        yaml_path = tmp_path / "cfg.yml"
        _write_yaml(
            yaml_path,
            "flights:\n  table: flights_tbl\n  dimensions:\n    origin: _.origin\n  measures:\n    flight_count: _.count()\n",
        )
        bundle = from_yaml(str(yaml_path), tables={"flights_tbl": sample_table})
        mcp = MCPSemanticModel(bundle)

        async with Client(mcp) as client:
            tool_names = {t.name for t in await client.list_tools()}
            assert "get_domain_context" not in tool_names
            assert "add_skill" not in tool_names
            # Sanity — existing tools still present
            assert "list_models" in tool_names
            assert "query_model" in tool_names

    @pytest.mark.asyncio
    async def test_no_skill_resources_registered(self, sample_table, tmp_path):
        yaml_path = tmp_path / "cfg.yml"
        _write_yaml(
            yaml_path,
            "flights:\n  table: flights_tbl\n  dimensions:\n    origin: _.origin\n  measures:\n    flight_count: _.count()\n",
        )
        bundle = from_yaml(str(yaml_path), tables={"flights_tbl": sample_table})
        mcp = MCPSemanticModel(bundle)

        async with Client(mcp) as client:
            uris = {str(r.uri) for r in await client.list_resources()}
            assert not any(u.startswith("skill://") for u in uris)
            assert "semantic://models" in uris


# ---------------------------------------------------------------------------
# Parent-level skills
# ---------------------------------------------------------------------------


class TestParentSkills:
    @pytest.mark.asyncio
    async def test_skill_resources_registered(self, sample_table, tmp_path):
        skills_dir = tmp_path / "skills"
        _write_skill(skills_dir / "form-990", "form-990", "tax form guidance")

        yaml_path = tmp_path / "cfg.yml"
        _write_yaml(
            yaml_path,
            "skills_dir: ./skills\nflights:\n  table: flights_tbl\n  dimensions:\n    origin: _.origin\n  measures:\n    flight_count: _.count()\n",
        )
        bundle = from_yaml(str(yaml_path), tables={"flights_tbl": sample_table})
        mcp = MCPSemanticModel(bundle)

        async with Client(mcp) as client:
            uris = {str(r.uri) for r in await client.list_resources()}
            assert "skill://form-990/SKILL.md" in uris
            assert "skill://form-990/_manifest" in uris

            body = await client.read_resource("skill://form-990/SKILL.md")
            assert "name: form-990" in body[0].text
            assert "body" in body[0].text

            manifest = json.loads(
                (await client.read_resource("skill://form-990/_manifest"))[0].text
            )
            assert manifest["skill_name"] == "form-990"
            assert manifest["scope"] == "parent"
            assert any(f["path"] == "SKILL.md" for f in manifest["files"])

    @pytest.mark.asyncio
    async def test_get_domain_context_shape(self, sample_table, tmp_path):
        skills_dir = tmp_path / "skills"
        _write_skill(skills_dir / "form-990", "form-990", "tax form guidance")
        _write_skill(skills_dir / "grants", "grants", "grant network glossary")

        yaml_path = tmp_path / "cfg.yml"
        _write_yaml(
            yaml_path,
            "skills_dir: ./skills\nflights:\n  table: flights_tbl\n  description: Flight data\n  dimensions:\n    origin: _.origin\n  measures:\n    flight_count: _.count()\n",
        )
        bundle = from_yaml(str(yaml_path), tables={"flights_tbl": sample_table})
        mcp = MCPSemanticModel(bundle)

        async with Client(mcp) as client:
            res = await client.call_tool("get_domain_context", {})
            payload = json.loads(res.content[0].text)
            parent_names = sorted(s["name"] for s in payload["parent_skills"])
            assert parent_names == ["form-990", "grants"]
            for s in payload["parent_skills"]:
                assert "uri_prefix" in s
                assert s["uri_prefix"].startswith("skill://")
                assert "files" in s
            assert payload["model_skills"] == {}
            model_names = sorted(m["name"] for m in payload["models"])
            assert model_names == ["flights"]


# ---------------------------------------------------------------------------
# Per-model skills
# ---------------------------------------------------------------------------


class TestModelScopedSkills:
    @pytest.mark.asyncio
    async def test_model_skill_uri_namespaced(self, sample_table, tmp_path):
        skills_dir = tmp_path / "skills" / "flights"
        _write_skill(skills_dir / "rev-analysis", "rev-analysis", "revenue trends")

        yaml_path = tmp_path / "cfg.yml"
        _write_yaml(
            yaml_path,
            "flights:\n  table: flights_tbl\n  skills_dir: ./skills/flights\n  dimensions:\n    origin: _.origin\n  measures:\n    flight_count: _.count()\n",
        )
        bundle = from_yaml(str(yaml_path), tables={"flights_tbl": sample_table})
        mcp = MCPSemanticModel(bundle)

        async with Client(mcp) as client:
            uris = {str(r.uri) for r in await client.list_resources()}
            # Model-scoped URI shape: skill://<model>/<skill_name>/SKILL.md
            assert "skill://flights/rev-analysis/SKILL.md" in uris
            assert "skill://flights/rev-analysis/_manifest" in uris

            ctx = json.loads((await client.call_tool("get_domain_context", {})).content[0].text)
            assert ctx["parent_skills"] == []
            assert "flights" in ctx["model_skills"]
            assert ctx["model_skills"]["flights"][0]["name"] == "rev-analysis"
            assert ctx["model_skills"]["flights"][0]["uri_prefix"] == (
                "skill://flights/rev-analysis/"
            )


# ---------------------------------------------------------------------------
# add_skill tool — write + re-register
# ---------------------------------------------------------------------------


class TestAddSkill:
    @pytest.mark.asyncio
    async def test_writes_skill_md_and_registers_resource(self, sample_table, tmp_path):
        skills_dir = tmp_path / "skills"
        skills_dir.mkdir()  # empty parent dir is enough to enable add_skill

        yaml_path = tmp_path / "cfg.yml"
        _write_yaml(
            yaml_path,
            "skills_dir: ./skills\nflights:\n  table: flights_tbl\n  dimensions:\n    origin: _.origin\n  measures:\n    flight_count: _.count()\n",
        )
        bundle = from_yaml(str(yaml_path), tables={"flights_tbl": sample_table})
        mcp = MCPSemanticModel(bundle)

        async with Client(mcp) as client:
            res = await client.call_tool(
                "add_skill",
                {
                    "name": "mission-alignment",
                    "description": "Use when comparing missions",
                    "steps": "## Steps\n1. Pull mission text\n2. Compare\n",
                },
            )
            payload = json.loads(res.content[0].text)
            assert payload["status"] == "created"
            assert payload["scope"] == "parent"
            assert payload["uri_prefix"] == "skill://mission-alignment/"

            # File on disk
            written = (skills_dir / "mission-alignment" / "SKILL.md").read_text(encoding="utf-8")
            assert "name: mission-alignment" in written
            assert "1. Pull mission text" in written

            # Resource registered at runtime
            uris = {str(r.uri) for r in await client.list_resources()}
            assert "skill://mission-alignment/SKILL.md" in uris

            # And get_domain_context picks it up on next call
            ctx = json.loads((await client.call_tool("get_domain_context", {})).content[0].text)
            names = [s["name"] for s in ctx["parent_skills"]]
            assert "mission-alignment" in names

    @pytest.mark.asyncio
    async def test_invalid_kebab_case_raises(self, sample_table, tmp_path):
        skills_dir = tmp_path / "skills"
        skills_dir.mkdir()
        yaml_path = tmp_path / "cfg.yml"
        _write_yaml(
            yaml_path,
            "skills_dir: ./skills\nflights:\n  table: flights_tbl\n  dimensions:\n    origin: _.origin\n  measures:\n    flight_count: _.count()\n",
        )
        bundle = from_yaml(str(yaml_path), tables={"flights_tbl": sample_table})
        mcp = MCPSemanticModel(bundle)

        async with Client(mcp) as client:
            with pytest.raises(ToolError, match="kebab-case"):
                await client.call_tool(
                    "add_skill",
                    {
                        "name": "Bad_Name",
                        "description": "x",
                        "steps": "x",
                    },
                )

    @pytest.mark.asyncio
    async def test_refuses_overwrite(self, sample_table, tmp_path):
        skills_dir = tmp_path / "skills"
        _write_skill(skills_dir / "exists", "exists", "already there")
        yaml_path = tmp_path / "cfg.yml"
        _write_yaml(
            yaml_path,
            "skills_dir: ./skills\nflights:\n  table: flights_tbl\n  dimensions:\n    origin: _.origin\n  measures:\n    flight_count: _.count()\n",
        )
        bundle = from_yaml(str(yaml_path), tables={"flights_tbl": sample_table})
        mcp = MCPSemanticModel(bundle)

        async with Client(mcp) as client:
            with pytest.raises(ToolError, match="already exists"):
                await client.call_tool(
                    "add_skill",
                    {
                        "name": "exists",
                        "description": "x",
                        "steps": "x",
                    },
                )

    @pytest.mark.asyncio
    async def test_model_name_falls_back_to_parent(self, sample_table, tmp_path):
        """If a model has no skills_dir but the parent does, model_name targets parent."""
        parent_skills = tmp_path / "parent"
        parent_skills.mkdir()
        yaml_path = tmp_path / "cfg.yml"
        _write_yaml(
            yaml_path,
            f"skills_dir: {parent_skills}\nflights:\n  table: flights_tbl\n  dimensions:\n    origin: _.origin\n  measures:\n    flight_count: _.count()\n",
        )
        bundle = from_yaml(str(yaml_path), tables={"flights_tbl": sample_table})
        mcp = MCPSemanticModel(bundle)

        async with Client(mcp) as client:
            res = await client.call_tool(
                "add_skill",
                {
                    "name": "fallback",
                    "description": "x",
                    "steps": "x",
                    "model_name": "flights",  # flights has no skills_dir
                },
            )
            payload = json.loads(res.content[0].text)
            # Falls back to parent scope
            assert payload["scope"] == "parent"
            assert (parent_skills / "fallback" / "SKILL.md").exists()


# ---------------------------------------------------------------------------
# Constructor flag suppression
# ---------------------------------------------------------------------------


class TestConstructorFlags:
    @pytest.mark.asyncio
    async def test_suppress_domain_context(self, sample_table, tmp_path):
        skills_dir = tmp_path / "skills"
        _write_skill(skills_dir / "demo", "demo", "x")
        yaml_path = tmp_path / "cfg.yml"
        _write_yaml(
            yaml_path,
            "skills_dir: ./skills\nflights:\n  table: flights_tbl\n  dimensions:\n    origin: _.origin\n  measures:\n    flight_count: _.count()\n",
        )
        bundle = from_yaml(str(yaml_path), tables={"flights_tbl": sample_table})
        mcp = MCPSemanticModel(bundle, include_domain_context_tool=False)

        async with Client(mcp) as client:
            tool_names = {t.name for t in await client.list_tools()}
            assert "get_domain_context" not in tool_names
            assert "add_skill" in tool_names  # still registered
            # Resources still registered (data, not tools)
            uris = {str(r.uri) for r in await client.list_resources()}
            assert "skill://demo/SKILL.md" in uris

    @pytest.mark.asyncio
    async def test_suppress_add_skill(self, sample_table, tmp_path):
        skills_dir = tmp_path / "skills"
        _write_skill(skills_dir / "demo", "demo", "x")
        yaml_path = tmp_path / "cfg.yml"
        _write_yaml(
            yaml_path,
            "skills_dir: ./skills\nflights:\n  table: flights_tbl\n  dimensions:\n    origin: _.origin\n  measures:\n    flight_count: _.count()\n",
        )
        bundle = from_yaml(str(yaml_path), tables={"flights_tbl": sample_table})
        mcp = MCPSemanticModel(bundle, include_add_skill_tool=False)

        async with Client(mcp) as client:
            tool_names = {t.name for t in await client.list_tools()}
            assert "add_skill" not in tool_names
            assert "get_domain_context" in tool_names

    @pytest.mark.asyncio
    async def test_plain_dict_models_still_supported(self, sample_table):
        """Backward compat: existing callers passing a plain dict get no skill features."""
        from boring_semantic_layer import to_semantic_table

        model = (
            to_semantic_table(sample_table, name="flights")
            .with_dimensions(origin=lambda t: t.origin)
            .with_measures(flight_count=lambda t: t.count())
        )
        mcp = MCPSemanticModel({"flights": model})  # plain dict, not bundle

        async with Client(mcp) as client:
            tool_names = {t.name for t in await client.list_tools()}
            assert "list_models" in tool_names
            assert "get_domain_context" not in tool_names
            assert "add_skill" not in tool_names


# ---------------------------------------------------------------------------
# Binary resource handling
# ---------------------------------------------------------------------------


class TestBinaryResources:
    @pytest.mark.asyncio
    async def test_pdf_served_as_blob(self, sample_table, tmp_path):
        skills_dir = tmp_path / "skills"
        _write_skill(skills_dir / "with-pdf", "with-pdf", "has pdf")
        pdf_bytes = b"%PDF-1.4\n%fake content\n%%EOF"
        (skills_dir / "with-pdf" / "guide.pdf").write_bytes(pdf_bytes)

        yaml_path = tmp_path / "cfg.yml"
        _write_yaml(
            yaml_path,
            "skills_dir: ./skills\nflights:\n  table: flights_tbl\n  dimensions:\n    origin: _.origin\n  measures:\n    flight_count: _.count()\n",
        )
        bundle = from_yaml(str(yaml_path), tables={"flights_tbl": sample_table})
        mcp = MCPSemanticModel(bundle)

        async with Client(mcp) as client:
            uris = {str(r.uri) for r in await client.list_resources()}
            assert "skill://with-pdf/guide.pdf" in uris

            content = await client.read_resource("skill://with-pdf/guide.pdf")
            blob = content[0]
            # FastMCP returns BlobResourceContents for bytes returns
            assert hasattr(blob, "blob")
            assert base64.b64decode(blob.blob) == pdf_bytes
            assert blob.mimeType == "application/pdf"
