"""Tests for _make_schema, especially Postgres VARCHAR(N) handling."""

from __future__ import annotations

from boring_semantic_layer.ops import _make_schema


def test_make_schema_plain_types():
    schema = _make_schema({"a": "string", "b": "int32"})
    assert "a" in schema
    assert "b" in schema


def test_make_schema_postgres_varchar():
    """Postgres VARCHAR(50) serialises as '!string(50)'; Schema must accept it."""
    schema = _make_schema({"col_a": "!string(50)", "col_b": "!int32"})
    assert "col_a" in schema
    assert "col_b" in schema


def test_make_schema_mixed_postgres_types():
    """Realistic Postgres join schema with VARCHAR, DECIMAL, and plain types."""
    schema = _make_schema({
        "snapshot_date": "!date",
        "balance": "!decimal(15, 2)",
        "bank_id": "string",
        "bank_name": "!string(50)",
        "bank": "!string(50)",
    })
    assert set(schema.names) == {"snapshot_date", "balance", "bank_id", "bank_name", "bank"}
    # decimal(15, 2) must be preserved
    assert "decimal" in str(schema["balance"])


def test_make_schema_string_without_length_unchanged():
    """Plain 'string' type must not be mangled by the regex."""
    schema = _make_schema({"a": "string", "b": "!string"})
    assert "a" in schema
    assert "b" in schema
