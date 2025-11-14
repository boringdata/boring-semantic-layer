"""Test MCP parameter validation and schema generation."""

import json
from typing import Annotated, Any

import ibis
import pytest
from pydantic import TypeAdapter, ValidationError
from pydantic.functional_validators import BeforeValidator

from boring_semantic_layer import MCPSemanticModel, to_semantic_table

# Mark all async tests to use anyio with asyncio backend only
pytestmark = pytest.mark.anyio


# Configure anyio to only use asyncio backend
@pytest.fixture
def anyio_backend():
    return "asyncio"


def _parse_json_string(v: Any) -> Any:
    """Parse JSON-stringified parameters (same as in mcp.py)."""
    if isinstance(v, str):
        try:
            return json.loads(v)
        except (json.JSONDecodeError, ValueError):
            return v
    return v


@pytest.fixture
def mcp_server():
    """Create an MCP server with test data for all tests."""
    test_data = ibis.memtable(
        {
            "a": [1, 2, 3],
            "b": ["x", "y", "z"],
        }
    )
    model = (
        to_semantic_table(test_data, name="test")
        .with_dimensions(
            a={"expr": lambda t: t.a},
            b={"expr": lambda t: t.b},
        )
        .with_measures(count={"expr": lambda t: t.count()})
    )
    return MCPSemanticModel(models={"test": model})


class TestJSONStringParsing:
    """Test that JSON-stringified parameters are parsed correctly."""

    def test_parse_json_string_with_array(self):
        """Test that JSON string arrays are parsed to actual arrays."""
        ParsedList = Annotated[list[str] | None, BeforeValidator(_parse_json_string)]
        adapter = TypeAdapter(ParsedList)

        # Claude Desktop sends this
        result = adapter.validate_python('["a", "b", "c"]')
        assert result == ["a", "b", "c"]
        assert isinstance(result, list)

    def test_parse_json_string_with_nested_array(self):
        """Test that nested JSON arrays (order_by) are parsed correctly."""
        ParsedOrderBy = Annotated[list[list[str]] | None, BeforeValidator(_parse_json_string)]
        adapter = TypeAdapter(ParsedOrderBy)

        result = adapter.validate_python('[["field", "asc"], ["field2", "desc"]]')
        assert result == [["field", "asc"], ["field2", "desc"]]

    def test_parse_json_string_with_object(self):
        """Test that JSON objects are parsed correctly."""
        ParsedDict = Annotated[dict[str, Any] | None, BeforeValidator(_parse_json_string)]
        adapter = TypeAdapter(ParsedDict)

        result = adapter.validate_python('{"start": "2024-01-01", "end": "2024-12-31"}')
        assert result == {"start": "2024-01-01", "end": "2024-12-31"}

    def test_actual_arrays_pass_through(self):
        """Test that actual arrays are not modified (backward compatibility)."""
        ParsedList = Annotated[list[str] | None, BeforeValidator(_parse_json_string)]
        adapter = TypeAdapter(ParsedList)

        result = adapter.validate_python(["a", "b", "c"])
        assert result == ["a", "b", "c"]

    def test_none_values_work(self):
        """Test that None values are handled correctly."""
        ParsedList = Annotated[list[str] | None, BeforeValidator(_parse_json_string)]
        adapter = TypeAdapter(ParsedList)

        result = adapter.validate_python(None)
        assert result is None

    def test_invalid_json_rejected(self):
        """Test that invalid JSON strings are rejected by Pydantic."""
        ParsedList = Annotated[list[str] | None, BeforeValidator(_parse_json_string)]
        adapter = TypeAdapter(ParsedList)

        with pytest.raises(ValidationError):
            adapter.validate_python("not valid json")

    def test_wrong_type_json_rejected(self):
        """Test that JSON of wrong type is rejected."""
        ParsedList = Annotated[list[str] | None, BeforeValidator(_parse_json_string)]
        adapter = TypeAdapter(ParsedList)

        with pytest.raises(ValidationError):
            # Valid JSON but wrong type (object instead of array)
            adapter.validate_python('{"key": "value"}')

    async def test_claude_desktop_json_dimensions(self, mcp_server):
        """Test that Claude Desktop's JSON-stringified dimensions work (Issue #97)."""
        tool = mcp_server._tool_manager._tools["query_model"]

        # Claude Desktop sends JSON-stringified arrays
        result = await tool.run(
            {
                "model_name": "test",
                "dimensions": '["b"]',  # JSON string instead of actual array
                "measures": '["count"]',
            }
        )

        result_data = json.loads(result.content[0].text)
        assert "records" in result_data
        assert len(result_data["records"]) == 3

    async def test_claude_desktop_json_order_by(self, mcp_server):
        """Test that Claude Desktop's JSON-stringified order_by works (Issue #97)."""
        tool = mcp_server._tool_manager._tools["query_model"]

        # Test nested array JSON string
        result = await tool.run(
            {
                "model_name": "test",
                "dimensions": '["b"]',
                "measures": '["count"]',
                "order_by": '[["count", "desc"]]',  # Nested JSON string
            }
        )

        result_data = json.loads(result.content[0].text)
        assert "records" in result_data

    async def test_backward_compatibility_actual_arrays(self, mcp_server):
        """Test that actual arrays still work (backward compatibility)."""
        tool = mcp_server._tool_manager._tools["query_model"]

        # Regular MCP clients send actual arrays
        result = await tool.run(
            {
                "model_name": "test",
                "dimensions": ["b"],  # Actual array
                "measures": ["count"],
            }
        )

        result_data = json.loads(result.content[0].text)
        assert "records" in result_data
        assert len(result_data["records"]) == 3

    async def test_json_filters(self, mcp_server):
        """Test that JSON-stringified filters work."""
        tool = mcp_server._tool_manager._tools["query_model"]

        result = await tool.run(
            {
                "model_name": "test",
                "dimensions": '["b"]',
                "measures": '["count"]',
                "filters": '[{"field": "b", "operator": "=", "value": "x"}]',
            }
        )

        result_data = json.loads(result.content[0].text)
        assert "records" in result_data
        assert len(result_data["records"]) == 1


class TestSchemaGeneration:
    """Test that schema generation includes proper 'items' definitions."""

    def test_order_by_has_items_in_schema(self, mcp_server):
        """Test that order_by has proper 'items' key for Azure OpenAI compatibility."""
        tool = mcp_server._tool_manager._tools["query_model"]
        schema = tool.model_dump()["parameters"]

        order_by_schema = schema["properties"]["order_by"]

        # Check anyOf contains array with items
        assert "anyOf" in order_by_schema

        array_schema = None
        for option in order_by_schema["anyOf"]:
            if option.get("type") == "array":
                array_schema = option
                break

        assert array_schema is not None, "order_by should have array type"
        assert "items" in array_schema, "order_by must have 'items' key for Azure OpenAI"

        # Verify nested structure
        items = array_schema["items"]
        assert items.get("type") == "array"
        assert "items" in items
        assert items["items"].get("type") == "string"

    def test_all_array_parameters_have_items(self, mcp_server):
        """Test that all array parameters have 'items' in their schema."""
        tool = mcp_server._tool_manager._tools["query_model"]
        schema = tool.model_dump()["parameters"]

        array_params = ["dimensions", "measures", "filters", "order_by"]

        for param in array_params:
            param_schema = schema["properties"][param]

            if "anyOf" in param_schema:
                for option in param_schema["anyOf"]:
                    if option.get("type") == "array":
                        assert "items" in option, f"{param} array must have 'items' key"

    def test_all_parameters_have_descriptions(self, mcp_server):
        """Test that all parameters have descriptions for better UX."""
        tool = mcp_server._tool_manager._tools["query_model"]
        schema = tool.model_dump()["parameters"]

        params = [
            "dimensions",
            "measures",
            "filters",
            "order_by",
            "limit",
            "time_grain",
            "time_range",
            "chart_spec",
        ]

        for param in params:
            param_schema = schema["properties"][param]
            assert "description" in param_schema, f"{param} should have description"
            assert len(param_schema["description"]) > 0

    def test_schema_structure_compatible_with_azure_openai(self, mcp_server):
        """Test that schema structure meets Azure OpenAI requirements."""
        tool = mcp_server._tool_manager._tools["query_model"]
        schema = tool.model_dump()["parameters"]

        # Azure OpenAI requires these fields
        assert "type" in schema
        assert schema["type"] == "object"
        assert "properties" in schema
        assert "required" in schema
        assert "model_name" in schema["required"]


class TestTypeHintImprovements:
    """Test that type hint changes from Sequence[tuple] to list[list] work correctly."""

    def test_order_by_accepts_list_of_lists(self):
        """Test that order_by accepts list[list[str]] format."""
        # This simulates the type change from Sequence[tuple[str, str]] to list[list[str]]
        OrderByType = Annotated[list[list[str]] | None, BeforeValidator(_parse_json_string)]
        adapter = TypeAdapter(OrderByType)

        # JSON format (what MCP sends)
        result = adapter.validate_python('[["field1", "asc"], ["field2", "desc"]]')
        assert result == [["field1", "asc"], ["field2", "desc"]]

        # Python format (backward compat)
        result = adapter.validate_python([["field1", "asc"], ["field2", "desc"]])
        assert result == [["field1", "asc"], ["field2", "desc"]]

    def test_list_type_generates_better_schema_than_sequence(self):
        """Test that list[str] generates clearer schema than Sequence[str]."""
        from pydantic import Field

        # New style (list)
        ListType = Annotated[list[str] | None, Field(default=None)]
        list_adapter = TypeAdapter(ListType)
        list_schema = list_adapter.json_schema()

        # Verify it has proper structure
        assert "anyOf" in list_schema
        for option in list_schema["anyOf"]:
            if option.get("type") == "array":
                assert "items" in option
                assert option["items"].get("type") == "string"


class TestIntegration:
    """Integration tests to verify the complete fix works."""

    def test_schema_has_all_required_fields(self, mcp_server):
        """Test that the generated schema has all required fields for MCP clients."""
        tool = mcp_server._tool_manager._tools["query_model"]
        schema = tool.model_dump()["parameters"]

        # Verify structure
        assert schema["type"] == "object"
        assert "properties" in schema
        assert "required" in schema

        # Verify all parameters exist
        expected_params = {
            "model_name",
            "dimensions",
            "measures",
            "filters",
            "order_by",
            "limit",
            "time_grain",
            "time_range",
            "chart_spec",
        }
        assert expected_params.issubset(set(schema["properties"].keys()))

        # Verify order_by specifically
        order_by = schema["properties"]["order_by"]
        assert "description" in order_by

        # Find array schema in anyOf
        for option in order_by["anyOf"]:
            if option.get("type") == "array":
                assert "items" in option, "order_by must have items for Azure OpenAI compatibility"

    def test_validator_function_exists_and_works(self):
        """Test that the _parse_json_string validator function works correctly."""
        # Test with string
        assert _parse_json_string('["a", "b"]') == ["a", "b"]

        # Test with list (pass through)
        assert _parse_json_string(["a", "b"]) == ["a", "b"]

        # Test with None
        assert _parse_json_string(None) is None

        # Test with invalid JSON (returns as-is for Pydantic to reject)
        assert _parse_json_string("invalid") == "invalid"

    async def test_end_to_end_claude_desktop_query(self, mcp_server):
        """Test a complete end-to-end query simulating Claude Desktop behavior."""
        tool = mcp_server._tool_manager._tools["query_model"]

        # Simulate what Claude Desktop actually sends
        result = await tool.run(
            {
                "model_name": "test",
                "dimensions": '["b"]',
                "measures": '["count"]',
                "order_by": '[["count", "desc"]]',
                "limit": 10,
            }
        )

        result_data = json.loads(result.content[0].text)
        assert "records" in result_data
        assert len(result_data["records"]) > 0

        # Verify the data makes sense
        for record in result_data["records"]:
            assert "b" in record
            assert "count" in record

    async def test_error_handling_invalid_json(self, mcp_server):
        """Test that invalid JSON strings are properly rejected."""
        tool = mcp_server._tool_manager._tools["query_model"]

        with pytest.raises(Exception):  # Should raise validation error
            await tool.run(
                {
                    "model_name": "test",
                    "dimensions": "not valid json",  # Invalid JSON
                }
            )

    async def test_mixed_json_and_native_params(self, mcp_server):
        """Test that JSON strings and native types can be mixed."""
        tool = mcp_server._tool_manager._tools["query_model"]

        result = await tool.run(
            {
                "model_name": "test",
                "dimensions": '["b"]',  # JSON string
                "measures": ["count"],  # Native array
                "limit": 5,  # Native int
            }
        )

        result_data = json.loads(result.content[0].text)
        assert "records" in result_data
        assert len(result_data["records"]) == 3
