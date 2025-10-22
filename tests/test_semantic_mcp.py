"""Tests for MCPSemanticModel using FastMCP client-server pattern with SemanticTable."""

import pytest
import json
import pandas as pd
import ibis

from boring_semantic_layer import MCPSemanticModel
from boring_semantic_layer.semantic_api import to_semantic_table
from fastmcp import Client
from fastmcp.exceptions import ToolError


@pytest.fixture(scope="module")
def con():
    """DuckDB connection for all tests."""
    return ibis.duckdb.connect(":memory:")


@pytest.fixture(scope="module")
def sample_models(con):
    """Create sample semantic tables for testing."""
    # Create sample data
    flights_df = pd.DataFrame({
        "origin": ["JFK", "LAX", "ORD"] * 10,
        "destination": ["LAX", "JFK", "DEN"] * 10,
        "carrier": ["AA", "UA", "DL"] * 10,
        "flight_date": pd.date_range("2024-01-01", periods=30, freq="D"),
        "dep_delay": [5.2, 8.1, 3.5] * 10,
    })

    carriers_df = pd.DataFrame({
        "code": ["AA", "UA", "DL"],
        "name": ["American", "United", "Delta"],
    })

    flights_tbl = con.create_table("flights", flights_df, overwrite=True)
    carriers_tbl = con.create_table("carriers", carriers_df, overwrite=True)

    # Model with time dimension
    flights_model = (
        to_semantic_table(flights_tbl, name="flights", description="Sample flights model")
        .with_dimensions(
            origin=lambda t: t.origin,
            destination=lambda t: t.destination,
            carrier=lambda t: t.carrier,
            flight_date={
                "expr": lambda t: t.flight_date,
                "description": "Flight departure date",
                "is_time_dimension": True,
                "smallest_time_grain": "day"
            }
        )
        .with_measures(
            flight_count={
                "expr": lambda t: t.count(),
                "description": "Total number of flights"
            },
            avg_delay={
                "expr": lambda t: t.dep_delay.mean(),
                "description": "Average departure delay"
            }
        )
    )

    # Model without time dimension
    carriers_model = (
        to_semantic_table(carriers_tbl, name="carriers")
        .with_dimensions(
            code={
                "expr": lambda t: t.code,
                "description": "Carrier code"
            },
            name=lambda t: t.name
        )
        .with_measures(
            carrier_count=lambda t: t.count()
        )
    )

    return {
        "flights": flights_model,
        "carriers": carriers_model,
    }


class TestMCPSemanticModelInitialization:
    """Test MCPSemanticModel initialization."""

    def test_init_with_models(self, sample_models):
        """Test initialization with semantic models."""
        mcp = MCPSemanticModel(models=sample_models, name="Test MCP Server")

        assert mcp.models == sample_models
        assert mcp.name == "Test MCP Server"

    def test_init_empty_models(self):
        """Test initialization with empty models dict."""
        mcp = MCPSemanticModel(models={}, name="Empty Server")

        assert mcp.models == {}
        assert mcp.name == "Empty Server"

    @pytest.mark.asyncio
    async def test_tools_are_registered(self, sample_models):
        """Test that all tools are registered during init."""
        mcp = MCPSemanticModel(models=sample_models)

        async with Client(mcp) as client:
            # Check that tools are registered
            tools = await client.list_tools()
            tool_names = [tool.name for tool in tools]
            assert "list_models" in tool_names
            assert "get_model" in tool_names
            assert "get_time_range" in tool_names
            assert "query_model" in tool_names


class TestListModelsTool:
    """Test list_models tool."""

    @pytest.mark.asyncio
    async def test_list_models_returns_all_names(self, sample_models):
        """Test that list_models returns all model names with descriptions."""
        mcp = MCPSemanticModel(models=sample_models)

        async with Client(mcp) as client:
            # Call the list_models tool
            result = await client.call_tool("list_models", {})
            data = json.loads(result.content[0].text)

            assert isinstance(data, dict)
            assert set(data.keys()) == {"flights", "carriers"}
            assert data["flights"] == "Sample flights model"
            assert data["carriers"] == "No description available"

    @pytest.mark.asyncio
    async def test_list_models_empty(self):
        """Test list_models with no models."""
        mcp = MCPSemanticModel(models={})

        async with Client(mcp) as client:
            result = await client.call_tool("list_models", {})
            data = json.loads(result.content[0].text)

            assert data == {}


class TestGetModelTool:
    """Test get_model tool."""

    @pytest.mark.asyncio
    async def test_get_model_returns_metadata(self, sample_models):
        """Test that get_model returns model metadata with dimensions and measures."""
        mcp = MCPSemanticModel(models=sample_models)

        async with Client(mcp) as client:
            result = await client.call_tool("get_model", {"model_name": "flights"})
            data = json.loads(result.content[0].text)

            assert data["name"] == "flights"

            # Check dimensions
            assert "origin" in data["dimensions"]
            assert "destination" in data["dimensions"]
            assert "carrier" in data["dimensions"]
            assert "flight_date" in data["dimensions"]

            # Check time dimension metadata
            flight_date = data["dimensions"]["flight_date"]
            assert flight_date["description"] == "Flight departure date"
            assert flight_date["is_time_dimension"] is True
            assert flight_date["smallest_time_grain"] == "day"

            # Check measures
            assert "flight_count" in data["measures"]
            assert "avg_delay" in data["measures"]
            assert data["measures"]["flight_count"]["description"] == "Total number of flights"

    @pytest.mark.asyncio
    async def test_get_model_nonexistent(self, sample_models):
        """Test get_model with non-existent model name."""
        mcp = MCPSemanticModel(models=sample_models)

        async with Client(mcp) as client:
            with pytest.raises(ToolError, match="Model nonexistent not found"):
                await client.call_tool("get_model", {"model_name": "nonexistent"})

    @pytest.mark.asyncio
    async def test_get_model_with_mixed_metadata(self, sample_models):
        """Test that get_model handles models with mixed metadata (some dims/measures have descriptions, some don't)."""
        mcp = MCPSemanticModel(models=sample_models)

        async with Client(mcp) as client:
            result = await client.call_tool("get_model", {"model_name": "carriers"})
            data = json.loads(result.content[0].text)

            # Dimension with description
            assert data["dimensions"]["code"]["description"] == "Carrier code"
            # Dimension without description
            assert data["dimensions"]["name"]["description"] is None

            # Measure without description
            assert data["measures"]["carrier_count"]["description"] is None


class TestGetTimeRangeTool:
    """Test get_time_range tool."""

    @pytest.mark.asyncio
    async def test_get_time_range_with_time_dimension(self, sample_models):
        """Test get_time_range with model that has time dimension."""
        mcp = MCPSemanticModel(models=sample_models)

        async with Client(mcp) as client:
            result = await client.call_tool("get_time_range", {"model_name": "flights"})
            data = json.loads(result.content[0].text)

            assert "start" in data
            assert "end" in data
            assert "time_dimension" in data
            assert data["time_dimension"] == "flight_date"
            # Check dates are from our sample data (2024-01-01 to 2024-01-30)
            assert "2024-01-01" in data["start"]
            assert "2024-01-30" in data["end"]

    @pytest.mark.asyncio
    async def test_get_time_range_without_time_dimension(self, sample_models):
        """Test get_time_range with model without time dimension."""
        mcp = MCPSemanticModel(models=sample_models)

        async with Client(mcp) as client:
            with pytest.raises(ToolError, match="has no time dimension"):
                await client.call_tool("get_time_range", {"model_name": "carriers"})

    @pytest.mark.asyncio
    async def test_get_time_range_nonexistent(self, sample_models):
        """Test get_time_range with non-existent model."""
        mcp = MCPSemanticModel(models=sample_models)

        async with Client(mcp) as client:
            with pytest.raises(ToolError, match="Model nonexistent not found"):
                await client.call_tool("get_time_range", {"model_name": "nonexistent"})


class TestQueryModelTool:
    """Test query_model tool."""

    @pytest.mark.asyncio
    async def test_query_model_basic(self, sample_models):
        """Test basic query with dimensions and measures."""
        mcp = MCPSemanticModel(models=sample_models)

        async with Client(mcp) as client:
            result = await client.call_tool(
                "query_model",
                {
                    "model_name": "flights",
                    "dimensions": ["carrier"],
                    "measures": ["flight_count", "avg_delay"],
                },
            )
            data = json.loads(result.content[0].text)

            # Check result format
            assert isinstance(data, dict)
            assert "records" in data
            assert isinstance(data["records"], list)
            assert len(data["records"]) == 3  # 3 unique carriers

            # Check each record has expected fields
            for record in data["records"]:
                assert "carrier" in record
                assert "flight_count" in record
                assert "avg_delay" in record

    @pytest.mark.asyncio
    async def test_query_model_with_filters(self, sample_models):
        """Test query with filters."""
        mcp = MCPSemanticModel(models=sample_models)

        async with Client(mcp) as client:
            filters = [{"field": "carrier", "operator": "=", "value": "AA"}]

            result = await client.call_tool(
                "query_model",
                {
                    "model_name": "flights",
                    "dimensions": ["carrier"],
                    "measures": ["flight_count"],
                    "filters": filters,
                },
            )
            data = json.loads(result.content[0].text)

            # Should only return AA
            assert len(data["records"]) == 1
            assert data["records"][0]["carrier"] == "AA"

    @pytest.mark.asyncio
    async def test_query_model_with_time_range(self, sample_models):
        """Test query with time_range and time_grain."""
        mcp = MCPSemanticModel(models=sample_models)

        async with Client(mcp) as client:
            result = await client.call_tool(
                "query_model",
                {
                    "model_name": "flights",
                    "dimensions": ["flight_date"],
                    "measures": ["flight_count"],
                    "time_range": {"start": "2024-01-01", "end": "2024-01-10"},
                    "time_grain": "TIME_GRAIN_DAY",
                },
            )
            data = json.loads(result.content[0].text)

            # Should have 10 days
            assert len(data["records"]) == 10

    @pytest.mark.asyncio
    async def test_query_model_with_order_and_limit(self, sample_models):
        """Test query with order_by and limit."""
        mcp = MCPSemanticModel(models=sample_models)

        async with Client(mcp) as client:
            result = await client.call_tool(
                "query_model",
                {
                    "model_name": "flights",
                    "dimensions": ["carrier"],
                    "measures": ["avg_delay"],
                    "order_by": [["avg_delay", "desc"]],
                    "limit": 2,
                },
            )
            data = json.loads(result.content[0].text)

            # Should have 2 records
            assert len(data["records"]) == 2
            # Should be ordered by avg_delay descending
            assert data["records"][0]["avg_delay"] >= data["records"][1]["avg_delay"]

    @pytest.mark.asyncio
    async def test_query_model_invalid_time_grain(self, sample_models):
        """Test query with time grain smaller than allowed."""
        mcp = MCPSemanticModel(models=sample_models)

        async with Client(mcp) as client:
            with pytest.raises(ToolError, match="finer than"):
                await client.call_tool(
                    "query_model",
                    {
                        "model_name": "flights",
                        "dimensions": ["flight_date"],
                        "measures": ["flight_count"],
                        "time_grain": "TIME_GRAIN_HOUR",  # Finer than allowed "day"
                    },
                )

    @pytest.mark.asyncio
    async def test_query_model_nonexistent(self, sample_models):
        """Test query with non-existent model."""
        mcp = MCPSemanticModel(models=sample_models)

        async with Client(mcp) as client:
            with pytest.raises(ToolError, match="Model nonexistent not found"):
                await client.call_tool(
                    "query_model",
                    {
                        "model_name": "nonexistent",
                        "dimensions": ["carrier"],
                        "measures": ["flight_count"],
                    },
                )


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
