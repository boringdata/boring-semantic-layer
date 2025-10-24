"""Tests for MCPSemanticModel using FastMCP client-server pattern with SemanticTable."""

import pytest
import pandas as pd
import ibis

from boring_semantic_layer import MCPSemanticModel, to_semantic_table
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
    flights_df = pd.DataFrame(
        {
            "origin": ["JFK", "LAX", "ORD"] * 10,
            "destination": ["LAX", "JFK", "DEN"] * 10,
            "carrier": ["AA", "UA", "DL"] * 10,
            "flight_date": pd.date_range("2024-01-01", periods=30, freq="D"),
            "dep_delay": [5.2, 8.1, 3.5] * 10,
        }
    )

    carriers_df = pd.DataFrame(
        {
            "code": ["AA", "UA", "DL"],
            "name": ["American", "United", "Delta"],
        }
    )

    flights_tbl = con.create_table("flights", flights_df, overwrite=True)
    carriers_tbl = con.create_table("carriers", carriers_df, overwrite=True)

    # Model with time dimension
    flights_model = (
        to_semantic_table(flights_tbl, name="flights")
        .with_dimensions(
            origin=lambda t: t.origin,
            destination=lambda t: t.destination,
            carrier=lambda t: t.carrier,
            flight_date={
                "expr": lambda t: t.flight_date,
                "description": "Flight departure date",
                "is_time_dimension": True,
                "smallest_time_grain": "day",
            },
        )
        .with_measures(
            flight_count={
                "expr": lambda t: t.count(),
                "description": "Total number of flights",
            },
            avg_delay={
                "expr": lambda t: t.dep_delay.mean(),
                "description": "Average departure delay",
            },
        )
    )

    # Model without time dimension
    carriers_model = (
        to_semantic_table(carriers_tbl, name="carriers")
        .with_dimensions(
            code={"expr": lambda t: t.code, "description": "Carrier code"},
            name=lambda t: t.name,
        )
        .with_measures(carrier_count=lambda t: t.count())
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


class TestListModels:
    """Test list_models tool."""

    @pytest.mark.asyncio
    async def test_list_models(self, sample_models):
        """Test listing all available models."""
        mcp = MCPSemanticModel(models=sample_models)

        async with Client(mcp) as client:
            result = await client.call_tool("list_models", {})
            models = result.structured_content["result"]

            assert "flights" in models
            assert "carriers" in models
            assert len(models) == 2

    @pytest.mark.asyncio
    async def test_list_models_empty(self):
        """Test listing models when none exist."""
        mcp = MCPSemanticModel(models={})

        async with Client(mcp) as client:
            result = await client.call_tool("list_models", {})
            models = result.structured_content["result"]

            assert models == {}


class TestGetModel:
    """Test get_model tool."""

    @pytest.mark.asyncio
    async def test_get_model_with_time_dimension(self, sample_models):
        """Test getting model details for flights model."""
        mcp = MCPSemanticModel(models=sample_models)

        async with Client(mcp) as client:
            result = await client.call_tool("get_model", {"model_name": "flights"})
            model_info = result.structured_content["result"]

            assert model_info["name"] == "flights"
            assert "origin" in model_info["dimensions"]
            assert "carrier" in model_info["dimensions"]
            assert "flight_date" in model_info["dimensions"]
            assert model_info["dimensions"]["flight_date"]["is_time_dimension"] is True
            assert (
                model_info["dimensions"]["flight_date"]["smallest_time_grain"] == "day"
            )

            assert "flight_count" in model_info["measures"]
            assert "avg_delay" in model_info["measures"]
            assert (
                model_info["measures"]["flight_count"]["description"]
                == "Total number of flights"
            )

    @pytest.mark.asyncio
    async def test_get_model_not_found(self, sample_models):
        """Test getting a non-existent model."""
        mcp = MCPSemanticModel(models=sample_models)

        async with Client(mcp) as client:
            with pytest.raises(ToolError, match="Model nonexistent not found"):
                await client.call_tool("get_model", {"model_name": "nonexistent"})


class TestGetTimeRange:
    """Test get_time_range tool."""

    @pytest.mark.asyncio
    async def test_get_time_range(self, sample_models):
        """Test getting time range for flights model."""
        mcp = MCPSemanticModel(models=sample_models)

        async with Client(mcp) as client:
            result = await client.call_tool("get_time_range", {"model_name": "flights"})
            time_range = result.structured_content["result"]

            assert "start" in time_range
            assert "end" in time_range
            assert time_range["start"].startswith("2024-01-01")
            assert time_range["end"].startswith("2024-01-30")

    @pytest.mark.asyncio
    async def test_get_time_range_no_time_dimension(self, sample_models):
        """Test getting time range for model without time dimension."""
        mcp = MCPSemanticModel(models=sample_models)

        async with Client(mcp) as client:
            with pytest.raises(ToolError, match="has no time dimension"):
                await client.call_tool("get_time_range", {"model_name": "carriers"})

    @pytest.mark.asyncio
    async def test_get_time_range_model_not_found(self, sample_models):
        """Test getting time range for non-existent model."""
        mcp = MCPSemanticModel(models=sample_models)

        async with Client(mcp) as client:
            with pytest.raises(ToolError, match="Model nonexistent not found"):
                await client.call_tool("get_time_range", {"model_name": "nonexistent"})


class TestQueryModel:
    """Test query_model tool."""

    @pytest.mark.asyncio
    async def test_simple_query(self, sample_models):
        """Test basic query with dimensions and measures."""
        mcp = MCPSemanticModel(models=sample_models)

        async with Client(mcp) as client:
            result = await client.call_tool(
                "query_model",
                {
                    "model_name": "flights",
                    "dimensions": ["carrier"],
                    "measures": ["flight_count"],
                },
            )

            assert result.content[0].text is not None
            assert "carrier" in result.content[0].text
            assert "flight_count" in result.content[0].text

    @pytest.mark.asyncio
    async def test_query_with_filter(self, sample_models):
        """Test query with filter."""
        mcp = MCPSemanticModel(models=sample_models)

        async with Client(mcp) as client:
            result = await client.call_tool(
                "query_model",
                {
                    "model_name": "flights",
                    "dimensions": ["carrier"],
                    "measures": ["flight_count"],
                    "filters": [{"field": "carrier", "operator": "=", "value": "AA"}],
                },
            )

            assert "AA" in result.content[0].text

    @pytest.mark.asyncio
    async def test_query_with_time_grain(self, sample_models):
        """Test query with time grain."""
        mcp = MCPSemanticModel(models=sample_models)

        async with Client(mcp) as client:
            result = await client.call_tool(
                "query_model",
                {
                    "model_name": "flights",
                    "dimensions": ["flight_date"],
                    "measures": ["flight_count"],
                    "time_grain": "TIME_GRAIN_MONTH",
                },
            )

            assert result.content[0].text is not None
            assert "flight_date" in result.content[0].text

    @pytest.mark.asyncio
    async def test_query_with_time_range(self, sample_models):
        """Test query with time range."""
        mcp = MCPSemanticModel(models=sample_models)

        async with Client(mcp) as client:
            result = await client.call_tool(
                "query_model",
                {
                    "model_name": "flights",
                    "dimensions": ["flight_date"],
                    "measures": ["flight_count"],
                    "time_range": {"start": "2024-01-01", "end": "2024-01-15"},
                },
            )

            assert result.content[0].text is not None

    @pytest.mark.asyncio
    async def test_query_with_order_by(self, sample_models):
        """Test query with ordering."""
        mcp = MCPSemanticModel(models=sample_models)

        async with Client(mcp) as client:
            result = await client.call_tool(
                "query_model",
                {
                    "model_name": "flights",
                    "dimensions": ["carrier"],
                    "measures": ["flight_count"],
                    "order_by": [["flight_count", "desc"]],
                },
            )

            assert result.content[0].text is not None

    @pytest.mark.asyncio
    async def test_query_with_limit(self, sample_models):
        """Test query with limit."""
        mcp = MCPSemanticModel(models=sample_models)

        async with Client(mcp) as client:
            result = await client.call_tool(
                "query_model",
                {
                    "model_name": "flights",
                    "dimensions": ["carrier"],
                    "measures": ["flight_count"],
                    "limit": 2,
                },
            )

            assert result.content[0].text is not None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
