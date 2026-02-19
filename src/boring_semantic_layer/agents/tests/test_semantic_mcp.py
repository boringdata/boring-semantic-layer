"""Tests for MCPSemanticModel using FastMCP client-server pattern with SemanticTable."""

import json

import ibis
import pandas as pd
import pytest
from fastmcp import Client
from fastmcp.exceptions import ToolError

from boring_semantic_layer import MCPSemanticModel, to_semantic_table


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
        },
    )

    carriers_df = pd.DataFrame(
        {
            "code": ["AA", "UA", "DL"],
            "name": ["American", "United", "Delta"],
        },
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
            models = json.loads(result.content[0].text)

            assert "flights" in models
            assert "carriers" in models
            assert len(models) == 2

    @pytest.mark.asyncio
    async def test_list_models_empty(self):
        """Test listing models when none exist."""
        mcp = MCPSemanticModel(models={})

        async with Client(mcp) as client:
            result = await client.call_tool("list_models", {})
            models = json.loads(result.content[0].text)

            assert models == {}


class TestGetModel:
    """Test get_model tool."""

    @pytest.mark.asyncio
    async def test_get_model_with_time_dimension(self, sample_models):
        """Test getting model details for flights model."""
        mcp = MCPSemanticModel(models=sample_models)

        async with Client(mcp) as client:
            result = await client.call_tool("get_model", {"model_name": "flights"})
            model_info = json.loads(result.content[0].text)

            assert model_info["name"] == "flights"
            assert "origin" in model_info["dimensions"]
            assert "carrier" in model_info["dimensions"]
            assert "flight_date" in model_info["dimensions"]
            assert model_info["dimensions"]["flight_date"]["is_time_dimension"] is True
            assert model_info["dimensions"]["flight_date"]["smallest_time_grain"] == "day"

            assert "flight_count" in model_info["measures"]
            assert "avg_delay" in model_info["measures"]
            assert (
                model_info["measures"]["flight_count"]["description"] == "Total number of flights"
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
            time_range = json.loads(result.content[0].text)

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

    @pytest.mark.asyncio
    async def test_query_with_get_chart_false_returns_records_only(self, sample_models):
        """Test query with get_chart=False returns only records."""
        mcp = MCPSemanticModel(models=sample_models)

        async with Client(mcp) as client:
            result = await client.call_tool(
                "query_model",
                {
                    "model_name": "flights",
                    "dimensions": ["carrier"],
                    "measures": ["flight_count"],
                    "get_chart": False,
                },
            )

            data = json.loads(result.content[0].text)
            assert "records" in data
            assert "chart" not in data
            assert isinstance(data["records"], list)
            assert len(data["records"]) > 0

    @pytest.mark.asyncio
    async def test_query_with_chart_spec_altair_json(self, sample_models):
        """Test query with chart_spec returns both records and chart (Altair)."""
        mcp = MCPSemanticModel(models=sample_models)

        async with Client(mcp) as client:
            result = await client.call_tool(
                "query_model",
                {
                    "model_name": "flights",
                    "dimensions": ["carrier"],
                    "measures": ["flight_count"],
                    "chart_backend": "altair",
                    "chart_format": "json",
                },
            )

            data = json.loads(result.content[0].text)
            assert "records" in data
            assert "chart" in data
            assert isinstance(data["records"], list)
            assert isinstance(data["chart"], dict)
            # New format wraps chart data
            assert data["chart"]["backend"] == "altair"
            assert data["chart"]["format"] == "json"
            # Altair JSON spec should have basic Vega-Lite structure
            assert "$schema" in data["chart"]["data"] or "mark" in data["chart"]["data"]

    @pytest.mark.asyncio
    async def test_query_with_chart_spec_plotly_json(self, sample_models):
        """Test query with chart_spec returns both records and chart (Plotly)."""
        mcp = MCPSemanticModel(models=sample_models)

        async with Client(mcp) as client:
            result = await client.call_tool(
                "query_model",
                {
                    "model_name": "flights",
                    "dimensions": ["carrier"],
                    "measures": ["flight_count"],
                    "chart_backend": "plotly",
                    "chart_format": "json",
                },
            )

            data = json.loads(result.content[0].text)
            assert "records" in data
            assert "chart" in data
            assert isinstance(data["records"], list)
            assert isinstance(data["chart"], dict)
            # New format wraps chart data
            assert data["chart"]["backend"] == "plotly"
            # Plotly JSON spec should have data and layout
            assert "data" in data["chart"]["data"] or "layout" in data["chart"]["data"]

    @pytest.mark.asyncio
    async def test_query_with_chart_spec_custom_spec(self, sample_models):
        """Test query with custom chart spec."""
        mcp = MCPSemanticModel(models=sample_models)

        async with Client(mcp) as client:
            result = await client.call_tool(
                "query_model",
                {
                    "model_name": "flights",
                    "dimensions": ["carrier"],
                    "measures": ["flight_count"],
                    "chart_backend": "altair",
                    "chart_format": "json",
                    "chart_spec": {
                        "spec": {"mark": "line", "title": "Custom Chart"},
                    },
                },
            )

            data = json.loads(result.content[0].text)
            assert "records" in data
            assert "chart" in data
            assert isinstance(data["chart"], dict)
            # Check that custom spec was applied - mark type should be line
            assert data["chart"]["data"].get("mark", {}).get("type") == "line"

    @pytest.mark.asyncio
    async def test_query_with_chart_spec_non_json_format(self, sample_models):
        """Test query with chart_spec using non-JSON format returns message."""
        mcp = MCPSemanticModel(models=sample_models)

        async with Client(mcp) as client:
            result = await client.call_tool(
                "query_model",
                {
                    "model_name": "flights",
                    "dimensions": ["carrier"],
                    "measures": ["flight_count"],
                    "chart_backend": "altair",
                    "chart_format": "static",
                },
            )

            data = json.loads(result.content[0].text)
            assert "records" in data
            assert "chart" in data
            assert "message" in data["chart"]
            assert "Use format='json'" in data["chart"]["message"]


class TestJoinedModels:
    """Test MCP with joined semantic models."""

    @pytest.fixture(scope="class")
    def joined_models(self, con):
        """Create joined semantic model for testing."""
        # Create sample data
        flights_df = pd.DataFrame(
            {
                "origin": ["JFK", "LAX", "ORD"] * 10,
                "carrier": ["AA", "UA", "DL"] * 10,
                "flight_date": pd.date_range("2024-01-01", periods=30, freq="D"),
            },
        )

        carriers_df = pd.DataFrame(
            {
                "code": ["AA", "UA", "DL"],
                "name": ["American", "United", "Delta"],
            },
        )

        flights_tbl = con.create_table("flights_joined", flights_df, overwrite=True)
        carriers_tbl = con.create_table("carriers_joined", carriers_df, overwrite=True)

        # Define carriers semantic table
        carriers = (
            to_semantic_table(carriers_tbl, name="carriers")
            .with_dimensions(
                code={
                    "expr": lambda t: t.code,
                    "description": "Carrier code",
                },
                name={
                    "expr": lambda t: t.name,
                    "description": "Full carrier name",
                },
            )
            .with_measures(
                carrier_count={
                    "expr": lambda t: t.count(),
                    "description": "Total number of carriers",
                }
            )
        )

        # Define flights semantic table with join to carriers
        flights = (
            to_semantic_table(flights_tbl, name="flights")
            .with_dimensions(
                origin={
                    "expr": lambda t: t.origin,
                    "description": "Origin airport code",
                },
                carrier={
                    "expr": lambda t: t.carrier,
                    "description": "Carrier code",
                },
                flight_date={
                    "expr": lambda t: t.flight_date,
                    "description": "Flight date",
                    "is_time_dimension": True,
                    "smallest_time_grain": "day",
                },
            )
            .with_measures(
                flight_count={
                    "expr": lambda t: t.count(),
                    "description": "Total number of flights",
                },
            )
            .join_one(carriers, lambda f, c: f.carrier == c.code)
        )

        return {"flights": flights, "carriers": carriers}

    @pytest.mark.asyncio
    async def test_get_model_joined(self, joined_models):
        """Test get_model on a joined semantic table."""
        mcp = MCPSemanticModel(models=joined_models)

        async with Client(mcp) as client:
            result = await client.call_tool("get_model", {"model_name": "flights"})
            model_info = json.loads(result.content[0].text)

            # Check that joined model has metadata from both sides (with prefixes)
            assert "flights.origin" in model_info["dimensions"]
            assert "flights.carrier" in model_info["dimensions"]
            assert "flights.flight_date" in model_info["dimensions"]

            # Check prefixed dimensions from joined table
            assert "carriers.code" in model_info["dimensions"]
            assert "carriers.name" in model_info["dimensions"]

            # Check measures (with prefixes)
            assert "flights.flight_count" in model_info["measures"]
            assert "carriers.carrier_count" in model_info["measures"]

    @pytest.mark.asyncio
    async def test_query_joined_model(self, joined_models):
        """Test querying a joined semantic model."""
        mcp = MCPSemanticModel(models=joined_models)

        async with Client(mcp) as client:
            result = await client.call_tool(
                "query_model",
                {
                    "model_name": "flights",
                    "dimensions": ["carriers.name"],
                    "measures": ["flights.flight_count"],
                },
            )

            data = json.loads(result.content[0].text)
            assert "records" in data
            records = data["records"]
            assert len(records) > 0
            assert "carriers.name" in records[0]
            assert "flights.flight_count" in records[0]

    @pytest.mark.asyncio
    async def test_query_joined_with_time_grain(self, joined_models):
        """Test querying joined model with time grain."""
        mcp = MCPSemanticModel(models=joined_models)

        async with Client(mcp) as client:
            result = await client.call_tool(
                "query_model",
                {
                    "model_name": "flights",
                    "dimensions": ["flights.flight_date", "carriers.name"],
                    "measures": ["flights.flight_count"],
                    "time_grain": "TIME_GRAIN_MONTH",
                },
            )

            data = json.loads(result.content[0].text)
            assert "records" in data
            records = data["records"]
            assert len(records) > 0
            assert "flights.flight_date" in records[0]
            assert "carriers.name" in records[0]
            assert "flights.flight_count" in records[0]

    @pytest.mark.asyncio
    async def test_get_time_range_joined(self, joined_models):
        """Test get_time_range on a joined model."""
        mcp = MCPSemanticModel(models=joined_models)

        async with Client(mcp) as client:
            result = await client.call_tool("get_time_range", {"model_name": "flights"})
            time_range = json.loads(result.content[0].text)

            assert "start" in time_range
            assert "end" in time_range
            assert time_range["start"].startswith("2024-01-01")
            assert time_range["end"].startswith("2024-01-30")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])


class TestDescriptionSupport:
    """Test description support in MCP."""

    @pytest.mark.asyncio
    async def test_mcp_base_model_with_description(self, con):
        """Test that MCP get_model includes description for base models."""
        flights_data = pd.DataFrame(
            {"carrier": ["AA", "UA"], "distance": [100, 200]},
        )
        flights_tbl = con.create_table("flights", flights_data, overwrite=True)

        flights = (
            to_semantic_table(
                flights_tbl,
                name="flights",
                description="Flight departure data",
            )
            .with_dimensions(carrier=lambda t: t.carrier)
            .with_measures(total_distance=lambda t: t.distance.sum())
        )

        mcp = MCPSemanticModel(models={"flights": flights})

        async with Client(mcp) as client:
            result = await client.call_tool("get_model", {"model_name": "flights"})
            model_info = json.loads(result.content[0].text)

            assert "description" in model_info
            assert model_info["description"] == "Flight departure data"

    @pytest.mark.asyncio
    async def test_mcp_base_model_without_description(self, con):
        """Test that MCP get_model works when description is not provided."""
        flights_data = pd.DataFrame(
            {"carrier": ["AA", "UA"], "distance": [100, 200]},
        )
        flights_tbl = con.create_table("flights", flights_data, overwrite=True)

        flights = (
            to_semantic_table(flights_tbl, name="flights")
            .with_dimensions(carrier=lambda t: t.carrier)
            .with_measures(total_distance=lambda t: t.distance.sum())
        )

        mcp = MCPSemanticModel(models={"flights": flights})

        async with Client(mcp) as client:
            result = await client.call_tool("get_model", {"model_name": "flights"})
            model_info = json.loads(result.content[0].text)

            assert "description" not in model_info

    @pytest.mark.asyncio
    async def test_mcp_joined_model_concatenates_descriptions(self, con):
        """Test that MCP constructs description for joined models."""
        flights_data = pd.DataFrame(
            {"carrier": ["AA", "UA"], "distance": [100, 200]},
        )
        carriers_data = pd.DataFrame(
            {"code": ["AA", "UA"], "name": ["American", "United"]},
        )
        flights_tbl = con.create_table("flights", flights_data, overwrite=True)
        carriers_tbl = con.create_table("carriers", carriers_data, overwrite=True)

        flights = (
            to_semantic_table(
                flights_tbl,
                name="flights",
                description="Flight departure data",
            )
            .with_dimensions(carrier=lambda t: t.carrier)
            .with_measures(total_distance=lambda t: t.distance.sum())
        )

        carriers = (
            to_semantic_table(
                carriers_tbl,
                name="carriers",
                description="Airline carrier information",
            )
            .with_dimensions(code=lambda t: t.code, name=lambda t: t.name)
            .with_measures(carrier_count=lambda t: t.count())
        )

        flights_with_carriers = flights.join_one(carriers, lambda f, c: f.carrier == c.code)

        mcp = MCPSemanticModel(models={"flights_with_carriers": flights_with_carriers})

        async with Client(mcp) as client:
            result = await client.call_tool(
                "get_model",
                {"model_name": "flights_with_carriers"},
            )
            model_info = json.loads(result.content[0].text)

            assert "description" in model_info
            assert "flights" in model_info["description"]
            assert "Flight departure data" in model_info["description"]
            assert "carriers" in model_info["description"]
            assert "Airline carrier information" in model_info["description"]
            assert "Joined model combining" in model_info["description"]
