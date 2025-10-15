"""Tests for MCPSemanticModel using FastMCP client-server pattern."""

import pytest
import json
from unittest.mock import Mock, patch
import pandas as pd

from boring_semantic_layer import (
    SemanticModel,
    MCPSemanticModel,
    DimensionSpec,
    MeasureSpec,
)
from fastmcp import Client
from fastmcp.exceptions import ToolError


@pytest.fixture
def mock_table():
    """Create a mock Ibis table."""
    table = Mock()
    table.get_name.return_value = "test_table"
    # Mock aggregate for get_time_range
    mock_result = pd.DataFrame(
        {"start": pd.to_datetime(["2024-01-01"]), "end": pd.to_datetime(["2024-12-31"])}
    )
    table.aggregate.return_value.execute.return_value = mock_result
    return table


@pytest.fixture
def sample_models(mock_table):
    """Create sample semantic models for testing."""
    # Model with time dimension
    flights_model = SemanticModel(
        name="flights",
        table=mock_table,
        description="Sample flights model",
        dimensions={
            "origin": lambda t: t.origin,
            "destination": lambda t: t.destination,
            "carrier": lambda t: t.carrier,
            "flight_date": lambda t: t.flight_date,
        },
        measures={
            "flight_count": lambda t: t.count(),
            "avg_delay": lambda t: t.dep_delay.mean(),
        },
        time_dimension="flight_date",
        smallest_time_grain="TIME_GRAIN_DAY",
    )

    # Model without time dimension
    carriers_model = SemanticModel(
        name="carriers",
        table=mock_table,
        dimensions={
            "code": lambda t: t.code,
            "name": lambda t: t.name,
        },
        measures={
            "carrier_count": lambda t: t.count(),
        },
        primary_key="code",
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
    async def test_get_model_returns_json_definition(self, sample_models):
        """Test that get_model returns model's json_definition."""
        mcp = MCPSemanticModel(models=sample_models)

        async with Client(mcp) as client:
            result = await client.call_tool("get_model", {"model_name": "flights"})
            data = json.loads(result.content[0].text)

            assert data["name"] == "flights"
            assert "origin" in data["dimensions"]
            assert "destination" in data["dimensions"]
            assert "carrier" in data["dimensions"]
            assert "flight_date" in data["dimensions"]
            assert "flight_count" in data["measures"]
            assert "avg_delay" in data["measures"]
            assert data["time_dimension"] == "flight_date"
            assert data["smallest_time_grain"] == "TIME_GRAIN_DAY"

    @pytest.mark.asyncio
    async def test_get_model_nonexistent(self, sample_models):
        """Test get_model with non-existent model name."""
        mcp = MCPSemanticModel(models=sample_models)

        async with Client(mcp) as client:
            with pytest.raises(ToolError, match="Model nonexistent not found"):
                await client.call_tool("get_model", {"model_name": "nonexistent"})

    @pytest.mark.asyncio
    async def test_get_model_with_descriptions(self, mock_table):
        """Test that get_model correctly serialises models with descriptions"""

        # Create model with mixed old and new style specs
        model_with_descriptions = SemanticModel(
            name="test_descriptions",
            table=mock_table,
            dimensions={
                "old_dimension": lambda t: t.old_col,
                "new_dimension": DimensionSpec(
                    expr=lambda t: t.new_col, description="New dimension description"
                ),
            },
            measures={
                "old_measure": lambda t: t.old_col,
                "new_measure": MeasureSpec(
                    expr=lambda t: t.new_col, description="New measure description"
                ),
            },
            description="This is a test model with descriptions",
        )

        models = {"test_descriptions": model_with_descriptions}
        mcp = MCPSemanticModel(models=models)

        async with Client(mcp) as client:
            result = await client.call_tool(
                "get_model", {"model_name": "test_descriptions"}
            )
            data = json.loads(result.content[0].text)

            # Verify model description
            assert data["description"] == "This is a test model with descriptions"

            # Verify dimension descriptions
            assert data["dimensions"]["old_dimension"]["description"] == ""
            assert (
                data["dimensions"]["new_dimension"]["description"]
                == "New dimension description"
            )

            # Verify measure descriptions
            assert data["measures"]["old_measure"]["description"] == ""
            assert (
                data["measures"]["new_measure"]["description"]
                == "New measure description"
            )


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
            # The mock returns 2024-01-01 and 2024-12-31
            assert "2024-01-01" in data["start"]
            assert "2024-12-31" in data["end"]

    @pytest.mark.asyncio
    async def test_get_time_range_without_time_dimension(self, sample_models):
        """Test get_time_range with model without time dimension."""
        mcp = MCPSemanticModel(models=sample_models)

        async with Client(mcp) as client:
            result = await client.call_tool(
                "get_time_range", {"model_name": "carriers"}
            )
            data = json.loads(result.content[0].text)

            assert "error" in data
            assert data["error"] == "Model does not have a time dimension"

    @pytest.mark.asyncio
    async def test_get_time_range_nonexistent(self, sample_models):
        """Test get_time_range with non-existent model."""
        mcp = MCPSemanticModel(models=sample_models)

        async with Client(mcp) as client:
            with pytest.raises(ToolError, match="Model nonexistent not found"):
                await client.call_tool("get_time_range", {"model_name": "nonexistent"})


class TestQueryModelTool:
    """Test query_model tool."""

    @pytest.fixture
    def mock_query_result(self):
        """Mock query result DataFrame."""
        df = pd.DataFrame(
            {
                "carrier": ["AA", "UA", "DL"],
                "flight_count": [100, 150, 200],
                "avg_delay": [5.2, 8.1, 3.5],
            }
        )
        return df

    @pytest.mark.asyncio
    async def test_query_model_basic(self, sample_models, mock_query_result):
        """Test basic query with dimensions and measures."""
        mcp = MCPSemanticModel(models=sample_models)

        # Mock the query chain by patching the query method on the class
        with patch(
            "boring_semantic_layer.semantic_model.SemanticModel.query"
        ) as mock_query:
            mock_query_expr = Mock()
            mock_query_expr.execute.return_value = mock_query_result
            mock_query.return_value = mock_query_expr

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

                # Check query was called with correct parameters
                mock_query.assert_called_once_with(
                    dimensions=["carrier"],
                    measures=["flight_count", "avg_delay"],
                    filters=[],
                    order_by=[],
                    limit=None,
                    time_range=None,
                    time_grain=None,
                )

                # Check result format
                assert isinstance(data, dict)
                assert "records" in data
                assert isinstance(data["records"], list)
                assert len(data["records"]) == 3
                assert data["records"][0]["carrier"] == "AA"
                assert data["records"][0]["flight_count"] == 100

    @pytest.mark.asyncio
    async def test_query_model_with_filters(self, sample_models, mock_query_result):
        """Test query with filters."""
        mcp = MCPSemanticModel(models=sample_models)

        with patch(
            "boring_semantic_layer.semantic_model.SemanticModel.query"
        ) as mock_query:
            mock_query_expr = Mock()
            mock_query_expr.execute.return_value = mock_query_result
            mock_query.return_value = mock_query_expr

            filters = [{"field": "origin", "operator": "=", "value": "JFK"}]

            async with Client(mcp) as client:
                await client.call_tool(
                    "query_model",
                    {
                        "model_name": "flights",
                        "dimensions": ["carrier"],
                        "measures": ["flight_count"],
                        "filters": filters,
                    },
                )

                # Check filters were passed correctly
                called_filters = mock_query.call_args[1]["filters"]
                assert called_filters == filters

    @pytest.mark.asyncio
    async def test_query_model_with_time_range(self, sample_models, mock_query_result):
        """Test query with time_range and time_grain."""
        mcp = MCPSemanticModel(models=sample_models)

        with patch(
            "boring_semantic_layer.semantic_model.SemanticModel.query"
        ) as mock_query:
            mock_query_expr = Mock()
            mock_query_expr.execute.return_value = mock_query_result
            mock_query.return_value = mock_query_expr

            time_range = {"start": "2024-01-01", "end": "2024-03-31"}

            async with Client(mcp) as client:
                await client.call_tool(
                    "query_model",
                    {
                        "model_name": "flights",
                        "dimensions": ["carrier"],
                        "measures": ["flight_count"],
                        "time_range": time_range,
                        "time_grain": "TIME_GRAIN_MONTH",
                    },
                )

                # Check time_range and time_grain were passed
                mock_query.assert_called_with(
                    dimensions=["carrier"],
                    measures=["flight_count"],
                    filters=[],
                    order_by=[],
                    limit=None,
                    time_range=time_range,
                    time_grain="TIME_GRAIN_MONTH",
                )

    @pytest.mark.asyncio
    async def test_query_model_with_order_and_limit(
        self, sample_models, mock_query_result
    ):
        """Test query with order_by and limit."""
        mcp = MCPSemanticModel(models=sample_models)

        with patch(
            "boring_semantic_layer.semantic_model.SemanticModel.query"
        ) as mock_query:
            mock_query_expr = Mock()
            mock_query_expr.execute.return_value = mock_query_result.head(2)
            mock_query.return_value = mock_query_expr

            async with Client(mcp) as client:
                await client.call_tool(
                    "query_model",
                    {
                        "model_name": "flights",
                        "dimensions": ["carrier"],
                        "measures": ["avg_delay"],
                        "order_by": [["avg_delay", "desc"]],
                        "limit": 10,
                    },
                )

                # Check order_by and limit were passed
                mock_query.assert_called_with(
                    dimensions=["carrier"],
                    measures=["avg_delay"],
                    filters=[],
                    order_by=[("avg_delay", "desc")],
                    limit=10,
                    time_range=None,
                    time_grain=None,
                )

    @pytest.mark.asyncio
    async def test_query_model_invalid_order_by(self, sample_models):
        """Test query with invalid order_by format."""
        mcp = MCPSemanticModel(models=sample_models)

        async with Client(mcp) as client:
            # Test non-list order_by
            with pytest.raises(
                ToolError,
                match="Input validation error: 'invalid' is not of type 'array'",
            ):
                await client.call_tool(
                    "query_model",
                    {
                        "model_name": "flights",
                        "dimensions": ["carrier"],
                        "measures": ["flight_count"],
                        "order_by": "invalid",
                    },
                )

            # Test invalid list format (missing direction)
            with pytest.raises(
                ToolError, match="Each order_by item must be a list with 2 elements"
            ):
                await client.call_tool(
                    "query_model",
                    {
                        "model_name": "flights",
                        "dimensions": ["carrier"],
                        "measures": ["flight_count"],
                        "order_by": [["field"]],  # Missing direction
                    },
                )

            # Test invalid direction - this will pass pydantic validation but fail our validation
            with pytest.raises(ToolError, match="Each order_by item must be"):
                await client.call_tool(
                    "query_model",
                    {
                        "model_name": "flights",
                        "dimensions": ["carrier"],
                        "measures": ["flight_count"],
                        "order_by": [["field", "invalid"]],
                    },
                )

    @pytest.mark.asyncio
    async def test_query_model_invalid_time_grain(self, sample_models):
        """Test query with time grain smaller than allowed."""
        mcp = MCPSemanticModel(models=sample_models)

        async with Client(mcp) as client:
            with pytest.raises(
                ToolError, match="Time grain TIME_GRAIN_SECOND is smaller than"
            ):
                await client.call_tool(
                    "query_model",
                    {
                        "model_name": "flights",
                        "dimensions": ["carrier"],
                        "measures": ["flight_count"],
                        "time_grain": "TIME_GRAIN_SECOND",
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

    @pytest.mark.asyncio
    async def test_query_model_with_chart_spec(self, sample_models, mock_query_result):
        """Test query with chart_spec returns both data and chart."""
        mcp = MCPSemanticModel(models=sample_models)

        with patch(
            "boring_semantic_layer.semantic_model.SemanticModel.query"
        ) as mock_query:
            mock_query_expr = Mock()
            mock_query_expr.execute.return_value = mock_query_result
            # Mock chart method to return a simple chart spec
            mock_chart = {"mark": "bar", "encoding": {"x": {"field": "carrier"}}}
            mock_query_expr.chart.return_value = mock_chart
            mock_query.return_value = mock_query_expr

            async with Client(mcp) as client:
                result = await client.call_tool(
                    "query_model",
                    {
                        "model_name": "flights",
                        "dimensions": ["carrier"],
                        "measures": ["flight_count"],
                        "chart_spec": True,
                    },
                )
                data = json.loads(result.content[0].text)

                # Should return combined data and chart
                assert isinstance(data, dict)
                assert "records" in data
                assert "chart" in data

                # Check records
                assert isinstance(data["records"], list)
                assert len(data["records"]) == 3
                assert data["records"][0]["carrier"] == "AA"
                assert data["records"][0]["flight_count"] == 100

                # Check chart
                assert data["chart"]["mark"] == "bar"
                assert "encoding" in data["chart"]

                # Verify chart was called with correct parameters
                mock_query_expr.chart.assert_called_once_with(spec=None, format="json")

    @pytest.mark.asyncio
    async def test_query_model_with_custom_chart_spec(
        self, sample_models, mock_query_result
    ):
        """Test query with custom chart_spec."""
        mcp = MCPSemanticModel(models=sample_models)

        with patch(
            "boring_semantic_layer.semantic_model.SemanticModel.query"
        ) as mock_query:
            mock_query_expr = Mock()
            mock_query_expr.execute.return_value = mock_query_result
            # Mock chart method to return a custom chart spec
            mock_chart = {
                "mark": "line",
                "title": "Custom Title",
                "encoding": {"x": {"field": "carrier"}},
            }
            mock_query_expr.chart.return_value = mock_chart
            mock_query.return_value = mock_query_expr

            custom_spec = {"title": "Custom Title", "mark": "line"}

            async with Client(mcp) as client:
                result = await client.call_tool(
                    "query_model",
                    {
                        "model_name": "flights",
                        "dimensions": ["carrier"],
                        "measures": ["flight_count"],
                        "chart_spec": custom_spec,
                        "chart_format": "interactive",
                    },
                )
                data = json.loads(result.content[0].text)

                # Should return combined data and chart
                assert isinstance(data, dict)
                assert "records" in data
                assert "chart" in data

                # Check that chart was called with custom spec and format
                mock_query_expr.chart.assert_called_once_with(
                    spec=custom_spec, format="interactive"
                )

    @pytest.mark.asyncio
    async def test_query_model_with_png_format(self, sample_models, mock_query_result):
        """Test query with PNG chart format."""
        mcp = MCPSemanticModel(models=sample_models)

        with patch(
            "boring_semantic_layer.semantic_model.SemanticModel.query"
        ) as mock_query:
            mock_query_expr = Mock()
            mock_query_expr.execute.return_value = mock_query_result
            # Mock chart method to return PNG bytes
            mock_png_bytes = b"fake_png_data"
            mock_query_expr.chart.return_value = mock_png_bytes
            mock_query.return_value = mock_query_expr

            async with Client(mcp) as client:
                result = await client.call_tool(
                    "query_model",
                    {
                        "model_name": "flights",
                        "dimensions": ["carrier"],
                        "measures": ["flight_count"],
                        "chart_spec": True,
                        "chart_format": "png",
                    },
                )
                data = json.loads(result.content[0].text)

                # Should return combined data and chart
                assert isinstance(data, dict)
                assert "records" in data
                assert "chart" in data

                # Check PNG format - should be raw bytes decoded to string
                chart = data["chart"]
                assert chart == mock_png_bytes.decode("utf-8")

    @pytest.mark.asyncio
    async def test_query_model_with_svg_format(self, sample_models, mock_query_result):
        """Test query with SVG chart format."""
        mcp = MCPSemanticModel(models=sample_models)

        with patch(
            "boring_semantic_layer.semantic_model.SemanticModel.query"
        ) as mock_query:
            mock_query_expr = Mock()
            mock_query_expr.execute.return_value = mock_query_result
            # Mock chart method to return SVG string
            mock_svg_string = "<svg>fake svg</svg>"
            mock_query_expr.chart.return_value = mock_svg_string
            mock_query.return_value = mock_query_expr

            async with Client(mcp) as client:
                result = await client.call_tool(
                    "query_model",
                    {
                        "model_name": "flights",
                        "dimensions": ["carrier"],
                        "measures": ["flight_count"],
                        "chart_spec": True,
                        "chart_format": "svg",
                    },
                )
                data = json.loads(result.content[0].text)

                # Should return combined data and chart
                assert isinstance(data, dict)
                assert "records" in data
                assert "chart" in data

                # Check SVG format - should be raw string
                chart = data["chart"]
                assert chart == mock_svg_string
