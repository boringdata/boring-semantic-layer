"""Tests for MCPSemanticModel."""

import pytest
from unittest.mock import Mock, patch
import pandas as pd

from boring_semantic_layer import SemanticModel, MCPSemanticModel
from mcp.server.fastmcp.exceptions import ToolError


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

        # Check that tools are registered
        tools = await mcp.list_tools()
        tool_names = [tool.name for tool in tools]
        assert "list_models" in tool_names
        assert "get_model" in tool_names
        assert "get_time_range" in tool_names
        assert "query_model" in tool_names


class TestListModelsTool:
    """Test list_models tool."""

    @pytest.mark.asyncio
    async def test_list_models_returns_all_names(self, sample_models):
        """Test that list_models returns all model names."""
        mcp = MCPSemanticModel(models=sample_models)

        # Call the list_models tool
        content_blocks, result_dict = await mcp.call_tool("list_models", {})
        result = result_dict["result"]

        assert set(result) == {"flights", "carriers"}

    @pytest.mark.asyncio
    async def test_list_models_empty(self):
        """Test list_models with no models."""
        mcp = MCPSemanticModel(models={})

        content_blocks, result_dict = await mcp.call_tool("list_models", {})
        result = result_dict["result"]

        assert result == []


class TestGetModelTool:
    """Test get_model tool."""

    @pytest.mark.asyncio
    async def test_get_model_returns_json_definition(self, sample_models):
        """Test that get_model returns model's json_definition."""
        mcp = MCPSemanticModel(models=sample_models)

        content_blocks, result_dict = await mcp.call_tool(
            "get_model", {"model_name": "flights"}
        )
        result = result_dict["result"]

        assert result["name"] == "flights"
        assert "origin" in result["dimensions"]
        assert "destination" in result["dimensions"]
        assert "carrier" in result["dimensions"]
        assert "flight_date" in result["dimensions"]
        assert "flight_count" in result["measures"]
        assert "avg_delay" in result["measures"]
        assert result["time_dimension"] == "flight_date"
        assert result["smallest_time_grain"] == "TIME_GRAIN_DAY"

    @pytest.mark.asyncio
    async def test_get_model_nonexistent(self, sample_models):
        """Test get_model with non-existent model name."""
        mcp = MCPSemanticModel(models=sample_models)

        with pytest.raises(ToolError, match="Model nonexistent not found"):
            await mcp.call_tool("get_model", {"model_name": "nonexistent"})


class TestGetTimeRangeTool:
    """Test get_time_range tool."""

    @pytest.mark.asyncio
    async def test_get_time_range_with_time_dimension(self, sample_models):
        """Test get_time_range with model that has time dimension."""
        mcp = MCPSemanticModel(models=sample_models)

        content_blocks, result_dict = await mcp.call_tool(
            "get_time_range", {"model_name": "flights"}
        )
        result = result_dict["result"]

        assert "start" in result
        assert "end" in result
        # The mock returns 2024-01-01 and 2024-12-31
        assert "2024-01-01" in result["start"]
        assert "2024-12-31" in result["end"]

    @pytest.mark.asyncio
    async def test_get_time_range_without_time_dimension(self, sample_models):
        """Test get_time_range with model without time dimension."""
        mcp = MCPSemanticModel(models=sample_models)

        content_blocks, result_dict = await mcp.call_tool(
            "get_time_range", {"model_name": "carriers"}
        )
        result = result_dict["result"]

        assert "error" in result
        assert result["error"] == "Model does not have a time dimension"

    @pytest.mark.asyncio
    async def test_get_time_range_nonexistent(self, sample_models):
        """Test get_time_range with non-existent model."""
        mcp = MCPSemanticModel(models=sample_models)

        with pytest.raises(ToolError, match="Model nonexistent not found"):
            await mcp.call_tool("get_time_range", {"model_name": "nonexistent"})


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

            content_blocks, result_dict = await mcp.call_tool(
                "query_model",
                {
                    "model_name": "flights",
                    "dimensions": ["carrier"],
                    "measures": ["flight_count", "avg_delay"],
                },
            )
            result = result_dict["result"]

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
            assert isinstance(result, dict)
            assert "records" in result
            assert isinstance(result["records"], list)
            assert len(result["records"]) == 3
            assert result["records"][0]["carrier"] == "AA"
            assert result["records"][0]["flight_count"] == 100

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
            content_blocks, result_dict = await mcp.call_tool(
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
            content_blocks, result_dict = await mcp.call_tool(
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

            content_blocks, result_dict = await mcp.call_tool(
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

        # Test non-list order_by
        with pytest.raises(ToolError, match="Input should be a valid list"):
            await mcp.call_tool(
                "query_model",
                {
                    "model_name": "flights",
                    "dimensions": ["carrier"],
                    "measures": ["flight_count"],
                    "order_by": "invalid",
                },
            )

        # Test invalid tuple format
        with pytest.raises(ToolError, match="Field required"):
            await mcp.call_tool(
                "query_model",
                {
                    "model_name": "flights",
                    "dimensions": ["carrier"],
                    "measures": ["flight_count"],
                    "order_by": [["field"]],  # Missing direction
                },
            )

        # Test invalid direction - this will pass pydantic validation but fail our validation
        with pytest.raises(ToolError, match="Each order_by tuple must be"):
            await mcp.call_tool(
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

        with pytest.raises(
            ToolError, match="Time grain TIME_GRAIN_SECOND is smaller than"
        ):
            await mcp.call_tool(
                "query_model",
                {
                    "model_name": "flights",
                    "dimensions": ["carrier"],
                    "measures": ["flight_count"],
                    "time_grain": "TIME_GRAIN_SECOND",  # Smaller than TIME_GRAIN_DAY
                },
            )

    @pytest.mark.asyncio
    async def test_query_model_nonexistent(self, sample_models):
        """Test query with non-existent model."""
        mcp = MCPSemanticModel(models=sample_models)

        with pytest.raises(ToolError, match="Model nonexistent not found"):
            await mcp.call_tool(
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

            content_blocks, result_dict = await mcp.call_tool(
                "query_model",
                {
                    "model_name": "flights",
                    "dimensions": ["carrier"],
                    "measures": ["flight_count"],
                    "chart_spec": True,  # Request chart with auto-detection
                },
            )
            result = result_dict["result"]

            # Should return combined data and chart
            assert isinstance(result, dict)
            assert "records" in result
            assert "chart" in result

            # Check records
            assert isinstance(result["records"], list)
            assert len(result["records"]) == 3
            assert result["records"][0]["carrier"] == "AA"
            assert result["records"][0]["flight_count"] == 100

            # Check chart
            assert result["chart"]["mark"] == "bar"
            assert "encoding" in result["chart"]

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
            content_blocks, result_dict = await mcp.call_tool(
                "query_model",
                {
                    "model_name": "flights",
                    "dimensions": ["carrier"],
                    "measures": ["flight_count"],
                    "chart_spec": custom_spec,
                    "chart_format": "interactive",
                },
            )
            result = result_dict["result"]

            # Should return combined data and chart
            assert isinstance(result, dict)
            assert "records" in result
            assert "chart" in result

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

            content_blocks, result_dict = await mcp.call_tool(
                "query_model",
                {
                    "model_name": "flights",
                    "dimensions": ["carrier"],
                    "measures": ["flight_count"],
                    "chart_spec": True,
                    "chart_format": "png",
                },
            )
            result = result_dict["result"]

            # Should return combined data and chart
            assert isinstance(result, dict)
            assert "records" in result
            assert "chart" in result

            # Check PNG format - should be raw bytes decoded to string
            chart = result["chart"]
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

            content_blocks, result_dict = await mcp.call_tool(
                "query_model",
                {
                    "model_name": "flights",
                    "dimensions": ["carrier"],
                    "measures": ["flight_count"],
                    "chart_spec": True,
                    "chart_format": "svg",
                },
            )
            result = result_dict["result"]

            # Should return combined data and chart
            assert isinstance(result, dict)
            assert "records" in result
            assert "chart" in result

            # Check SVG format - should be raw string
            chart = result["chart"]
            assert chart == mock_svg_string
