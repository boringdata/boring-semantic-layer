"""
ECharts chart backend for boring-semantic-layer.

This module provides the interface and types for generating ECharts
chart specifications from semantic layer query results.

Example:
    >>> from boring_semantic_layer.chart.echarts import (
    ...     EChartsBackend,
    ...     EChartsChartType,
    ...     EChartsOption,
    ... )
    >>> backend = EChartsBackend()
    >>> spec = backend.generate_spec(
    ...     df,
    ...     chart_type="bar",
    ...     x="category",
    ...     y="value",
    ...     title="Sales by Category"
    ... )
"""

from .backend import EChartsBackend
from .interface import EChartsBackendInterface
from .types import (
    DEFAULT_COLOR_PALETTE,
    AxisConfig,
    AxisType,
    DatasetConfig,
    EChartsChartType,
    EChartsOption,
    EChartsSeriesType,
    GridConfig,
    LegendConfig,
    LegendPosition,
    Orient,
    SeriesConfig,
    TitleConfig,
    TooltipConfig,
    TooltipTrigger,
)

__all__ = [
    # Implementation
    "EChartsBackend",
    # Interface
    "EChartsBackendInterface",
    # Chart types
    "EChartsChartType",
    "EChartsSeriesType",
    # Enums
    "AxisType",
    "Orient",
    "LegendPosition",
    "TooltipTrigger",
    # TypedDicts
    "EChartsOption",
    "TitleConfig",
    "LegendConfig",
    "TooltipConfig",
    "GridConfig",
    "AxisConfig",
    "SeriesConfig",
    "DatasetConfig",
    # Constants
    "DEFAULT_COLOR_PALETTE",
]
