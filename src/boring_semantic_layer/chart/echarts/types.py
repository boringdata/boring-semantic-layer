"""
ECharts-specific type definitions and enums.

Defines the type system for ECharts chart specifications,
including chart types, series types, and configuration options.
"""

from enum import Enum
from typing import Any, TypedDict


class EChartsChartType(str, Enum):
    """Supported ECharts chart types."""

    BAR = "bar"
    LINE = "line"
    PIE = "pie"
    SCATTER = "scatter"
    HEATMAP = "heatmap"
    GAUGE = "gauge"
    FUNNEL = "funnel"
    RADAR = "radar"
    TREEMAP = "treemap"
    SUNBURST = "sunburst"
    CANDLESTICK = "candlestick"
    BOXPLOT = "boxplot"


class EChartsSeriesType(str, Enum):
    """ECharts series types for composite charts."""

    BAR = "bar"
    LINE = "line"
    PIE = "pie"
    SCATTER = "scatter"
    EFFECTSCATTER = "effectScatter"
    HEATMAP = "heatmap"
    GAUGE = "gauge"
    FUNNEL = "funnel"
    RADAR = "radar"
    TREEMAP = "treemap"
    SUNBURST = "sunburst"
    CANDLESTICK = "candlestick"
    BOXPLOT = "boxplot"


class AxisType(str, Enum):
    """ECharts axis types."""

    VALUE = "value"
    CATEGORY = "category"
    TIME = "time"
    LOG = "log"


class Orient(str, Enum):
    """Orientation options."""

    HORIZONTAL = "horizontal"
    VERTICAL = "vertical"


class LegendPosition(str, Enum):
    """Legend position presets."""

    TOP = "top"
    BOTTOM = "bottom"
    LEFT = "left"
    RIGHT = "right"


class TooltipTrigger(str, Enum):
    """Tooltip trigger types."""

    ITEM = "item"
    AXIS = "axis"
    NONE = "none"


class TitleConfig(TypedDict, total=False):
    """ECharts title configuration."""

    text: str
    subtext: str
    left: str | int
    top: str | int
    textStyle: dict[str, Any]


class LegendConfig(TypedDict, total=False):
    """ECharts legend configuration."""

    show: bool
    orient: str
    left: str | int
    top: str | int
    data: list[str]


class TooltipConfig(TypedDict, total=False):
    """ECharts tooltip configuration."""

    show: bool
    trigger: str
    formatter: str


class GridConfig(TypedDict, total=False):
    """ECharts grid configuration."""

    left: str | int
    right: str | int
    top: str | int
    bottom: str | int
    containLabel: bool


class AxisConfig(TypedDict, total=False):
    """ECharts axis configuration."""

    type: str
    name: str
    data: list[Any]
    nameLocation: str
    axisLabel: dict[str, Any]
    splitLine: dict[str, Any]


class SeriesConfig(TypedDict, total=False):
    """ECharts series configuration."""

    name: str
    type: str
    data: list[Any]
    encode: dict[str, str | list[str]]
    itemStyle: dict[str, Any]
    emphasis: dict[str, Any]
    label: dict[str, Any]


class DatasetConfig(TypedDict, total=False):
    """ECharts dataset configuration."""

    source: list[list[Any]] | list[dict[str, Any]]
    dimensions: list[str | dict[str, str]]


class EChartsOption(TypedDict, total=False):
    """
    Complete ECharts option specification.

    This TypedDict provides type hints for the main ECharts option object.
    See https://echarts.apache.org/en/option.html for full documentation.
    """

    title: TitleConfig | list[TitleConfig]
    legend: LegendConfig
    tooltip: TooltipConfig
    grid: GridConfig | list[GridConfig]
    xAxis: AxisConfig | list[AxisConfig]
    yAxis: AxisConfig | list[AxisConfig]
    series: list[SeriesConfig]
    dataset: DatasetConfig | list[DatasetConfig]
    color: list[str]
    backgroundColor: str
    animation: bool
    animationDuration: int


# Default color palette (ECharts default)
DEFAULT_COLOR_PALETTE: list[str] = [
    "#5470c6",
    "#91cc75",
    "#fac858",
    "#ee6666",
    "#73c0de",
    "#3ba272",
    "#fc8452",
    "#9a60b4",
    "#ea7ccc",
]
