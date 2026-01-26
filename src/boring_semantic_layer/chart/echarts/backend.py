"""
ECharts backend implementation.

Provides concrete implementation of EChartsBackendInterface for generating
ECharts option specifications from data.
"""

from __future__ import annotations

from typing import Any

from .interface import EChartsBackendInterface
from .types import (
    AxisConfig,
    AxisType,
    DEFAULT_COLOR_PALETTE,
    EChartsChartType,
    EChartsOption,
    GridConfig,
    LegendConfig,
    SeriesConfig,
    TitleConfig,
    TooltipConfig,
    TooltipTrigger,
)


def _to_records(data: Any) -> list[dict[str, Any]]:
    """Convert data to list of records."""
    # Handle pandas DataFrame
    if hasattr(data, "to_dict"):
        return data.to_dict("records")  # type: ignore
    
    # Handle dict with 'data' key
    if isinstance(data, dict) and "data" in data:
        return data["data"]
    
    # Handle list of dicts
    if isinstance(data, list):
        return data
    
    raise ValueError(f"Unsupported data type: {type(data)}")


def _extract_column(records: list[dict[str, Any]], field: str) -> list[Any]:
    """Extract values for a field from records."""
    return [r.get(field) for r in records]


def _group_by_color(
    records: list[dict[str, Any]],
    x_field: str,
    y_field: str,
    color_field: str,
) -> dict[str, list[dict[str, Any]]]:
    """Group records by color field for multi-series charts."""
    groups: dict[str, list[dict[str, Any]]] = {}
    for record in records:
        key = str(record.get(color_field, ""))
        if key not in groups:
            groups[key] = []
        groups[key].append(record)
    return groups


class EChartsBackend(EChartsBackendInterface):
    """
    ECharts specification generator.
    
    Generates ECharts option specs from data for various chart types.
    Supports pandas DataFrames and list of dicts as input.
    
    Example:
        >>> backend = EChartsBackend()
        >>> spec = backend.generate_spec(
        ...     df,
        ...     chart_type="bar",
        ...     x="category",
        ...     y="value",
        ...     title="Sales by Category"
        ... )
    """

    # Virtual chart types that map to real types with options
    _VIRTUAL_TYPES = {"area": "line"}

    def supported_chart_types(self) -> list[str]:
        """Return list of supported chart types."""
        base_types = [ct.value for ct in EChartsChartType]
        return base_types + list(self._VIRTUAL_TYPES.keys())

    def generate_spec(
        self,
        data: Any,
        chart_type: str | EChartsChartType,
        **kwargs: Any,
    ) -> EChartsOption:
        """
        Generate ECharts option specification from data.
        
        Args:
            data: DataFrame or dict with 'data' key containing records
            chart_type: Chart type (bar, line, pie, scatter, area, etc.)
            **kwargs: Chart options:
                - x: Field name for x-axis (categories)
                - y: Field name for y-axis (values)
                - color: Field for series grouping
                - title: Chart title string
                - subtitle: Chart subtitle
                - x_label: X-axis label
                - y_label: Y-axis label
                - show_legend: Whether to show legend (default True)
                - show_tooltip: Whether to show tooltip (default True)
                - animation: Whether to animate (default True)
                - colors: Custom color palette
                - overrides: Dict of raw ECharts options to merge
        
        Returns:
            EChartsOption dict ready for echarts.setOption()
        """
        # Normalize chart type to string
        if isinstance(chart_type, EChartsChartType):
            chart_type_str = chart_type.value
        else:
            chart_type_str = chart_type
        
        # Handle virtual types (area -> line with areaStyle)
        if chart_type_str == "area":
            kwargs["_area_style"] = True
            chart_type_str = "line"
        
        # Validate after resolving virtual types
        chart_type_str = self.validate_chart_type(chart_type_str)
        records = _to_records(data)
        
        # Generate base spec based on chart type
        generators = {
            "bar": self._generate_bar,
            "line": self._generate_line,
            "pie": self._generate_pie,
            "scatter": self._generate_scatter,
        }
        
        generator = generators.get(chart_type_str)
        if generator is None:
            raise ValueError(
                f"Chart type '{chart_type_str}' is recognized but not yet implemented. "
                f"Implemented types: {list(generators.keys())}"
            )
        
        spec = generator(records, **kwargs)
        
        # Apply overrides if provided
        overrides = kwargs.get("overrides")
        if overrides:
            spec = self.merge_options(spec, overrides)
        
        return spec

    def _build_base_options(self, **kwargs: Any) -> EChartsOption:
        """Build common base options for all chart types."""
        spec: EChartsOption = {}
        
        # Title
        title_text = kwargs.get("title")
        if title_text:
            title_config: TitleConfig = {"text": title_text}
            subtitle = kwargs.get("subtitle")
            if subtitle:
                title_config["subtext"] = subtitle
            spec["title"] = title_config
        
        # Tooltip
        if kwargs.get("show_tooltip", True):
            spec["tooltip"] = TooltipConfig(show=True)
        
        # Animation
        spec["animation"] = kwargs.get("animation", True)
        
        # Custom colors
        colors = kwargs.get("colors")
        if colors:
            spec["color"] = colors
        else:
            spec["color"] = DEFAULT_COLOR_PALETTE
        
        return spec

    def _build_cartesian_axes(
        self,
        categories: list[Any],
        **kwargs: Any,
    ) -> tuple[AxisConfig, AxisConfig]:
        """Build x and y axis configs for cartesian charts."""
        x_axis: AxisConfig = {
            "type": AxisType.CATEGORY.value,
            "data": categories,
        }
        if kwargs.get("x_label"):
            x_axis["name"] = kwargs["x_label"]
        
        y_axis: AxisConfig = {
            "type": AxisType.VALUE.value,
        }
        if kwargs.get("y_label"):
            y_axis["name"] = kwargs["y_label"]
        
        return x_axis, y_axis

    def _generate_bar(
        self,
        records: list[dict[str, Any]],
        **kwargs: Any,
    ) -> EChartsOption:
        """Generate bar chart spec."""
        x_field = kwargs.get("x")
        y_field = kwargs.get("y")
        color_field = kwargs.get("color")
        
        if not x_field or not y_field:
            raise ValueError("Bar chart requires 'x' and 'y' field arguments")
        
        spec = self._build_base_options(**kwargs)
        
        # Enable axis trigger for bar charts
        spec["tooltip"] = TooltipConfig(
            show=kwargs.get("show_tooltip", True),
            trigger=TooltipTrigger.AXIS.value,
        )
        
        # Grid with padding
        spec["grid"] = GridConfig(
            left="3%",
            right="4%",
            bottom="3%",
            containLabel=True,
        )
        
        if color_field:
            # Multi-series bar chart
            groups = _group_by_color(records, x_field, y_field, color_field)
            
            # Get unique categories from all records
            categories = list(dict.fromkeys(_extract_column(records, x_field)))
            x_axis, y_axis = self._build_cartesian_axes(categories, **kwargs)
            spec["xAxis"] = x_axis
            spec["yAxis"] = y_axis
            
            # Build series for each group
            series: list[SeriesConfig] = []
            for name, group_records in groups.items():
                # Build a map of x -> y for this group
                value_map = {r[x_field]: r[y_field] for r in group_records}
                values = [value_map.get(cat, 0) for cat in categories]
                
                series.append(SeriesConfig(
                    name=name,
                    type="bar",
                    data=values,
                ))
            spec["series"] = series
            
            # Show legend for multi-series
            if kwargs.get("show_legend", True):
                spec["legend"] = LegendConfig(
                    show=True,
                    data=list(groups.keys()),
                )
        else:
            # Single series bar chart
            categories = _extract_column(records, x_field)
            values = _extract_column(records, y_field)
            
            x_axis, y_axis = self._build_cartesian_axes(categories, **kwargs)
            spec["xAxis"] = x_axis
            spec["yAxis"] = y_axis
            
            spec["series"] = [
                SeriesConfig(
                    type="bar",
                    data=values,
                )
            ]
        
        return spec

    def _generate_line(
        self,
        records: list[dict[str, Any]],
        **kwargs: Any,
    ) -> EChartsOption:
        """Generate line chart spec (also handles area with _area_style)."""
        x_field = kwargs.get("x")
        y_field = kwargs.get("y")
        color_field = kwargs.get("color")
        area_style = kwargs.get("_area_style", False)
        
        if not x_field or not y_field:
            raise ValueError("Line chart requires 'x' and 'y' field arguments")
        
        spec = self._build_base_options(**kwargs)
        
        # Enable axis trigger for line charts
        spec["tooltip"] = TooltipConfig(
            show=kwargs.get("show_tooltip", True),
            trigger=TooltipTrigger.AXIS.value,
        )
        
        # Grid with padding
        spec["grid"] = GridConfig(
            left="3%",
            right="4%",
            bottom="3%",
            containLabel=True,
        )
        
        if color_field:
            # Multi-series line chart
            groups = _group_by_color(records, x_field, y_field, color_field)
            
            # Get unique categories from all records
            categories = list(dict.fromkeys(_extract_column(records, x_field)))
            x_axis, y_axis = self._build_cartesian_axes(categories, **kwargs)
            spec["xAxis"] = x_axis
            spec["yAxis"] = y_axis
            
            series: list[SeriesConfig] = []
            for name, group_records in groups.items():
                value_map = {r[x_field]: r[y_field] for r in group_records}
                values = [value_map.get(cat, None) for cat in categories]
                
                series_config = SeriesConfig(
                    name=name,
                    type="line",
                    data=values,
                )
                if area_style:
                    series_config["areaStyle"] = {}  # type: ignore
                series.append(series_config)
            spec["series"] = series
            
            if kwargs.get("show_legend", True):
                spec["legend"] = LegendConfig(
                    show=True,
                    data=list(groups.keys()),
                )
        else:
            # Single series line chart
            categories = _extract_column(records, x_field)
            values = _extract_column(records, y_field)
            
            x_axis, y_axis = self._build_cartesian_axes(categories, **kwargs)
            spec["xAxis"] = x_axis
            spec["yAxis"] = y_axis
            
            series_config = SeriesConfig(
                type="line",
                data=values,
            )
            if area_style:
                series_config["areaStyle"] = {}  # type: ignore
            
            spec["series"] = [series_config]
        
        return spec

    def _generate_pie(
        self,
        records: list[dict[str, Any]],
        **kwargs: Any,
    ) -> EChartsOption:
        """Generate pie chart spec."""
        # For pie, we need name and value fields
        name_field = kwargs.get("x") or kwargs.get("name")
        value_field = kwargs.get("y") or kwargs.get("value")
        
        if not name_field or not value_field:
            raise ValueError(
                "Pie chart requires 'x'/'name' and 'y'/'value' field arguments"
            )
        
        spec = self._build_base_options(**kwargs)
        
        # Pie uses item trigger
        spec["tooltip"] = TooltipConfig(
            show=kwargs.get("show_tooltip", True),
            trigger=TooltipTrigger.ITEM.value,
        )
        
        # Build pie data as list of {name, value} dicts
        pie_data = [
            {"name": str(r.get(name_field, "")), "value": r.get(value_field, 0)}
            for r in records
        ]
        
        # Show legend
        if kwargs.get("show_legend", True):
            spec["legend"] = LegendConfig(
                show=True,
                orient="horizontal",
                top="bottom",
            )
        
        spec["series"] = [
            SeriesConfig(
                type="pie",
                data=pie_data,  # type: ignore
                label={"show": True, "formatter": "{b}: {d}%"},
            )
        ]
        
        return spec

    def _generate_scatter(
        self,
        records: list[dict[str, Any]],
        **kwargs: Any,
    ) -> EChartsOption:
        """Generate scatter chart spec."""
        x_field = kwargs.get("x")
        y_field = kwargs.get("y")
        color_field = kwargs.get("color")
        
        if not x_field or not y_field:
            raise ValueError("Scatter chart requires 'x' and 'y' field arguments")
        
        spec = self._build_base_options(**kwargs)
        
        # Scatter uses item trigger
        spec["tooltip"] = TooltipConfig(
            show=kwargs.get("show_tooltip", True),
            trigger=TooltipTrigger.ITEM.value,
        )
        
        # Grid with padding
        spec["grid"] = GridConfig(
            left="3%",
            right="4%",
            bottom="3%",
            containLabel=True,
        )
        
        # Scatter uses value axes for both x and y
        x_axis: AxisConfig = {"type": AxisType.VALUE.value}
        y_axis: AxisConfig = {"type": AxisType.VALUE.value}
        
        if kwargs.get("x_label"):
            x_axis["name"] = kwargs["x_label"]
        if kwargs.get("y_label"):
            y_axis["name"] = kwargs["y_label"]
        
        spec["xAxis"] = x_axis
        spec["yAxis"] = y_axis
        
        if color_field:
            # Multi-series scatter
            groups = _group_by_color(records, x_field, y_field, color_field)
            
            series: list[SeriesConfig] = []
            for name, group_records in groups.items():
                # Scatter data is [[x, y], [x, y], ...]
                scatter_data = [
                    [r.get(x_field), r.get(y_field)]
                    for r in group_records
                ]
                series.append(SeriesConfig(
                    name=name,
                    type="scatter",
                    data=scatter_data,  # type: ignore
                ))
            spec["series"] = series
            
            if kwargs.get("show_legend", True):
                spec["legend"] = LegendConfig(
                    show=True,
                    data=list(groups.keys()),
                )
        else:
            # Single series scatter
            scatter_data = [
                [r.get(x_field), r.get(y_field)]
                for r in records
            ]
            
            spec["series"] = [
                SeriesConfig(
                    type="scatter",
                    data=scatter_data,  # type: ignore
                )
            ]
        
        return spec
