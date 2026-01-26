"""
ECharts backend implementation.

Provides concrete implementation of EChartsBackendInterface for generating
ECharts option specifications from data.
"""

from __future__ import annotations

import re
from datetime import date, datetime
from typing import Any

from .interface import EChartsBackendInterface
from .types import (
    DEFAULT_COLOR_PALETTE,
    AxisConfig,
    AxisType,
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


# Date-related patterns for column name detection
_DATE_PATTERNS = re.compile(
    r"(date|time|timestamp|datetime|day|month|year|week|quarter|period|created|updated|modified)(_at|_on)?$",
    re.IGNORECASE,
)


def _is_date_column_name(name: str) -> bool:
    """Check if column name suggests date/time data."""
    return bool(_DATE_PATTERNS.search(name))


def _is_date_value(value: Any) -> bool:
    """Check if a value looks like a date."""
    if value is None:
        return False
    if isinstance(value, date | datetime):
        return True
    if not isinstance(value, str):
        return False
    # Try common date patterns
    date_patterns = [
        r"^\d{4}-\d{2}-\d{2}",  # ISO date
        r"^\d{2}/\d{2}/\d{4}",  # US date
        r"^\d{2}\.\d{2}\.\d{4}",  # EU date
        r"^\d{4}/\d{2}/\d{2}",  # Alt ISO
    ]
    return any(re.match(p, str(value)) for p in date_patterns)


def _is_numeric(value: Any) -> bool:
    """Check if a value is numeric."""
    if value is None:
        return False
    if isinstance(value, bool):
        return False
    if isinstance(value, int | float):
        return True
    if isinstance(value, str):
        try:
            float(value)
            return True
        except ValueError:
            return False
    return False


def _get_column_info(records: list[dict[str, Any]], field: str) -> dict[str, Any]:
    """Analyze a column and return info about its type and cardinality."""
    values = [r.get(field) for r in records if r.get(field) is not None]
    if not values:
        return {"is_numeric": False, "is_date": False, "cardinality": 0, "count": 0}
    
    unique_values = set(str(v) for v in values)
    cardinality = len(unique_values)
    
    # Check if numeric (sample first few non-null values)
    sample = values[:min(10, len(values))]
    is_numeric = all(_is_numeric(v) for v in sample)
    
    # Check if date-like
    is_date = _is_date_column_name(field) or all(_is_date_value(v) for v in sample)
    
    return {
        "is_numeric": is_numeric,
        "is_date": is_date,
        "cardinality": cardinality,
        "count": len(values),
    }


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
        return base_types + list(self._VIRTUAL_TYPES.keys()) + ["auto"]

    def detect_chart_type(
        self,
        data: Any,
        x: str | None,
        y: str | None,
        color: str | None = None,
    ) -> str:
        """
        Auto-detect best chart type based on data shape.
        
        Rules:
        - If no x/y specified and data has 2 columns: scatter
        - If x is categorical (few unique values) and y is numeric: bar
        - If x looks like dates/time: line
        - If only one numeric column: pie (use other column for labels)
        - If color specified with categorical x: grouped bar
        - Default: bar
        
        Args:
            data: Input data (DataFrame or list of dicts)
            x: Field name for x-axis (optional)
            y: Field name for y-axis (optional)
            color: Field for color grouping (optional)
        
        Returns:
            Detected chart type string (bar, line, pie, scatter)
        """
        records = _to_records(data)
        if not records:
            return "bar"  # Default for empty data
        
        # Get column names
        columns = list(records[0].keys())
        row_count = len(records)
        
        # If no x/y specified
        if not x and not y:
            if len(columns) == 2:
                # Two columns - check if both numeric for scatter
                col1_info = _get_column_info(records, columns[0])
                col2_info = _get_column_info(records, columns[1])
                if col1_info["is_numeric"] and col2_info["is_numeric"]:
                    return "scatter"
            # Default: use first column as x, second as y
            if len(columns) >= 2:
                x = columns[0]
                y = columns[1]
            else:
                return "bar"
        
        # Analyze x and y columns
        x_info = _get_column_info(records, x) if x else {"is_numeric": False, "is_date": False, "cardinality": 0}
        y_info = _get_column_info(records, y) if y else {"is_numeric": False, "is_date": False, "cardinality": 0}
        
        # Rule: If x is date-like, use line chart
        if x_info["is_date"]:
            return "line"
        
        # Rule: If many rows (>50) and x is numeric (continuous), use line
        if row_count > 50 and x_info["is_numeric"] and x_info["cardinality"] > 20:
            return "line"
        
        # Rule: If both are numeric, scatter for exploring relationships
        if x_info["is_numeric"] and y_info["is_numeric"]:
            if row_count <= 500:
                return "scatter"
            # Too many points for scatter, use line
            return "line"
        
        # Rule: If x is categorical with very few categories and y is numeric -> pie
        # Pie is best for showing parts of a whole (<=6 slices is readable)
        if (
            not x_info["is_numeric"]
            and y_info["is_numeric"]
            and x_info["cardinality"] <= 6
            and not color  # Don't use pie for grouped data
        ):
            return "pie"
        
        # Rule: If x is categorical (low cardinality) and y is numeric -> bar
        if not x_info["is_numeric"] and y_info["is_numeric"] and x_info["cardinality"] < 20:
            return "bar"
        
        # Default: bar chart
        return "bar"

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
            chart_type: Chart type (bar, line, pie, scatter, area, auto, etc.)
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
        
        # Handle auto detection
        if chart_type_str == "auto":
            chart_type_str = self.detect_chart_type(
                data,
                x=kwargs.get("x"),
                y=kwargs.get("y"),
                color=kwargs.get("color"),
            )
        
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
                values = [value_map.get(cat) for cat in categories]
                
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
