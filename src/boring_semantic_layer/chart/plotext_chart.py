"""
Plotext backend for chart visualization.

Provides terminal-based charting through the plotext library.
"""

from collections.abc import Sequence
from typing import Any

from .base import ChartBackend
from .utils import (
    clean_field_name,
    detect_chart_type_generic,
    get_chart_detection_params,
    override_chart_type_from_spec,
    sort_dataframe_for_line_chart,
    validate_format,
)


def _format_datetime_labels(dates: list[Any]) -> list[str]:
    """Convert datetime objects to formatted string labels."""
    if not dates or not hasattr(dates[0], "strftime"):
        return None

    sample = dates[0]
    # Monthly/quarterly data - use YYYY-MM format
    if hasattr(sample, "day") and sample.day == 1:
        return [x.strftime("%Y-%m") if hasattr(x, "strftime") else str(x) for x in dates]
    # Daily data - use YYYY-MM-DD format
    return [x.strftime("%Y-%m-%d") if hasattr(x, "strftime") else str(x) for x in dates]


def _setup_axis_ticks(
    plt, positions: list, labels: list[str], axis: str = "x", max_labels: int = 12
):
    """Set axis tick labels with automatic truncation for long lists."""
    if len(labels) > max_labels:
        step = max(1, len(labels) // 10)
        tick_positions = list(range(0, len(labels), step))
        tick_labels = [labels[i] for i in tick_positions]
        if axis == "x":
            plt.xticks(tick_positions, tick_labels)
        else:
            plt.yticks(tick_positions, tick_labels)
    else:
        if axis == "x":
            plt.xticks(positions, labels)
        else:
            plt.yticks(positions, labels)


def _convert_to_positions(data: list[Any]) -> tuple[list[int], list[str] | None]:
    """Convert non-numeric data to numeric positions and string labels."""
    if not data:
        return [], None

    # Already numeric
    if isinstance(data[0], int | float):
        return data, None

    # Datetime or categorical
    labels = _format_datetime_labels(data)
    if labels is None:
        labels = [str(x) for x in data]

    positions = list(range(len(labels)))
    return positions, labels


class PlotextBackend(ChartBackend):
    """Plotext terminal chart backend implementation."""

    def detect_chart_type(
        self,
        dimensions: Sequence[str],
        measures: Sequence[str],
        time_dimension: str | None = None,
    ) -> str | None:
        """
        Auto-detect appropriate chart type based on query structure.

        Uses shared detection logic with Plotext-specific adjustments:
        - "indicator" → "simple" (terminal text display)
        - Two non-time dimensions → "scatter" instead of "heatmap"
        - "table" → None (skip charting for complex queries)
        """
        # Use shared generic detection
        chart_type = detect_chart_type_generic(list(dimensions), list(measures), time_dimension)

        # Plotext-specific overrides
        overrides = {
            "indicator": "simple",  # Terminal text display
            "heatmap": "scatter",  # Plotext doesn't have good heatmap support
            "table": None,  # Skip charting for complex queries
        }
        return overrides.get(chart_type, chart_type)

    def prepare_data(
        self,
        df: Any,
        dimensions: Sequence[str],
        measures: Sequence[str],
        chart_type: str,
        time_dimension: str | None = None,
    ) -> tuple[Any, dict[str, Any]]:
        """Prepare data for Plotext chart creation."""
        # Sort line charts to avoid zigzag connections
        if chart_type == "line":
            df = sort_dataframe_for_line_chart(df, list(dimensions), time_dimension)

        params = {
            "dimensions": dimensions,
            "measures": measures,
            "time_dimension": time_dimension,
        }

        return df, params

    def create_chart(
        self,
        df: Any,
        params: dict[str, Any],
        chart_type: str | None,
        spec: dict[str, Any] | None = None,
    ) -> Any:
        """Create Plotext chart (renders to terminal)."""
        import plotext as plt

        # Skip charting if chart_type is None (complex query not suitable for charting)
        if chart_type is None:
            return None

        plt.clear_figure()
        plt.clear_data()

        # Override chart type from spec if provided (using shared utility)
        chart_type = override_chart_type_from_spec(chart_type, spec)

        # Extract spec parameters
        spec = spec or {}
        theme = spec.get("theme", "pro")
        height = spec.get("height", 40)
        width = spec.get("width")
        show_grid = spec.get("grid", True)
        chart_title = spec.get("title")
        marker_style = spec.get("marker")

        # Configure plot
        plt.theme(theme)
        plt.plotsize(width, height)

        try:
            plt.canvas_color("default")
            plt.axes_color("default")
        except AttributeError:
            pass

        if show_grid:
            plt.grid(True, True)

        dimensions = params["dimensions"]
        measures = params["measures"]
        time_dimension = params["time_dimension"]

        # Route to chart-specific renderer
        if chart_type == "simple":
            self._create_simple_chart(plt, df, measures, chart_title)
        elif chart_type == "bar":
            self._create_bar_chart(plt, df, dimensions, measures, chart_title)
        elif chart_type == "line":
            self._create_line_chart(
                plt, df, dimensions, measures, time_dimension, marker_style, chart_title
            )
        elif chart_type == "scatter":
            self._create_scatter_chart(plt, df, dimensions, measures, marker_style, chart_title)

        return plt

    def _create_simple_chart(self, plt, df, measures, chart_title):
        """Create simple value display."""
        if measures and len(df) > 0:
            value = df[measures[0]].iloc[0]
            plt.text(f"{measures[0]}: {value}", x=0.5, y=0.5)
            plt.title(chart_title or measures[0])

    def _create_bar_chart(self, plt, df, dimensions, measures, chart_title):
        """Create bar chart."""
        if not dimensions or not measures:
            return

        if len(measures) == 1:
            # Single measure bar chart
            x_positions, x_labels = _convert_to_positions(df[dimensions[0]].tolist())
            y_data = df[measures[0]].tolist()

            plt.bar(x_positions, y_data, label=measures[0])

            if x_labels:
                _setup_axis_ticks(plt, x_positions, x_labels, "x")

            plt.xlabel(clean_field_name(dimensions[0]))
            plt.ylabel(clean_field_name(measures[0]))

            title = (
                chart_title
                or f"{clean_field_name(measures[0])} by {clean_field_name(dimensions[0])}"
            )
        else:
            # Multiple measures - grouped bars
            x_labels = df[dimensions[0]].astype(str).tolist()
            x_positions = list(range(len(x_labels)))

            for i, measure in enumerate(measures):
                y_data = df[measure].tolist()
                offset = (i - len(measures) / 2) * 0.2
                plt.bar(
                    [x + offset for x in x_positions],
                    y_data,
                    label=clean_field_name(measure),
                    width=0.2,
                )

            plt.xticks(x_positions, x_labels)
            plt.xlabel(clean_field_name(dimensions[0]))
            plt.ylabel("Value")

            clean_measures = [clean_field_name(m) for m in measures]
            title = (
                chart_title or f"{', '.join(clean_measures)} by {clean_field_name(dimensions[0])}"
            )

        plt.title(title)

    def _create_line_chart(
        self, plt, df, dimensions, measures, time_dimension, marker_style, chart_title
    ):
        """Create line chart."""
        if not dimensions or not measures:
            return

        # Multi-series: time + category dimension
        if time_dimension and len(dimensions) >= 2:
            self._create_multi_series_line(
                plt, df, dimensions, measures, time_dimension, marker_style, chart_title
            )
        # Multiple measures
        elif len(measures) > 1:
            self._create_multi_measure_line(
                plt, df, dimensions, measures, marker_style, chart_title
            )
        # Single line or composite dimensions
        else:
            self._create_single_line(plt, df, dimensions, measures, marker_style, chart_title)

    def _create_multi_series_line(
        self, plt, df, dimensions, measures, time_dimension, marker_style, chart_title
    ):
        """Create multi-series line chart (time + category)."""
        non_time_dims = [d for d in dimensions if d != time_dimension]
        if not non_time_dims:
            return

        categories = df[non_time_dims[0]].unique()

        # Get x-axis data from first category
        first_category_data = df[df[non_time_dims[0]] == categories[0]]
        x_positions, x_labels = _convert_to_positions(first_category_data[time_dimension].tolist())

        # Plot each category
        for category in categories:
            category_data = df[df[non_time_dims[0]] == category]
            y_data = category_data[measures[0]].tolist()

            plot_args = {"label": str(category)}
            if marker_style:
                plot_args["marker"] = marker_style

            plt.plot(x_positions, y_data, **plot_args)

        if x_labels:
            _setup_axis_ticks(plt, x_positions, x_labels, "x")

        plt.xlabel(clean_field_name(time_dimension))
        plt.ylabel(clean_field_name(measures[0]))

        title = (
            chart_title
            or f"{clean_field_name(measures[0])} over {clean_field_name(time_dimension)}"
        )
        plt.title(title)

    def _create_multi_measure_line(self, plt, df, dimensions, measures, marker_style, chart_title):
        """Create line chart with multiple measures."""
        x_positions, x_labels = _convert_to_positions(df[dimensions[0]].tolist())

        for measure in measures:
            y_data = df[measure].tolist()
            plot_args = {"label": clean_field_name(measure)}
            if marker_style:
                plot_args["marker"] = marker_style

            plt.plot(x_positions, y_data, **plot_args)

        if x_labels:
            _setup_axis_ticks(plt, x_positions, x_labels, "x")

        plt.xlabel(clean_field_name(dimensions[0]))
        plt.ylabel("Value")

        clean_measures = [clean_field_name(m) for m in measures]
        title = chart_title or f"{', '.join(clean_measures)} over {clean_field_name(dimensions[0])}"
        plt.title(title)

    def _create_single_line(self, plt, df, dimensions, measures, marker_style, chart_title):
        """Create single line chart."""
        # Composite dimensions (e.g., year + quarter)
        if len(dimensions) >= 2:
            x_labels = ["-".join(str(row[dim]) for dim in dimensions) for _, row in df.iterrows()]
            x_positions = list(range(len(x_labels)))
            y_data = df[measures[0]].tolist()
            xlabel = " + ".join(clean_field_name(d) for d in dimensions)
        else:
            x_positions, x_labels = _convert_to_positions(df[dimensions[0]].tolist())
            y_data = df[measures[0]].tolist()
            xlabel = clean_field_name(dimensions[0])

        plot_args = {"label": clean_field_name(measures[0])}
        if marker_style:
            plot_args["marker"] = marker_style

        plt.plot(x_positions, y_data, **plot_args)

        if x_labels:
            _setup_axis_ticks(plt, x_positions, x_labels, "x")

        plt.xlabel(xlabel)
        plt.ylabel(clean_field_name(measures[0]))

        title = chart_title or f"{clean_field_name(measures[0])} over {xlabel}"
        plt.title(title)

    def _create_scatter_chart(self, plt, df, dimensions, measures, marker_style, chart_title):
        """Create scatter plot."""
        if len(dimensions) < 2:
            return

        x_positions, x_labels = _convert_to_positions(df[dimensions[0]].tolist())
        y_positions, y_labels = _convert_to_positions(df[dimensions[1]].tolist())

        marker = marker_style or "•"
        # TODO: Use measures[0] for marker size or color if available
        plt.scatter(x_positions, y_positions, marker=marker)

        if x_labels:
            _setup_axis_ticks(plt, x_positions, x_labels, "x")
        if y_labels:
            _setup_axis_ticks(plt, y_positions, y_labels, "y")

        plt.xlabel(clean_field_name(dimensions[0]))
        plt.ylabel(clean_field_name(dimensions[1]))

        title = (
            chart_title or f"{clean_field_name(dimensions[1])} vs {clean_field_name(dimensions[0])}"
        )
        plt.title(title)

    def format_output(self, chart_obj: Any, format: str = "static") -> Any:
        """Format Plotext chart output."""
        # Skip if no chart (complex query not suitable for charting)
        if chart_obj is None:
            return None

        # Validate format
        validate_format(format, ["static", "interactive", "string"])

        if format in ("static", "interactive"):
            chart_obj.show()
            return None
        else:  # string
            return chart_obj.build()


def chart(
    semantic_aggregate: Any,
    spec: dict[str, Any] | None = None,
    show_table: bool = True,
    limit: int = 10,
) -> None:
    """
    Render a semantic aggregate query as a Plotext chart.

    Args:
        semantic_aggregate: The SemanticAggregate object to visualize
        spec: Optional chart specification dict (theme, height, width, grid, title, marker, chart_type)
        show_table: Whether to display the data table below the chart
        limit: Maximum number of rows to display in the table
    """
    # Execute query first
    df = semantic_aggregate.execute()

    # Extract chart detection parameters
    dimensions, measures, time_dimension = get_chart_detection_params(semantic_aggregate, df)

    # Create and render chart
    backend = PlotextBackend()
    chart_type = backend.detect_chart_type(dimensions, measures, time_dimension)
    df_prepared, params = backend.prepare_data(
        df,
        dimensions,
        measures,
        chart_type,
        time_dimension,
    )
    chart_obj = backend.create_chart(df_prepared, params, chart_type, spec)
    backend.format_output(chart_obj, "static")

    # Display table
    if show_table:
        display_table(df, limit)


def display_table(df: Any, limit: int = 10) -> None:
    """Display DataFrame as an ASCII table in the terminal."""
    # Get column names and calculate widths
    columns = list(df.columns)
    widths = [len(str(col)) for col in columns]

    # Update widths based on data
    for _, row in df.head(limit).iterrows():
        for i, val in enumerate(row):
            widths[i] = max(widths[i], len(str(val)))

    # Build table with box drawing characters
    lines = []

    # Top border
    top = "┌" + "┬".join("─" * (w + 2) for w in widths) + "┐"
    lines.append(top)

    # Header row
    header_cells = []
    for col, width in zip(columns, widths):
        padding = width - len(str(col))
        header_cells.append(f" {col}{' ' * padding} ")
    lines.append("│" + "│".join(header_cells) + "│")

    # Header separator
    separator = "├" + "┼".join("─" * (w + 2) for w in widths) + "┤"
    lines.append(separator)

    # Data rows
    for _, row in df.head(limit).iterrows():
        row_cells = []
        for val, width in zip(row, widths):
            val_str = str(val)
            padding = width - len(val_str)
            row_cells.append(f" {val_str}{' ' * padding} ")
        lines.append("│" + "│".join(row_cells) + "│")

    # Bottom border
    bottom = "└" + "┴".join("─" * (w + 2) for w in widths) + "┘"
    lines.append(bottom)

    # Print the table
    print("\n".join(lines))

    # Add row count info if truncated
    row_count = len(df)
    if row_count > limit:
        print(f"\nShowing {limit} of {row_count} rows")
