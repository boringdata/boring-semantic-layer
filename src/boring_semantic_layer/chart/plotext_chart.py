"""
Plotext backend for chart visualization.

Provides terminal-based charting through the plotext library.
"""

from collections.abc import Sequence
from typing import Any

from .base import ChartBackend


class PlotextBackend(ChartBackend):
    """Plotext terminal chart backend implementation."""

    def detect_chart_type(
        self,
        dimensions: Sequence[str],
        measures: Sequence[str],
        time_dimension: str | None = None,
        time_grain: str | None = None,
    ) -> str:
        """
        Auto-detect appropriate chart type based on query structure for Plotext backend.

        Args:
            dimensions: List of dimension field names from the query
            measures: List of measure field names from the query
            time_dimension: Optional time dimension field name for temporal detection
            time_grain: Optional time grain (unused in Plotext)

        Returns:
            str: Chart type identifier ("bar", "line", "scatter", "table")
        """
        num_dims = len(dimensions)
        num_measures = len(measures)

        # Single value - simple text display
        if num_dims == 0 and num_measures == 1:
            return "simple"

        # Check if we have a time dimension
        has_time = time_dimension and time_dimension in dimensions

        # Single dimension, single measure
        if num_dims == 1 and num_measures == 1:
            return "line" if has_time else "bar"

        # Single dimension, multiple measures - grouped chart
        if num_dims == 1 and num_measures >= 2:
            return "line" if has_time else "bar"

        # Time series with additional dimension(s) - multi-line chart
        if has_time and num_dims >= 2 and num_measures == 1:
            return "line"

        # Two dimensions, one measure
        if num_dims == 2 and num_measures == 1:
            # If one dimension is temporal, use line chart; otherwise scatter
            return "line" if has_time else "scatter"

        # Default for complex queries - table
        return "table"

    def prepare_data(
        self,
        df: Any,
        dimensions: Sequence[str],
        measures: Sequence[str],
        chart_type: str,
        time_dimension: str | None = None,
    ) -> tuple[Any, dict[str, Any]]:
        """
        Prepare data for Plotext chart creation.

        Args:
            df: Pandas DataFrame with query results
            dimensions: List of dimension names
            measures: List of measure names
            chart_type: The chart type string (bar, line, scatter, etc.)
            time_dimension: Optional time dimension name

        Returns:
            tuple: (dataframe, params_dict) where:
                - dataframe: Processed pandas DataFrame ready for plotting
                - params_dict: Dict of parameters for plotext functions
        """

        # Handle data sorting for line charts to avoid zigzag connections
        if chart_type == "line" and dimensions:
            if time_dimension and time_dimension in dimensions:
                # Sort by time dimension for temporal data
                sort_cols = [time_dimension]
                non_time_dims = [d for d in dimensions if d != time_dimension]
                if non_time_dims:
                    sort_cols.extend(non_time_dims)
                df = df.sort_values(by=sort_cols)
            else:
                # For categorical data converted to line, sort by x-axis for consistency
                df = df.sort_values(by=dimensions[0])

        # Build parameters for plotext
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
        chart_type: str,
        spec: dict[str, Any] | None = None,
    ) -> Any:
        """
        Create Plotext chart (renders to terminal).

        Args:
            df: Processed DataFrame
            params: Parameters from prepare_data
            chart_type: Chart type string
            spec: Optional custom specification (can override chart_type)

        Returns:
            plotext module with configured chart (ready to show())
        """
        import plotext as plt

        # Clear any previous plots
        plt.clear_figure()
        plt.clear_data()

        # Extract style parameters from spec
        theme = spec.get("theme", "pro") if spec else "pro"
        height = (
            spec.get("height", 40) if spec else 40
        )  # Increased from 30 to 40 for even better resolution
        width = spec.get("width") if spec else None  # Allow explicit width control
        show_grid = spec.get("grid", True) if spec else True
        chart_title = spec.get("title") if spec else None
        marker_style = spec.get("marker") if spec else None

        # Set theme after clearing (plotext requires this order)
        plt.theme(theme)

        # Set plot size
        plt.plotsize(width, height)

        # Enable canvas mode for better rendering quality
        try:
            # Plotext's canvas mode provides smoother lines
            plt.canvas_color("default")
            plt.axes_color("default")
        except AttributeError:
            # Older plotext versions may not have these methods
            pass

        # Enable grid if requested
        if show_grid:
            plt.grid(True, True)  # horizontal, vertical

        # Override chart type from spec if provided
        if spec and "chart_type" in spec:
            chart_type = spec["chart_type"]

        dimensions = params.get("dimensions", [])
        measures = params.get("measures", [])
        time_dimension = params.get("time_dimension")

        # Simple value display
        if chart_type == "simple":
            if measures and len(df) > 0:
                value = df[measures[0]].iloc[0]
                plt.text(f"{measures[0]}: {value}", x=0.5, y=0.5)
                plt.title(measures[0])
                # Override with custom title if provided
                if chart_title:
                    plt.title(chart_title)
            return plt

        # Table display
        if chart_type == "table":
            # Plotext doesn't have built-in table support, so we'll create a simple text representation
            plt.theme("clear")
            table_str = df.to_string(index=False)
            # Split into lines and display
            lines = table_str.split("\n")
            plt.text("\n".join(lines), x=0, y=0)
            plt.title("Data Table")
            # Override with custom title if provided
            if chart_title:
                plt.title(chart_title)
            return plt

        # Bar chart
        if chart_type == "bar":
            if dimensions and measures:
                if len(measures) == 1:
                    # Single measure bar chart
                    x_data = df[dimensions[0]].tolist()
                    y_data = df[measures[0]].tolist()

                    # Convert x_data to strings if they're not numeric
                    x_labels = None
                    if hasattr(x_data[0], "strftime") or not isinstance(
                        x_data[0], int | float
                    ):  # datetime
                        x_labels = [str(x) for x in x_data]
                        x_data = list(range(len(x_data)))

                    plt.bar(x_data, y_data, label=measures[0])

                    # Set x-tick labels AFTER creating the bar chart
                    if x_labels is not None:
                        # For many categories, show only a subset of labels (max 10-12 labels)
                        if len(x_labels) > 12:
                            step = max(1, len(x_labels) // 10)
                            xtick_positions = list(range(0, len(x_labels), step))
                            xtick_labels = [x_labels[i] for i in xtick_positions]
                            plt.xticks(xtick_positions, xtick_labels)
                        else:
                            plt.xticks(x_data, x_labels)
                    # Strip model prefix from dimension names for cleaner display
                    xlabel = dimensions[0].split(".")[-1] if "." in dimensions[0] else dimensions[0]
                    ylabel = measures[0].split(".")[-1] if "." in measures[0] else measures[0]
                    plt.xlabel(xlabel)
                    plt.ylabel(ylabel)
                else:
                    # Multiple measures - grouped bars
                    x_labels = df[dimensions[0]].astype(str).tolist()
                    x_data = list(range(len(x_labels)))

                    for i, measure in enumerate(measures):
                        y_data = df[measure].tolist()
                        # Offset bars for grouping
                        offset = (i - len(measures) / 2) * 0.2
                        plt.bar([x + offset for x in x_data], y_data, label=measure, width=0.2)

                    plt.xticks(x_data, x_labels)
                    xlabel = dimensions[0].split(".")[-1] if "." in dimensions[0] else dimensions[0]
                    plt.xlabel(xlabel)
                    plt.ylabel("Value")

                # Strip model prefixes from title for cleaner display
                clean_measures = [m.split(".")[-1] if "." in m else m for m in measures]
                clean_dim = dimensions[0].split(".")[-1] if "." in dimensions[0] else dimensions[0]
                plt.title(f"{', '.join(clean_measures)} by {clean_dim}")
                # Override with custom title if provided
                if chart_title:
                    plt.title(chart_title)

        # Line chart
        elif chart_type == "line":
            if dimensions and measures:
                # Check for multiple series (time + category dimension)
                if time_dimension and len(dimensions) >= 2:
                    non_time_dims = [d for d in dimensions if d != time_dimension]
                    if non_time_dims:
                        # Multi-line chart by category
                        categories = df[non_time_dims[0]].unique()

                        # Get time data for x-axis labels
                        first_category = categories[0]
                        category_data = df[df[non_time_dims[0]] == first_category]
                        x_data_original = category_data[time_dimension].tolist()

                        # Prepare x-axis: convert datetime to string labels
                        x_labels = None
                        if hasattr(x_data_original[0], "strftime"):
                            # Format datetime - use shorter format for better readability
                            # Try to detect if it's monthly/quarterly data by checking if day is 1
                            sample = x_data_original[0]
                            if hasattr(sample, "day") and sample.day == 1:
                                # Monthly/quarterly data - use YYYY-MM format
                                x_labels = [
                                    x.strftime("%Y-%m") if hasattr(x, "strftime") else str(x)
                                    for x in x_data_original
                                ]
                            else:
                                # Daily data - use YYYY-MM-DD format
                                x_labels = [
                                    x.strftime("%Y-%m-%d") if hasattr(x, "strftime") else str(x)
                                    for x in x_data_original
                                ]
                            x_positions = list(range(len(x_labels)))
                        else:
                            x_positions = x_data_original

                        # Plot each category
                        for category in categories:
                            category_data = df[df[non_time_dims[0]] == category]
                            y_data = category_data[measures[0]].tolist()
                            if marker_style:
                                plt.plot(
                                    x_positions, y_data, label=str(category), marker=marker_style
                                )
                            else:
                                plt.plot(x_positions, y_data, label=str(category))

                        # Set x-tick labels if we have datetime data
                        if x_labels is not None:
                            # For many time points, show only a subset of labels (max 10-12 labels)
                            if len(x_labels) > 12:
                                step = max(1, len(x_labels) // 10)
                                xtick_positions = list(range(0, len(x_labels), step))
                                xtick_labels = [x_labels[i] for i in xtick_positions]
                                plt.xticks(xtick_positions, xtick_labels)
                            else:
                                plt.xticks(x_positions, x_labels)

                        # Strip model prefixes for cleaner display
                        xlabel = (
                            time_dimension.split(".")[-1]
                            if "." in time_dimension
                            else time_dimension
                        )
                        ylabel = measures[0].split(".")[-1] if "." in measures[0] else measures[0]
                        plt.xlabel(xlabel)
                        plt.ylabel(ylabel)
                        clean_measure = (
                            measures[0].split(".")[-1] if "." in measures[0] else measures[0]
                        )
                        clean_time = (
                            time_dimension.split(".")[-1]
                            if "." in time_dimension
                            else time_dimension
                        )
                        plt.title(f"{clean_measure} over {clean_time}")
                        # Override with custom title if provided
                        if chart_title:
                            plt.title(chart_title)
                elif len(measures) > 1:
                    # Multiple measures as separate lines
                    x_data = df[dimensions[0]].tolist()

                    # Convert datetime to string labels if needed
                    x_labels = None
                    if hasattr(x_data[0], "strftime"):
                        # Format datetime - use shorter format for better readability
                        sample = x_data[0]
                        if hasattr(sample, "day") and sample.day == 1:
                            # Monthly/quarterly data - use YYYY-MM format
                            x_labels = [
                                x.strftime("%Y-%m") if hasattr(x, "strftime") else str(x)
                                for x in x_data
                            ]
                        else:
                            # Daily data - use YYYY-MM-DD format
                            x_labels = [
                                x.strftime("%Y-%m-%d") if hasattr(x, "strftime") else str(x)
                                for x in x_data
                            ]
                        x_positions = list(range(len(x_labels)))
                    elif not isinstance(x_data[0], int | float):
                        x_labels = [str(x) for x in x_data]
                        x_positions = list(range(len(x_data)))
                    else:
                        x_positions = x_data

                    for measure in measures:
                        y_data = df[measure].tolist()
                        clean_label = measure.split(".")[-1] if "." in measure else measure
                        if marker_style:
                            plt.plot(x_positions, y_data, label=clean_label, marker=marker_style)
                        else:
                            plt.plot(x_positions, y_data, label=clean_label)

                    # Set x-tick labels if we converted to positions
                    if x_labels is not None:
                        # For many time points, show only a subset of labels (max 10-12 labels)
                        if len(x_labels) > 12:
                            step = max(1, len(x_labels) // 10)
                            xtick_positions = list(range(0, len(x_labels), step))
                            xtick_labels = [x_labels[i] for i in xtick_positions]
                            plt.xticks(xtick_positions, xtick_labels)
                        else:
                            plt.xticks(x_positions, x_labels)

                    xlabel = dimensions[0].split(".")[-1] if "." in dimensions[0] else dimensions[0]
                    plt.xlabel(xlabel)
                    plt.ylabel("Value")
                    clean_measures = [m.split(".")[-1] if "." in m else m for m in measures]
                    clean_dim = (
                        dimensions[0].split(".")[-1] if "." in dimensions[0] else dimensions[0]
                    )
                    plt.title(f"{', '.join(clean_measures)} over {clean_dim}")
                    # Override with custom title if provided
                    if chart_title:
                        plt.title(chart_title)
                else:
                    # Single line chart (or composite dimensions like year+quarter)
                    # If we have multiple dimensions, create composite labels
                    if len(dimensions) >= 2:
                        # Create composite x-axis labels from all dimensions
                        x_labels = []
                        for _, row in df.iterrows():
                            label_parts = [str(row[dim]) for dim in dimensions]
                            x_labels.append("-".join(label_parts))
                        x_positions = list(range(len(x_labels)))
                        y_data = df[measures[0]].tolist()
                    else:
                        # Single dimension case
                        x_data = df[dimensions[0]].tolist()
                        y_data = df[measures[0]].tolist()

                        # Convert datetime to string labels if needed
                        x_labels = None
                        if hasattr(x_data[0], "strftime"):
                            # Format datetime as readable string
                            x_labels = [
                                x.strftime("%Y-%m-%d") if hasattr(x, "strftime") else str(x)
                                for x in x_data
                            ]
                            x_positions = list(range(len(x_labels)))
                        elif not isinstance(x_data[0], int | float):
                            x_labels = [str(x) for x in x_data]
                            x_positions = list(range(len(x_data)))
                        else:
                            x_positions = x_data

                    clean_label = measures[0].split(".")[-1] if "." in measures[0] else measures[0]
                    if marker_style:
                        plt.plot(x_positions, y_data, label=clean_label, marker=marker_style)
                    else:
                        plt.plot(x_positions, y_data, label=clean_label)

                    # Set x-tick labels if we converted to positions
                    if x_labels is not None:
                        # For many time points, show only a subset of labels (max 10-12 labels)
                        if len(x_labels) > 12:
                            step = max(1, len(x_labels) // 10)
                            xtick_positions = list(range(0, len(x_labels), step))
                            xtick_labels = [x_labels[i] for i in xtick_positions]
                            plt.xticks(xtick_positions, xtick_labels)
                        else:
                            plt.xticks(x_positions, x_labels)

                    # Labels and title
                    if len(dimensions) >= 2:
                        xlabel = " + ".join(
                            [d.split(".")[-1] if "." in d else d for d in dimensions]
                        )
                    else:
                        xlabel = (
                            dimensions[0].split(".")[-1] if "." in dimensions[0] else dimensions[0]
                        )

                    ylabel = measures[0].split(".")[-1] if "." in measures[0] else measures[0]
                    plt.xlabel(xlabel)
                    plt.ylabel(ylabel)
                    clean_measure = (
                        measures[0].split(".")[-1] if "." in measures[0] else measures[0]
                    )
                    plt.title(f"{clean_measure} over {xlabel}")
                    # Override with custom title if provided
                    if chart_title:
                        plt.title(chart_title)

        # Scatter plot
        elif chart_type == "scatter" and len(dimensions) >= 2 and measures:
            x_data = df[dimensions[0]].tolist()
            y_data = df[dimensions[1]].tolist()

            # Store original labels before converting to numeric
            x_labels = None
            y_labels = None

            # Convert to numeric if needed
            if not isinstance(x_data[0], int | float):
                x_labels = [str(x) for x in x_data]
                x_data = list(range(len(x_data)))
            if not isinstance(y_data[0], int | float):
                y_labels = [str(y) for y in y_data]
                y_data = list(range(len(y_data)))

            # Use measure for marker size if available
            scatter_marker = marker_style if marker_style else "•"
            if measures:
                df[measures[0]].tolist()
                plt.scatter(x_data, y_data, marker=scatter_marker)
            else:
                plt.scatter(x_data, y_data, marker=scatter_marker)

            # Set tick labels if we converted categorical data
            if x_labels:
                plt.xticks(list(range(len(x_labels))), x_labels)
            if y_labels:
                plt.yticks(list(range(len(y_labels))), y_labels)

            xlabel = dimensions[0].split(".")[-1] if "." in dimensions[0] else dimensions[0]
            ylabel = dimensions[1].split(".")[-1] if "." in dimensions[1] else dimensions[1]
            plt.xlabel(xlabel)
            plt.ylabel(ylabel)
            clean_dim1 = dimensions[1].split(".")[-1] if "." in dimensions[1] else dimensions[1]
            clean_dim0 = dimensions[0].split(".")[-1] if "." in dimensions[0] else dimensions[0]
            plt.title(f"{clean_dim1} vs {clean_dim0}")
            # Override with custom title if provided
            if chart_title:
                plt.title(chart_title)

        return plt

    def format_output(self, chart_obj: Any, format: str = "static") -> Any:
        """
        Format Plotext chart output.

        Args:
            chart_obj: Plotext plt module with configured chart
            format: Output format ("static", "interactive", "string")

        Returns:
            For "static"/"interactive": shows plot and returns None
            For "string": returns string representation of the plot
        """
        if format == "static" or format == "interactive":
            # Display the plot in the terminal
            chart_obj.show()
            return None
        elif format == "string":
            # Return string representation (build without showing)
            return chart_obj.build()
        else:
            raise ValueError(
                f"Unsupported format: {format}. "
                "Supported formats: 'static', 'interactive', 'string'"
            )
