"""
Base chart backend interface for semantic API visualizations.

Defines the abstract interface that all chart backends must implement.
"""

from abc import ABC, abstractmethod
from collections.abc import Sequence
from typing import Any


class ChartBackend(ABC):
    """
    Abstract base class for chart backends.

    All chart backend implementations must inherit from this class and
    implement the required methods.
    """

    @abstractmethod
    def detect_chart_type(
        self,
        dimensions: Sequence[str],
        measures: Sequence[str],
        time_dimension: str | None = None,
        time_grain: str | None = None,
    ) -> str | dict[str, Any]:
        """
        Auto-detect appropriate chart type/spec based on query structure.

        Args:
            dimensions: List of dimension field names from the query
            measures: List of measure field names from the query
            time_dimension: Optional time dimension field name for temporal detection
            time_grain: Optional time grain for temporal formatting

        Returns:
            Chart type identifier (string) or chart specification (dict)
        """
        pass

    @abstractmethod
    def prepare_data(
        self,
        df: Any,
        dimensions: Sequence[str],
        measures: Sequence[str],
        chart_type: str | dict[str, Any],
        time_dimension: str | None = None,
    ) -> tuple[Any, dict[str, Any]]:
        """
        Prepare dataframe and parameters for chart creation.

        Args:
            df: Pandas DataFrame with query results
            dimensions: List of dimension names
            measures: List of measure names
            chart_type: Chart type or specification from detect_chart_type
            time_dimension: Optional time dimension name

        Returns:
            tuple: (processed_dataframe, parameters_dict)
        """
        pass

    @abstractmethod
    def create_chart(
        self,
        df: Any,
        params: dict[str, Any],
        chart_type: str | dict[str, Any],
        spec: dict[str, Any] | None = None,
    ) -> Any:
        """
        Create the chart object using backend-specific library.

        Args:
            df: Processed dataframe from prepare_data
            params: Parameters dict from prepare_data
            chart_type: Chart type or specification
            spec: Optional custom specification to override defaults

        Returns:
            Chart object (backend-specific type)
        """
        pass

    @abstractmethod
    def format_output(
        self,
        chart_obj: Any,
        format: str = "static",
    ) -> Any:
        """
        Format chart output according to requested format.

        Args:
            chart_obj: Chart object created by create_chart
            format: Output format ("static", "interactive", "json", "png", "svg")

        Returns:
            Formatted chart (type depends on format)
        """
        pass

    def render(
        self,
        semantic_aggregate: Any,
        spec: dict[str, Any] | None = None,
        format: str = "static",
    ) -> Any:
        """
        Main entry point for rendering a chart.

        This is the high-level method that orchestrates the entire chart creation
        process by calling the other abstract methods in sequence.

        Args:
            semantic_aggregate: The SemanticAggregate object to visualize
            spec: Optional chart specification dict (backend-specific format)
            format: Output format ("static", "interactive", "json", "png", "svg")

        Returns:
            Chart object or formatted output
        """
        from ..ops import _find_all_root_models, _get_merged_fields

        # Extract dimensions and measures from the operation chain
        aggregate_op = semantic_aggregate.op()
        while hasattr(aggregate_op, "source") and not hasattr(aggregate_op, "aggs"):
            aggregate_op = aggregate_op.source

        dimensions = list(aggregate_op.keys)
        measures = list(aggregate_op.aggs.keys())

        # Try to detect time dimension from source
        time_dimension = None
        time_grain = None
        all_roots = _find_all_root_models(aggregate_op.source)
        if all_roots:
            dims_dict = _get_merged_fields(all_roots, "dimensions")
            for dim_name in dimensions:
                if dim_name in dims_dict:
                    dim_obj = dims_dict[dim_name]
                    if hasattr(dim_obj, "is_time_dimension") and dim_obj.is_time_dimension:
                        time_dimension = dim_name
                        break

        # Execute query to get data
        df = semantic_aggregate.execute()

        # Detect chart type
        chart_type = self.detect_chart_type(dimensions, measures, time_dimension, time_grain)

        # Prepare data
        df, params = self.prepare_data(df, dimensions, measures, chart_type, time_dimension)

        # Create chart
        chart_obj = self.create_chart(df, params, chart_type, spec)

        # Format output
        return self.format_output(chart_obj, format)
