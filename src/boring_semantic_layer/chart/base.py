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

        # Handle mutate operations - they wrap the aggregate
        mutated_columns = []
        if (
            hasattr(aggregate_op, "__class__")
            and aggregate_op.__class__.__name__ == "SemanticMutateOp"
        ):
            # Extract mutated column names from the post transformations
            if hasattr(aggregate_op, "post"):
                mutated_columns = list(aggregate_op.post.keys())
            # Navigate to the underlying aggregate op
            aggregate_op = aggregate_op.source

        while hasattr(aggregate_op, "source") and not hasattr(aggregate_op, "aggs"):
            aggregate_op = aggregate_op.source

        dimensions = list(aggregate_op.keys)
        # Combine original aggregated measures with mutated columns
        measures = list(aggregate_op.aggs.keys()) + mutated_columns

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

        # If no time dimension found from metadata, check dataframe column types
        if not time_dimension and len(dimensions) > 0:
            import pandas as pd

            for dim_name in dimensions:
                if dim_name in df.columns:
                    dtype = df[dim_name].dtype
                    # Check if column is datetime or date type
                    if pd.api.types.is_datetime64_any_dtype(dtype):
                        time_dimension = dim_name
                        break

            # Also check if dimension is derived from a time dimension using dependency graph
            if not time_dimension and all_roots:
                try:
                    # Get the dependency graph from the semantic aggregate
                    graph = semantic_aggregate.graph

                    # Check each dimension to see if it depends on a time dimension
                    for dim_name in dimensions:
                        if dim_name in graph:
                            # Get all predecessors (dependencies) of this dimension
                            predecessors = graph.predecessors(dim_name)

                            # Check if any predecessor is a time dimension
                            for pred_name in predecessors:
                                # Check if this predecessor is a time dimension in the model
                                if pred_name in dims_dict:
                                    pred_obj = dims_dict[pred_name]
                                    if (
                                        hasattr(pred_obj, "is_time_dimension")
                                        and pred_obj.is_time_dimension
                                    ):
                                        time_dimension = dim_name
                                        break

                            if time_dimension:
                                break
                except (AttributeError, KeyError):
                    # Graph not available or dimension not in graph
                    pass

        # Detect chart type
        chart_type = self.detect_chart_type(dimensions, measures, time_dimension, time_grain)

        # Prepare data
        df, params = self.prepare_data(df, dimensions, measures, chart_type, time_dimension)

        # Create chart
        chart_obj = self.create_chart(df, params, chart_type, spec)

        # Format output
        return self.format_output(chart_obj, format)
