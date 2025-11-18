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
        time_grain: str | None = None,
    ) -> tuple[Any, dict[str, Any]]:
        """
        Prepare dataframe and parameters for chart creation.

        Args:
            df: Pandas DataFrame with query results
            dimensions: List of dimension names
            measures: List of measure names
            chart_type: Chart type or specification from detect_chart_type
            time_dimension: Optional time dimension name
            time_grain: Optional requested time grain identifier

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
