"""
ECharts chart backend interface.

Defines the abstract interface for ECharts-specific chart generation.
This extends the base ChartBackend with ECharts-specific methods.
"""

from abc import ABC, abstractmethod
from typing import Any

from .types import EChartsChartType, EChartsOption


class EChartsBackendInterface(ABC):
    """
    Abstract interface for ECharts chart backends.

    This interface defines the contract for generating ECharts option
    specifications from data. Implementations should handle data transformation
    and spec generation for various chart types.

    The interface is designed to be compatible with the existing ChartBackend
    pattern while providing ECharts-specific functionality.
    """

    @abstractmethod
    def generate_spec(
        self,
        data: Any,
        chart_type: str | EChartsChartType,
        **kwargs: Any,
    ) -> EChartsOption:
        """
        Generate ECharts option specification from data.

        Args:
            data: Input data (typically pandas DataFrame or list of dicts)
            chart_type: Type of chart to generate (e.g., "bar", "line", "pie")
            **kwargs: Additional chart configuration options:
                - title: Chart title
                - x_field: Field name for x-axis
                - y_field: Field name(s) for y-axis
                - color_field: Field for color encoding
                - theme: Color theme to apply

        Returns:
            EChartsOption dict ready to be passed to ECharts renderer

        Example:
            >>> backend = EChartsBackend()
            >>> spec = backend.generate_spec(
            ...     df,
            ...     chart_type="bar",
            ...     x_field="category",
            ...     y_field="value",
            ...     title="Sales by Category"
            ... )
        """
        pass

    @abstractmethod
    def supported_chart_types(self) -> list[str]:
        """
        Return list of supported chart types.

        Returns:
            List of chart type identifiers (e.g., ["bar", "line", "pie"])
        """
        pass

    def validate_chart_type(self, chart_type: str | EChartsChartType) -> str:
        """
        Validate and normalize chart type.

        Args:
            chart_type: Chart type to validate (string or enum)

        Returns:
            Normalized chart type string

        Raises:
            ValueError: If chart type is not supported
        """
        if isinstance(chart_type, EChartsChartType):
            chart_type = chart_type.value

        supported = self.supported_chart_types()
        if chart_type not in supported:
            raise ValueError(
                f"Unsupported chart type: {chart_type}. " f"Supported types: {', '.join(supported)}"
            )
        return chart_type

    def merge_options(
        self,
        base: EChartsOption,
        overrides: dict[str, Any] | None,
    ) -> EChartsOption:
        """
        Merge base options with user overrides.

        Performs a shallow merge where override values replace base values.
        For nested dicts, a shallow merge is performed at the first level.

        Args:
            base: Base ECharts option specification
            overrides: User-provided overrides (optional)

        Returns:
            Merged EChartsOption
        """
        if not overrides:
            return base

        result: EChartsOption = dict(base)  # type: ignore
        for key, value in overrides.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                # Shallow merge for nested dicts
                result[key] = {**result[key], **value}  # type: ignore
            else:
                result[key] = value  # type: ignore
        return result
