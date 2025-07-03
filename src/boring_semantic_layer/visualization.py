"""Chart visualization and processing for boring semantic layer."""

from typing import Dict, Any, List, Optional, Set
import pandas as pd


class ChartProcessor:
    """Process and validate Vega-Lite chart specifications."""

    @staticmethod
    def validate_vega_lite_spec(spec: Dict[str, Any]) -> None:
        """
        Validate the basic structure of a Vega-Lite specification.

        Args:
            spec: The Vega-Lite specification dictionary

        Raises:
            ValueError: If the specification is invalid
        """
        if not isinstance(spec, dict):
            raise ValueError("Chart specification must be a dictionary")

        # Check for required fields
        if (
            "mark" not in spec
            and "layer" not in spec
            and "hconcat" not in spec
            and "vconcat" not in spec
        ):
            raise ValueError(
                "Chart specification must have either 'mark', 'layer', 'hconcat', or 'vconcat'"
            )

        # Validate encoding if present
        if "encoding" in spec:
            if not isinstance(spec["encoding"], dict):
                raise ValueError("'encoding' must be a dictionary")

            # Check for at least one encoding channel
            if not spec["encoding"]:
                raise ValueError("'encoding' must have at least one channel")

    @staticmethod
    def extract_referenced_fields(spec: Dict[str, Any]) -> Set[str]:
        """
        Extract all field names referenced in the Vega-Lite specification.

        Args:
            spec: The Vega-Lite specification dictionary

        Returns:
            Set of field names referenced in the specification
        """
        fields = set()

        def extract_from_encoding(encoding: Dict[str, Any]) -> None:
            """Extract fields from an encoding object."""
            for channel, channel_def in encoding.items():
                if isinstance(channel_def, dict) and "field" in channel_def:
                    fields.add(channel_def["field"])

        def extract_from_spec(s: Dict[str, Any]) -> None:
            """Recursively extract fields from a specification."""
            if "encoding" in s:
                extract_from_encoding(s["encoding"])

            # Handle layered/faceted specs
            if "layer" in s:
                for layer in s["layer"]:
                    extract_from_spec(layer)

            if "hconcat" in s:
                for chart in s["hconcat"]:
                    extract_from_spec(chart)

            if "vconcat" in s:
                for chart in s["vconcat"]:
                    extract_from_spec(chart)

            if "facet" in s and isinstance(s["facet"], dict) and "field" in s["facet"]:
                fields.add(s["facet"]["field"])

            if "repeat" in s and "spec" in s:
                # For repeat specs, we need to check the spec template
                extract_from_spec(s["spec"])

        extract_from_spec(spec)
        return fields

    @staticmethod
    def validate_fields_exist(fields: Set[str], df: pd.DataFrame) -> None:
        """
        Validate that all referenced fields exist in the DataFrame.

        Args:
            fields: Set of field names to validate
            df: The DataFrame to check against

        Raises:
            ValueError: If any field is not found in the DataFrame
        """
        df_columns = set(df.columns)
        missing_fields = fields - df_columns

        if missing_fields:
            available = ", ".join(sorted(df_columns))
            missing = ", ".join(sorted(missing_fields))
            raise ValueError(
                f"Chart references fields not found in data: {missing}. "
                f"Available fields: {available}"
            )

    @staticmethod
    def inject_data(spec: Dict[str, Any], df: pd.DataFrame) -> Dict[str, Any]:
        """
        Inject query results into the Vega-Lite specification.

        Args:
            spec: The Vega-Lite specification dictionary
            df: The DataFrame containing query results

        Returns:
            A new specification with the data injected
        """
        # Create a copy to avoid modifying the original
        spec_copy = spec.copy()

        # Convert DataFrame to Vega-Lite data format
        data_values = df.to_dict(orient="records")

        # Inject the data
        spec_copy["data"] = {"values": data_values}

        return spec_copy

    @classmethod
    def process_chart(cls, spec: Dict[str, Any], df: pd.DataFrame) -> Dict[str, Any]:
        """
        Process a chart specification with query results.

        Args:
            spec: The Vega-Lite specification dictionary
            df: The DataFrame containing query results

        Returns:
            A processed specification with data injected

        Raises:
            ValueError: If the specification is invalid or references unknown fields
        """
        # Validate the specification structure
        cls.validate_vega_lite_spec(spec)

        # Extract referenced fields
        fields = cls.extract_referenced_fields(spec)

        # Validate fields exist in DataFrame
        cls.validate_fields_exist(fields, df)

        # Inject the data
        return cls.inject_data(spec, df)


class ChartTypeDetector:
    """Detect appropriate chart types based on data characteristics."""

    @staticmethod
    def detect_chart_type(
        dimensions: List[str],
        measures: List[str],
        df: Optional[pd.DataFrame] = None,
        time_dimension: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Detect an appropriate chart type based on query characteristics.

        Args:
            dimensions: List of dimension names
            measures: List of measure names
            df: Optional DataFrame to analyze data characteristics
            time_dimension: Optional name of the time dimension

        Returns:
            A basic Vega-Lite specification with appropriate mark type
        """
        num_dims = len(dimensions)
        num_measures = len(measures)

        # Default to simple specifications
        if num_dims == 0 and num_measures == 1:
            # Single value
            return {
                "mark": "text",
                "encoding": {"text": {"field": measures[0], "type": "quantitative"}},
            }

        # Check if we have a time dimension
        has_time = time_dimension and time_dimension in dimensions

        if num_dims == 1 and num_measures == 1:
            if has_time:
                # Time series - use line chart
                return {
                    "mark": "line",
                    "encoding": {
                        "x": {"field": dimensions[0], "type": "temporal"},
                        "y": {"field": measures[0], "type": "quantitative"},
                    },
                }
            else:
                # Categorical dimension - use bar chart
                return {
                    "mark": "bar",
                    "encoding": {
                        "x": {"field": dimensions[0], "type": "nominal"},
                        "y": {"field": measures[0], "type": "quantitative"},
                    },
                }

        if num_dims == 1 and num_measures == 2:
            # Simple side-by-side comparison - just show first measure
            # (User can provide custom spec for more complex visualizations)
            return {
                "mark": "bar",
                "encoding": {
                    "x": {"field": dimensions[0], "type": "nominal"},
                    "y": {"field": measures[0], "type": "quantitative"},
                    "color": {"value": "steelblue"},
                },
            }

        if num_dims == 2 and num_measures == 1:
            # Heatmap or grouped bar
            return {
                "mark": "rect",
                "encoding": {
                    "x": {"field": dimensions[0], "type": "nominal"},
                    "y": {"field": dimensions[1], "type": "nominal"},
                    "color": {"field": measures[0], "type": "quantitative"},
                },
            }

        # Default to table-like visualization
        return {
            "mark": "text",
            "encoding": {
                "text": {"value": "Complex query - consider custom visualization"}
            },
        }
