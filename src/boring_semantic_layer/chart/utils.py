"""
Shared utilities for chart backends.

Common functionality used across Plotext, Altair, and Plotly backends.
"""

from typing import Any


def clean_field_name(name: str, remove_prefix: bool = True) -> str:
    """
    Clean field name for display.

    Args:
        name: Field name that may contain model prefix (e.g., "model.field")
        remove_prefix: Whether to remove the model prefix for display

    Returns:
        Cleaned field name
    """
    if remove_prefix and "." in name:
        return name.split(".")[-1]
    return name


def sanitize_field_name_for_vega(field: str) -> str:
    """
    Sanitize field names for Vega-Lite compatibility.

    Vega-Lite interprets dots as nested field accessors, which causes issues
    with transforms like fold. Replace dots with underscores to avoid this.

    Args:
        field: Field name that may contain dots

    Returns:
        Sanitized field name safe for Vega-Lite (dots replaced with underscores)
    """
    return field.replace(".", "_")


def detect_time_dimension_from_dtype(df: Any, dimensions: list[str]) -> str | None:
    """
    Detect time dimension from dataframe column types.

    Args:
        df: Pandas DataFrame
        dimensions: List of dimension names to check

    Returns:
        Name of time dimension if found, None otherwise
    """
    import pandas as pd

    for dim_name in dimensions:
        if dim_name in df.columns:
            dtype = df[dim_name].dtype
            if pd.api.types.is_datetime64_any_dtype(dtype):
                return dim_name
    return None


def detect_time_dimension_from_graph(
    semantic_aggregate: Any,
    dimensions: list[str],
    dims_dict: dict[str, Any],
) -> str | None:
    """
    Detect time dimension from dependency graph.

    Checks if any dimension is derived from a time dimension by traversing
    the dependency graph.

    Args:
        semantic_aggregate: SemanticAggregate object with graph attribute
        dimensions: List of dimension names to check
        dims_dict: Dictionary of dimension objects from merged fields

    Returns:
        Name of time dimension if found, None otherwise
    """
    try:
        graph = semantic_aggregate.graph

        for dim_name in dimensions:
            if dim_name not in graph:
                continue

            # Check predecessors (dependencies)
            for pred_name in graph.predecessors(dim_name):
                if pred_name not in dims_dict:
                    continue

                pred_obj = dims_dict[pred_name]
                if hasattr(pred_obj, "is_time_dimension") and pred_obj.is_time_dimension:
                    return dim_name

    except (AttributeError, KeyError):
        pass

    return None


def extract_aggregate_metadata(
    semantic_aggregate: Any,
) -> tuple[list[str], list[str], list[str], Any]:
    """
    Extract dimensions, measures, mutated columns, and aggregate op.

    Traverses the operation chain to find aggregate and mutate operations.

    Args:
        semantic_aggregate: SemanticAggregate object

    Returns:
        Tuple of (dimensions, measures, mutated_columns, aggregate_op)
    """
    aggregate_op = semantic_aggregate.op()

    # Handle mutate operations - they wrap the aggregate
    mutated_columns = []
    current_op = aggregate_op

    # Unwrap to find mutate operation
    while (
        hasattr(current_op, "source")
        and not hasattr(current_op, "aggs")
        and not (
            hasattr(current_op, "__class__") and current_op.__class__.__name__ == "SemanticMutateOp"
        )
    ):
        current_op = current_op.source

    # Extract mutated columns
    if hasattr(current_op, "__class__") and current_op.__class__.__name__ == "SemanticMutateOp":
        if hasattr(current_op, "post"):
            mutated_columns = list(current_op.post.keys())
        current_op = current_op.source

    # Find aggregate operation
    while hasattr(current_op, "source") and not hasattr(current_op, "aggs"):
        current_op = current_op.source

    aggregate_op = current_op
    dimensions = list(aggregate_op.keys)
    measures = list(aggregate_op.aggs.keys()) + mutated_columns

    return dimensions, measures, mutated_columns, aggregate_op


def detect_time_dimension(
    semantic_aggregate: Any,
    dimensions: list[str],
    df: Any | None = None,
) -> str | None:
    """
    Detect time dimension using multiple strategies.

    Tries in order:
    1. Check dimension metadata (is_time_dimension attribute)
    2. Check dataframe column dtypes (if df provided)
    3. Check dependency graph for derived time dimensions

    Args:
        semantic_aggregate: SemanticAggregate object
        dimensions: List of dimension names
        df: Optional DataFrame to check column types

    Returns:
        Name of time dimension if found, None otherwise
    """
    from ..ops import _find_all_root_models, _get_merged_fields

    # Get root models and dimension dictionary
    aggregate_op = semantic_aggregate.op()

    # Unwrap to find aggregate
    while hasattr(aggregate_op, "source") and not hasattr(aggregate_op, "aggs"):
        aggregate_op = aggregate_op.source

    all_roots = _find_all_root_models(aggregate_op.source)
    if not all_roots:
        return None

    dims_dict = _get_merged_fields(all_roots, "dimensions")

    # Strategy 1: Check metadata
    for dim_name in dimensions:
        if dim_name in dims_dict:
            dim_obj = dims_dict[dim_name]
            if hasattr(dim_obj, "is_time_dimension") and dim_obj.is_time_dimension:
                return dim_name

    # Strategy 2: Check dataframe dtypes
    if df is not None:
        time_dim = detect_time_dimension_from_dtype(df, dimensions)
        if time_dim:
            return time_dim

    # Strategy 3: Check dependency graph
    return detect_time_dimension_from_graph(semantic_aggregate, dimensions, dims_dict)


def get_chart_detection_params(
    semantic_aggregate: Any,
    df: Any | None = None,
) -> tuple[list[str], list[str], str | None, str | None]:
    """
    Extract all parameters needed for chart type detection.

    Convenience function that combines metadata extraction and time detection.

    Args:
        semantic_aggregate: SemanticAggregate object
        df: Optional DataFrame for time dimension detection

    Returns:
        Tuple of (dimensions, measures, time_dimension, time_grain)
    """
    dimensions, measures, _, aggregate_op = extract_aggregate_metadata(semantic_aggregate)
    time_dimension = detect_time_dimension(semantic_aggregate, dimensions, df)
    time_grain = getattr(semantic_aggregate, "_chart_time_grain", None)
    if time_grain is None and aggregate_op is not None:
        time_grain = getattr(aggregate_op, "_chart_time_grain", None)

    return dimensions, measures, time_dimension, time_grain


def has_time_dimension(dimensions: list[str], time_dimension: str | None) -> bool:
    """
    Check if query has a time dimension.

    Args:
        dimensions: List of dimension names
        time_dimension: Optional time dimension name

    Returns:
        True if time_dimension exists and is in dimensions
    """
    return time_dimension is not None and time_dimension in dimensions


def override_chart_type_from_spec(chart_type: str, spec: dict[str, Any] | None) -> str:
    """
    Override chart type from spec if provided.

    Args:
        chart_type: Auto-detected chart type
        spec: Optional specification dict that may contain "chart_type"

    Returns:
        Chart type (overridden if spec contains "chart_type", otherwise original)
    """
    if spec and "chart_type" in spec:
        return spec["chart_type"]
    return chart_type


def validate_format(format: str, supported_formats: list[str]) -> None:
    """
    Validate output format and raise error if unsupported.

    Args:
        format: Requested output format
        supported_formats: List of supported formats for this backend

    Raises:
        ValueError: If format is not in supported_formats
    """
    if format not in supported_formats:
        raise ValueError(
            f"Unsupported format: {format}. Supported formats: {', '.join(repr(f) for f in supported_formats)}"
        )


def detect_chart_type_generic(
    dimensions: list[str],
    measures: list[str],
    time_dimension: str | None,
) -> str:
    """
    Generic chart type detection logic shared across all backends.

    This provides a consistent baseline chart type detection that backends
    can use directly or override with their own logic.

    Args:
        dimensions: List of dimension field names
        measures: List of measure field names
        time_dimension: Optional time dimension field name

    Returns:
        Chart type identifier: "indicator", "bar", "line", "heatmap", "scatter", or "table"

    Chart type selection logic:
        - No dimensions + 1 measure → indicator (single value)
        - 1 dimension + any measures:
          - If time dimension → line chart
          - Otherwise → bar chart
        - Multiple dimensions + time dimension → line chart (time series with categories)
        - 2 dimensions + 1 measure (no time) → heatmap
        - 2 dimensions (one is time) → line chart
        - Otherwise → table (complex multi-dimensional)
    """
    num_dims = len(dimensions)
    num_measures = len(measures)

    # Single value - indicator
    if num_dims == 0 and num_measures == 1:
        return "indicator"

    # Check if we have a time dimension
    has_time = has_time_dimension(dimensions, time_dimension)

    # Single dimension, any number of measures
    if num_dims == 1:
        return "line" if has_time else "bar"

    # Time series with additional dimension(s) - multi-line chart
    if has_time and num_dims >= 2 and num_measures == 1:
        return "line"

    # Two dimensions, one measure (no time) - heatmap
    if num_dims == 2 and num_measures == 1 and not has_time:
        return "heatmap"

    # Two dimensions with time - line chart
    if num_dims == 2 and has_time:
        return "line"

    # Default for complex queries - table
    return "table"


def sort_dataframe_for_line_chart(
    df: Any,
    dimensions: list[str],
    time_dimension: str | None,
) -> Any:
    """
    Sort dataframe for line charts to avoid zigzag connections.

    Sorts by time dimension first (if present), then by other dimensions.

    Args:
        df: Pandas DataFrame
        dimensions: List of dimension names
        time_dimension: Optional time dimension name

    Returns:
        Sorted DataFrame
    """
    if not dimensions:
        return df

    if time_dimension and time_dimension in dimensions:
        # Sort by time first, then other dimensions
        sort_cols = [time_dimension]
        non_time_dims = [d for d in dimensions if d != time_dimension]
        if non_time_dims:
            sort_cols.extend(non_time_dims)
        return df.sort_values(by=sort_cols)
    else:
        # Sort by first dimension
        return df.sort_values(by=dimensions[0])


def convert_datetime_to_strings(df: Any) -> Any:
    """
    Convert datetime columns to ISO format strings.

    Workaround for Plotly/Kaleido datetime rendering bugs in static exports.
    Strips time component if all times are midnight for cleaner labels.

    Args:
        df: Pandas DataFrame

    Returns:
        DataFrame with datetime columns converted to strings
    """
    import pandas as pd

    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].dt.strftime("%Y-%m-%d %H:%M:%S")
            # Strip time if all midnight
            if df[col].str.endswith(" 00:00:00").all():
                df[col] = df[col].str.replace(" 00:00:00", "")

    return df


def get_non_time_dimensions(dimensions: list[str], time_dimension: str | None) -> list[str]:
    """
    Get list of non-time dimensions.

    Args:
        dimensions: List of all dimension names
        time_dimension: Optional time dimension name

    Returns:
        List of dimension names excluding the time dimension
    """
    if time_dimension is None:
        return list(dimensions)
    return [d for d in dimensions if d != time_dimension]


def sanitize_spec_for_vega(spec: dict[str, Any]) -> dict[str, Any]:
    """
    Recursively sanitize field names in a Vega-Lite chart specification.

    Replaces dots with underscores in all field references to avoid
    Vega-Lite's nested field accessor interpretation.

    Args:
        spec: Chart specification dictionary

    Returns:
        Sanitized specification with field names converted
    """
    if isinstance(spec, dict):
        result = {}
        for key, value in spec.items():
            # Sanitize field names in encoding and other field references
            if key == "field" and isinstance(value, str):
                result[key] = sanitize_field_name_for_vega(value)
            # Sanitize field names in fold transforms
            elif key == "fold" and isinstance(value, list):
                result[key] = [sanitize_field_name_for_vega(f) for f in value]
            else:
                result[key] = sanitize_spec_for_vega(value)
        return result
    elif isinstance(spec, list):
        return [sanitize_spec_for_vega(item) for item in spec]
    else:
        return spec


def melt_dataframe_for_multiple_measures(
    df: Any,
    measures: list[str],
) -> tuple[Any, str, str]:
    """
    Reshape dataframe from wide to long format for multiple measures.

    Used when multiple measures need to be displayed as separate series
    (e.g., grouped bar chart, multi-line chart).

    Args:
        df: Pandas DataFrame in wide format
        measures: List of measure column names to melt

    Returns:
        Tuple of (melted_dataframe, measure_column_name, value_column_name)
    """
    import pandas as pd

    id_cols = [col for col in df.columns if col not in measures]
    df_melted = pd.melt(
        df,
        id_vars=id_cols,
        value_vars=measures,
        var_name="measure",
        value_name="value",
    )

    return df_melted, "measure", "value"


def pivot_dataframe_for_heatmap(
    df: Any,
    dimensions: list[str],
    measure: str,
) -> Any:
    """
    Pivot dataframe to create heatmap matrix.

    Args:
        df: Pandas DataFrame
        dimensions: List of exactly 2 dimension names [x_dim, y_dim]
        measure: Name of measure column to use as values

    Returns:
        Pivoted DataFrame suitable for heatmap rendering
    """
    if len(dimensions) < 2:
        raise ValueError("Heatmap requires at least 2 dimensions")

    return df.pivot(
        index=dimensions[1],
        columns=dimensions[0],
        values=measure,
    )
