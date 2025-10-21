import pytest
from pathlib import Path
import malloy
from malloy.data.duckdb import DuckDbConnection
import asyncio
import importlib
import sys
import pytest

# Import utilities
from test_malloy_benchmark_utils import (
    normalize_dataframe_dtypes,
    compare_dataframes,
    normalize_for_comparison,
)

QUERIES = [
    ("comparing_timeframe", "query_1", []),
    ("comparing_timeframe", "query_2", []),
    ("comparing_timeframe", "query_3", []),
    ("percent_of_total", "query_1", []),
    ("percent_of_total", "query_2", []),
    ("percent_of_total", "query_3", []),
    ("percent_of_total", "query_4", []),
    (
        "cohorts",
        "query_1",
        ["cohorts"],
    ),  # Flatten cohorts column to match BSL flat structure
    (
        "cohorts",
        "query_2",
        ["cohorts"],
    ),  # Flatten cohorts column to match BSL flat structure
    ("cohorts", "query_3", []),  # Already flat
    ("moving_avg", "query_1", []),
    ("moving_avg", "query_2", []),
    ("moving_avg", "query_3", []),
]

BASE_PATH = Path(__file__).parent / "malloy_benchmark"
sys.path.append(str(BASE_PATH))


async def run_malloy_query(query_file: str, query_name: str):
    # Save original sys.argv to avoid conflicts with malloy's absl flags
    import sys
    original_argv = sys.argv.copy()
    try:
        # Set sys.argv to just the script name to avoid pytest flag conflicts
        sys.argv = [sys.argv[0]]

        with malloy.Runtime() as runtime:
            runtime.add_connection(DuckDbConnection(home_dir="."))

            data = await runtime.load_file(BASE_PATH / query_file).run(
                named_query=query_name
            )

            df = data.to_dataframe()

        return df
    finally:
        # Restore original sys.argv
        sys.argv = original_argv


def _flatten_nested_malloy_result(df_malloy, columns_to_flatten: list[str]):
    """Flatten nested Malloy results by expanding nested columns into flat structure."""
    if not columns_to_flatten:
        return df_malloy

    import pandas as pd
    import numpy as np

    df_result = df_malloy.copy()

    for col in columns_to_flatten:
        if col in df_result.columns:
            # Extract nested data and create flat rows
            flattened_rows = []

            for _, row in df_result.iterrows():
                # Get the main row data (excluding the nested column)
                base_data = {k: v for k, v in row.items() if k != col}

                # Extract nested data (could be numpy array, list, or other structure)
                nested_data = row[col]

                # Handle numpy arrays (common in Malloy nested results)
                if isinstance(nested_data, np.ndarray):
                    nested_data = nested_data.tolist()

                if isinstance(nested_data, (list, tuple)) and nested_data:
                    # If nested data is a list, create one row per nested item
                    for nested_item in nested_data:
                        if isinstance(nested_item, dict):
                            # Merge base data with nested item data
                            flattened_row = {**base_data, **nested_item}
                            flattened_rows.append(flattened_row)
                        else:
                            # If nested item is not a dict, just add it as a value
                            flattened_row = {**base_data, f"{col}_value": nested_item}
                            flattened_rows.append(flattened_row)
                else:
                    # If no nested data or empty, keep the base row without the nested column
                    flattened_rows.append(base_data)

            # Create new DataFrame from flattened rows
            if flattened_rows:
                df_result = pd.DataFrame(flattened_rows)
            else:
                # If no data, just drop the nested column
                df_result = df_result.drop(columns=[col])

    return df_result


@pytest.mark.parametrize(
    "query_file,query_name,flatten_columns",
    QUERIES,
    ids=[f"{qf}_{qn}" for qf, qn, _ in QUERIES],
)
def test_malloy_query(query_file: str, query_name: str, flatten_columns: list[str]):
    """Test individual Malloy query against BSL implementation"""
    # Run Malloy query
    df_malloy = asyncio.run(run_malloy_query(f"{query_file}.malloy", query_name))

    # Flatten nested Malloy results if needed
    df_malloy = _flatten_nested_malloy_result(df_malloy, flatten_columns)

    # Run BSL query
    module = importlib.import_module(query_file)
    query_bsl = getattr(module, query_name)
    df_bsl = query_bsl.execute()

    # Ensure BSL columns match Malloy column order
    df_bsl = df_bsl[df_malloy.columns]

    # Normalize both DataFrames for comparison (handles datetime.date vs string issues)
    df_malloy, df_bsl = normalize_for_comparison(df_malloy, df_bsl)

    # Final dtype normalization to match Malloy dtypes
    df_bsl = normalize_dataframe_dtypes(target_df=df_bsl, reference_df=df_malloy)

    # Compare DataFrames
    diff_analysis = compare_dataframes(
        df1=df_malloy,
        df2=df_bsl,
        df1_name="Malloy",
        df2_name="BSL",
        print_report=not df_malloy.equals(df_bsl),
    )

    # Assert that DataFrames are identical
    assert diff_analysis.analyze()["identical"], (
        f"DataFrames do not match for {query_file}.{query_name}"
    )
