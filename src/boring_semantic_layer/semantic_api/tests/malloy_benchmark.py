from pathlib import Path
import malloy
from malloy.data.duckdb import DuckDbConnection
import asyncio
import importlib
import sys
import pytest

# Import utilities
from malloy_benchmark_utils import normalize_dataframe_dtypes, compare_dataframes

QUERIES = [
    ("comparing_timeframe", "query_1"),
    ("comparing_timeframe", "query_2"),
    ("comparing_timeframe", "query_3"),
    ("percent_of_total", "query_1"),
    ("percent_of_total", "query_2"),
    ("percent_of_total", "query_3"),
    ("percent_of_total", "query_4"),
    ("cohorts", "query_1"),
    ("cohorts", "query_2"),
    ("cohorts", "query_3"),
]

BASE_PATH = Path(__file__).parent / "malloy_benchmark"
sys.path.append(str(BASE_PATH))


async def run_malloy_query(query_file: str, query_name: str):
    with malloy.Runtime() as runtime:
        runtime.add_connection(DuckDbConnection(home_dir="."))

        data = await runtime.load_file(BASE_PATH / query_file).run(
            named_query=query_name
        )

        df = data.to_dataframe()

    return df


@pytest.mark.parametrize(
    "query_file,query_name", QUERIES, ids=[f"{qf}_{qn}" for qf, qn in QUERIES]
)
def test_malloy_query(query_file: str, query_name: str):
    """Test individual Malloy query against BSL implementation"""
    # Run Malloy query
    df_malloy = asyncio.run(run_malloy_query(f"{query_file}.malloy", query_name))

    # Run BSL query
    module = importlib.import_module(query_file)
    query_bsl = getattr(module, query_name)
    df_bsl = query_bsl.execute()

    # Ensure BSL columns match Malloy column order
    df_bsl = df_bsl[df_malloy.columns]

    # Normalize BSL dtypes to match Malloy dtypes
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
