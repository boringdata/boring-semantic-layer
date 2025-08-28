from pathlib import Path
import malloy
from malloy.data.duckdb import DuckDbConnection
import asyncio
import importlib
import sys

# Import utilities
from malloy_benchmark_utils import normalize_dataframe_dtypes, compare_dataframes

QUERIES = {"comparing_timeframe": ["query_1", "query_2", "query_3"]}

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


def test_malloy_queries():
    for query_file, queries in QUERIES.items():
        for query_name in queries:
            df_malloy = asyncio.run(
                run_malloy_query(f"{query_file}.malloy", query_name)
            )
            module = importlib.import_module(query_file)
            query_bsl = getattr(module, query_name)
            df_bsl = query_bsl.execute()

            # Ensure BSL columns match Malloy column order
            df_bsl = df_bsl[df_malloy.columns]

            # Normalize BSL dtypes to match Malloy dtypes using simple mapping
            df_bsl = normalize_dataframe_dtypes(
                target_df=df_bsl, reference_df=df_malloy
            )

            # Use comprehensive DataFrame comparison (no sorting - queries should include order_by)
            diff_analysis = compare_dataframes(
                df1=df_malloy,
                df2=df_bsl,
                df1_name="Malloy",
                df2_name="BSL",
                print_report=not df_malloy.equals(df_bsl),  # Only print if different
            )

            if not diff_analysis.analyze()["identical"]:
                assert False, f"DataFrames do not match for {query_file}.{query_name}"
