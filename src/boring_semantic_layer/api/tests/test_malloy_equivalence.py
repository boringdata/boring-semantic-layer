import pytest
from pathlib import Path
import malloy
from malloy.data.duckdb import DuckDbConnection
import asyncio
import importlib
import sys
import gc
from test_malloy_equivalence_utils import (
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
    ("cohorts", "query_1", ["cohorts"]),
    ("cohorts", "query_2", ["cohorts"]),
    ("cohorts", "query_3", []),
    ("moving_avg", "query_1", []),
    ("moving_avg", "query_2", []),
    ("moving_avg", "query_3", []),
]

BASE_PATH = Path(__file__).parent / "malloy_equivalence"
sys.path.append(str(BASE_PATH))


async def run_malloy_query(query_file: str, query_name: str):
    original_argv = sys.argv.copy()
    try:
        sys.argv = [sys.argv[0]]
        with malloy.Runtime() as runtime:
            runtime.add_connection(DuckDbConnection(home_dir="."))
            data = await runtime.load_file(BASE_PATH / query_file).run(named_query=query_name)
            df = data.to_dataframe()

        gc.collect()
        await asyncio.sleep(0.01)
        return df
    finally:
        sys.argv = original_argv


def _flatten_nested_malloy_result(df_malloy, columns_to_flatten: list[str]):
    import pandas as pd
    import numpy as np
    from functools import reduce

    def normalize_nested_data(data):
        return data.tolist() if isinstance(data, np.ndarray) else data

    def expand_row(row, col):
        base_data = {k: v for k, v in row.items() if k != col}
        nested_data = normalize_nested_data(row[col])

        if isinstance(nested_data, (list, tuple)) and nested_data:
            return [
                {**base_data, **item} if isinstance(item, dict)
                else {**base_data, f"{col}_value": item}
                for item in nested_data
            ]
        return [base_data]

    def flatten_column(df, col):
        if col not in df.columns:
            return df
        rows = [expanded for _, row in df.iterrows() for expanded in expand_row(row, col)]
        return pd.DataFrame(rows) if rows else df.drop(columns=[col])

    return reduce(flatten_column, columns_to_flatten, df_malloy) if columns_to_flatten else df_malloy


@pytest.mark.slow
@pytest.mark.parametrize(
    "query_file,query_name,flatten_columns",
    QUERIES,
    ids=[f"{qf}_{qn}" for qf, qn, _ in QUERIES],
)
def test_malloy_query(query_file: str, query_name: str, flatten_columns: list[str]):
    df_malloy = asyncio.run(run_malloy_query(f"{query_file}.malloy", query_name))
    df_malloy = _flatten_nested_malloy_result(df_malloy, flatten_columns)

    module = importlib.import_module(query_file)
    query_bsl = getattr(module, query_name)
    df_bsl = query_bsl.execute()[df_malloy.columns]

    df_malloy, df_bsl = normalize_for_comparison(df_malloy, df_bsl)
    df_bsl = normalize_dataframe_dtypes(target_df=df_bsl, reference_df=df_malloy)

    diff_analysis = compare_dataframes(
        df1=df_malloy,
        df2=df_bsl,
        df1_name="Malloy",
        df2_name="BSL",
        print_report=not df_malloy.equals(df_bsl),
    )

    assert diff_analysis.analyze()["identical"], f"DataFrames do not match for {query_file}.{query_name}"
