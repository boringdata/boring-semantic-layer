"""Pytest configuration and fixtures for integration tests."""

import pytest
from pathlib import Path
import sys
import importlib
import asyncio
import gc
import pandas as pd
import numpy as np
from functools import reduce
from toolz import curry
from typing import Tuple, Any, Dict


TEST_CASES: Tuple[Tuple[str, str, Tuple[str, ...]], ...] = (
    ("comparing_timeframe", "query_1", ()),
    ("comparing_timeframe", "query_2", ()),
    ("comparing_timeframe", "query_3", ()),
    ("percent_of_total", "query_1", ()),
    ("percent_of_total", "query_2", ()),
    ("percent_of_total", "query_3", ()),
    ("percent_of_total", "query_4", ()),
    ("cohorts", "query_1", ("cohorts",)),
    ("cohorts", "query_2", ("cohorts",)),
    ("cohorts", "query_3", ()),
    ("moving_avg", "query_1", ()),
    ("moving_avg", "query_2", ()),
    ("moving_avg", "query_3", ()),
)


@pytest.fixture(scope="session")
def malloy_integration_test_cases():
    return TEST_CASES


@pytest.fixture(scope="session")
def malloy_data_path():
    return Path(__file__).parent / "malloy_data"


@curry
def make_normalize_nested_data(data: Any) -> Any:
    return data.tolist() if isinstance(data, np.ndarray) else data


@curry
def make_expand_row(col: str, row: Dict[str, Any]) -> Tuple[Dict[str, Any], ...]:
    base_data = {k: v for k, v in row.items() if k != col}
    nested_data = make_normalize_nested_data(row[col])

    if isinstance(nested_data, (list, tuple)) and nested_data:
        return tuple(
            {**base_data, **item}
            if isinstance(item, dict)
            else {**base_data, f"{col}_value": item}
            for item in nested_data
        )
    return (base_data,)


@curry
def make_flatten_column(col: str, df: pd.DataFrame) -> pd.DataFrame:
    if col not in df.columns:
        return df

    rows = tuple(
        expanded for _, row in df.iterrows() for expanded in make_expand_row(col, row)
    )
    return pd.DataFrame(rows) if rows else df.drop(columns=[col])


@curry
def make_flatten_dataframe(
    columns_to_flatten: Tuple[str, ...], df: pd.DataFrame
) -> pd.DataFrame:
    if not columns_to_flatten:
        return df
    return reduce(
        lambda acc, col: make_flatten_column(col, acc), columns_to_flatten, df
    )


@pytest.fixture
def flatten_nested_malloy_result():
    return make_flatten_dataframe


@pytest.fixture
def malloy_query_runner(malloy_data_path):
    import malloy
    from malloy.data.duckdb import DuckDbConnection

    async def _run_query(query_file: str, query_name: str):
        original_argv = sys.argv.copy()
        try:
            sys.argv = [sys.argv[0]]
            with malloy.Runtime() as runtime:
                runtime.add_connection(DuckDbConnection(home_dir="."))
                data = await runtime.load_file(malloy_data_path / query_file).run(
                    named_query=query_name
                )
                df = data.to_dataframe()

            gc.collect()
            await asyncio.sleep(0.01)
            return df
        finally:
            sys.argv = original_argv

    return _run_query


@curry
def make_ensure_path_in_sys(path: Path) -> None:
    path_str = str(path)
    if path_str not in sys.path:
        sys.path.append(path_str)


@curry
def make_load_query(module_name: str, query_name: str) -> Any:
    module = importlib.import_module(module_name)
    return getattr(module, query_name)


@pytest.fixture
def bsl_query_loader(malloy_data_path):
    make_ensure_path_in_sys(malloy_data_path)
    return make_load_query
