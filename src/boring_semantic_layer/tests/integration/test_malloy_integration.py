"""Integration tests comparing Malloy queries with BSL equivalents."""

import pytest
import asyncio
from toolz import pipe
from typing import Tuple
from .conftest import TEST_CASES
from .integration_utils import (
    make_normalize_dataframe_dtypes,
    make_compare_dataframes,
    make_normalize_for_comparison,
    make_extract_columns,
    make_execute_query,
)


def pytest_generate_tests(metafunc):
    if "query_file" in metafunc.fixturenames:
        metafunc.parametrize(
            "query_file,query_name,flatten_columns",
            TEST_CASES,
            ids=[f"{qf}_{qn}" for qf, qn, _ in TEST_CASES],
        )


@pytest.mark.slow
def test_malloy_bsl_integration(
    query_file: str,
    query_name: str,
    flatten_columns: Tuple[str, ...],
    malloy_query_runner,
    flatten_nested_malloy_result,
    bsl_query_loader,
):
    df_malloy = pipe(
        f"{query_file}.malloy",
        lambda file: asyncio.run(malloy_query_runner(file, query_name)),
        flatten_nested_malloy_result(flatten_columns),
    )

    df_bsl = pipe(
        bsl_query_loader(query_file, query_name),
        make_execute_query,
        make_extract_columns(tuple(df_malloy.columns)),
    )

    df_malloy_normalized, df_bsl_normalized = make_normalize_for_comparison(
        df_malloy, df_bsl
    )
    df_bsl_final = make_normalize_dataframe_dtypes(
        df_malloy_normalized, df_bsl_normalized
    )

    diff_analysis = make_compare_dataframes(
        df1=df_malloy_normalized,
        df2=df_bsl_final,
        df1_name="Malloy",
        df2_name="BSL",
        should_print=not df_malloy_normalized.equals(df_bsl_final),
    )

    assert diff_analysis.is_identical, (
        f"DataFrames do not match for {query_file}.{query_name}"
    )
