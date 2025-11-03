"""
Utility functions for DataFrame comparison and dtype normalization in Malloy benchmarks.
"""

from functools import reduce
from typing import Any

import pandas as pd
from attrs import frozen
from toolz import curry


@frozen
class DtypeConversion:
    column: str
    source_dtype: str
    target_dtype: str
    success: bool
    error: str | None = None

    def format_log(self) -> str:
        if self.success:
            return f"  {self.column}: {self.source_dtype} → {self.target_dtype}"
        return f"  {self.column}: {self.source_dtype} → {self.target_dtype} (FAILED: {self.error})"


@frozen
class NormalizationLog:
    column: str
    message: str

    def format_log(self) -> str:
        return f"  {self.column}: {self.message}"


@curry
def make_convert_column(
    reference_dtype: Any,
    col: str,
    df: pd.DataFrame,
) -> tuple[pd.DataFrame, DtypeConversion | None]:
    if col not in df.columns:
        return df, None

    source_dtype = df[col].dtype
    if source_dtype == reference_dtype:
        return df, None

    try:
        df_modified = df.copy()
        df_modified[col] = df_modified[col].astype(reference_dtype)
        return df_modified, DtypeConversion(
            column=col,
            source_dtype=str(source_dtype),
            target_dtype=str(reference_dtype),
            success=True,
        )
    except Exception as e:
        return df, DtypeConversion(
            column=col,
            source_dtype=str(source_dtype),
            target_dtype=str(reference_dtype),
            success=False,
            error=str(e),
        )


@curry
def make_normalize_column(
    dtype_map: dict[str, Any],
    col: str,
    state: tuple[pd.DataFrame, tuple[DtypeConversion, ...]],
) -> tuple[pd.DataFrame, tuple[DtypeConversion, ...]]:
    df, conversions = state
    if col not in dtype_map:
        return state

    df_modified, conversion = make_convert_column(dtype_map[col], col, df)
    new_conversions = conversions + (conversion,) if conversion else conversions
    return df_modified, new_conversions


def do_print_conversions(conversions: tuple[DtypeConversion, ...]) -> None:
    if conversions:
        print("Dtype conversions applied:")
        print("\n".join(conv.format_log() for conv in conversions))


@curry
def make_normalize_dataframe_dtypes(
    reference_df: pd.DataFrame,
    target_df: pd.DataFrame,
) -> pd.DataFrame:
    dtype_map = {col: reference_df[col].dtype for col in reference_df.columns}

    initial_state = (target_df.copy(), ())
    final_df, conversions = reduce(
        lambda state, col: make_normalize_column(dtype_map, col, state),
        dtype_map.keys(),
        initial_state,
    )

    do_print_conversions(conversions)
    return final_df


@curry
def make_check_date_like(sample: Any) -> bool:
    return (
        hasattr(sample, "year")
        and hasattr(sample, "month")
        and hasattr(sample, "day")
        and not hasattr(sample, "hour")
    )


@curry
def make_normalize_date_column(
    col: str,
    df1: pd.DataFrame,
    df2: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, NormalizationLog | None]:
    if df1[col].dtype != "object" or df2[col].dtype != "object":
        return df1, df2, None

    if len(df1) == 0 or len(df2) == 0:
        return df1, df2, None

    df1_sample = df1[col].iloc[0]
    df2_sample = df2[col].iloc[0]

    df1_is_date = make_check_date_like(df1_sample)
    df2_is_date = make_check_date_like(df2_sample)
    df1_is_string = isinstance(df1_sample, str)
    df2_is_string = isinstance(df2_sample, str)

    if (df1_is_date and df2_is_string) or (df2_is_date and df1_is_string):
        df1_modified = df1.copy()
        df2_modified = df2.copy()
        df1_modified[col] = df1_modified[col].astype(str)
        df2_modified[col] = df2_modified[col].astype(str)

        log = NormalizationLog(
            column=col,
            message="converted both to string for date comparison",
        )
        return df1_modified, df2_modified, log

    return df1, df2, None


def make_normalize_for_comparison(
    df1: pd.DataFrame,
    df2: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    common_cols = tuple(set(df1.columns) & set(df2.columns))

    def process_column(
        state: tuple[pd.DataFrame, pd.DataFrame, tuple[NormalizationLog, ...]],
        col: str,
    ) -> tuple[pd.DataFrame, pd.DataFrame, tuple[NormalizationLog, ...]]:
        df1_curr, df2_curr, logs = state
        df1_new, df2_new, log = make_normalize_date_column(col, df1_curr, df2_curr)
        new_logs = logs + (log,) if log else logs
        return df1_new, df2_new, new_logs

    initial_state = (df1.copy(), df2.copy(), ())
    df1_final, df2_final, logs = reduce(process_column, common_cols, initial_state)

    if logs:
        print("DataFrame comparison normalizations applied:")
        print("\n".join(log.format_log() for log in logs))

    return df1_final, df2_final


@frozen
class ShapeAnalysis:
    df1_shape: tuple[int, int]
    df2_shape: tuple[int, int]

    @property
    def is_equal(self) -> bool:
        return self.df1_shape == self.df2_shape


@frozen
class ColumnAnalysis:
    df1_only: tuple[str, ...]
    df2_only: tuple[str, ...]
    common: tuple[str, ...]
    df1_order: tuple[str, ...]
    df2_order: tuple[str, ...]

    @property
    def is_equal(self) -> bool:
        return len(self.df1_only) == 0 and len(self.df2_only) == 0

    @property
    def is_order_equal(self) -> bool:
        return self.df1_order == self.df2_order


@frozen
class DtypeDifference:
    column: str
    df1_dtype: str
    df2_dtype: str


@frozen
class DtypeAnalysis:
    differences: tuple[DtypeDifference, ...]

    @property
    def all_equal(self) -> bool:
        return len(self.differences) == 0


@frozen
class MissingRowsAnalysis:
    df1_only: pd.DataFrame
    df2_only: pd.DataFrame
    error: str | None = None

    @property
    def df1_only_count(self) -> int:
        return len(self.df1_only) if self.df1_only is not None else 0

    @property
    def df2_only_count(self) -> int:
        return len(self.df2_only) if self.df2_only is not None else 0


@frozen
class ValueDifference:
    row_index: int
    column_diffs: dict[str, tuple[Any, Any]]


@frozen
class ValueAnalysis:
    differences: tuple[ValueDifference, ...]
    error: str | None = None

    @property
    def has_differences(self) -> bool:
        return len(self.differences) > 0

    @property
    def total_differing_rows(self) -> int:
        return len(self.differences)


@frozen
class DiffSummary:
    issues: tuple[str, ...]

    @property
    def total_issues(self) -> int:
        return len(self.issues)

    @property
    def is_identical(self) -> bool:
        return len(self.issues) == 0


@frozen
class DataFrameAnalysis:
    shape: ShapeAnalysis
    columns: ColumnAnalysis
    dtypes: DtypeAnalysis
    missing_rows: MissingRowsAnalysis
    values: ValueAnalysis
    summary: DiffSummary
    df1_name: str
    df2_name: str

    @property
    def is_identical(self) -> bool:
        return self.summary.is_identical


@curry
def make_analyze_shapes(df1: pd.DataFrame, df2: pd.DataFrame) -> ShapeAnalysis:
    return ShapeAnalysis(df1_shape=df1.shape, df2_shape=df2.shape)


@curry
def make_analyze_columns(df1: pd.DataFrame, df2: pd.DataFrame) -> ColumnAnalysis:
    cols1 = set(df1.columns)
    cols2 = set(df2.columns)

    return ColumnAnalysis(
        df1_only=tuple(sorted(cols1 - cols2)),
        df2_only=tuple(sorted(cols2 - cols1)),
        common=tuple(sorted(cols1 & cols2)),
        df1_order=tuple(df1.columns),
        df2_order=tuple(df2.columns),
    )


@curry
def make_analyze_dtypes(df1: pd.DataFrame, df2: pd.DataFrame) -> DtypeAnalysis:
    common_cols = set(df1.columns) & set(df2.columns)

    differences = tuple(
        DtypeDifference(
            column=col,
            df1_dtype=str(df1[col].dtype),
            df2_dtype=str(df2[col].dtype),
        )
        for col in common_cols
        if df1[col].dtype != df2[col].dtype
    )

    return DtypeAnalysis(differences=differences)


@curry
def make_analyze_missing_rows(
    df1: pd.DataFrame,
    df2: pd.DataFrame,
) -> MissingRowsAnalysis:
    common_cols = list(set(df1.columns) & set(df2.columns))

    if not common_cols:
        return MissingRowsAnalysis(
            df1_only=pd.DataFrame(),
            df2_only=pd.DataFrame(),
            error="No common columns for row comparison",
        )

    try:
        df1_common = df1[common_cols].copy()
        df2_common = df2[common_cols].copy()

        df1_only = (
            df1_common.merge(df2_common, how="left", indicator=True)
            .query('_merge == "left_only"')
            .drop("_merge", axis=1)
            .drop_duplicates()
        )

        df2_only = (
            df2_common.merge(df1_common, how="left", indicator=True)
            .query('_merge == "left_only"')
            .drop("_merge", axis=1)
            .drop_duplicates()
        )

        return MissingRowsAnalysis(df1_only=df1_only, df2_only=df2_only)

    except Exception as e:
        return MissingRowsAnalysis(
            df1_only=pd.DataFrame(),
            df2_only=pd.DataFrame(),
            error=f"Could not compare rows: {str(e)}",
        )


@curry
def make_find_row_differences(
    common_cols: tuple[str, ...],
    df1: pd.DataFrame,
    df2: pd.DataFrame,
    idx: int,
) -> ValueDifference | None:
    row_diffs = {}
    for col in common_cols:
        val1 = df1.loc[idx, col]
        val2 = df2.loc[idx, col]

        if pd.isna(val1) and pd.isna(val2):
            continue
        elif val1 != val2 or (pd.isna(val1) != pd.isna(val2)):
            row_diffs[col] = (val1, val2)

    if row_diffs:
        return ValueDifference(row_index=idx, column_diffs=row_diffs)
    return None


@curry
def make_analyze_values(df1: pd.DataFrame, df2: pd.DataFrame) -> ValueAnalysis:
    common_cols = tuple(set(df1.columns) & set(df2.columns))

    if not common_cols or len(df1) != len(df2):
        return ValueAnalysis(
            differences=(),
            error="Cannot compare values - different shapes or no common columns",
        )

    try:
        df1_common = df1[list(common_cols)].reset_index(drop=True)
        df2_common = df2[list(common_cols)].reset_index(drop=True)

        differences = tuple(
            diff
            for idx in range(len(df1_common))
            if (
                diff := make_find_row_differences(
                    common_cols,
                    df1_common,
                    df2_common,
                    idx,
                )
            )
        )

        return ValueAnalysis(differences=differences[:10])

    except Exception as e:
        return ValueAnalysis(
            differences=(),
            error=f"Could not compare values: {str(e)}",
        )


@curry
def make_generate_summary(
    df1_name: str,
    df2_name: str,
    shape: ShapeAnalysis,
    columns: ColumnAnalysis,
    dtypes: DtypeAnalysis,
    missing: MissingRowsAnalysis,
    values: ValueAnalysis,
) -> DiffSummary:
    issues = []

    if not shape.is_equal:
        issues.append(f"Shape difference: {shape.df1_shape} vs {shape.df2_shape}")

    if columns.df1_only:
        issues.append(f"Columns only in {df1_name}: {columns.df1_only}")
    if columns.df2_only:
        issues.append(f"Columns only in {df2_name}: {columns.df2_only}")
    if not columns.is_order_equal and columns.is_equal:
        issues.append("Column order differs")

    if not dtypes.all_equal:
        issues.append(f"Dtype differences in {len(dtypes.differences)} columns")

    if missing.df1_only_count > 0:
        issues.append(f"{missing.df1_only_count} rows only in {df1_name}")
    if missing.df2_only_count > 0:
        issues.append(f"{missing.df2_only_count} rows only in {df2_name}")

    if values.has_differences:
        issues.append(f"{values.total_differing_rows} rows have value differences")

    return DiffSummary(issues=tuple(issues))


@curry
def make_full_analysis(
    df1_name: str,
    df2_name: str,
    df1: pd.DataFrame,
    df2: pd.DataFrame,
) -> DataFrameAnalysis:
    shape_analysis = make_analyze_shapes(df1, df2)
    column_analysis = make_analyze_columns(df1, df2)
    dtype_analysis = make_analyze_dtypes(df1, df2)
    missing_analysis = make_analyze_missing_rows(df1, df2)
    value_analysis = make_analyze_values(df1, df2)

    summary = make_generate_summary(
        df1_name,
        df2_name,
        shape_analysis,
        column_analysis,
        dtype_analysis,
        missing_analysis,
        value_analysis,
    )

    return DataFrameAnalysis(
        shape=shape_analysis,
        columns=column_analysis,
        dtypes=dtype_analysis,
        missing_rows=missing_analysis,
        values=value_analysis,
        summary=summary,
        df1_name=df1_name,
        df2_name=df2_name,
    )


def do_print_analysis(analysis: DataFrameAnalysis) -> None:
    if analysis.is_identical:
        return

    print(f"DataFrames differ: {analysis.df1_name} vs {analysis.df2_name}")

    if not analysis.shape.is_equal:
        print(f"  Shape: {analysis.shape.df1_shape} vs {analysis.shape.df2_shape}")

    if analysis.columns.df1_only:
        print(f"  Columns only in {analysis.df1_name}: {analysis.columns.df1_only}")
    if analysis.columns.df2_only:
        print(f"  Columns only in {analysis.df2_name}: {analysis.columns.df2_only}")

    if analysis.missing_rows.df1_only_count > 0:
        print(
            f"  Rows only in {analysis.df1_name}: {analysis.missing_rows.df1_only_count}",
        )
    if analysis.missing_rows.df2_only_count > 0:
        print(
            f"  Rows only in {analysis.df2_name}: {analysis.missing_rows.df2_only_count}",
        )

    if analysis.values.has_differences:
        print(f"  Rows with value differences: {analysis.values.total_differing_rows}")


@curry
def make_compare_dataframes(
    df1_name: str,
    df2_name: str,
    should_print: bool,
    df1: pd.DataFrame,
    df2: pd.DataFrame,
) -> DataFrameAnalysis:
    analysis = make_full_analysis(df1_name, df2_name, df1, df2)

    if should_print:
        do_print_analysis(analysis)

    return analysis


@curry
def make_extract_columns(columns: tuple[str, ...], df: pd.DataFrame) -> pd.DataFrame:
    return df[list(columns)]


def make_execute_query(query: Any) -> pd.DataFrame:
    return query.execute()


def normalize_dataframe_dtypes(
    target_df: pd.DataFrame,
    reference_df: pd.DataFrame,
) -> pd.DataFrame:
    return make_normalize_dataframe_dtypes(reference_df, target_df)


def normalize_for_comparison(
    df1: pd.DataFrame,
    df2: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    return make_normalize_for_comparison(df1, df2)


def compare_dataframes(
    df1: pd.DataFrame,
    df2: pd.DataFrame,
    df1_name: str = "DataFrame1",
    df2_name: str = "DataFrame2",
    print_report: bool = True,
) -> DataFrameAnalysis:
    return make_compare_dataframes(df1_name, df2_name, print_report, df1, df2)
