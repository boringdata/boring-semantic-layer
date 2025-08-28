"""
Utility functions for DataFrame comparison and dtype normalization in Malloy benchmarks.
"""

import pandas as pd
from typing import Dict, Any


def normalize_dataframe_dtypes(
    target_df: pd.DataFrame, reference_df: pd.DataFrame
) -> pd.DataFrame:
    """
    Normalize dtypes of target_df to match reference_df for comparison.
    Uses a simple mapping approach for common dtype conversions.

    Args:
        target_df: DataFrame to adjust
        reference_df: DataFrame with reference dtypes

    Returns:
        DataFrame with adjusted dtypes
    """
    normalized_df = target_df.copy()
    conversion_log = []

    # Create mapping of reference dtypes
    dtype_map = {col: reference_df[col].dtype for col in reference_df.columns}

    for col in dtype_map:
        if col not in normalized_df.columns:
            continue

        target_dtype = normalized_df[col].dtype
        reference_dtype = dtype_map[col]

        if target_dtype == reference_dtype:
            continue

        try:
            # Simple astype conversion - let pandas handle the details
            normalized_df[col] = normalized_df[col].astype(reference_dtype)
            conversion_log.append(f"  {col}: {target_dtype} → {reference_dtype}")

        except Exception as e:
            # Log failed conversions but continue
            conversion_log.append(
                f"  {col}: {target_dtype} → {reference_dtype} (FAILED: {e})"
            )
            continue

    if conversion_log:
        print("Dtype conversions applied:")
        print("\n".join(conversion_log))

    return normalized_df


class DataFrameDiff:
    """Comprehensive DataFrame comparison utility."""

    def __init__(
        self,
        df1: pd.DataFrame,
        df2: pd.DataFrame,
        df1_name: str = "DF1",
        df2_name: str = "DF2",
    ):
        self.df1 = df1.copy()
        self.df2 = df2.copy()
        self.df1_name = df1_name
        self.df2_name = df2_name
        self._analysis = None

    def analyze(self) -> Dict[str, Any]:
        """Perform comprehensive diff analysis."""
        if self._analysis is not None:
            return self._analysis

        analysis = {
            "identical": False,
            "shape_diff": self._analyze_shapes(),
            "column_diff": self._analyze_columns(),
            "dtype_diff": self._analyze_dtypes(),
            "missing_rows": self._analyze_missing_rows(),
            "value_diff": self._analyze_value_differences(),
            "summary": {},
        }

        # Generate summary
        analysis["summary"] = self._generate_summary(analysis)
        analysis["identical"] = self._are_identical(analysis)

        self._analysis = analysis
        return analysis

    def _analyze_shapes(self) -> Dict[str, Any]:
        """Compare DataFrame shapes."""
        return {
            self.df1_name: self.df1.shape,
            self.df2_name: self.df2.shape,
            "equal": self.df1.shape == self.df2.shape,
        }

    def _analyze_columns(self) -> Dict[str, Any]:
        """Compare DataFrame columns."""
        cols1 = set(self.df1.columns)
        cols2 = set(self.df2.columns)

        return {
            f"{self.df1_name}_only": sorted(cols1 - cols2),
            f"{self.df2_name}_only": sorted(cols2 - cols1),
            "common": sorted(cols1 & cols2),
            "equal": cols1 == cols2,
            "order_equal": list(self.df1.columns) == list(self.df2.columns),
        }

    def _analyze_dtypes(self) -> Dict[str, Any]:
        """Compare DataFrame dtypes for common columns."""
        common_cols = set(self.df1.columns) & set(self.df2.columns)
        dtype_diffs = {}

        for col in common_cols:
            dtype1 = self.df1[col].dtype
            dtype2 = self.df2[col].dtype
            if dtype1 != dtype2:
                dtype_diffs[col] = {
                    self.df1_name: str(dtype1),
                    self.df2_name: str(dtype2),
                }

        return {"differences": dtype_diffs, "all_equal": len(dtype_diffs) == 0}

    def _analyze_missing_rows(self) -> Dict[str, Any]:
        """Find rows that exist in one DataFrame but not the other."""
        common_cols = list(set(self.df1.columns) & set(self.df2.columns))

        if not common_cols:
            return {
                f"{self.df1_name}_only": len(self.df1),
                f"{self.df2_name}_only": len(self.df2),
                "details": "No common columns for row comparison",
            }

        # Use merge with indicator to find missing rows
        try:
            # Ensure both DataFrames have same columns for comparison
            df1_common = self.df1[common_cols].copy()
            df2_common = self.df2[common_cols].copy()

            # Find rows in df1 but not in df2
            df1_only = (
                df1_common.merge(df2_common, how="left", indicator=True)
                .query('_merge == "left_only"')
                .drop("_merge", axis=1)
                .drop_duplicates()
            )

            # Find rows in df2 but not in df1
            df2_only = (
                df2_common.merge(df1_common, how="left", indicator=True)
                .query('_merge == "left_only"')
                .drop("_merge", axis=1)
                .drop_duplicates()
            )

            return {
                f"{self.df1_name}_only": df1_only,
                f"{self.df2_name}_only": df2_only,
                f"{self.df1_name}_only_count": len(df1_only),
                f"{self.df2_name}_only_count": len(df2_only),
            }

        except Exception as e:
            return {
                "error": f"Could not compare rows: {str(e)}",
                f"{self.df1_name}_only_count": 0,
                f"{self.df2_name}_only_count": 0,
            }

    def _analyze_value_differences(self) -> Dict[str, Any]:
        """Find specific value differences in common rows."""
        common_cols = list(set(self.df1.columns) & set(self.df2.columns))

        if not common_cols or len(self.df1) != len(self.df2):
            return {
                "details": "Cannot compare values - different shapes or no common columns"
            }

        try:
            value_diffs = []

            # Compare DataFrames with same columns
            df1_common = self.df1[common_cols].reset_index(drop=True)
            df2_common = self.df2[common_cols].reset_index(drop=True)

            # Find rows with any differences
            for idx in range(len(df1_common)):
                row_diffs = {}
                for col in common_cols:
                    val1 = df1_common.loc[idx, col]
                    val2 = df2_common.loc[idx, col]

                    # Handle NaN comparisons
                    if pd.isna(val1) and pd.isna(val2):
                        continue
                    elif val1 != val2 or (pd.isna(val1) != pd.isna(val2)):
                        row_diffs[col] = {self.df1_name: val1, self.df2_name: val2}

                if row_diffs:
                    value_diffs.append({"row_index": idx, "differences": row_diffs})

            return {
                "row_differences": value_diffs[
                    :10
                ],  # Limit to first 10 for readability
                "total_differing_rows": len(value_diffs),
                "has_differences": len(value_diffs) > 0,
            }

        except Exception as e:
            return {"error": f"Could not compare values: {str(e)}"}

    def _generate_summary(self, analysis: Dict[str, Any]) -> Dict[str, Any]:
        """Generate a high-level summary of differences."""
        issues = []

        # Shape issues
        if not analysis["shape_diff"]["equal"]:
            issues.append(
                f"Shape difference: {analysis['shape_diff'][self.df1_name]} vs {analysis['shape_diff'][self.df2_name]}"
            )

        # Column issues
        col_diff = analysis["column_diff"]
        if col_diff[f"{self.df1_name}_only"]:
            issues.append(
                f"Columns only in {self.df1_name}: {col_diff[f'{self.df1_name}_only']}"
            )
        if col_diff[f"{self.df2_name}_only"]:
            issues.append(
                f"Columns only in {self.df2_name}: {col_diff[f'{self.df2_name}_only']}"
            )
        if not col_diff["order_equal"] and col_diff["equal"]:
            issues.append("Column order differs")

        # Dtype issues
        if not analysis["dtype_diff"]["all_equal"]:
            dtype_count = len(analysis["dtype_diff"]["differences"])
            issues.append(f"Dtype differences in {dtype_count} columns")

        # Missing rows
        missing = analysis["missing_rows"]
        if missing.get(f"{self.df1_name}_only_count", 0) > 0:
            issues.append(
                f"{missing[f'{self.df1_name}_only_count']} rows only in {self.df1_name}"
            )
        if missing.get(f"{self.df2_name}_only_count", 0) > 0:
            issues.append(
                f"{missing[f'{self.df2_name}_only_count']} rows only in {self.df2_name}"
            )

        # Value differences
        value_diff = analysis["value_diff"]
        if value_diff.get("has_differences", False):
            issues.append(
                f"{value_diff['total_differing_rows']} rows have value differences"
            )

        return {
            "total_issues": len(issues),
            "issues": issues,
            "is_identical": len(issues) == 0,
        }

    def _are_identical(self, analysis: Dict[str, Any]) -> bool:
        """Determine if DataFrames are identical."""
        return analysis["summary"]["is_identical"]

    def print_report(self) -> None:
        """Print a simple diff report."""
        analysis = self.analyze()
        summary = analysis["summary"]

        if summary["is_identical"]:
            return  # No output for identical DataFrames

        # Just print the key differences
        print(f"DataFrames differ: {self.df1_name} vs {self.df2_name}")

        # Shape difference
        shape_diff = analysis["shape_diff"]
        if not shape_diff["equal"]:
            print(
                f"  Shape: {shape_diff[self.df1_name]} vs {shape_diff[self.df2_name]}"
            )

        # Column differences
        col_diff = analysis["column_diff"]
        if col_diff[f"{self.df1_name}_only"]:
            print(
                f"  Columns only in {self.df1_name}: {col_diff[f'{self.df1_name}_only']}"
            )
        if col_diff[f"{self.df2_name}_only"]:
            print(
                f"  Columns only in {self.df2_name}: {col_diff[f'{self.df2_name}_only']}"
            )

        # Missing rows (just counts)
        missing = analysis["missing_rows"]
        df1_only_count = missing.get(f"{self.df1_name}_only_count", 0)
        df2_only_count = missing.get(f"{self.df2_name}_only_count", 0)
        if df1_only_count > 0:
            print(f"  Rows only in {self.df1_name}: {df1_only_count}")
        if df2_only_count > 0:
            print(f"  Rows only in {self.df2_name}: {df2_only_count}")

        # Value differences (just count)
        value_diff = analysis["value_diff"]
        if value_diff.get("has_differences", False):
            print(
                f"  Rows with value differences: {value_diff['total_differing_rows']}"
            )


def compare_dataframes(
    df1: pd.DataFrame,
    df2: pd.DataFrame,
    df1_name: str = "DataFrame1",
    df2_name: str = "DataFrame2",
    print_report: bool = True,
) -> DataFrameDiff:
    """
    Compare two DataFrames and return detailed analysis.

    Args:
        df1: First DataFrame
        df2: Second DataFrame
        df1_name: Name for first DataFrame in reports
        df2_name: Name for second DataFrame in reports
        print_report: Whether to print the comparison report

    Returns:
        DataFrameDiff object with analysis results
    """
    diff = DataFrameDiff(df1, df2, df1_name, df2_name)

    if print_report:
        diff.print_report()

    return diff
