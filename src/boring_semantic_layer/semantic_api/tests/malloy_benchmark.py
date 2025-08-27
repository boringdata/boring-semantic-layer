from pathlib import Path
import ibis
import os
import malloy 
from malloy.data.duckdb import DuckDbConnection
import asyncio
import importlib
import pandas as pd

QUERIES = {
    "comparing_timeframe": [
        "query_1"
    ]
}
import sys
BASE_PATH = Path(__file__).parent / "malloy_benchmark"
sys.path.append(str(BASE_PATH))

async def run_malloy_query(query_file: str, query_name: str):

    with malloy.Runtime() as runtime:
        runtime.add_connection(DuckDbConnection(home_dir="."))

        data = await runtime.load_file(BASE_PATH / query_file ).run(named_query=query_name)

        df = data.to_dataframe()

    return df

def test_malloy_queries():
    for query_file, queries in QUERIES.items():
        for query_name in queries:
            df_malloy = asyncio.run(run_malloy_query(f"{query_file}.malloy", query_name))
            module = importlib.import_module(query_file)
            query_bsl = getattr(module, query_name)
            df_bsl = query_bsl.execute()

            # Sort both DataFrames by all columns for consistent comparison
            common_cols = list(df_malloy.columns)
            df_malloy_sorted = df_malloy.sort_values(by=common_cols).reset_index(drop=True)
            df_bsl_sorted = df_bsl[common_cols].sort_values(by=common_cols).reset_index(drop=True)
            
            if not df_malloy_sorted.equals(df_bsl_sorted):
                print(f"\nDifference found in {query_file}.{query_name}:")
                print(f"Shape - Malloy: {df_malloy_sorted.shape}, BSL: {df_bsl_sorted.shape}")
                
                try:
                    diff = df_malloy_sorted.compare(df_bsl_sorted)
                    print("Row-by-row differences (left=Malloy, right=BSL):")
                    print(diff.to_string())
                except ValueError as e:
                    print(f"Cannot compare DataFrames: {e}")
                    print("Malloy DataFrame (sorted):")
                    print(df_malloy_sorted.to_string())
                    print("\nBSL DataFrame (sorted):")
                    print(df_bsl_sorted.to_string())
                    
                    # Show side-by-side comparison for differing rows
                    if len(df_malloy_sorted) == len(df_bsl_sorted):
                        print("\nRow differences:")
                        for i in range(len(df_malloy_sorted)):
                            malloy_row = df_malloy_sorted.iloc[i]
                            bsl_row = df_bsl_sorted.iloc[i]
                            if not malloy_row.equals(bsl_row):
                                print(f"\nRow {i}:")
                                print(f"Malloy: {malloy_row.to_dict()}")
                                print(f"BSL:    {bsl_row.to_dict()}")
                    else:
                        # Different shapes - find missing rows
                        print("\nFinding missing rows due to different shapes:")
                        
                        # Find rows in Malloy but not in BSL
                        malloy_not_in_bsl = df_malloy_sorted.merge(df_bsl_sorted, how='left', indicator=True).query('_merge == "left_only"').drop('_merge', axis=1)
                        if not malloy_not_in_bsl.empty:
                            print(f"\nRows in Malloy but NOT in BSL ({len(malloy_not_in_bsl)} rows):")
                            print(malloy_not_in_bsl.to_string())
                        
                        # Find rows in BSL but not in Malloy
                        bsl_not_in_malloy = df_bsl_sorted.merge(df_malloy_sorted, how='left', indicator=True).query('_merge == "left_only"').drop('_merge', axis=1)
                        if not bsl_not_in_malloy.empty:
                            print(f"\nRows in BSL but NOT in Malloy ({len(bsl_not_in_malloy)} rows):")
                            print(bsl_not_in_malloy.to_string())
                
                assert False, f"DataFrames do not match for {query_file}.{query_name}"