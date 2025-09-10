#!/usr/bin/env python3
"""
Demo script showing deferred-based dimensions and measures
with the boring semantic layer.

Run:
    python example_semantic_api/example_deferred_demo.py
"""
import pandas as pd
import ibis
from ibis.common.deferred import _

from boring_semantic_layer.semantic_api.api import to_semantic_table


def main() -> None:
    # Create an in-memory table with sample data
    df = pd.DataFrame({'x': [1, 2, 2, 3], 'y': [10, 20, 30, 40]})
    con = ibis.duckdb.connect(':memory:')
    tbl = con.create_table('t', df)

    # Define a semantic table using Deferred for dimensions and measures
    st = (
        to_semantic_table(tbl, name='t')
        .with_dimensions(x=_.x)
        .with_measures(sum_y=_.y.sum())
    )

    # Inspect the raw Dimension and Measure objects
    print('Dimension expr object:', st.op().dimensions['x'])
    print('Measure expr object:  ', st.op().measures['sum_y'])

    # Accessing attributes resolves Deferred expressions against the table
    print('Resolved st.x expression:', st.x)
    print('Resolved st.sum_y expression:', st.sum_y)

    # Show the native Ibis expression
    print('Native Ibis expression:')
    print(st.to_ibis())

    # Execute a semantic group_by + aggregate
    result = st.group_by('x').aggregate(sum_y=lambda t: t.sum_y).execute()
    print('Result of group_by/aggregate:')
    print(result)


if __name__ == '__main__':
    main()