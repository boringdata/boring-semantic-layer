"""Example demonstrating chart visualization functionality."""

import pandas as pd
import ibis
from boring_semantic_layer import SemanticModel


def main():
    # Create sample data
    dates = pd.date_range(start="2023-01-01", end="2023-12-31", freq="D")
    categories = ["Electronics", "Clothing", "Food", "Books"]

    # Generate synthetic sales data
    import numpy as np

    np.random.seed(42)

    records = []
    for date in dates:
        for category in categories:
            # Add some seasonality and randomness
            base_sales = 1000 + 500 * np.sin(date.dayofyear * 2 * np.pi / 365)
            category_factor = {
                "Electronics": 1.5,
                "Clothing": 1.2,
                "Food": 1.0,
                "Books": 0.8,
            }[category]
            sales = base_sales * category_factor + np.random.normal(0, 100)
            records.append(
                {
                    "date": date,
                    "category": category,
                    "sales": max(0, sales),
                    "units": max(1, int(sales / 50 + np.random.normal(0, 5))),
                }
            )

    df = pd.DataFrame(records)

    # Create connection and table
    con = ibis.duckdb.connect(":memory:")
    sales_table = con.create_table("sales", df)

    # Define semantic model
    sales_model = SemanticModel(
        table=sales_table,
        dimensions={
            "category": lambda t: t.category,
            "date": lambda t: t.date,
            "month": lambda t: t.date.month(),
            "quarter": lambda t: t.date.quarter(),
        },
        measures={
            "total_sales": lambda t: t.sales.sum(),
            "avg_sales": lambda t: t.sales.mean(),
            "total_units": lambda t: t.units.sum(),
        },
        time_dimension="date",
        smallest_time_grain="TIME_GRAIN_DAY",
    )

    # Example 1: Simple bar chart with explicit specification
    print("Example 1: Category Sales Bar Chart")
    query1 = sales_model.query(
        dimensions=["category"],
        measures=["total_sales"],
        chart={
            "mark": "bar",
            "encoding": {
                "x": {
                    "field": "category",
                    "type": "nominal",
                    "title": "Product Category",
                },
                "y": {
                    "field": "total_sales",
                    "type": "quantitative",
                    "title": "Total Sales ($)",
                },
            },
            "title": "Total Sales by Category",
        },
    )

    # Get the Vega-Lite spec with data
    chart_spec1 = query1.chart()
    print(f"Generated {len(chart_spec1['data']['values'])} data points")
    print()

    # Example 2: Time series with auto-detection
    print("Example 2: Monthly Sales Time Series (Auto-detected)")
    query2 = sales_model.query(
        dimensions=["date"], measures=["total_sales"], time_grain="TIME_GRAIN_MONTH"
    )

    # Auto-detect chart type (will be line chart for time series)
    chart_spec2 = query2.chart()
    print(f"Auto-detected chart type: {chart_spec2['mark']}")
    print(f"Generated {len(chart_spec2['data']['values'])} monthly data points")
    print()

    # Example 3: Grouped bar chart for multiple measures
    print("Example 3: Sales and Units by Category")
    query3 = sales_model.query(
        dimensions=["category"], measures=["total_sales", "total_units"]
    )

    chart_spec3 = query3.chart()
    print(
        f"Generated multi-measure chart with {len(chart_spec3['data']['values'])} data points"
    )
    print()

    # Example 4: Heatmap for two dimensions
    print("Example 4: Sales Heatmap by Quarter and Category")
    query4 = sales_model.query(
        dimensions=["quarter", "category"],
        measures=["avg_sales"],
        chart={
            "mark": "rect",
            "encoding": {
                "x": {"field": "quarter", "type": "ordinal", "title": "Quarter"},
                "y": {"field": "category", "type": "nominal", "title": "Category"},
                "color": {
                    "field": "avg_sales",
                    "type": "quantitative",
                    "title": "Avg Sales",
                    "scale": {"scheme": "viridis"},
                },
            },
            "title": "Average Sales by Quarter and Category",
        },
    )

    chart_spec4 = query4.chart()
    print(f"Generated heatmap with {len(chart_spec4['data']['values'])} cells")
    print()

    # Example 5: Filtered time series
    print("Example 5: Electronics Sales Over Time")
    query5 = sales_model.query(
        dimensions=["date"],
        measures=["total_sales"],
        filters=[{"field": "category", "operator": "=", "value": "Electronics"}],
        time_grain="TIME_GRAIN_WEEK",
        time_range={"start": "2023-06-01T00:00:00Z", "end": "2023-08-31T23:59:59Z"},
        chart={
            "mark": {"type": "line", "point": True},
            "encoding": {
                "x": {"field": "date", "type": "temporal", "title": "Week"},
                "y": {
                    "field": "total_sales",
                    "type": "quantitative",
                    "title": "Sales ($)",
                },
            },
            "title": "Weekly Electronics Sales (Summer 2023)",
        },
    )

    chart_spec5 = query5.chart()
    print(
        f"Generated filtered time series with {len(chart_spec5['data']['values'])} weeks"
    )
    print()

    # Example 6: Using render() to create Altair chart (requires altair installation)
    print("Example 6: Rendering with Altair")
    try:
        # This will create an actual Altair chart object
        query1.render()
        print("Successfully created Altair chart object")
        # In a Jupyter notebook, this would display the chart
        # altair_chart.show()
    except ImportError:
        print(
            "Altair not installed. Install with: pip install 'boring-semantic-layer[visualization]'"
        )

    # Example 7: Auto-detection with partial specification
    print("\nExample 7: Auto-detection with Custom Styling")
    query7 = sales_model.query(
        dimensions=["category"],
        measures=["avg_sales"],
        chart={
            # Mark type will be auto-detected as "bar"
            "encoding": {
                "x": {"field": "category", "type": "nominal"},
                "y": {"field": "avg_sales", "type": "quantitative"},
                "color": {"value": "steelblue"},
            },
            "width": 400,
            "height": 300,
        },
    )

    chart_spec7 = query7.chart()
    print(f"Chart with auto-detected mark type: {chart_spec7['mark']}")

    # Print sample of actual data
    print("\nSample data from first query:")
    for i, row in enumerate(chart_spec1["data"]["values"]):
        print(f"  {row}")
        if i >= 2:
            break


if __name__ == "__main__":
    main()
