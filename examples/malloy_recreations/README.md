# Malloy Recreations in BSL v2

This directory contains BSL v2 recreations of popular Malloy semantic models
from the [malloy-samples](https://github.com/malloydata/malloy-samples)
repository.

## Overview

The goal of these recreations is to demonstrate how BSL v2 can handle the same
complex analytical use cases as Malloy, showcasing feature parity and ergonomic
similarities between the two semantic layer approaches.

## Available Models

### 1. Airports Model (`airports_bsl.py`)

**Source**: [faa/airports.malloy](https://github.com/malloydata/malloy-samples/blob/main/faa/airports.malloy)

A simple model demonstrating:
- Basic dimension and measure definitions
- Filtering (WHERE clauses)
- Multiple analytical views
- Hierarchical aggregations (simulating Malloy's nested views)

**Key Malloy Features Recreated**:
- `rename`: Column aliasing via dimensions
- `measure`: Aggregation measures
- `view`: Analytical queries as reusable functions
- `nest`: Hierarchical drill-downs (simulated)

**Run the demo**:
```bash
python examples/malloy_recreations/airports_bsl.py
```

### 2. E-commerce Model (`ecommerce_bsl.py`)

**Source**: [ecommerce/ecommerce.malloy](https://github.com/malloydata/malloy-samples/blob/main/ecommerce/ecommerce.malloy)

A comprehensive model demonstrating:
- Multi-table joins (users, products, inventory, orders)
- Computed dimensions (full_name formatting, gross_margin, customer tiers)
- Filtered measures (year-based, status-based)
- Percent of total calculations
- Top N queries
- YoY growth analysis
- 14+ analytical views including dashboards

**Key Malloy Features Recreated**:
- `join_one`: Foreign key joins to dimension tables
- `dimension`: Computed fields (title case formatting, CASE expressions for tiers)
- `measure`: Complex aggregations with filters
- `view`: Multiple pre-defined analytical queries
  - `top_categories`, `top_brands`, `top_products`, `top_customers`
  - `by_year`, `by_month`, `sales_by_state`
  - `orders_by_status`, `frequent_returners`
  - `sales_summary_yoy`
- Filtered aggregates: `total_sales { where: year(created_at) = 2022 }`

**Run the demo**:
```bash
python examples/malloy_recreations/ecommerce_bsl.py
```

## Feature Mapping: Malloy → BSL v2

### Source & Table Definition

**Malloy**:
```malloy
source: airports is duckdb.table('../data/airports.parquet') extend {
  measure: airport_count is count()
}
```

**BSL v2**:
```python
airports_tbl = con.read_parquet('../data/airports.parquet')
airports_st = (
    to_semantic_table(airports_tbl, name="airports")
    .with_measures(
        airport_count=lambda t: t.count()
    )
)
```

### Dimensions (Computed Fields)

**Malloy**:
```malloy
dimension: full_name is concat(
  upper(substr(first_name, 1, 1)), lower(substr(first_name, 2)),
  ' ',
  upper(substr(last_name, 1, 1)), lower(substr(last_name, 2))
)
```

**BSL v2**:
```python
.with_dimensions(
    full_name=lambda t: (
        t.first_name.substr(0, 1).upper() + t.first_name.substr(1).lower() +
        " " +
        t.last_name.substr(0, 1).upper() + t.last_name.substr(1).lower()
    )
)
```

### Tiered Dimensions (CASE Expressions)

**Malloy**:
```malloy
dimension: lifetime_order_tier is lifetime_orders ?
  pick '1 to 3' when  < 4
  pick '4 to 6' when < 7
  pick '7 to 10' when < 11
  else '11+'
```

**BSL v2**:
```python
.with_dimensions(
    lifetime_order_tier=lambda t: (
        ibis.case()
        .when(t.lifetime_orders < 4, "1 to 3")
        .when(t.lifetime_orders < 7, "4 to 6")
        .when(t.lifetime_orders < 11, "7 to 10")
        .else_("11+")
        .end()
    )
)
```

### Joins

**Malloy**:
```malloy
join_one:
  users with user_id
  inventory_items with inventory_item_id
```

**BSL v2**:
```python
# Perform joins at the Ibis table level before creating semantic table
order_items_enriched = (
    order_items_tbl
    .join(users_tbl, order_items_tbl.user_id == users_tbl.id, how="left")
    .join(inventory_tbl, order_items_tbl.inventory_item_id == inventory_tbl.id, how="left")
)
# Then create semantic table from joined result
order_items_st = to_semantic_table(order_items_enriched, name="order_items")
```

### Filtered Measures

**Malloy**:
```malloy
measure:
  total_sales_2022 is total_sales { where: year(created_at) = 2022 }
  total_sales_2021 is total_sales { where: year(created_at) = 2021 }
```

**BSL v2**:
```python
.with_measures(
    total_sales_2022=lambda t: (t.sale_price * (t.created_at.year() == 2022).cast("int")).sum(),
    total_sales_2021=lambda t: (t.sale_price * (t.created_at.year() == 2021).cast("int")).sum(),
)
```

### Views (Analytical Queries)

**Malloy**:
```malloy
view: top_brands is {
  top: 5
  group_by: inventory_items.product_brand
  aggregate: total_sales, percent_of_sales, total_gross_margin
}
```

**BSL v2**:
```python
def view_top_brands(order_items_st, limit=5):
    return (
        order_items_st
        .group_by("product_brand")
        .aggregate("total_sales", "total_gross_margin")
        .mutate(
            percent_of_sales=lambda t: t["total_sales"] / t.all(t["total_sales"])
        )
        .order_by(ibis.desc("total_sales"))
        .limit(limit)
    )
```

### Percent of Total

**Malloy**:
```malloy
measure: percent_of_sales is total_sales / all(total_sales)
```

**BSL v2**:
```python
.mutate(
    percent_of_sales=lambda t: t["total_sales"] / t.all(t["total_sales"])
)
```

### Inline Dimension Creation

**Malloy**:
```malloy
view: by_year is {
  group_by: created_year is year(created_at)
  aggregate: total_sales
}
```

**BSL v2**:
```python
# Method 1: Add dimension first
st_with_year = order_items_st.with_dimensions(
    created_year=lambda t: t.created_at.year()
)
result = st_with_year.group_by("created_year").aggregate("total_sales")

# Method 2: Or compute in aggregate (for simple transformations)
result = (
    order_items_st
    .group_by("created_at")
    .aggregate("total_sales")
    .mutate(created_year=lambda t: t["created_at"].year())
)
```

## Key Differences & Design Choices

### 1. **Nested Views**
- **Malloy**: Supports nested result structures with `nest:` keyword
- **BSL v2**: Returns flat DataFrames; nesting can be simulated with filtering or handled client-side

### 2. **Inline Dimension Creation**
- **Malloy**: Allows `group_by: new_dim is expression` syntax
- **BSL v2**: Requires `.with_dimensions()` first, then `.group_by()` (more explicit)

### 3. **Join Syntax**
- **Malloy**: `join_one: table_name with foreign_key` (declarative)
- **BSL v2**: Explicit Ibis joins before semantic table creation (more control over join types and conditions)

### 4. **Views as Functions**
- **Malloy**: Views are properties of the source
- **BSL v2**: Views are Python functions that take semantic tables as input (more composable and testable)

## BSL v2 Advantages Demonstrated

1. **Python-Native**: Full access to Python ecosystem (pandas, numpy, etc.)
2. **Type Safety**: Ibis provides compile-time query validation
3. **Composability**: Semantic tables can be dynamically combined and extended
4. **Flexibility**: Mix semantic measures with ad-hoc aggregations freely
5. **Testability**: Views are regular Python functions, easy to unit test

## What's Next?

To extend these examples:

1. **Add More Models**: Convert additional Malloy samples (IMDB, GA4, auto_recalls)
2. **Create Tests**: Write pytest tests that compare BSL v2 output with Malloy output
3. **Add Real Data**: Replace sample data with actual Parquet files from malloy-samples
4. **Build Dashboards**: Use these semantic tables with Streamlit, Plotly, or other viz tools

## Resources

- **Malloy Samples**: https://github.com/malloydata/malloy-samples
- **Malloy Documentation**: https://docs.malloydata.dev
- **BSL v2 Documentation**: See `README.md` at project root
- **Ibis Documentation**: https://ibis-project.org/

## Contributing

Found a Malloy pattern that's not yet represented? Contributions welcome!

1. Fork the repo
2. Add a new model in `examples/malloy_recreations/`
3. Document the Malloy → BSL v2 mapping
4. Submit a PR
