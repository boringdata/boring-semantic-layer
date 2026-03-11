"""
semantic_config.py — Edit this file to define your semantic models.

Each SemanticModel wraps a DuckDB table and declares:
  - dimensions: columns or expressions used to group/slice data
  - measures:   aggregations (sum, count, avg, etc.)

The MODELS dict at the bottom is required — main.py and the API import from it.
"""
import ibis
from ibis import _
import pathlib
import boring_semantic_layer as bsl

# Connect to DuckDB (change the path if you use a different file)
DB_PATH = pathlib.Path(__file__).parent / "data" / "tpch.duckdb"
conn = ibis.duckdb.connect(str(DB_PATH))

# define semantic tables
orders = bsl.to_semantic_table(
        conn.table("orders"),
        name="orders"
    ).with_dimensions(
        orderkey=_.o_orderkey,
        custkey=_.o_custkey,
        order_date=_.o_orderdate,
        status=_.o_orderstatus,
        priority=_.o_orderpriority,
        clerk=_.o_clerk,
    ).with_measures(
        total_price=_.o_totalprice.sum(),
        order_count=_.count(),
        avg_price=_.o_totalprice.mean(),
    )

lineitem = bsl.to_semantic_table(
        conn.table("lineitem"),
        name="lineitem"
    ).with_dimensions(
        orderkey=_.l_orderkey,
        partkey=_.l_partkey,
        suppkey=_.l_suppkey,
        ship_mode=_.l_shipmode,
        return_flag=_.l_returnflag,
        line_status=_.l_linestatus,
        ship_date=_.l_shipdate,
    ).with_measures(
        quantity=_.l_quantity.sum(),
        revenue=_.l_extendedprice.sum(),
        avg_discount=_.l_discount.mean(),
        line_count=_.count(),
    )

customer = bsl.to_semantic_table(
        conn.table("customer"),
        name="customer"
    ).with_dimensions(
        custkey=_.c_custkey,
        nationkey=_.c_nationkey,
        mktsegment=_.c_mktsegment,
        phone=_.c_phone,
    ).with_measures(
        customer_count=_.count(),
        total_acctbal=_.c_acctbal.sum(),
        avg_acctbal=_.c_acctbal.mean(),
    )

supplier = bsl.to_semantic_table(
        conn.table("supplier"),
        name="supplier"
    ).with_dimensions(
        suppkey=_.s_suppkey,
        nationkey=_.s_nationkey,
        phone=_.s_phone,
    ).with_measures(
        supplier_count=_.count(),
        total_acctbal=_.s_acctbal.sum(),
        avg_acctbal=_.s_acctbal.mean(),
    )

part = bsl.to_semantic_table(
        conn.table("part"),
        name="part"
    ).with_dimensions(
        partkey=_.p_partkey,
        brand=_.p_brand,
        type=_.p_type,
        size=_.p_size,
        container=_.p_container,
        mfgr=_.p_mfgr,
    ).with_measures(
        part_count=_.count(),
        avg_retail_price=_.p_retailprice.mean(),
        total_retail_price=_.p_retailprice.sum(),
    )

partsupp = bsl.to_semantic_table(
        conn.table("partsupp"),
        name="partsupp"
    ).with_dimensions(
        partkey=_.ps_partkey,
        suppkey=_.ps_suppkey,
    ).with_measures(
        total_availqty=_.ps_availqty.sum(),
        avg_availqty=_.ps_availqty.mean(),
        total_supplycost=_.ps_supplycost.sum(),
        avg_supplycost=_.ps_supplycost.mean(),
        partsupp_count=_.count(),
    )

nation = bsl.to_semantic_table(
        conn.table("nation"),
        name="nation"
    ).with_dimensions(
        nationkey=_.n_nationkey,
        regionkey=_.n_regionkey,
        name=_.n_name,
    ).with_measures(
        nation_count=_.count(),
    )

region = bsl.to_semantic_table(
        conn.table("region"),
        name="region"
    ).with_dimensions(
        regionkey=_.r_regionkey,
        name=_.r_name,
    ).with_measures(
        region_count=_.count(),
    )


# Create Semantic Models

# Region to Nation
region_to_nation = region.join_one(
    nation,
    lambda r, n: r.r_regionkey == n.n_regionkey
)

region_to_nation_to_customer = region_to_nation.join_one(
    customer,
    lambda r, c: r.n_nationkey == c.c_nationkey
)

region_to_nation_to_customer_to_orders = region_to_nation_to_customer.join_one(
    orders,
    lambda r, o: r.c_custkey == o.o_custkey
)

# REQUIRED: the API reads this dict to discover available models
MODELS = {
    "orders":          orders,
    "lineitem":        lineitem,
    "customer":        customer,
    "supplier":        supplier,
    "part":            part,
    "partsupp":        partsupp,
    "nation":          nation,
    "region":          region,
    "region_to_nation_to_customer_to_orders":  region_to_nation_to_customer_to_orders
}