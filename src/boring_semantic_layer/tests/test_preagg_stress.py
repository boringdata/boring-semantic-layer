"""Comprehensive stress tests for per-source pre-aggregation.

Each test class uses wide tables (10+ columns, 20-50 rows) with deeply
nested multi-arm joins.  Every expected value is hand-verified against
the raw fixture data so that fan-out / chasm-trap inflation is caught
immediately.

Scenario 1 – E-Commerce Deep Chain + Chasm
Scenario 2 – HR Triple Chasm Trap
Scenario 3 – Supply Chain Mixed Cardinalities + Calculated Measures
"""

import ibis
import pandas as pd
import pytest
from ibis import _

from boring_semantic_layer import to_semantic_table


# ---------------------------------------------------------------------------
# Scenario 1: E-Commerce Deep Chain + Chasm
# ---------------------------------------------------------------------------
class TestEcommerceDeepChainAndChasm:
    """Deep chain: customers → orders → order_items → products
    Chasm:  orders → order_items (join_many) + orders → reviews (join_many)

    Tables
    ------
    customers  25 rows, 11 cols
    orders     12 rows, 12 cols
    order_items 28 rows, 11 cols
    products    5 rows, 10 cols
    reviews    24 rows, 10 cols
    """

    @pytest.fixture()
    def models(self):
        con = ibis.duckdb.connect(":memory:")

        # -- customers (25 rows, 11 columns) ---------------------------------
        customers_tbl = con.create_table(
            "customers",
            pd.DataFrame(
                {
                    "customer_id": list(range(1, 26)),
                    "first_name": [f"First{i}" for i in range(1, 26)],
                    "last_name": [f"Last{i}" for i in range(1, 26)],
                    "email": [f"c{i}@example.com" for i in range(1, 26)],
                    "phone": [f"555-{i:04d}" for i in range(1, 26)],
                    "city": (["New York", "Chicago", "LA", "Houston", "Phoenix"] * 5),
                    "state": (["NY", "IL", "CA", "TX", "AZ"] * 5),
                    "zip_code": (["10001", "60601", "90001", "77001", "85001"] * 5),
                    "tier": (
                        ["gold"] * 5
                        + ["silver"] * 10
                        + ["bronze"] * 10
                    ),
                    "signup_year": ([2020, 2021, 2022, 2023, 2024] * 5),
                    "is_active": ([True] * 20 + [False] * 5),
                }
            ),
        )

        # -- orders (12 rows, 12 columns) ------------------------------------
        # customer 1: orders 1,2   (amounts 200, 300 = 500)
        # customer 2: orders 3,4,5 (amounts 150, 250, 100 = 500)
        # customer 3: orders 6,7   (amounts 400, 200 = 600)
        # customer 4: orders 8     (amount 350)
        # customer 5: orders 9,10  (amounts 100, 150 = 250)
        # customer 6: order 11     (amount 300)
        # customer 7: order 12     (amount 300)
        # Total revenue = 500+500+600+350+250+300+300 = 2800
        orders_tbl = con.create_table(
            "orders",
            pd.DataFrame(
                {
                    "order_id": list(range(1, 13)),
                    "customer_id": [1, 1, 2, 2, 2, 3, 3, 4, 5, 5, 6, 7],
                    "order_date": pd.to_datetime(
                        [
                            "2024-01-15", "2024-02-20", "2024-01-10",
                            "2024-03-05", "2024-04-12", "2024-02-28",
                            "2024-03-15", "2024-01-22", "2024-04-01",
                            "2024-05-10", "2024-06-01", "2024-06-15",
                        ]
                    ),
                    "amount": [200, 300, 150, 250, 100, 400, 200, 350, 100, 150, 300, 300],
                    "tax": [20, 30, 15, 25, 10, 40, 20, 35, 10, 15, 30, 30],
                    "shipping_fee": [10, 10, 15, 15, 15, 20, 20, 10, 5, 5, 10, 10],
                    "discount": [0, 50, 0, 25, 0, 0, 0, 50, 0, 0, 30, 0],
                    "status": [
                        "shipped", "delivered", "delivered", "shipped",
                        "pending", "delivered", "shipped", "delivered",
                        "pending", "shipped", "delivered", "pending",
                    ],
                    "channel": [
                        "web", "app", "web", "web", "app",
                        "web", "app", "web", "app", "web",
                        "web", "app",
                    ],
                    "payment_method": [
                        "credit", "debit", "credit", "paypal", "credit",
                        "debit", "credit", "paypal", "credit", "debit",
                        "credit", "paypal",
                    ],
                    "warehouse_id": [1, 1, 2, 2, 2, 1, 1, 3, 3, 3, 2, 1],
                    "is_gift": [False, True, False, False, True, False, False, True, False, False, False, False],
                }
            ),
        )

        # -- order_items (28 rows, 11 columns) --------------------------------
        # order 1: 2 items  (unit_price 50,60  qty 1,2  → line_total 50,120)
        # order 2: 3 items  (unit_price 40,30,80  qty 1,1,2  → 40,30,160)
        # order 3: 2 items  (unit_price 70,80  qty 1,1  → 70,80)
        # order 4: 3 items  (unit_price 60,50,40  qty 1,2,1  → 60,100,40)
        # order 5: 1 item   (unit_price 100  qty 1  → 100)
        # order 6: 4 items  (unit_price 90,80,70,60  qty 1,1,1,1 → 90,80,70,60)
        # order 7: 2 items  (unit_price 100,100  qty 1,1  → 100,100)
        # order 8: 3 items  (unit_price 50,100,50  qty 2,1,2  → 100,100,100)
        # order 9: 1 item   (unit_price 100  qty 1  → 100)
        # order 10: 2 items (unit_price 60,40  qty 1,1  → 60,40)
        # order 11: 3 items (unit_price 80,70,50  qty 1,1,2  → 80,70,100)
        # order 12: 2 items (unit_price 100,100  qty 1,1  → 100,100)
        # Total items = 2+3+2+3+1+4+2+3+1+2+3+2 = 28
        orders_for_items = [1]*2 + [2]*3 + [3]*2 + [4]*3 + [5]*1 + [6]*4 + [7]*2 + [8]*3 + [9]*1 + [10]*2 + [11]*3 + [12]*2
        order_items_tbl = con.create_table(
            "order_items",
            pd.DataFrame(
                {
                    "item_id": list(range(1, 29)),
                    "order_id": orders_for_items,
                    "product_id": [
                        1, 2,         # order 1
                        3, 4, 5,      # order 2
                        1, 2,         # order 3
                        3, 4, 5,      # order 4
                        1,            # order 5
                        2, 3, 4, 5,   # order 6
                        1, 2,         # order 7
                        3, 4, 5,      # order 8
                        1,            # order 9
                        2, 3,         # order 10
                        4, 5, 1,      # order 11
                        2, 3,         # order 12
                    ],
                    "quantity": [
                        1, 2,
                        1, 1, 2,
                        1, 1,
                        1, 2, 1,
                        1,
                        1, 1, 1, 1,
                        1, 1,
                        2, 1, 2,
                        1,
                        1, 1,
                        1, 1, 2,
                        1, 1,
                    ],
                    "unit_price": [
                        50, 60,
                        40, 30, 80,
                        70, 80,
                        60, 50, 40,
                        100,
                        90, 80, 70, 60,
                        100, 100,
                        50, 100, 50,
                        100,
                        60, 40,
                        80, 70, 50,
                        100, 100,
                    ],
                    "line_total": [
                        50, 120,
                        40, 30, 160,
                        70, 80,
                        60, 100, 40,
                        100,
                        90, 80, 70, 60,
                        100, 100,
                        100, 100, 100,
                        100,
                        60, 40,
                        80, 70, 100,
                        100, 100,
                    ],
                    "sku": [f"SKU-{i:04d}" for i in range(1, 29)],
                    "is_returned": [False] * 24 + [True] * 4,
                    "discount_pct": [0.0] * 14 + [0.1] * 7 + [0.2] * 7,
                    "weight_kg": [0.5] * 10 + [1.0] * 10 + [1.5] * 8,
                    "category_code": (["A", "B", "C", "D"] * 7),
                }
            ),
        )

        # -- products (5 rows, 10 columns) ------------------------------------
        products_tbl = con.create_table(
            "products",
            pd.DataFrame(
                {
                    "product_id": [1, 2, 3, 4, 5],
                    "product_name": ["Widget", "Gadget", "Gizmo", "Doohickey", "Thingamajig"],
                    "brand": ["BrandA", "BrandB", "BrandA", "BrandC", "BrandB"],
                    "category": ["Electronics", "Electronics", "Home", "Home", "Sports"],
                    "base_price": [50, 60, 40, 30, 80],
                    "cost": [25, 30, 20, 15, 40],
                    "weight": [0.5, 1.0, 0.8, 0.3, 1.5],
                    "color": ["Red", "Blue", "Green", "Yellow", "Black"],
                    "rating": [4.5, 4.2, 3.8, 4.0, 4.7],
                    "in_stock": [True, True, False, True, True],
                }
            ),
        )

        # -- reviews (24 rows, 10 columns) ------------------------------------
        # order 1: 2 reviews   order 2: 3 reviews   order 3: 2 reviews
        # order 4: 3 reviews   order 5: 1 review    order 6: 3 reviews
        # order 7: 2 reviews   order 8: 2 reviews   order 9: 1 review
        # order 10: 2 reviews  order 11: 2 reviews  order 12: 1 review
        # Total reviews = 2+3+2+3+1+3+2+2+1+2+2+1 = 24
        reviews_order_ids = [1]*2 + [2]*3 + [3]*2 + [4]*3 + [5]*1 + [6]*3 + [7]*2 + [8]*2 + [9]*1 + [10]*2 + [11]*2 + [12]*1
        reviews_tbl = con.create_table(
            "reviews",
            pd.DataFrame(
                {
                    "review_id": list(range(1, 25)),
                    "order_id": reviews_order_ids,
                    "rating": ([5, 4, 3, 5, 4, 3, 2, 5, 4, 3, 5, 4] * 2),
                    "title": [f"Review {i}" for i in range(1, 25)],
                    "body": [f"This is review body {i}" for i in range(1, 25)],
                    "helpful_votes": [i % 5 for i in range(1, 25)],
                    "verified": [True, False] * 12,
                    "reviewer_name": [f"Reviewer{i}" for i in range(1, 25)],
                    "review_date": pd.to_datetime(["2024-03-01"] * 12 + ["2024-04-01"] * 12),
                    "sentiment": (["positive", "neutral", "negative"] * 8),
                }
            ),
        )

        # -- semantic tables ---------------------------------------------------
        customers_st = (
            to_semantic_table(customers_tbl, name="customers")
            .with_dimensions(
                customer_id=lambda t: t.customer_id,
                tier=lambda t: t.tier,
                city=lambda t: t.city,
                state=lambda t: t.state,
                is_active=lambda t: t.is_active,
            )
            .with_measures(
                customer_count=_.count(),
            )
        )

        orders_st = (
            to_semantic_table(orders_tbl, name="orders")
            .with_dimensions(
                order_id=lambda t: t.order_id,
                customer_id=lambda t: t.customer_id,
                status=lambda t: t.status,
                channel=lambda t: t.channel,
            )
            .with_measures(
                total_revenue=_.amount.sum(),
                order_count=_.count(),
                distinct_orders=_.order_id.nunique(),
                avg_order_value=_.amount.mean(),
            )
        )

        order_items_st = (
            to_semantic_table(order_items_tbl, name="order_items")
            .with_dimensions(
                item_id=lambda t: t.item_id,
                order_id=lambda t: t.order_id,
                product_id=lambda t: t.product_id,
            )
            .with_measures(
                item_count=_.count(),
                total_quantity=_.quantity.sum(),
                total_line_value=_.line_total.sum(),
            )
        )

        products_st = (
            to_semantic_table(products_tbl, name="products")
            .with_dimensions(
                product_id=lambda t: t.product_id,
                product_name=lambda t: t.product_name,
                brand=lambda t: t.brand,
                category=lambda t: t.category,
            )
            .with_measures(
                product_count=_.count(),
                avg_base_price=_.base_price.mean(),
            )
        )

        reviews_st = (
            to_semantic_table(reviews_tbl, name="reviews")
            .with_dimensions(
                review_id=lambda t: t.review_id,
                order_id=lambda t: t.order_id,
                sentiment=lambda t: t.sentiment,
            )
            .with_measures(
                review_count=_.count(),
                avg_rating=_.rating.mean(),
                total_helpful_votes=_.helpful_votes.sum(),
            )
        )

        return {
            "customers": customers_st,
            "orders": orders_st,
            "order_items": order_items_st,
            "products": products_st,
            "reviews": reviews_st,
        }

    # -- helpers ---------------------------------------------------------------

    def _deep_chain(self, m):
        """customers → orders → order_items → products"""
        return (
            m["customers"]
            .join_many(m["orders"], on="customer_id")
            .join_many(m["order_items"], on="order_id")
            .join_one(m["products"], on="product_id")
        )

    def _chasm(self, m):
        """orders with two join_many arms: order_items + reviews"""
        return (
            m["orders"]
            .join_many(m["order_items"], on="order_id")
            .join_many(m["reviews"], on="order_id")
        )

    # -- tests -----------------------------------------------------------------

    def test_global_total_revenue(self, models):
        """Global orders.total_revenue = 2800, not inflated by order_items."""
        joined = models["customers"].join_many(models["orders"], on="customer_id")
        joined = joined.join_many(models["order_items"], on="order_id")
        df = joined.aggregate("orders.total_revenue").execute()

        # 200+300+150+250+100+400+200+350+100+150+300+300 = 2800
        assert df["orders.total_revenue"].iloc[0] == 2800

    def test_customer_level_order_count(self, models):
        """Customer 1 has 2 orders, not 5 (2+3 items across orders)."""
        joined = (
            models["customers"]
            .join_many(models["orders"], on="customer_id")
            .join_many(models["order_items"], on="order_id")
        )
        df = (
            joined.group_by("customers.customer_id")
            .aggregate("orders.order_count")
            .execute()
        )

        c1 = df[df["customers.customer_id"] == 1]
        assert c1["orders.order_count"].iloc[0] == 2  # orders 1, 2

        c2 = df[df["customers.customer_id"] == 2]
        assert c2["orders.order_count"].iloc[0] == 3  # orders 3, 4, 5

        c3 = df[df["customers.customer_id"] == 3]
        assert c3["orders.order_count"].iloc[0] == 2  # orders 6, 7

    def test_chasm_items_and_reviews_independent(self, models):
        """Chasm: item_count=28 and review_count=24 (not cross-product)."""
        joined = self._chasm(models)
        df = joined.aggregate(
            "order_items.item_count",
            "reviews.review_count",
        ).execute()

        assert df["order_items.item_count"].iloc[0] == 28
        assert df["reviews.review_count"].iloc[0] == 24

    def test_revenue_by_customer_tier(self, models):
        """Revenue by tier sums correctly through the join chain.

        gold   (customers 1-5):  c1=500 + c2=500 + c3=600 + c4=350 + c5=250 = 2200
        silver (customers 6-15): c6=300 + c7=300 = 600  (others have no orders)
        bronze (customers 16-25): 0 (no orders)
        """
        joined = (
            models["customers"]
            .join_many(models["orders"], on="customer_id")
        )
        df = (
            joined.group_by("customers.tier")
            .aggregate("orders.total_revenue")
            .execute()
        )

        gold = df[df["customers.tier"] == "gold"]
        assert gold["orders.total_revenue"].iloc[0] == 2200

        silver = df[df["customers.tier"] == "silver"]
        assert silver["orders.total_revenue"].iloc[0] == 600

    def test_item_count_deep_chain(self, models):
        """item_count = 28 through the full 4-table deep chain."""
        joined = self._deep_chain(models)
        df = joined.aggregate("order_items.item_count").execute()

        assert df["order_items.item_count"].iloc[0] == 28

    def test_distinct_orders_survives_fanout(self, models):
        """distinct_orders = 12 even when fanned out through order_items."""
        joined = (
            models["customers"]
            .join_many(models["orders"], on="customer_id")
            .join_many(models["order_items"], on="order_id")
        )
        df = joined.aggregate("orders.distinct_orders").execute()

        assert df["orders.distinct_orders"].iloc[0] == 12


# ---------------------------------------------------------------------------
# Scenario 2: HR Triple Chasm Trap
# ---------------------------------------------------------------------------
class TestHRTripleChasmTrap:
    """Triple chasm from employees:
        departments → employees (join_many)
            → projects   (join_many)   [arm 1]
            → timesheets (join_many)   [arm 2]
            → training   (join_many)   [arm 3]

    Tables
    ------
    departments      5 rows, 11 cols
    employees       20 rows, 12 cols
    projects        25 rows, 11 cols
    timesheets      40 rows, 10 cols
    training_records 30 rows, 10 cols
    """

    @pytest.fixture()
    def models(self):
        con = ibis.duckdb.connect(":memory:")

        # -- departments (5 rows, 11 columns) --------------------------------
        departments_tbl = con.create_table(
            "departments",
            pd.DataFrame(
                {
                    "dept_id": [1, 2, 3, 4, 5],
                    "dept_name": ["Engineering", "Sales", "Marketing", "Finance", "HR"],
                    "dept_code": ["ENG", "SAL", "MKT", "FIN", "HRD"],
                    "floor": [3, 2, 2, 4, 1],
                    "building": ["A", "A", "B", "B", "A"],
                    "manager_name": ["Alice", "Bob", "Carol", "Dave", "Eve"],
                    "budget": [500_000, 300_000, 250_000, 400_000, 200_000],
                    "headcount_target": [10, 8, 6, 5, 4],
                    "founded_year": [2015, 2016, 2017, 2018, 2019],
                    "is_revenue_center": [True, True, True, False, False],
                    "region": ["West", "East", "West", "East", "West"],
                }
            ),
        )

        # -- employees (20 rows, 12 columns) ---------------------------------
        # Dept 1 (ENG): emp 1-6   (6 employees)  salaries: 80k,85k,90k,75k,95k,70k = 495k
        # Dept 2 (SAL): emp 7-10  (4 employees)  salaries: 60k,65k,55k,70k = 250k
        # Dept 3 (MKT): emp 11-14 (4 employees)  salaries: 58k,62k,55k,65k = 240k
        # Dept 4 (FIN): emp 15-18 (4 employees)  salaries: 72k,78k,68k,82k = 300k
        # Dept 5 (HR):  emp 19-20 (2 employees)  salaries: 52k,58k = 110k
        # Total: 20 employees, total salary = 1,395,000
        employees_tbl = con.create_table(
            "employees",
            pd.DataFrame(
                {
                    "emp_id": list(range(1, 21)),
                    "dept_id": [1]*6 + [2]*4 + [3]*4 + [4]*4 + [5]*2,
                    "first_name": [f"Emp{i}" for i in range(1, 21)],
                    "last_name": [f"Smith{i}" for i in range(1, 21)],
                    "email": [f"emp{i}@company.com" for i in range(1, 21)],
                    "salary": [
                        80_000, 85_000, 90_000, 75_000, 95_000, 70_000,  # ENG
                        60_000, 65_000, 55_000, 70_000,                  # SAL
                        58_000, 62_000, 55_000, 65_000,                  # MKT
                        72_000, 78_000, 68_000, 82_000,                  # FIN
                        52_000, 58_000,                                  # HR
                    ],
                    "hire_date": pd.to_datetime(
                        [f"20{20 + i % 5}-0{1 + i % 9}-15" for i in range(20)]
                    ),
                    "title": (["Senior", "Junior", "Mid", "Lead"] * 5),
                    "level": ([3, 1, 2, 4] * 5),
                    "is_manager": ([False] * 16 + [True] * 4),
                    "performance_score": ([4.0, 3.5, 4.5, 3.0, 5.0] * 4),
                    "remote": [True, False] * 10,
                }
            ),
        )

        # -- projects (25 rows, 11 columns) -----------------------------------
        # emp 1: 2 projects  (budgets 50k, 30k = 80k)
        # emp 2: 2 projects  (budgets 20k, 15k = 35k)
        # emp 3: 1 project   (budget 40k)
        # emp 4: 1 project   (budget 0k — internal)
        # emp 5: 0 projects
        # emp 6: 0 projects
        # emp 7: 2 projects  (budgets 10k, 25k = 35k)
        # emp 8: 1 project   (budget 15k)
        # emp 9: 2 projects  (budgets 5k, 10k = 15k)
        # emp 10: 1 project  (budget 20k)
        # emp 11: 2 projects (budgets 30k, 10k = 40k)
        # emp 12: 1 project  (budget 25k)
        # emp 13: 1 project  (budget 15k)
        # emp 14: 1 project  (budget 20k)
        # emp 15: 2 projects (budgets 35k, 15k = 50k)
        # emp 16: 1 project  (budget 40k)
        # emp 17: 1 project  (budget 20k)
        # emp 18: 1 project  (budget 30k)
        # emp 19: 1 project  (budget 10k)
        # emp 20: 1 project  (budget 15k)
        # Total: 25 projects
        # ENG projects (emp 1-4): 80k+35k+40k+0 = 155k
        projects_tbl = con.create_table(
            "projects",
            pd.DataFrame(
                {
                    "project_id": list(range(1, 26)),
                    "emp_id": [
                        1, 1, 2, 2, 3, 4,                 # ENG (6 projects)
                        7, 7, 8, 9, 9, 10,                # SAL (6 projects)
                        11, 11, 12, 13, 14,                # MKT (5 projects)
                        15, 15, 16, 17, 18,                # FIN (5 projects)
                        19, 20, 20,                        # HR  (3 projects)
                    ],
                    "project_name": [f"Project-{i}" for i in range(1, 26)],
                    "budget": [
                        50_000, 30_000, 20_000, 15_000, 40_000, 0,
                        10_000, 25_000, 15_000, 5_000, 10_000, 20_000,
                        30_000, 10_000, 25_000, 15_000, 20_000,
                        35_000, 15_000, 40_000, 20_000, 30_000,
                        10_000, 15_000, 15_000,
                    ],
                    "start_date": pd.to_datetime(["2024-01-01"] * 25),
                    "end_date": pd.to_datetime(["2024-12-31"] * 25),
                    "status": (["active", "completed", "on_hold", "active", "active"] * 5),
                    "priority": (["high", "medium", "low"] * 8 + ["high"]),
                    "client": [f"Client-{i % 5}" for i in range(1, 26)],
                    "category": (["internal", "external"] * 12 + ["internal"]),
                    "risk_level": (["low", "medium", "high"] * 8 + ["low"]),
                }
            ),
        )

        # -- timesheets (40 rows, 10 columns) ----------------------------------
        # emp 1: 3 timesheets  (hours 8,6,4 = 18)
        # emp 2: 2 timesheets  (hours 7,5 = 12)
        # emp 3: 3 timesheets  (hours 8,8,6 = 22)
        # emp 4: 2 timesheets  (hours 4,4 = 8)
        # emp 5: 2 timesheets  (hours 6,8 = 14)
        # emp 6: 3 timesheets  (hours 7,5,4 = 16)
        # emp 7: 2 timesheets  (hours 8,4 = 12)
        # emp 8: 2 timesheets  (hours 6,6 = 12)
        # emp 9: 1 timesheet   (hours 8)
        # emp 10: 2 timesheets (hours 7,5 = 12)
        # emp 11: 2 timesheets (hours 4,6 = 10)
        # emp 12: 2 timesheets (hours 8,4 = 12)
        # emp 13: 1 timesheet  (hours 6)
        # emp 14: 2 timesheets (hours 5,7 = 12)
        # emp 15: 2 timesheets (hours 8,8 = 16)
        # emp 16: 2 timesheets (hours 6,4 = 10)
        # emp 17: 1 timesheet  (hours 8)
        # emp 18: 2 timesheets (hours 7,5 = 12)
        # emp 19: 2 timesheets (hours 4,6 = 10)
        # emp 20: 1 timesheet  (hours 8)
        # Total: 40 timesheets
        # ENG total hours (emp 1-6): 18+12+22+8+14+16 = 90  [wait, let me recalc...]
        # Actually let me just set up exact ENG hours = 18+12+22+8+14+16 = 90
        # But per the plan: "ENG hours=100" — let me adjust:
        # emp 1: 3 ts (8,8,4=20), emp 2: 2 ts (7,5=12), emp 3: 3 ts (8,8,6=22)
        # emp 4: 2 ts (4,4=8), emp 5: 2 ts (8,8=16), emp 6: 3 ts (8,8,6=22)
        # ENG hours = 20+12+22+8+16+22 = 100
        timesheets_tbl = con.create_table(
            "timesheets",
            pd.DataFrame(
                {
                    "timesheet_id": list(range(1, 41)),
                    "emp_id": [
                        1, 1, 1, 2, 2, 3, 3, 3, 4, 4,             # ENG: 10 ts
                        5, 5, 6, 6, 6,                             # ENG: 5 ts
                        7, 7, 8, 8, 9, 10, 10,                     # SAL: 7 ts
                        11, 11, 12, 12, 13, 14, 14,                # MKT: 7 ts
                        15, 15, 16, 16, 17, 18, 18,                # FIN: 7 ts
                        19, 19, 20, 20,                            # HR:  4 ts
                    ],
                    "work_date": pd.to_datetime(
                        [f"2024-0{1 + i % 9}-{10 + i % 20}" for i in range(40)]
                    ),
                    "hours": [
                        8, 8, 4, 7, 5, 8, 8, 6, 4, 4,            # ENG emp 1-4
                        8, 8, 8, 8, 6,                             # ENG emp 5-6
                        8, 4, 6, 6, 8, 7, 5,                      # SAL
                        4, 6, 8, 4, 6, 5, 7,                      # MKT
                        8, 8, 6, 4, 8, 7, 5,                      # FIN
                        4, 6, 4, 4,                                # HR
                    ],
                    "task_type": (["coding", "meeting", "review", "planning"] * 10),
                    "billable": [True, False] * 20,
                    "approved": [True] * 35 + [False] * 5,
                    "rate": [50.0] * 40,
                    "notes": [f"Timesheet note {i}" for i in range(1, 41)],
                    "project_code": [f"PRJ-{i % 10}" for i in range(1, 41)],
                    "overtime": [False] * 30 + [True] * 10,
                }
            ),
        )

        # -- training_records (30 rows, 10 columns) ----------------------------
        # emp 1: 2 trainings   emp 2: 2   emp 3: 2   emp 4: 1   emp 5: 1   emp 6: 1  → ENG: 9
        # emp 7: 2   emp 8: 1   emp 9: 1   emp 10: 1                                  → SAL: 5
        # emp 11: 2  emp 12: 1  emp 13: 1  emp 14: 1                                  → MKT: 5
        # emp 15: 2  emp 16: 1  emp 17: 1  emp 18: 1                                  → FIN: 5
        # emp 19: 3  emp 20: 3                                                          → HR:  6
        # Total = 9 + 5 + 5 + 5 + 6 = 30
        training_tbl = con.create_table(
            "training_records",
            pd.DataFrame(
                {
                    "training_id": list(range(1, 31)),
                    "emp_id": [
                        1, 1, 2, 2, 3, 3, 4, 5, 6,               # ENG: 9
                        7, 7, 8, 9, 10,                            # SAL: 5
                        11, 11, 12, 13, 14,                        # MKT: 5
                        15, 15, 16, 17, 18,                        # FIN: 5
                        19, 19, 19, 20, 20, 20,                    # HR:  6
                    ],
                    "course_name": [f"Course-{i}" for i in range(1, 31)],
                    "course_category": (["technical", "soft_skills", "compliance"] * 10),
                    "hours": [8, 4, 8, 4, 8, 4, 8, 4, 8, 4, 8, 4, 8, 4, 8, 4, 8, 4, 8, 4, 8, 4, 8, 4, 8, 4, 8, 4, 8, 4],
                    "completion_date": pd.to_datetime(["2024-03-01"] * 15 + ["2024-06-01"] * 15),
                    "score": [85, 90, 78, 92, 88, 95, 80, 87, 91, 76, 83, 89, 94, 77, 86, 82, 93, 79, 84, 90, 88, 75, 91, 85, 80, 87, 92, 78, 83, 96],
                    "passed": [True] * 25 + [False] * 5,
                    "instructor": [f"Instructor-{i % 5}" for i in range(1, 31)],
                    "cost": [500, 300, 500, 300, 500, 300, 500, 300, 500, 300, 500, 300, 500, 300, 500, 300, 500, 300, 500, 300, 500, 300, 500, 300, 500, 300, 500, 300, 500, 300],
                }
            ),
        )

        # -- semantic tables ---------------------------------------------------
        departments_st = (
            to_semantic_table(departments_tbl, name="departments")
            .with_dimensions(
                dept_id=lambda t: t.dept_id,
                dept_name=lambda t: t.dept_name,
                dept_code=lambda t: t.dept_code,
                region=lambda t: t.region,
            )
            .with_measures(
                dept_count=_.count(),
                total_budget=_.budget.sum(),
            )
        )

        employees_st = (
            to_semantic_table(employees_tbl, name="employees")
            .with_dimensions(
                emp_id=lambda t: t.emp_id,
                dept_id=lambda t: t.dept_id,
                title=lambda t: t.title,
                is_manager=lambda t: t.is_manager,
            )
            .with_measures(
                headcount=_.count(),
                total_salary=_.salary.sum(),
                avg_salary=_.salary.mean(),
                distinct_employees=_.emp_id.nunique(),
            )
        )

        projects_st = (
            to_semantic_table(projects_tbl, name="projects")
            .with_dimensions(
                project_id=lambda t: t.project_id,
                emp_id=lambda t: t.emp_id,
                status=lambda t: t.status,
            )
            .with_measures(
                project_count=_.count(),
                total_project_budget=_.budget.sum(),
            )
        )

        timesheets_st = (
            to_semantic_table(timesheets_tbl, name="timesheets")
            .with_dimensions(
                timesheet_id=lambda t: t.timesheet_id,
                emp_id=lambda t: t.emp_id,
                task_type=lambda t: t.task_type,
            )
            .with_measures(
                timesheet_count=_.count(),
                total_hours=_.hours.sum(),
            )
        )

        training_st = (
            to_semantic_table(training_tbl, name="training")
            .with_dimensions(
                training_id=lambda t: t.training_id,
                emp_id=lambda t: t.emp_id,
                course_category=lambda t: t.course_category,
            )
            .with_measures(
                training_count=_.count(),
                total_training_hours=_.hours.sum(),
            )
        )

        return {
            "departments": departments_st,
            "employees": employees_st,
            "projects": projects_st,
            "timesheets": timesheets_st,
            "training": training_st,
        }

    # -- helpers ---------------------------------------------------------------

    def _triple_chasm(self, m):
        """departments → employees → {projects, timesheets, training}"""
        return (
            m["departments"]
            .join_many(m["employees"], on="dept_id")
            .join_many(m["projects"], on="emp_id")
            .join_many(m["timesheets"], on="emp_id")
            .join_many(m["training"], on="emp_id")
        )

    # -- tests -----------------------------------------------------------------

    def test_engineering_headcount(self, models):
        """ENG has 6 employees, not inflated by projects×timesheets×training."""
        joined = self._triple_chasm(models)
        df = (
            joined.group_by("departments.dept_name")
            .aggregate("employees.headcount")
            .execute()
        )

        eng = df[df["departments.dept_name"] == "Engineering"]
        assert eng["employees.headcount"].iloc[0] == 6

    def test_engineering_total_salary(self, models):
        """ENG total_salary = 495,000 (not inflated by child arms)."""
        joined = self._triple_chasm(models)
        df = (
            joined.group_by("departments.dept_name")
            .aggregate("employees.total_salary")
            .execute()
        )

        eng = df[df["departments.dept_name"] == "Engineering"]
        assert eng["employees.total_salary"].iloc[0] == 495_000

    def test_three_child_counts_independent(self, models):
        """Global: project_count=25, timesheet_count=40, training_count=30."""
        joined = self._triple_chasm(models)
        df = joined.aggregate(
            "projects.project_count",
            "timesheets.timesheet_count",
            "training.training_count",
        ).execute()

        assert df["projects.project_count"].iloc[0] == 25
        assert df["timesheets.timesheet_count"].iloc[0] == 40
        assert df["training.training_count"].iloc[0] == 30

    def test_dept_level_multi_arm(self, models):
        """ENG: project_budget=155k, total_hours=100, training_count=9."""
        joined = self._triple_chasm(models)
        df = (
            joined.group_by("departments.dept_name")
            .aggregate(
                "projects.total_project_budget",
                "timesheets.total_hours",
                "training.training_count",
            )
            .execute()
        )

        eng = df[df["departments.dept_name"] == "Engineering"]
        assert eng["projects.total_project_budget"].iloc[0] == 155_000
        assert eng["timesheets.total_hours"].iloc[0] == 100
        assert eng["training.training_count"].iloc[0] == 9

    def test_nunique_employees(self, models):
        """distinct_employees = 20 across the triple chasm."""
        joined = self._triple_chasm(models)
        df = joined.aggregate("employees.distinct_employees").execute()

        assert df["employees.distinct_employees"].iloc[0] == 20

    def test_global_totals(self, models):
        """Global salary = 1,395,000 and headcount = 20."""
        joined = self._triple_chasm(models)
        df = joined.aggregate(
            "employees.total_salary",
            "employees.headcount",
        ).execute()

        assert df["employees.total_salary"].iloc[0] == 1_395_000
        assert df["employees.headcount"].iloc[0] == 20


# ---------------------------------------------------------------------------
# Scenario 3: Supply Chain Mixed Cardinalities + Calculated Measures
# ---------------------------------------------------------------------------
class TestSupplyChainMixedCardinalities:
    """Mixed cardinalities:
        warehouses → inventory (join_many) → suppliers (join_one)
        warehouses → shipments (join_many) → quality_checks (join_many)
    Chasm: inventory + shipments from warehouses.
    Nested fan-out: shipments → quality_checks.

    Tables
    ------
    warehouses      5 rows, 11 cols
    inventory      30 rows, 11 cols
    shipments      35 rows, 12 cols
    suppliers      10 rows, 10 cols
    quality_checks 45 rows, 10 cols
    """

    @pytest.fixture()
    def models(self):
        con = ibis.duckdb.connect(":memory:")

        # -- warehouses (5 rows, 11 columns) ----------------------------------
        warehouses_tbl = con.create_table(
            "warehouses",
            pd.DataFrame(
                {
                    "warehouse_id": [1, 2, 3, 4, 5],
                    "warehouse_name": ["West Hub", "East Hub", "Central Hub", "North Hub", "South Hub"],
                    "city": ["Los Angeles", "New York", "Chicago", "Seattle", "Miami"],
                    "state": ["CA", "NY", "IL", "WA", "FL"],
                    "capacity": [10_000, 8_000, 12_000, 6_000, 9_000],
                    "manager": ["Mgr-A", "Mgr-B", "Mgr-C", "Mgr-D", "Mgr-E"],
                    "zone": ["Pacific", "Atlantic", "Central", "Pacific", "Atlantic"],
                    "open_year": [2010, 2012, 2015, 2018, 2020],
                    "sq_footage": [50_000, 40_000, 60_000, 30_000, 45_000],
                    "is_automated": [True, False, True, False, True],
                    "monthly_rent": [20_000, 18_000, 22_000, 15_000, 19_000],
                }
            ),
        )

        # -- inventory (30 rows, 11 columns) ----------------------------------
        # WH1: 8 items  (qty: 200,150,100,180,120,100,150,200 = 1200)
        # WH2: 7 items  (qty: 100,200,80,150,120,100,150 = 900)
        # WH3: 6 items  (qty: 180,120,200,100,150,150 = 900)
        # WH4: 5 items  (qty: 100,150,80,120,150 = 600)
        # WH5: 4 items  (qty: 200,200,200,200 = 800)
        # Total: 30 items, total qty = 1200+900+900+600+800 = 4400
        #
        # For computed value (unit_cost * qty_on_hand):
        # WH1: 10*200 + 15*150 + 20*100 + 12*180 + 18*120 + 10*100 + 15*150 + 20*200
        #     = 2000 + 2250 + 2000 + 2160 + 2160 + 1000 + 2250 + 4000 = 17820
        # Hmm, the plan says WH1 = 22,000. Let me design it to hit that:
        # WH1: 8 items, I'll set unit_cost to get exactly 22000
        # qty: 200,150,100,180,120,100,150,200 = 1200
        # unit_cost: 20,15,10,15,20,10,15,20
        # value: 4000+2250+1000+2700+2400+1000+2250+4000 = 19600 (not 22000)
        # Let me adjust: unit_cost 20,20,10,15,20,10,15,20
        # value: 4000+3000+1000+2700+2400+1000+2250+4000 = 20350 (still not)
        # Simpler: qty = 200,150,100,200,150,100,150,200 = 1250 ... no, plan says 1200.
        # Let me just pick values that work:
        # WH1 qty = [200,150,100,150,100,100,150,250] = 1200 ✓
        # WH1 unit_cost = [20,20,15,15,20,15,20,15]
        # value = 4000+3000+1500+2250+2000+1500+3000+3750 = 21000 ... close
        # WH1 unit_cost = [25,20,15,15,20,15,20,15]
        # value = 5000+3000+1500+2250+2000+1500+3000+3750 = 22000 ✓ !
        warehouses_for_inv = [1]*8 + [2]*7 + [3]*6 + [4]*5 + [5]*4
        inventory_tbl = con.create_table(
            "inventory",
            pd.DataFrame(
                {
                    "inventory_id": list(range(1, 31)),
                    "warehouse_id": warehouses_for_inv,
                    "supplier_id": [
                        1, 2, 3, 4, 5, 1, 2, 3,                   # WH1
                        4, 5, 6, 7, 8, 9, 10,                     # WH2
                        1, 3, 5, 7, 9, 2,                          # WH3
                        4, 6, 8, 10, 1,                            # WH4
                        3, 5, 7, 9,                                # WH5
                    ],
                    "product_sku": [f"INV-{i:04d}" for i in range(1, 31)],
                    "qty_on_hand": [
                        200, 150, 100, 150, 100, 100, 150, 250,   # WH1 = 1200
                        100, 200, 80, 150, 120, 100, 150,         # WH2 = 900
                        180, 120, 200, 100, 150, 150,              # WH3 = 900
                        100, 150, 80, 120, 150,                    # WH4 = 600
                        200, 200, 200, 200,                        # WH5 = 800
                    ],
                    "unit_cost": [
                        25, 20, 15, 15, 20, 15, 20, 15,           # WH1 → value = 22000
                        20, 15, 25, 10, 20, 15, 10,               # WH2
                        15, 20, 10, 25, 15, 20,                   # WH3
                        10, 15, 25, 20, 15,                       # WH4
                        20, 15, 10, 25,                            # WH5
                    ],
                    "reorder_point": [50] * 30,
                    "last_restock": pd.to_datetime(["2024-01-15"] * 15 + ["2024-02-15"] * 15),
                    "bin_location": [f"Bin-{chr(65 + i % 10)}{i}" for i in range(30)],
                    "category": (["raw", "finished", "wip"] * 10),
                    "is_hazardous": [False] * 25 + [True] * 5,
                }
            ),
        )

        # -- shipments (35 rows, 12 columns) -----------------------------------
        # WH1: 10 shipments (cost: 800 each = 8000)
        # WH2: 8 shipments  (cost: 750 each = 6000)
        # WH3: 7 shipments  (cost: 650 each = 4550)
        # WH4: 5 shipments  (cost: 500 each = 2500)
        # WH5: 5 shipments  (cost: 840 each = 4200)
        # Total: 35 shipments, total cost = 8000+6000+4550+2500+4200 = 25250
        warehouses_for_ship = [1]*10 + [2]*8 + [3]*7 + [4]*5 + [5]*5
        shipments_tbl = con.create_table(
            "shipments",
            pd.DataFrame(
                {
                    "shipment_id": list(range(1, 36)),
                    "warehouse_id": warehouses_for_ship,
                    "destination": [f"Dest-{i % 15}" for i in range(1, 36)],
                    "ship_date": pd.to_datetime(
                        [f"2024-0{1 + i % 9}-{10 + i % 15}" for i in range(35)]
                    ),
                    "delivery_date": pd.to_datetime(
                        [f"2024-0{1 + i % 9}-{15 + i % 10}" for i in range(35)]
                    ),
                    "shipping_cost": (
                        [800] * 10                                 # WH1 = 8000
                        + [750] * 8                                # WH2 = 6000
                        + [650] * 7                                # WH3 = 4550
                        + [500] * 5                                # WH4 = 2500
                        + [840] * 5                                # WH5 = 4200
                    ),
                    "carrier": (["FedEx", "UPS", "USPS", "DHL", "Amazon"] * 7),
                    "weight_kg": [10 + i for i in range(35)],
                    "status": (["delivered", "in_transit", "pending"] * 11 + ["delivered", "delivered"]),
                    "tracking_number": [f"TRK-{i:06d}" for i in range(1, 36)],
                    "is_expedited": [True, False, False] * 11 + [True, False],
                    "insurance_value": [100] * 35,
                }
            ),
        )

        # -- suppliers (10 rows, 10 columns) -----------------------------------
        suppliers_tbl = con.create_table(
            "suppliers",
            pd.DataFrame(
                {
                    "supplier_id": list(range(1, 11)),
                    "supplier_name": [f"Supplier-{i}" for i in range(1, 11)],
                    "contact_name": [f"Contact-{i}" for i in range(1, 11)],
                    "contact_email": [f"s{i}@supplier.com" for i in range(1, 11)],
                    "country": (["US", "China", "Germany", "Japan", "India"] * 2),
                    "lead_time_days": [7, 14, 21, 10, 30, 7, 14, 21, 10, 30],
                    "min_order_qty": [100, 200, 50, 150, 100, 200, 50, 150, 100, 200],
                    "payment_terms": (["Net30", "Net60"] * 5),
                    "rating": [4.5, 4.0, 3.8, 4.2, 3.5, 4.7, 4.1, 3.9, 4.3, 3.6],
                    "is_preferred": [True, False, True, False, True, True, False, True, False, True],
                }
            ),
        )

        # -- quality_checks (45 rows, 10 columns) -----------------------------
        # Shipment 1-10 (WH1):  ship 1→2qc, 2→1, 3→2, 4→1, 5→2, 6→1, 7→2, 8→1, 9→2, 10→1 = 15 qcs
        # Shipment 11-18 (WH2): ship 11→2, 12→1, 13→1, 14→2, 15→1, 16→1, 17→1, 18→1 = 10 qcs
        # Shipment 19-25 (WH3): ship 19→2, 20→1, 21→1, 22→1, 23→1, 24→1, 25→1 = 8 qcs
        # Shipment 26-30 (WH4): ship 26→2, 27→1, 28→1, 29→1, 30→1 = 6 qcs
        # Shipment 31-35 (WH5): ship 31→2, 32→1, 33→1, 34→1, 35→1 = 6 qcs
        # Total = 15 + 10 + 8 + 6 + 6 = 45 ✓
        #
        # WH1 defect_count: we need to track for defect_rate calc
        # defect_found: let's say 3 out of 15 for WH1 → rate = 3/15 = 0.2
        qc_shipment_ids = [
            1, 1, 2, 3, 3, 4, 5, 5, 6, 7, 7, 8, 9, 9, 10,       # WH1: 15
            11, 11, 12, 13, 14, 14, 15, 16, 17, 18,               # WH2: 10
            19, 19, 20, 21, 22, 23, 24, 25,                       # WH3: 8
            26, 26, 27, 28, 29, 30,                                # WH4: 6
            31, 31, 32, 33, 34, 35,                                # WH5: 6
        ]
        quality_checks_tbl = con.create_table(
            "quality_checks",
            pd.DataFrame(
                {
                    "qc_id": list(range(1, 46)),
                    "shipment_id": qc_shipment_ids,
                    "inspector": [f"Inspector-{i % 5}" for i in range(1, 46)],
                    "check_date": pd.to_datetime(
                        [f"2024-0{1 + i % 9}-{10 + i % 15}" for i in range(45)]
                    ),
                    "check_type": (["visual", "weight", "dimension"] * 15),
                    "result": (["pass", "pass", "fail"] * 15),
                    "defect_found": (
                        [True, False, False] * 15
                    ),
                    "severity": (["low", "medium", "high"] * 15),
                    "notes": [f"QC note {i}" for i in range(1, 46)],
                    "duration_min": [15, 20, 10, 25, 30] * 9,
                }
            ),
        )

        # -- semantic tables ---------------------------------------------------
        warehouses_st = (
            to_semantic_table(warehouses_tbl, name="warehouses")
            .with_dimensions(
                warehouse_id=lambda t: t.warehouse_id,
                warehouse_name=lambda t: t.warehouse_name,
                zone=lambda t: t.zone,
                city=lambda t: t.city,
            )
            .with_measures(
                warehouse_count=_.count(),
                total_capacity=_.capacity.sum(),
            )
        )

        inventory_st = (
            to_semantic_table(inventory_tbl, name="inventory")
            .with_dimensions(
                inventory_id=lambda t: t.inventory_id,
                warehouse_id=lambda t: t.warehouse_id,
                supplier_id=lambda t: t.supplier_id,
                category=lambda t: t.category,
            )
            .with_measures(
                inventory_item_count=_.count(),
                total_qty_on_hand=_.qty_on_hand.sum(),
                inventory_value=lambda t: (t.unit_cost * t.qty_on_hand).sum(),
            )
        )

        shipments_st = (
            to_semantic_table(shipments_tbl, name="shipments")
            .with_dimensions(
                shipment_id=lambda t: t.shipment_id,
                warehouse_id=lambda t: t.warehouse_id,
                carrier=lambda t: t.carrier,
            )
            .with_measures(
                shipment_count=_.count(),
                total_shipping_cost=_.shipping_cost.sum(),
            )
        )

        suppliers_st = (
            to_semantic_table(suppliers_tbl, name="suppliers")
            .with_dimensions(
                supplier_id=lambda t: t.supplier_id,
                supplier_name=lambda t: t.supplier_name,
                country=lambda t: t.country,
            )
            .with_measures(
                supplier_count=_.count(),
            )
        )

        quality_checks_st = (
            to_semantic_table(quality_checks_tbl, name="quality_checks")
            .with_dimensions(
                qc_id=lambda t: t.qc_id,
                shipment_id=lambda t: t.shipment_id,
                check_type=lambda t: t.check_type,
            )
            .with_measures(
                qc_count=_.count(),
                defect_count=_.defect_found.sum(),
            )
        )

        return {
            "warehouses": warehouses_st,
            "inventory": inventory_st,
            "shipments": shipments_st,
            "suppliers": suppliers_st,
            "quality_checks": quality_checks_st,
        }

    # -- helpers ---------------------------------------------------------------

    def _chasm_with_nested(self, m):
        """warehouses → inventory + shipments → quality_checks"""
        return (
            m["warehouses"]
            .join_many(m["inventory"], on="warehouse_id")
            .join_many(m["shipments"], on="warehouse_id")
            .join_many(m["quality_checks"], on="shipment_id")
        )

    def _inventory_chain(self, m):
        """warehouses → inventory → suppliers (join_one)"""
        return (
            m["warehouses"]
            .join_many(m["inventory"], on="warehouse_id")
            .join_one(m["suppliers"], on="supplier_id")
        )

    # -- tests -----------------------------------------------------------------

    def test_warehouse_inventory_and_shipping_chasm(self, models):
        """WH1: qty_on_hand=1200, shipping_cost=8000 (not inflated)."""
        joined = (
            models["warehouses"]
            .join_many(models["inventory"], on="warehouse_id")
            .join_many(models["shipments"], on="warehouse_id")
        )
        df = (
            joined.group_by("warehouses.warehouse_name")
            .aggregate(
                "inventory.total_qty_on_hand",
                "shipments.total_shipping_cost",
            )
            .execute()
        )

        wh1 = df[df["warehouses.warehouse_name"] == "West Hub"]
        assert wh1["inventory.total_qty_on_hand"].iloc[0] == 1200
        assert wh1["shipments.total_shipping_cost"].iloc[0] == 8000

    def test_global_nested_fanout(self, models):
        """Global: qty=4400, shipping=25250, qc_count=45."""
        joined = self._chasm_with_nested(models)
        df = joined.aggregate(
            "inventory.total_qty_on_hand",
            "shipments.total_shipping_cost",
            "quality_checks.qc_count",
        ).execute()

        assert df["inventory.total_qty_on_hand"].iloc[0] == 4400
        assert df["shipments.total_shipping_cost"].iloc[0] == 25250
        assert df["quality_checks.qc_count"].iloc[0] == 45

    def test_computed_inventory_value(self, models):
        """Expression measure: WH1 inventory_value = 22,000."""
        joined = (
            models["warehouses"]
            .join_many(models["inventory"], on="warehouse_id")
        )
        df = (
            joined.group_by("warehouses.warehouse_name")
            .aggregate("inventory.inventory_value")
            .execute()
        )

        wh1 = df[df["warehouses.warehouse_name"] == "West Hub"]
        assert wh1["inventory.inventory_value"].iloc[0] == 22_000

    def test_mutate_calculated_measure(self, models):
        """Post-agg calculated: WH1 cost_per_shipment=800."""
        joined = (
            models["warehouses"]
            .join_many(models["shipments"], on="warehouse_id")
        )
        df = (
            joined.group_by("warehouses.warehouse_name")
            .aggregate(
                "shipments.total_shipping_cost",
                "shipments.shipment_count",
            )
            .mutate(
                cost_per_shipment=lambda t: t["shipments.total_shipping_cost"]
                / t["shipments.shipment_count"]
            )
            .execute()
        )

        wh1 = df[df["warehouses.warehouse_name"] == "West Hub"]
        assert wh1["cost_per_shipment"].iloc[0] == 800

    def test_warehouse_count_not_inflated(self, models):
        """warehouse_count = 5 despite triple fan-out."""
        joined = self._chasm_with_nested(models)
        df = joined.aggregate("warehouses.warehouse_count").execute()

        assert df["warehouses.warehouse_count"].iloc[0] == 5

    def test_shipment_count_through_nested_qc(self, models):
        """shipment_count = 35, not inflated by quality_checks."""
        joined = (
            models["warehouses"]
            .join_many(models["shipments"], on="warehouse_id")
            .join_many(models["quality_checks"], on="shipment_id")
        )
        df = joined.aggregate("shipments.shipment_count").execute()

        assert df["shipments.shipment_count"].iloc[0] == 35
