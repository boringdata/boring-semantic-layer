"""Tests documenting classic BI traps (fan-out, chasm, double-counting,
convergent path) and safe aggregation patterns through the BSL semantic API.

Each test class sets up in-memory DuckDB tables that trigger a specific trap,
then asserts the *actual* BSL behavior — both the problematic (inflated) results
and the workarounds that produce correct numbers.
"""

import ibis
import pandas as pd
import pytest
from ibis import _

from boring_semantic_layer import to_semantic_table


# ---------------------------------------------------------------------------
# TestFanOutTrap
# ---------------------------------------------------------------------------
class TestFanOutTrap:
    """Fan-out: parent-level measures are inflated when joined to a child table
    via ``join_many``.

    Fixture
    -------
    orders (3 rows, total_amount sums to 300)
        order_id  customer_id  amount
        1         10           100
        2         10           120
        3         20            80

    line_items (6 rows — 2 per order)
        item_id  order_id  qty
        1        1         1
        2        1         2
        3        2         1
        4        2         3
        5        3         1
        6        3         1
    """

    @pytest.fixture()
    def models(self):
        con = ibis.duckdb.connect(":memory:")

        orders_tbl = con.create_table(
            "orders",
            pd.DataFrame(
                {
                    "order_id": [1, 2, 3],
                    "customer_id": [10, 10, 20],
                    "amount": [100, 120, 80],
                }
            ),
        )
        line_items_tbl = con.create_table(
            "line_items",
            pd.DataFrame(
                {
                    "item_id": [1, 2, 3, 4, 5, 6],
                    "order_id": [1, 1, 2, 2, 3, 3],
                    "qty": [1, 2, 1, 3, 1, 1],
                }
            ),
        )

        orders_st = (
            to_semantic_table(orders_tbl, name="orders")
            .with_dimensions(
                order_id=lambda t: t.order_id,
                customer_id=lambda t: t.customer_id,
            )
            .with_measures(
                total_amount=_.amount.sum(),
                order_count=_.count(),
                distinct_orders=_.order_id.nunique(),
            )
        )
        line_items_st = (
            to_semantic_table(line_items_tbl, name="line_items")
            .with_dimensions(
                item_id=lambda t: t.item_id,
                order_id=lambda t: t.order_id,
            )
            .with_measures(
                item_count=_.count(),
                total_qty=_.qty.sum(),
            )
        )
        return {"orders": orders_st, "line_items": line_items_st}

    # -- tests ---------------------------------------------------------------

    def test_fanout_naive_join_inflates_parent_measure(self, models):
        """Pre-aggregation prevents fan-out: parent measures are aggregated
        at the source table before joining, so SUM(amount) = 300 (correct).
        """
        joined = models["orders"].join_many(models["line_items"], on="order_id")
        df = joined.aggregate("orders.total_amount").execute()

        correct_total = 300  # 100 + 120 + 80
        assert df["orders.total_amount"].iloc[0] == correct_total

    def test_fanout_leaf_measure_unaffected(self, models):
        """Leaf-level measures (line_items.item_count) are correct on a join."""
        joined = models["orders"].join_many(models["line_items"], on="order_id")
        df = joined.aggregate("line_items.item_count").execute()

        assert df["line_items.item_count"].iloc[0] == 6  # correct

    def test_fanout_avoided_by_aggregating_at_source_level(self, models):
        """Aggregating at the source table (no join) gives the correct value."""
        df = models["orders"].aggregate("total_amount").execute()

        assert df["total_amount"].iloc[0] == 300  # correct

    def test_fanout_count_vs_nunique(self, models):
        """Pre-aggregation makes both count() and nunique() correct."""
        joined = models["orders"].join_many(models["line_items"], on="order_id")
        df = joined.aggregate(
            "orders.order_count",
            "orders.distinct_orders",
        ).execute()

        # Both are correct thanks to per-source pre-aggregation
        assert df["orders.order_count"].iloc[0] == 3  # correct
        assert df["orders.distinct_orders"].iloc[0] == 3  # correct


# ---------------------------------------------------------------------------
# TestChasmTrap
# ---------------------------------------------------------------------------
class TestChasmTrap:
    """Chasm trap: two ``join_many`` arms from the same parent create a
    cross-product, inflating both arms.

    Fixture
    -------
    customers (2 rows)
        customer_id  name
        1            Alice
        2            Bob

    orders (3 rows — Alice=2, Bob=1)
        order_id  customer_id  amount
        1         1            100
        2         1            200
        3         2            150

    tickets (3 rows — Alice=1, Bob=2)
        ticket_id  customer_id  priority
        1          1            high
        2          2            low
        3          2            medium
    """

    @pytest.fixture()
    def models(self):
        con = ibis.duckdb.connect(":memory:")

        customers_tbl = con.create_table(
            "customers",
            pd.DataFrame(
                {
                    "customer_id": [1, 2],
                    "name": ["Alice", "Bob"],
                }
            ),
        )
        orders_tbl = con.create_table(
            "orders",
            pd.DataFrame(
                {
                    "order_id": [1, 2, 3],
                    "customer_id": [1, 1, 2],
                    "amount": [100, 200, 150],
                }
            ),
        )
        tickets_tbl = con.create_table(
            "tickets",
            pd.DataFrame(
                {
                    "ticket_id": [1, 2, 3],
                    "customer_id": [1, 2, 2],
                    "priority": ["high", "low", "medium"],
                }
            ),
        )

        customers_st = (
            to_semantic_table(customers_tbl, name="customers")
            .with_dimensions(
                customer_id=lambda t: t.customer_id,
                name=lambda t: t.name,
            )
            .with_measures(customer_count=_.count())
        )
        orders_st = (
            to_semantic_table(orders_tbl, name="orders")
            .with_dimensions(
                order_id=lambda t: t.order_id,
                customer_id=lambda t: t.customer_id,
            )
            .with_measures(
                order_count=_.count(),
                total_amount=_.amount.sum(),
            )
        )
        tickets_st = (
            to_semantic_table(tickets_tbl, name="tickets")
            .with_dimensions(
                ticket_id=lambda t: t.ticket_id,
                customer_id=lambda t: t.customer_id,
            )
            .with_measures(ticket_count=_.count())
        )
        return {
            "customers": customers_st,
            "orders": orders_st,
            "tickets": tickets_st,
        }

    # -- tests ---------------------------------------------------------------

    def test_chasm_cross_product_prevented_by_preagg(self, models):
        """Per-source pre-aggregation prevents the chasm trap.

        Each ``join_many`` arm is aggregated independently on its own raw
        table, so there is no cross-product and no column collision.
        """
        joined = (
            models["customers"]
            .join_many(models["orders"], on="customer_id")
            .join_many(models["tickets"], on="customer_id")
        )
        df = joined.aggregate(
            "orders.order_count",
            "tickets.ticket_count",
        ).execute()

        assert df["orders.order_count"].iloc[0] == 3
        assert df["tickets.ticket_count"].iloc[0] == 3

    def test_chasm_single_arm_correct(self, models):
        """Each join arm individually produces the correct result."""
        # orders arm only
        joined_orders = models["customers"].join_many(
            models["orders"], on="customer_id"
        )
        df_o = joined_orders.aggregate("orders.order_count").execute()
        assert df_o["orders.order_count"].iloc[0] == 3  # correct

        # tickets arm only
        joined_tickets = models["customers"].join_many(
            models["tickets"], on="customer_id"
        )
        df_t = joined_tickets.aggregate("tickets.ticket_count").execute()
        assert df_t["tickets.ticket_count"].iloc[0] == 3  # correct

    def test_chasm_workaround_separate_queries(self, models):
        """Aggregate each arm separately, then combine — correct values."""
        df_orders = (
            models["orders"]
            .group_by("customer_id")
            .aggregate("order_count")
            .execute()
        )
        df_tickets = (
            models["tickets"]
            .group_by("customer_id")
            .aggregate("ticket_count")
            .execute()
        )

        merged = df_orders.merge(df_tickets, on="customer_id", how="outer")
        assert merged["order_count"].sum() == 3
        assert merged["ticket_count"].sum() == 3


# ---------------------------------------------------------------------------
# TestDoubleCounting
# ---------------------------------------------------------------------------
class TestDoubleCounting:
    """Double-counting (multi-level fan-out): intermediate-level measures are
    multiplied by leaf cardinality.

    Fixture
    -------
    departments (2 rows)
        dept_id  dept_name
        1        Engineering
        2        Sales

    employees (4 rows — Eng=2, Sales=2)
        emp_id  dept_id  salary
        1       1        80000
        2       1        90000
        3       2        60000
        4       2        70000

    tasks (7 rows — various per employee)
        task_id  emp_id  hours
        1        1       8
        2        1       4
        3        2       6
        4        3       3
        5        3       5
        6        4       7
        7        4       2
    """

    @pytest.fixture()
    def models(self):
        con = ibis.duckdb.connect(":memory:")

        depts_tbl = con.create_table(
            "departments",
            pd.DataFrame(
                {
                    "dept_id": [1, 2],
                    "dept_name": ["Engineering", "Sales"],
                }
            ),
        )
        emps_tbl = con.create_table(
            "employees",
            pd.DataFrame(
                {
                    "emp_id": [1, 2, 3, 4],
                    "dept_id": [1, 1, 2, 2],
                    "salary": [80_000, 90_000, 60_000, 70_000],
                }
            ),
        )
        tasks_tbl = con.create_table(
            "tasks",
            pd.DataFrame(
                {
                    "task_id": [1, 2, 3, 4, 5, 6, 7],
                    "emp_id": [1, 1, 2, 3, 3, 4, 4],
                    "hours": [8, 4, 6, 3, 5, 7, 2],
                }
            ),
        )

        depts_st = (
            to_semantic_table(depts_tbl, name="departments")
            .with_dimensions(
                dept_id=lambda t: t.dept_id,
                dept_name=lambda t: t.dept_name,
            )
            .with_measures(dept_count=_.count())
        )
        emps_st = (
            to_semantic_table(emps_tbl, name="employees")
            .with_dimensions(
                emp_id=lambda t: t.emp_id,
                dept_id=lambda t: t.dept_id,
            )
            .with_measures(
                emp_count=_.count(),
                total_salary=_.salary.sum(),
            )
        )
        tasks_st = (
            to_semantic_table(tasks_tbl, name="tasks")
            .with_dimensions(
                task_id=lambda t: t.task_id,
                emp_id=lambda t: t.emp_id,
            )
            .with_measures(
                task_count=_.count(),
                total_hours=_.hours.sum(),
            )
        )
        return {
            "departments": depts_st,
            "employees": emps_st,
            "tasks": tasks_st,
        }

    def _build_chain(self, m):
        return (
            m["departments"]
            .join_many(m["employees"], on="dept_id")
            .join_many(m["tasks"], on="emp_id")
        )

    # -- tests ---------------------------------------------------------------

    def test_double_counting_intermediate_measure(self, models):
        """Pre-aggregation prevents double-counting of intermediate measures.

        employees.total_salary is pre-aggregated on the raw employees table
        before joining with tasks, so the correct sum (300k) is returned.
        """
        joined = self._build_chain(models)
        df = joined.aggregate("employees.total_salary").execute()

        correct = 300_000
        assert df["employees.total_salary"].iloc[0] == correct

    def test_double_counting_leaf_measure_correct(self, models):
        """Leaf-level task_count is unaffected by the join chain."""
        joined = self._build_chain(models)
        df = joined.aggregate("tasks.task_count").execute()

        assert df["tasks.task_count"].iloc[0] == 7  # correct

    def test_double_counting_workaround_aggregate_then_join(self, models):
        """Pre-aggregate employees, then combine with departments — correct."""
        df_salary = (
            models["employees"]
            .group_by("dept_id")
            .aggregate("total_salary")
            .execute()
        )

        assert df_salary["total_salary"].sum() == 300_000  # correct


# ---------------------------------------------------------------------------
# TestConvergentPathTrap
# ---------------------------------------------------------------------------
class TestConvergentPathTrap:
    """Convergent path (diamond join): two join paths lead to the same
    logical table (airports), potentially creating duplicate rows.

    Fixture
    -------
    airports (3 rows)
        airport_id  city
        1           New York
        2           Chicago
        3           Los Angeles

    flights (4 rows)
        flight_id  origin_id  dest_id  passengers
        1          1          2        150
        2          2          3        120
        3          3          1        200
        4          1          3        180
    """

    @pytest.fixture()
    def models(self):
        con = ibis.duckdb.connect(":memory:")

        airports_tbl = con.create_table(
            "airports",
            pd.DataFrame(
                {
                    "airport_id": [1, 2, 3],
                    "city": ["New York", "Chicago", "Los Angeles"],
                }
            ),
        )
        # We need two separate ibis references to the airports table so
        # BSL treats them as distinct semantic tables with different names.
        origin_airports_tbl = con.create_table(
            "origin_airports",
            pd.DataFrame(
                {
                    "airport_id": [1, 2, 3],
                    "city": ["New York", "Chicago", "Los Angeles"],
                }
            ),
        )
        dest_airports_tbl = con.create_table(
            "dest_airports",
            pd.DataFrame(
                {
                    "airport_id": [1, 2, 3],
                    "city": ["New York", "Chicago", "Los Angeles"],
                }
            ),
        )

        flights_tbl = con.create_table(
            "flights",
            pd.DataFrame(
                {
                    "flight_id": [1, 2, 3, 4],
                    "origin_id": [1, 2, 3, 1],
                    "dest_id": [2, 3, 1, 3],
                    "passengers": [150, 120, 200, 180],
                }
            ),
        )

        flights_st = (
            to_semantic_table(flights_tbl, name="flights")
            .with_dimensions(
                flight_id=lambda t: t.flight_id,
                origin_id=lambda t: t.origin_id,
                dest_id=lambda t: t.dest_id,
            )
            .with_measures(
                flight_count=_.count(),
                total_passengers=_.passengers.sum(),
            )
        )
        origin_st = (
            to_semantic_table(origin_airports_tbl, name="origins")
            .with_dimensions(
                airport_id=lambda t: t.airport_id,
                city=lambda t: t.city,
            )
        )
        dest_st = (
            to_semantic_table(dest_airports_tbl, name="destinations")
            .with_dimensions(
                airport_id=lambda t: t.airport_id,
                city=lambda t: t.city,
            )
        )
        return {
            "flights": flights_st,
            "origins": origin_st,
            "destinations": dest_st,
        }

    # -- tests ---------------------------------------------------------------

    def test_convergent_path_duplicate_rows(self, models):
        """Joining flights to both origin and destination airport tables.

        Since each join is ``join_one`` (each flight has exactly one origin
        and one destination), the row count should remain 4 — no fan-out.
        This verifies that BSL handles the diamond pattern correctly when
        the two paths are modelled as separate semantic tables.
        """
        joined = (
            models["flights"]
            .join_one(
                models["origins"],
                on=lambda l, r: l.origin_id == r.airport_id,
            )
            .join_one(
                models["destinations"],
                on=lambda l, r: l.dest_id == r.airport_id,
            )
        )
        df = joined.aggregate("flights.flight_count").execute()

        # join_one should not create extra rows — count stays at 4
        assert df["flights.flight_count"].iloc[0] == 4

    def test_convergent_path_measures_correct(self, models):
        """Measures from the fact table remain correct across the diamond join."""
        joined = (
            models["flights"]
            .join_one(
                models["origins"],
                on=lambda l, r: l.origin_id == r.airport_id,
            )
            .join_one(
                models["destinations"],
                on=lambda l, r: l.dest_id == r.airport_id,
            )
        )
        df = (
            joined.group_by("origins.city")
            .aggregate("flights.total_passengers")
            .execute()
        )

        # New York as origin: flight 1 (150) + flight 4 (180) = 330
        ny = df[df["origins.city"] == "New York"]
        assert ny["flights.total_passengers"].iloc[0] == 330

        # Total passengers unchanged: 150 + 120 + 200 + 180 = 650
        assert df["flights.total_passengers"].sum() == 650


# ---------------------------------------------------------------------------
# TestSafeAggregationPatterns
# ---------------------------------------------------------------------------
class TestSafeAggregationPatterns:
    """Demonstrate BSL patterns that avoid all BI traps."""

    @pytest.fixture()
    def models(self):
        """Reuse the fan-out fixture: orders → line_items."""
        con = ibis.duckdb.connect(":memory:")

        orders_tbl = con.create_table(
            "orders",
            pd.DataFrame(
                {
                    "order_id": [1, 2, 3],
                    "customer_id": [10, 10, 20],
                    "amount": [100, 120, 80],
                }
            ),
        )
        line_items_tbl = con.create_table(
            "line_items",
            pd.DataFrame(
                {
                    "item_id": [1, 2, 3, 4, 5, 6],
                    "order_id": [1, 1, 2, 2, 3, 3],
                    "qty": [1, 2, 1, 3, 1, 1],
                }
            ),
        )

        orders_st = (
            to_semantic_table(orders_tbl, name="orders")
            .with_dimensions(
                order_id=lambda t: t.order_id,
                customer_id=lambda t: t.customer_id,
            )
            .with_measures(
                total_amount=_.amount.sum(),
                order_count=_.count(),
                distinct_orders=_.order_id.nunique(),
            )
        )
        line_items_st = (
            to_semantic_table(line_items_tbl, name="line_items")
            .with_dimensions(
                item_id=lambda t: t.item_id,
                order_id=lambda t: t.order_id,
            )
            .with_measures(
                item_count=_.count(),
                total_qty=_.qty.sum(),
            )
        )
        return {"orders": orders_st, "line_items": line_items_st}

    # -- tests ---------------------------------------------------------------

    def test_safe_pattern_measures_at_leaf_level(self, models):
        """Leaf-level measures aggregate correctly across any join tree."""
        joined = models["orders"].join_many(models["line_items"], on="order_id")
        df = joined.aggregate(
            "line_items.item_count",
            "line_items.total_qty",
        ).execute()

        assert df["line_items.item_count"].iloc[0] == 6
        assert df["line_items.total_qty"].iloc[0] == 9  # 1+2+1+3+1+1

    def test_safe_pattern_nunique_across_joins(self, models):
        """nunique() is safe across fan-out joins — distinct counts are immune."""
        joined = models["orders"].join_many(models["line_items"], on="order_id")
        df = joined.aggregate("orders.distinct_orders").execute()

        # Even though rows are fanned out, nunique gives the correct answer
        assert df["orders.distinct_orders"].iloc[0] == 3

    def test_safe_pattern_pre_aggregate_then_join(self, models):
        """Aggregate first at the source table, avoid the join entirely."""
        df_orders = models["orders"].aggregate("total_amount").execute()
        df_items = models["line_items"].aggregate("item_count").execute()

        assert df_orders["total_amount"].iloc[0] == 300
        assert df_items["item_count"].iloc[0] == 6


# ---------------------------------------------------------------------------
# TestNonAdditiveMeasures
# ---------------------------------------------------------------------------
class TestNonAdditiveMeasures:
    """Non-additive measures (AVG/MEAN) must be decomposed into SUM + COUNT
    before pre-aggregation, then recomputed as SUM/COUNT after re-aggregation.

    Fixture
    -------
    customers (3 rows)
        customer_id  region
        1            West
        2            West
        3            East

    orders (6 rows)
        order_id  customer_id  amount
        1         1            100
        2         1            200
        3         2            300
        4         2            400
        5         3            500
        6         3            600
    """

    @pytest.fixture()
    def models(self):
        con = ibis.duckdb.connect(":memory:")

        customers_tbl = con.create_table(
            "customers",
            pd.DataFrame(
                {
                    "customer_id": [1, 2, 3],
                    "region": ["West", "West", "East"],
                }
            ),
        )
        orders_tbl = con.create_table(
            "orders",
            pd.DataFrame(
                {
                    "order_id": [1, 2, 3, 4, 5, 6],
                    "customer_id": [1, 1, 2, 2, 3, 3],
                    "amount": [100, 200, 300, 400, 500, 600],
                }
            ),
        )

        customers_st = (
            to_semantic_table(customers_tbl, name="customers")
            .with_dimensions(
                customer_id=lambda t: t.customer_id,
                region=lambda t: t.region,
            )
            .with_measures(customer_count=_.count())
        )
        orders_st = (
            to_semantic_table(orders_tbl, name="orders")
            .with_dimensions(
                order_id=lambda t: t.order_id,
                customer_id=lambda t: t.customer_id,
            )
            .with_measures(
                order_count=_.count(),
                total_amount=_.amount.sum(),
                avg_amount=_.amount.mean(),
            )
        )
        return {"customers": customers_st, "orders": orders_st}

    def test_mean_measure_cross_table_groupby(self, models):
        """GROUP BY customers.region, aggregate orders.avg_amount should
        compute a flat average per region, not sum of per-group averages.

        West customers (1, 2) have orders: 100, 200, 300, 400 → avg = 250
        East customer (3) has orders: 500, 600 → avg = 550
        """
        joined = models["customers"].join_many(
            models["orders"], on="customer_id"
        )
        df = (
            joined.group_by("customers.region")
            .aggregate("orders.avg_amount")
            .execute()
        )

        west = df[df["customers.region"] == "West"]
        east = df[df["customers.region"] == "East"]
        assert west["orders.avg_amount"].iloc[0] == pytest.approx(250.0)
        assert east["orders.avg_amount"].iloc[0] == pytest.approx(550.0)

    def test_mean_measure_scalar(self, models):
        """Scalar aggregate orders.avg_amount across the join should still
        produce the correct flat average: (100+200+300+400+500+600)/6 = 350.
        """
        joined = models["customers"].join_many(
            models["orders"], on="customer_id"
        )
        df = joined.aggregate("orders.avg_amount").execute()
        assert df["orders.avg_amount"].iloc[0] == pytest.approx(350.0)


# ---------------------------------------------------------------------------
# TestFilterPushdown
# ---------------------------------------------------------------------------
class TestFilterPushdown:
    """Cross-table filter pushdown: filters between join and aggregate
    should be pushed down to per-table pre-aggregation instead of
    disabling pre-aggregation entirely.

    Fixture
    -------
    customers (3 rows)
        customer_id  region   ltv
        1            West     1000
        2            East     2000
        3            West     1500

    orders (5 rows)
        order_id  customer_id  amount
        1         1            50
        2         1            150
        3         2            200
        4         3            300
        5         3            400
    """

    @pytest.fixture()
    def models(self):
        con = ibis.duckdb.connect(":memory:")

        customers_tbl = con.create_table(
            "customers",
            pd.DataFrame(
                {
                    "customer_id": [1, 2, 3],
                    "region": ["West", "East", "West"],
                    "ltv": [1000, 2000, 1500],
                }
            ),
        )
        orders_tbl = con.create_table(
            "orders",
            pd.DataFrame(
                {
                    "order_id": [1, 2, 3, 4, 5],
                    "customer_id": [1, 1, 2, 3, 3],
                    "amount": [50, 150, 200, 300, 400],
                }
            ),
        )

        customers_st = (
            to_semantic_table(customers_tbl, name="customers")
            .with_dimensions(
                customer_id=lambda t: t.customer_id,
                region=lambda t: t.region,
            )
            .with_measures(
                customer_count=_.count(),
                total_ltv=_.ltv.sum(),
            )
        )
        orders_st = (
            to_semantic_table(orders_tbl, name="orders")
            .with_dimensions(
                order_id=lambda t: t.order_id,
                customer_id=lambda t: t.customer_id,
            )
            .with_measures(
                order_count=_.count(),
                total_amount=_.amount.sum(),
            )
        )
        return {"customers": customers_st, "orders": orders_st}

    def test_filter_pushdown_many_side(self, models):
        """Filter on a many-side column (amount > 100) should push down
        to the orders table and restrict the aggregate.

        Orders with amount > 100: #2(150), #3(200), #4(300), #5(400) → 4 orders
        """
        joined = models["customers"].join_many(
            models["orders"], on="customer_id"
        )
        df = (
            joined.filter(lambda t: t.amount > 100)
            .aggregate("orders.order_count")
            .execute()
        )
        assert df["orders.order_count"].iloc[0] == 4

    def test_filter_pushdown_restricts_one_side(self, models):
        """Filter on a many-side column (amount > 100) should restrict the
        dim bridge so that only matching customers contribute to one-side measures.

        Orders with amount > 100 come from customers 1,2,3.
        Customer LTVs: 1→1000, 2→2000, 3→1500. Total = 4500.
        """
        joined = models["customers"].join_many(
            models["orders"], on="customer_id"
        )
        df = (
            joined.filter(lambda t: t.amount > 100)
            .aggregate("customers.total_ltv")
            .execute()
        )
        assert df["customers.total_ltv"].iloc[0] == 4500

    def test_filter_pushdown_one_side_dim(self, models):
        """Filter on a one-side dimension (region == 'West') should
        correctly restrict via the dim bridge.

        West customers: 1 and 3.
        Customer 1 orders: #1(50), #2(150) → 2
        Customer 3 orders: #4(300), #5(400) → 2
        Total: 4 orders
        """
        joined = models["customers"].join_many(
            models["orders"], on="customer_id"
        )
        df = (
            joined.filter(lambda t: t.region == "West")
            .aggregate("orders.order_count")
            .execute()
        )
        assert df["orders.order_count"].iloc[0] == 4

    def test_filter_with_chasm(self, models):
        """Filter with two join_many arms — verify both arms correct.

        Use orders and a second many-arm (reuse orders as 'orders2' with
        different measures to create a chasm scenario).
        """
        con = ibis.duckdb.connect(":memory:")

        customers_tbl = con.create_table(
            "customers",
            pd.DataFrame(
                {
                    "customer_id": [1, 2],
                    "region": ["West", "East"],
                }
            ),
        )
        orders_tbl = con.create_table(
            "orders",
            pd.DataFrame(
                {
                    "order_id": [1, 2, 3],
                    "customer_id": [1, 1, 2],
                    "amount": [100, 200, 300],
                }
            ),
        )
        tickets_tbl = con.create_table(
            "tickets",
            pd.DataFrame(
                {
                    "ticket_id": [1, 2, 3],
                    "customer_id": [1, 2, 2],
                    "priority": ["high", "low", "medium"],
                }
            ),
        )

        customers_st = (
            to_semantic_table(customers_tbl, name="customers")
            .with_dimensions(
                customer_id=lambda t: t.customer_id,
                region=lambda t: t.region,
            )
            .with_measures(customer_count=_.count())
        )
        orders_st = (
            to_semantic_table(orders_tbl, name="orders")
            .with_dimensions(
                order_id=lambda t: t.order_id,
                customer_id=lambda t: t.customer_id,
            )
            .with_measures(
                order_count=_.count(),
                total_amount=_.amount.sum(),
            )
        )
        tickets_st = (
            to_semantic_table(tickets_tbl, name="tickets")
            .with_dimensions(
                ticket_id=lambda t: t.ticket_id,
                customer_id=lambda t: t.customer_id,
            )
            .with_measures(ticket_count=_.count())
        )

        joined = (
            customers_st
            .join_many(orders_st, on="customer_id")
            .join_many(tickets_st, on="customer_id")
        )

        # Filter on region == 'West' (customer 1 only)
        # Customer 1: 2 orders, 1 ticket
        df = (
            joined.filter(lambda t: t.region == "West")
            .aggregate("orders.order_count", "tickets.ticket_count")
            .execute()
        )
        assert df["orders.order_count"].iloc[0] == 2
        assert df["tickets.ticket_count"].iloc[0] == 1


# ---------------------------------------------------------------------------
# TestMinMaxReaggregation
# ---------------------------------------------------------------------------
class TestMinMaxReaggregation:
    """MIN and MAX measures must re-aggregate with min()/max(), not sum().

    Fixture
    -------
    customers (3 rows)
        customer_id  region
        1            West
        2            West
        3            East

    orders (6 rows)
        order_id  customer_id  amount
        1         1            100
        2         1            500
        3         2            200
        4         2            300
        5         3            50
        6         3            800
    """

    @pytest.fixture()
    def models(self):
        con = ibis.duckdb.connect(":memory:")

        customers_tbl = con.create_table(
            "customers",
            pd.DataFrame(
                {
                    "customer_id": [1, 2, 3],
                    "region": ["West", "West", "East"],
                }
            ),
        )
        orders_tbl = con.create_table(
            "orders",
            pd.DataFrame(
                {
                    "order_id": [1, 2, 3, 4, 5, 6],
                    "customer_id": [1, 1, 2, 2, 3, 3],
                    "amount": [100, 500, 200, 300, 50, 800],
                }
            ),
        )

        customers_st = (
            to_semantic_table(customers_tbl, name="customers")
            .with_dimensions(
                customer_id=lambda t: t.customer_id,
                region=lambda t: t.region,
            )
            .with_measures(customer_count=_.count())
        )
        orders_st = (
            to_semantic_table(orders_tbl, name="orders")
            .with_dimensions(
                order_id=lambda t: t.order_id,
                customer_id=lambda t: t.customer_id,
            )
            .with_measures(
                order_count=_.count(),
                total_amount=_.amount.sum(),
                min_amount=_.amount.min(),
                max_amount=_.amount.max(),
                avg_amount=_.amount.mean(),
            )
        )
        return {"customers": customers_st, "orders": orders_st}

    def test_min_across_join_scalar(self, models):
        """Scalar MIN across join_many should return the global minimum."""
        joined = models["customers"].join_many(
            models["orders"], on="customer_id"
        )
        df = joined.aggregate("orders.min_amount").execute()
        assert df["orders.min_amount"].iloc[0] == 50

    def test_max_across_join_scalar(self, models):
        """Scalar MAX across join_many should return the global maximum."""
        joined = models["customers"].join_many(
            models["orders"], on="customer_id"
        )
        df = joined.aggregate("orders.max_amount").execute()
        assert df["orders.max_amount"].iloc[0] == 800

    def test_min_max_with_group_by(self, models):
        """MIN/MAX grouped by a cross-table dimension should be correct.

        West customers (1, 2): orders [100, 500, 200, 300] → min=100, max=500
        East customer (3): orders [50, 800] → min=50, max=800
        """
        joined = models["customers"].join_many(
            models["orders"], on="customer_id"
        )
        df = (
            joined.group_by("customers.region")
            .aggregate("orders.min_amount", "orders.max_amount")
            .execute()
        )

        west = df[df["customers.region"] == "West"]
        east = df[df["customers.region"] == "East"]
        assert west["orders.min_amount"].iloc[0] == 100
        assert west["orders.max_amount"].iloc[0] == 500
        assert east["orders.min_amount"].iloc[0] == 50
        assert east["orders.max_amount"].iloc[0] == 800

    def test_min_max_sum_mean_mix(self, models):
        """All aggregation types together in one query should be correct.

        Global: amounts = [100, 500, 200, 300, 50, 800]
        sum=1950, min=50, max=800, avg=325, count=6
        """
        joined = models["customers"].join_many(
            models["orders"], on="customer_id"
        )
        df = joined.aggregate(
            "orders.total_amount",
            "orders.min_amount",
            "orders.max_amount",
            "orders.avg_amount",
            "orders.order_count",
        ).execute()

        assert df["orders.total_amount"].iloc[0] == 1950
        assert df["orders.min_amount"].iloc[0] == 50
        assert df["orders.max_amount"].iloc[0] == 800
        assert df["orders.avg_amount"].iloc[0] == pytest.approx(325.0)
        assert df["orders.order_count"].iloc[0] == 6

    def test_min_max_sum_mean_with_group_by(self, models):
        """All aggregation types grouped by region should be correct.

        West: [100, 500, 200, 300] → sum=1100, min=100, max=500, avg=275
        East: [50, 800] → sum=850, min=50, max=800, avg=425
        """
        joined = models["customers"].join_many(
            models["orders"], on="customer_id"
        )
        df = (
            joined.group_by("customers.region")
            .aggregate(
                "orders.total_amount",
                "orders.min_amount",
                "orders.max_amount",
                "orders.avg_amount",
            )
            .execute()
        )

        west = df[df["customers.region"] == "West"]
        east = df[df["customers.region"] == "East"]
        assert west["orders.total_amount"].iloc[0] == 1100
        assert west["orders.min_amount"].iloc[0] == 100
        assert west["orders.max_amount"].iloc[0] == 500
        assert west["orders.avg_amount"].iloc[0] == pytest.approx(275.0)
        assert east["orders.total_amount"].iloc[0] == 850
        assert east["orders.min_amount"].iloc[0] == 50
        assert east["orders.max_amount"].iloc[0] == 800
        assert east["orders.avg_amount"].iloc[0] == pytest.approx(425.0)


# ---------------------------------------------------------------------------
# TestMultipleMeanMeasures
# ---------------------------------------------------------------------------
class TestMultipleMeanMeasures:
    """Multiple MEAN measures on the same table, possibly on different columns.

    Fixture
    -------
    customers (2 rows)
        customer_id  region
        1            West
        2            East

    orders (4 rows)
        order_id  customer_id  amount  weight
        1         1            100     10
        2         1            200     30
        3         2            300     20
        4         2            400     40
    """

    @pytest.fixture()
    def models(self):
        con = ibis.duckdb.connect(":memory:")

        customers_tbl = con.create_table(
            "customers",
            pd.DataFrame(
                {
                    "customer_id": [1, 2],
                    "region": ["West", "East"],
                }
            ),
        )
        orders_tbl = con.create_table(
            "orders",
            pd.DataFrame(
                {
                    "order_id": [1, 2, 3, 4],
                    "customer_id": [1, 1, 2, 2],
                    "amount": [100, 200, 300, 400],
                    "weight": [10, 30, 20, 40],
                }
            ),
        )

        customers_st = (
            to_semantic_table(customers_tbl, name="customers")
            .with_dimensions(
                customer_id=lambda t: t.customer_id,
                region=lambda t: t.region,
            )
            .with_measures(customer_count=_.count())
        )
        orders_st = (
            to_semantic_table(orders_tbl, name="orders")
            .with_dimensions(
                order_id=lambda t: t.order_id,
                customer_id=lambda t: t.customer_id,
            )
            .with_measures(
                avg_amount=_.amount.mean(),
                avg_weight=_.weight.mean(),
                total_amount=_.amount.sum(),
            )
        )
        return {"customers": customers_st, "orders": orders_st}

    def test_two_means_scalar(self, models):
        """Two MEAN measures on different columns, scalar aggregate.

        avg_amount = (100+200+300+400)/4 = 250
        avg_weight = (10+30+20+40)/4 = 25
        """
        joined = models["customers"].join_many(
            models["orders"], on="customer_id"
        )
        df = joined.aggregate(
            "orders.avg_amount", "orders.avg_weight"
        ).execute()
        assert df["orders.avg_amount"].iloc[0] == pytest.approx(250.0)
        assert df["orders.avg_weight"].iloc[0] == pytest.approx(25.0)

    def test_two_means_with_group_by(self, models):
        """Two MEAN measures grouped by cross-table dimension.

        West (cust 1): avg_amount=(100+200)/2=150, avg_weight=(10+30)/2=20
        East (cust 2): avg_amount=(300+400)/2=350, avg_weight=(20+40)/2=30
        """
        joined = models["customers"].join_many(
            models["orders"], on="customer_id"
        )
        df = (
            joined.group_by("customers.region")
            .aggregate("orders.avg_amount", "orders.avg_weight")
            .execute()
        )

        west = df[df["customers.region"] == "West"]
        east = df[df["customers.region"] == "East"]
        assert west["orders.avg_amount"].iloc[0] == pytest.approx(150.0)
        assert west["orders.avg_weight"].iloc[0] == pytest.approx(20.0)
        assert east["orders.avg_amount"].iloc[0] == pytest.approx(350.0)
        assert east["orders.avg_weight"].iloc[0] == pytest.approx(30.0)

    def test_mean_plus_sum_same_table(self, models):
        """MEAN and SUM on the same table should both be correct.

        Global: avg_amount=250, total_amount=1000
        """
        joined = models["customers"].join_many(
            models["orders"], on="customer_id"
        )
        df = joined.aggregate(
            "orders.avg_amount", "orders.total_amount"
        ).execute()
        assert df["orders.avg_amount"].iloc[0] == pytest.approx(250.0)
        assert df["orders.total_amount"].iloc[0] == 1000

    def test_mean_plus_sum_with_group_by(self, models):
        """MEAN and SUM grouped by region.

        West: avg_amount=150, total_amount=300
        East: avg_amount=350, total_amount=700
        """
        joined = models["customers"].join_many(
            models["orders"], on="customer_id"
        )
        df = (
            joined.group_by("customers.region")
            .aggregate("orders.avg_amount", "orders.total_amount")
            .execute()
        )

        west = df[df["customers.region"] == "West"]
        east = df[df["customers.region"] == "East"]
        assert west["orders.avg_amount"].iloc[0] == pytest.approx(150.0)
        assert west["orders.total_amount"].iloc[0] == 300
        assert east["orders.avg_amount"].iloc[0] == pytest.approx(350.0)
        assert east["orders.total_amount"].iloc[0] == 700


# ---------------------------------------------------------------------------
# TestFilterPushdownEdgeCases
# ---------------------------------------------------------------------------
class TestFilterPushdownEdgeCases:
    """Edge cases for filter pushdown with pre-aggregation.

    Fixture
    -------
    customers (3 rows)
        customer_id  region   status
        1            West     active
        2            East     active
        3            West     inactive

    orders (6 rows)
        order_id  customer_id  amount  category
        1         1            100     A
        2         1            200     B
        3         2            300     A
        4         2            400     B
        5         3            500     A
        6         3            600     A
    """

    @pytest.fixture()
    def models(self):
        con = ibis.duckdb.connect(":memory:")

        customers_tbl = con.create_table(
            "customers",
            pd.DataFrame(
                {
                    "customer_id": [1, 2, 3],
                    "region": ["West", "East", "West"],
                    "status": ["active", "active", "inactive"],
                }
            ),
        )
        orders_tbl = con.create_table(
            "orders",
            pd.DataFrame(
                {
                    "order_id": [1, 2, 3, 4, 5, 6],
                    "customer_id": [1, 1, 2, 2, 3, 3],
                    "amount": [100, 200, 300, 400, 500, 600],
                    "category": ["A", "B", "A", "B", "A", "A"],
                }
            ),
        )

        customers_st = (
            to_semantic_table(customers_tbl, name="customers")
            .with_dimensions(
                customer_id=lambda t: t.customer_id,
                region=lambda t: t.region,
                status=lambda t: t.status,
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
                category=lambda t: t.category,
            )
            .with_measures(
                order_count=_.count(),
                total_amount=_.amount.sum(),
            )
        )
        return {"customers": customers_st, "orders": orders_st}

    def test_chained_filters_same_table(self, models):
        """Multiple chained .filter() calls on the same table's columns.

        filter(amount > 100) then filter(amount < 500):
        Orders passing: #2(200), #3(300), #4(400) → 3 orders, sum=900
        """
        joined = models["customers"].join_many(
            models["orders"], on="customer_id"
        )
        df = (
            joined.filter(lambda t: t.amount > 100)
            .filter(lambda t: t.amount < 500)
            .aggregate("orders.order_count", "orders.total_amount")
            .execute()
        )
        assert df["orders.order_count"].iloc[0] == 3
        assert df["orders.total_amount"].iloc[0] == 900

    def test_chained_filters_cross_table(self, models):
        """Chained filters where one is pushable and one is cross-table.

        filter(amount > 200) → orders #3,#4,#5,#6 (customers 2,3)
        filter(region == 'West') → customer 3 only
        Customer 3 orders > 200: #5(500), #6(600) → 2 orders
        """
        joined = models["customers"].join_many(
            models["orders"], on="customer_id"
        )
        df = (
            joined.filter(lambda t: t.amount > 200)
            .filter(lambda t: t.region == "West")
            .aggregate("orders.order_count")
            .execute()
        )
        assert df["orders.order_count"].iloc[0] == 2

    def test_filter_on_many_side_dimension(self, models):
        """Filter on a many-side dimension column (not a measure).

        category == 'A': orders #1(100),#3(300),#5(500),#6(600) → 4 orders, sum=1500
        """
        joined = models["customers"].join_many(
            models["orders"], on="customer_id"
        )
        df = (
            joined.filter(lambda t: t.category == "A")
            .aggregate("orders.order_count", "orders.total_amount")
            .execute()
        )
        assert df["orders.order_count"].iloc[0] == 4
        assert df["orders.total_amount"].iloc[0] == 1500

    def test_filter_with_group_by(self, models):
        """Filter + group_by combination.

        filter(amount > 100) removes order #1.
        Remaining by region:
        West (cust 1,3): #2(200),#5(500),#6(600) → 3 orders
        East (cust 2): #3(300),#4(400) → 2 orders
        """
        joined = models["customers"].join_many(
            models["orders"], on="customer_id"
        )
        df = (
            joined.filter(lambda t: t.amount > 100)
            .group_by("customers.region")
            .aggregate("orders.order_count")
            .execute()
        )

        west = df[df["customers.region"] == "West"]
        east = df[df["customers.region"] == "East"]
        assert west["orders.order_count"].iloc[0] == 3
        assert east["orders.order_count"].iloc[0] == 2

    def test_filter_with_mean_measure(self, models):
        """Filter combined with a MEAN measure.

        filter(amount > 100) → orders [200, 300, 400, 500, 600]
        avg = (200+300+400+500+600)/5 = 400
        """
        con = ibis.duckdb.connect(":memory:")
        customers_tbl = con.create_table(
            "customers",
            pd.DataFrame(
                {"customer_id": [1, 2, 3], "region": ["W", "E", "W"]}
            ),
        )
        orders_tbl = con.create_table(
            "orders",
            pd.DataFrame(
                {
                    "order_id": [1, 2, 3, 4, 5, 6],
                    "customer_id": [1, 1, 2, 2, 3, 3],
                    "amount": [100, 200, 300, 400, 500, 600],
                }
            ),
        )
        customers_st = (
            to_semantic_table(customers_tbl, name="customers")
            .with_dimensions(customer_id=lambda t: t.customer_id)
            .with_measures(customer_count=_.count())
        )
        orders_st = (
            to_semantic_table(orders_tbl, name="orders")
            .with_dimensions(
                order_id=lambda t: t.order_id,
                customer_id=lambda t: t.customer_id,
            )
            .with_measures(avg_amount=_.amount.mean())
        )

        joined = customers_st.join_many(orders_st, on="customer_id")
        df = (
            joined.filter(lambda t: t.amount > 100)
            .aggregate("orders.avg_amount")
            .execute()
        )
        assert df["orders.avg_amount"].iloc[0] == pytest.approx(400.0)

    def test_filter_producing_empty_result(self, models):
        """Filter that eliminates all rows should produce empty or zero results."""
        joined = models["customers"].join_many(
            models["orders"], on="customer_id"
        )
        df = (
            joined.filter(lambda t: t.amount > 10000)
            .aggregate("orders.order_count")
            .execute()
        )
        # With all rows filtered, count should be 0
        assert df["orders.order_count"].iloc[0] == 0


# ---------------------------------------------------------------------------
# TestNullForeignKeys
# ---------------------------------------------------------------------------
class TestNullForeignKeys:
    """Dimension bridges with NULL foreign keys should not lose data.

    Fixture
    -------
    customers (3 rows, one with NULL region)
        customer_id  region
        1            West
        2            NULL
        3            East

    orders (4 rows)
        order_id  customer_id  amount
        1         1            100
        2         2            200
        3         2            300
        4         3            400
    """

    @pytest.fixture()
    def models(self):
        con = ibis.duckdb.connect(":memory:")

        customers_tbl = con.create_table(
            "customers",
            pd.DataFrame(
                {
                    "customer_id": [1, 2, 3],
                    "region": ["West", None, "East"],
                }
            ),
        )
        orders_tbl = con.create_table(
            "orders",
            pd.DataFrame(
                {
                    "order_id": [1, 2, 3, 4],
                    "customer_id": [1, 2, 2, 3],
                    "amount": [100, 200, 300, 400],
                }
            ),
        )

        customers_st = (
            to_semantic_table(customers_tbl, name="customers")
            .with_dimensions(
                customer_id=lambda t: t.customer_id,
                region=lambda t: t.region,
            )
            .with_measures(customer_count=_.count())
        )
        orders_st = (
            to_semantic_table(orders_tbl, name="orders")
            .with_dimensions(
                order_id=lambda t: t.order_id,
                customer_id=lambda t: t.customer_id,
            )
            .with_measures(
                order_count=_.count(),
                total_amount=_.amount.sum(),
            )
        )
        return {"customers": customers_st, "orders": orders_st}

    def test_total_not_lost_with_null_fk(self, models):
        """Total amount should include orders for the NULL-region customer.

        All orders: 100+200+300+400 = 1000
        """
        joined = models["customers"].join_many(
            models["orders"], on="customer_id"
        )
        df = joined.aggregate("orders.total_amount").execute()
        assert df["orders.total_amount"].iloc[0] == 1000

    def test_group_by_with_null_region(self, models):
        """Group by region should produce 3 groups (West, East, NULL).

        West (cust 1): 100
        NULL (cust 2): 200+300=500
        East (cust 3): 400
        Total: 1000
        """
        joined = models["customers"].join_many(
            models["orders"], on="customer_id"
        )
        df = (
            joined.group_by("customers.region")
            .aggregate("orders.total_amount")
            .execute()
        )
        assert df["orders.total_amount"].sum() == 1000


# ---------------------------------------------------------------------------
# TestMultiDimensionalGroupBy
# ---------------------------------------------------------------------------
class TestMultiDimensionalGroupBy:
    """Group by dimensions from multiple tables simultaneously.

    Fixture
    -------
    customers (3 rows)
        customer_id  region
        1            West
        2            East
        3            West

    orders (6 rows)
        order_id  customer_id  amount  status
        1         1            100     paid
        2         1            200     pending
        3         2            300     paid
        4         2            400     paid
        5         3            500     pending
        6         3            600     paid
    """

    @pytest.fixture()
    def models(self):
        con = ibis.duckdb.connect(":memory:")

        customers_tbl = con.create_table(
            "customers",
            pd.DataFrame(
                {
                    "customer_id": [1, 2, 3],
                    "region": ["West", "East", "West"],
                }
            ),
        )
        orders_tbl = con.create_table(
            "orders",
            pd.DataFrame(
                {
                    "order_id": [1, 2, 3, 4, 5, 6],
                    "customer_id": [1, 1, 2, 2, 3, 3],
                    "amount": [100, 200, 300, 400, 500, 600],
                    "status": ["paid", "pending", "paid", "paid", "pending", "paid"],
                }
            ),
        )

        customers_st = (
            to_semantic_table(customers_tbl, name="customers")
            .with_dimensions(
                customer_id=lambda t: t.customer_id,
                region=lambda t: t.region,
            )
            .with_measures(customer_count=_.count())
        )
        orders_st = (
            to_semantic_table(orders_tbl, name="orders")
            .with_dimensions(
                order_id=lambda t: t.order_id,
                customer_id=lambda t: t.customer_id,
                status=lambda t: t.status,
            )
            .with_measures(
                order_count=_.count(),
                total_amount=_.amount.sum(),
            )
        )
        return {"customers": customers_st, "orders": orders_st}

    def test_group_by_two_dims_from_different_tables(self, models):
        """Group by region (customers) and status (orders).

        West/paid: #1(100), #6(600) → 2 orders, sum=700
        West/pending: #2(200), #5(500) → 2 orders, sum=700
        East/paid: #3(300), #4(400) → 2 orders, sum=700
        Total: 6 orders, sum=2100
        """
        joined = models["customers"].join_many(
            models["orders"], on="customer_id"
        )
        df = (
            joined.group_by("customers.region", "orders.status")
            .aggregate("orders.order_count", "orders.total_amount")
            .execute()
        )

        assert df["orders.order_count"].sum() == 6
        assert df["orders.total_amount"].sum() == 2100

        west_paid = df[
            (df["customers.region"] == "West") & (df["orders.status"] == "paid")
        ]
        assert west_paid["orders.order_count"].iloc[0] == 2
        assert west_paid["orders.total_amount"].iloc[0] == 700

    def test_group_by_same_table_dim(self, models):
        """Group by two dimensions both from the many-side table.

        status/customer_id → should just group orders directly.
        Total across all groups: 6 orders, sum=2100
        """
        joined = models["customers"].join_many(
            models["orders"], on="customer_id"
        )
        df = (
            joined.group_by("orders.status", "orders.customer_id")
            .aggregate("orders.total_amount")
            .execute()
        )
        assert df["orders.total_amount"].sum() == 2100


# ---------------------------------------------------------------------------
# TestChasmFilterPushdown
# ---------------------------------------------------------------------------
class TestChasmFilterPushdown:
    """Filters with chasm traps (multiple join_many arms).

    Fixture
    -------
    departments (2 rows)
        dept_id  dept_name
        1        Engineering
        2        Sales

    projects (4 rows)
        project_id  dept_id  budget
        1           1        50000
        2           1        30000
        3           2        20000
        4           2        10000

    expenses (4 rows)
        expense_id  dept_id  amount
        1           1        5000
        2           1        8000
        3           2        3000
        4           2        2000
    """

    @pytest.fixture()
    def models(self):
        con = ibis.duckdb.connect(":memory:")

        depts_tbl = con.create_table(
            "departments",
            pd.DataFrame(
                {
                    "dept_id": [1, 2],
                    "dept_name": ["Engineering", "Sales"],
                }
            ),
        )
        projects_tbl = con.create_table(
            "projects",
            pd.DataFrame(
                {
                    "project_id": [1, 2, 3, 4],
                    "dept_id": [1, 1, 2, 2],
                    "budget": [50000, 30000, 20000, 10000],
                }
            ),
        )
        expenses_tbl = con.create_table(
            "expenses",
            pd.DataFrame(
                {
                    "expense_id": [1, 2, 3, 4],
                    "dept_id": [1, 1, 2, 2],
                    "amount": [5000, 8000, 3000, 2000],
                }
            ),
        )

        depts_st = (
            to_semantic_table(depts_tbl, name="departments")
            .with_dimensions(
                dept_id=lambda t: t.dept_id,
                dept_name=lambda t: t.dept_name,
            )
            .with_measures(dept_count=_.count())
        )
        projects_st = (
            to_semantic_table(projects_tbl, name="projects")
            .with_dimensions(
                project_id=lambda t: t.project_id,
                dept_id=lambda t: t.dept_id,
            )
            .with_measures(
                project_count=_.count(),
                total_budget=_.budget.sum(),
            )
        )
        expenses_st = (
            to_semantic_table(expenses_tbl, name="expenses")
            .with_dimensions(
                expense_id=lambda t: t.expense_id,
                dept_id=lambda t: t.dept_id,
            )
            .with_measures(
                expense_count=_.count(),
                total_expenses=_.amount.sum(),
            )
        )
        return {
            "departments": depts_st,
            "projects": projects_st,
            "expenses": expenses_st,
        }

    def test_chasm_filter_on_one_side_dim(self, models):
        """Filter on root dimension with two join_many arms.

        Engineering (dept 1): 2 projects, 2 expenses
        """
        joined = (
            models["departments"]
            .join_many(models["projects"], on="dept_id")
            .join_many(models["expenses"], on="dept_id")
        )
        df = (
            joined.filter(lambda t: t.dept_name == "Engineering")
            .aggregate("projects.project_count", "expenses.expense_count")
            .execute()
        )
        assert df["projects.project_count"].iloc[0] == 2
        assert df["expenses.expense_count"].iloc[0] == 2

    def test_chasm_filter_on_many_side(self, models):
        """Filter on many-side with two chasm arms.

        filter(budget > 20000): projects #1(50k) and #2(30k) → dept_id=1
        Total budget after filter: 80000
        Expenses for dept 1: 5000+8000 = 13000
        """
        joined = (
            models["departments"]
            .join_many(models["projects"], on="dept_id")
            .join_many(models["expenses"], on="dept_id")
        )
        df = (
            joined.filter(lambda t: t.budget > 20000)
            .aggregate("projects.total_budget", "expenses.total_expenses")
            .execute()
        )
        assert df["projects.total_budget"].iloc[0] == 80000
        assert df["expenses.total_expenses"].iloc[0] == 13000

    def test_chasm_no_filter_baseline(self, models):
        """Baseline: chasm without filter should give correct totals."""
        joined = (
            models["departments"]
            .join_many(models["projects"], on="dept_id")
            .join_many(models["expenses"], on="dept_id")
        )
        df = joined.aggregate(
            "projects.total_budget", "expenses.total_expenses"
        ).execute()
        assert df["projects.total_budget"].iloc[0] == 110000
        assert df["expenses.total_expenses"].iloc[0] == 18000


# ---------------------------------------------------------------------------
# TestDeepChainEdgeCases
# ---------------------------------------------------------------------------
class TestDeepChainEdgeCases:
    """Edge cases with 3+ table deep chains.

    Fixture (regions → stores → orders):
    -------
    regions (2 rows)
        region_id  region_name
        1          North
        2          South

    stores (3 rows)
        store_id  region_id  store_name
        1         1          Store A
        2         1          Store B
        3         2          Store C

    orders (6 rows)
        order_id  store_id  amount
        1         1         100
        2         1         200
        3         2         300
        4         2         400
        5         3         500
        6         3         600
    """

    @pytest.fixture()
    def models(self):
        con = ibis.duckdb.connect(":memory:")

        regions_tbl = con.create_table(
            "regions",
            pd.DataFrame(
                {"region_id": [1, 2], "region_name": ["North", "South"]}
            ),
        )
        stores_tbl = con.create_table(
            "stores",
            pd.DataFrame(
                {
                    "store_id": [1, 2, 3],
                    "region_id": [1, 1, 2],
                    "store_name": ["Store A", "Store B", "Store C"],
                }
            ),
        )
        orders_tbl = con.create_table(
            "orders",
            pd.DataFrame(
                {
                    "order_id": [1, 2, 3, 4, 5, 6],
                    "store_id": [1, 1, 2, 2, 3, 3],
                    "amount": [100, 200, 300, 400, 500, 600],
                }
            ),
        )

        regions_st = (
            to_semantic_table(regions_tbl, name="regions")
            .with_dimensions(
                region_id=lambda t: t.region_id,
                region_name=lambda t: t.region_name,
            )
            .with_measures(region_count=_.count())
        )
        stores_st = (
            to_semantic_table(stores_tbl, name="stores")
            .with_dimensions(
                store_id=lambda t: t.store_id,
                region_id=lambda t: t.region_id,
                store_name=lambda t: t.store_name,
            )
            .with_measures(store_count=_.count())
        )
        orders_st = (
            to_semantic_table(orders_tbl, name="orders")
            .with_dimensions(
                order_id=lambda t: t.order_id,
                store_id=lambda t: t.store_id,
            )
            .with_measures(
                order_count=_.count(),
                total_amount=_.amount.sum(),
                min_amount=_.amount.min(),
                max_amount=_.amount.max(),
                avg_amount=_.amount.mean(),
            )
        )
        return {"regions": regions_st, "stores": stores_st, "orders": orders_st}

    def test_three_table_chain_total(self, models):
        """3-table chain: total amount should not be inflated.

        Total: 100+200+300+400+500+600 = 2100
        """
        joined = (
            models["regions"]
            .join_many(models["stores"], on="region_id")
            .join_many(models["orders"], on="store_id")
        )
        df = joined.aggregate("orders.total_amount").execute()
        assert df["orders.total_amount"].iloc[0] == 2100

    def test_three_table_chain_group_by_root(self, models):
        """Group by root dim (region_name) aggregating leaf measures.

        North (stores A,B): orders [100,200,300,400] → sum=1000, count=4
        South (store C): orders [500,600] → sum=1100, count=2
        """
        joined = (
            models["regions"]
            .join_many(models["stores"], on="region_id")
            .join_many(models["orders"], on="store_id")
        )
        df = (
            joined.group_by("regions.region_name")
            .aggregate("orders.total_amount", "orders.order_count")
            .execute()
        )

        north = df[df["regions.region_name"] == "North"]
        south = df[df["regions.region_name"] == "South"]
        assert north["orders.total_amount"].iloc[0] == 1000
        assert north["orders.order_count"].iloc[0] == 4
        assert south["orders.total_amount"].iloc[0] == 1100
        assert south["orders.order_count"].iloc[0] == 2

    def test_three_table_chain_min_max_group_by_root(self, models):
        """MIN/MAX through 3-table chain grouped by root.

        North: amounts [100,200,300,400] → min=100, max=400
        South: amounts [500,600] → min=500, max=600
        """
        joined = (
            models["regions"]
            .join_many(models["stores"], on="region_id")
            .join_many(models["orders"], on="store_id")
        )
        df = (
            joined.group_by("regions.region_name")
            .aggregate("orders.min_amount", "orders.max_amount")
            .execute()
        )

        north = df[df["regions.region_name"] == "North"]
        south = df[df["regions.region_name"] == "South"]
        assert north["orders.min_amount"].iloc[0] == 100
        assert north["orders.max_amount"].iloc[0] == 400
        assert south["orders.min_amount"].iloc[0] == 500
        assert south["orders.max_amount"].iloc[0] == 600

    def test_three_table_chain_mean_group_by_root(self, models):
        """MEAN through 3-table chain grouped by root.

        North: amounts [100,200,300,400] → avg=250
        South: amounts [500,600] → avg=550
        """
        joined = (
            models["regions"]
            .join_many(models["stores"], on="region_id")
            .join_many(models["orders"], on="store_id")
        )
        df = (
            joined.group_by("regions.region_name")
            .aggregate("orders.avg_amount")
            .execute()
        )

        north = df[df["regions.region_name"] == "North"]
        south = df[df["regions.region_name"] == "South"]
        assert north["orders.avg_amount"].iloc[0] == pytest.approx(250.0)
        assert south["orders.avg_amount"].iloc[0] == pytest.approx(550.0)

    def test_three_table_chain_all_agg_types(self, models):
        """All aggregation types through a 3-table chain, grouped by root.

        North: amounts [100,200,300,400] → sum=1000, min=100, max=400, avg=250, count=4
        South: amounts [500,600] → sum=1100, min=500, max=600, avg=550, count=2
        """
        joined = (
            models["regions"]
            .join_many(models["stores"], on="region_id")
            .join_many(models["orders"], on="store_id")
        )
        df = (
            joined.group_by("regions.region_name")
            .aggregate(
                "orders.total_amount",
                "orders.min_amount",
                "orders.max_amount",
                "orders.avg_amount",
                "orders.order_count",
            )
            .execute()
        )

        north = df[df["regions.region_name"] == "North"]
        south = df[df["regions.region_name"] == "South"]

        assert north["orders.total_amount"].iloc[0] == 1000
        assert north["orders.min_amount"].iloc[0] == 100
        assert north["orders.max_amount"].iloc[0] == 400
        assert north["orders.avg_amount"].iloc[0] == pytest.approx(250.0)
        assert north["orders.order_count"].iloc[0] == 4

        assert south["orders.total_amount"].iloc[0] == 1100
        assert south["orders.min_amount"].iloc[0] == 500
        assert south["orders.max_amount"].iloc[0] == 600
        assert south["orders.avg_amount"].iloc[0] == pytest.approx(550.0)
        assert south["orders.order_count"].iloc[0] == 2

    def test_intermediate_measure_not_inflated(self, models):
        """Intermediate table (stores) measure should not be inflated by leaf fan-out.

        store_count = 3 (not inflated by orders)
        """
        joined = (
            models["regions"]
            .join_many(models["stores"], on="region_id")
            .join_many(models["orders"], on="store_id")
        )
        df = joined.aggregate("stores.store_count").execute()
        assert df["stores.store_count"].iloc[0] == 3

    def test_filter_on_leaf_restricts_chain(self, models):
        """Filter on leaf table should restrict correctly through 3-table chain.

        filter(amount > 300): orders #4(400),#5(500),#6(600)
        stores involved: 2(region 1),3(region 2)
        order_count=3, total=1500
        """
        joined = (
            models["regions"]
            .join_many(models["stores"], on="region_id")
            .join_many(models["orders"], on="store_id")
        )
        df = (
            joined.filter(lambda t: t.amount > 300)
            .aggregate("orders.order_count", "orders.total_amount")
            .execute()
        )
        assert df["orders.order_count"].iloc[0] == 3
        assert df["orders.total_amount"].iloc[0] == 1500


# ---------------------------------------------------------------------------
# TestLeftJoinUnmatchedRows
# ---------------------------------------------------------------------------
class TestLeftJoinUnmatchedRows:
    """Left join with unmatched dimension rows: pre-aggregation must preserve
    dimension entries that have no matching rows on the right side.

    Fixture
    -------
    customers (4 rows — 2 West, 2 East)
        customer_id  name    region
        1            Alice   West
        2            Bob     West
        3            Carol   East
        4            Dave    East

    orders (3 rows — only West customers have orders)
        order_id  customer_id  amount
        1         1            100
        2         1            200
        3         2            300
    """

    @pytest.fixture()
    def models(self):
        con = ibis.duckdb.connect(":memory:")

        customers_tbl = con.create_table(
            "customers",
            pd.DataFrame(
                {
                    "customer_id": [1, 2, 3, 4],
                    "name": ["Alice", "Bob", "Carol", "Dave"],
                    "region": ["West", "West", "East", "East"],
                }
            ),
        )
        orders_tbl = con.create_table(
            "orders",
            pd.DataFrame(
                {
                    "order_id": [1, 2, 3],
                    "customer_id": [1, 1, 2],
                    "amount": [100, 200, 300],
                }
            ),
        )

        customers_st = (
            to_semantic_table(customers_tbl, name="customers")
            .with_dimensions(
                customer_id=lambda t: t.customer_id,
                name=lambda t: t.name,
                region=lambda t: t.region,
            )
            .with_measures(customer_count=_.count())
        )
        orders_st = (
            to_semantic_table(orders_tbl, name="orders")
            .with_dimensions(
                order_id=lambda t: t.order_id,
                customer_id=lambda t: t.customer_id,
            )
            .with_measures(
                order_count=_.count(),
                total_amount=_.amount.sum(),
                avg_amount=_.amount.mean(),
            )
        )
        return {"customers": customers_st, "orders": orders_st}

    def test_unmatched_rows_preserved(self, models):
        """East region has no orders but must still appear in results."""
        joined = models["customers"].join_many(
            models["orders"], on="customer_id"
        )
        df = (
            joined.group_by("customers.region")
            .aggregate("orders.order_count")
            .execute()
        )
        assert len(df) == 2
        east = df[df["customers.region"] == "East"]
        assert len(east) == 1

    def test_unmatched_measures_are_null(self, models):
        """Unmatched dimension values have NULL (not 0) for aggregated measures."""
        joined = models["customers"].join_many(
            models["orders"], on="customer_id"
        )
        df = (
            joined.group_by("customers.region")
            .aggregate("orders.total_amount")
            .execute()
        )
        east = df[df["customers.region"] == "East"]
        assert east["orders.total_amount"].isna().iloc[0]

    def test_matched_measures_correct(self, models):
        """West region totals are still correct."""
        joined = models["customers"].join_many(
            models["orders"], on="customer_id"
        )
        df = (
            joined.group_by("customers.region")
            .aggregate("orders.total_amount")
            .execute()
        )
        west = df[df["customers.region"] == "West"]
        assert west["orders.total_amount"].iloc[0] == 600  # 100 + 200 + 300

    def test_scalar_aggregate_only_counts_matched(self, models):
        """Grand total should only count matched rows (no NULLs added)."""
        joined = models["customers"].join_many(
            models["orders"], on="customer_id"
        )
        df = joined.aggregate("orders.total_amount").execute()
        assert df["orders.total_amount"].iloc[0] == 600

    def test_by_name_grouping_unmatched(self, models):
        """Individual customers Carol and Dave appear with NULL measures."""
        joined = models["customers"].join_many(
            models["orders"], on="customer_id"
        )
        df = (
            joined.group_by("customers.name")
            .aggregate("orders.total_amount")
            .execute()
        )
        carol = df[df["customers.name"] == "Carol"]
        dave = df[df["customers.name"] == "Dave"]
        assert len(carol) == 1
        assert carol["orders.total_amount"].isna().iloc[0]
        assert len(dave) == 1
        assert dave["orders.total_amount"].isna().iloc[0]

    def test_both_sides_measures(self, models):
        """Aggregating measures from both left AND right — all rows appear."""
        joined = models["customers"].join_many(
            models["orders"], on="customer_id"
        )
        df = (
            joined.group_by("customers.region")
            .aggregate("customers.customer_count", "orders.order_count")
            .execute()
        )
        assert len(df) == 2
        west = df[df["customers.region"] == "West"]
        east = df[df["customers.region"] == "East"]
        assert west["customers.customer_count"].iloc[0] == 2
        assert west["orders.order_count"].iloc[0] == 3
        assert east["customers.customer_count"].iloc[0] == 2
