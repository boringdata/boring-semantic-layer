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
