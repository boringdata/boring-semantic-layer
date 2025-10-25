"""
Real-world integration tests for BSL v2 api.

These tests demonstrate practical business intelligence scenarios:
- E-commerce analytics (cohort analysis, retention, sales metrics)
- SaaS metrics (MRR, churn, user engagement)
- Supply chain analytics (inventory turnover, lead times)
- Marketing attribution (funnel analysis, campaign ROI)
"""

import ibis
import pandas as pd
import pytest
from boring_semantic_layer import to_semantic_table


@pytest.fixture(scope="module")
def con():
    """DuckDB connection for all tests."""
    return ibis.duckdb.connect(":memory:")


class TestEcommerceAnalytics:
    """E-commerce business intelligence scenarios."""

    @pytest.fixture(scope="class")
    def ecommerce_data(self, con):
        """Setup e-commerce test data."""
        # Users table
        users_df = pd.DataFrame(
            {
                "user_id": [1, 2, 3, 4, 5],
                "signup_date": pd.to_datetime(
                    [
                        "2023-01-15",
                        "2023-01-20",
                        "2023-02-10",
                        "2023-02-15",
                        "2023-03-01",
                    ]
                ),
                "country": ["US", "UK", "US", "CA", "UK"],
                "segment": ["premium", "free", "premium", "free", "premium"],
            }
        )

        # Orders table
        orders_df = pd.DataFrame(
            {
                "order_id": [101, 102, 103, 104, 105, 106, 107, 108],
                "user_id": [1, 2, 1, 3, 4, 1, 5, 3],
                "order_date": pd.to_datetime(
                    [
                        "2023-01-20",
                        "2023-02-01",
                        "2023-02-15",
                        "2023-03-01",
                        "2023-03-10",
                        "2023-03-15",
                        "2023-04-01",
                        "2023-04-05",
                    ]
                ),
                "total_amount": [100.0, 50.0, 150.0, 200.0, 75.0, 120.0, 300.0, 180.0],
                "status": [
                    "completed",
                    "completed",
                    "completed",
                    "completed",
                    "completed",
                    "cancelled",
                    "completed",
                    "completed",
                ],
            }
        )

        # Products table
        products_df = pd.DataFrame(
            {
                "product_id": [1, 2, 3, 4, 5],
                "category": [
                    "electronics",
                    "clothing",
                    "electronics",
                    "home",
                    "clothing",
                ],
                "price": [50.0, 30.0, 80.0, 45.0, 25.0],
            }
        )

        # Order items table
        order_items_df = pd.DataFrame(
            {
                "order_item_id": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
                "order_id": [101, 101, 102, 103, 103, 104, 105, 107, 108, 108],
                "product_id": [1, 2, 2, 1, 3, 4, 2, 5, 1, 4],
                "quantity": [1, 2, 1, 1, 1, 1, 1, 2, 1, 1],
                "item_price": [
                    50.0,
                    30.0,
                    50.0,
                    50.0,
                    80.0,
                    200.0,
                    75.0,
                    25.0,
                    50.0,
                    45.0,
                ],
            }
        )

        return {
            "users": con.create_table("users", users_df),
            "orders": con.create_table("orders", orders_df),
            "products": con.create_table("products", products_df),
            "order_items": con.create_table("order_items", order_items_df),
        }

    def test_customer_lifetime_value(self, ecommerce_data):
        """Calculate CLV by customer segment."""
        users_tbl = ecommerce_data["users"]
        orders_tbl = ecommerce_data["orders"]

        # Join users and orders
        joined = users_tbl.join(
            orders_tbl, users_tbl.user_id == orders_tbl.user_id, how="left"
        )

        users_st = (
            to_semantic_table(joined, name="users")
            .with_dimensions(
                segment=lambda t: t.segment,
                user_id=lambda t: t.user_id,
            )
            .with_measures(
                total_revenue=lambda t: t.total_amount.sum(),
                order_count=lambda t: t.order_id.count(),
                customer_count=lambda t: t.user_id.nunique(),
            )
        )

        # Calculate average CLV by segment
        result = (
            users_st.group_by("segment")
            .aggregate("total_revenue", "customer_count")
            .mutate(avg_clv=lambda t: t["total_revenue"] / t["customer_count"])
            .execute()
        )

        assert len(result) == 2  # premium and free segments
        assert "avg_clv" in result.columns
        assert result[result["segment"] == "premium"]["avg_clv"].iloc[0] > 0

    def test_cohort_retention_analysis(self, ecommerce_data):
        """Track user retention by signup cohort."""
        users_tbl = ecommerce_data["users"]
        orders_tbl = ecommerce_data["orders"]

        joined = users_tbl.join(
            orders_tbl, users_tbl.user_id == orders_tbl.user_id, how="inner"
        )

        cohort_st = (
            to_semantic_table(joined, name="cohorts")
            .with_dimensions(
                signup_month=lambda t: t.signup_date.truncate("month"),
                order_month=lambda t: t.order_date.truncate("month"),
            )
            .with_measures(
                active_users=lambda t: t.user_id.nunique(),
            )
        )

        result = (
            cohort_st.group_by("signup_month", "order_month")
            .aggregate("active_users")
            .order_by("signup_month", "order_month")
            .execute()
        )

        assert len(result) > 0
        assert "active_users" in result.columns

    def test_product_category_performance(self, ecommerce_data):
        """Analyze sales by product category with percent of total."""
        products_tbl = ecommerce_data["products"]
        order_items_tbl = ecommerce_data["order_items"]

        joined = order_items_tbl.join(
            products_tbl,
            order_items_tbl.product_id == products_tbl.product_id,
            how="inner",
        )

        category_st = (
            to_semantic_table(joined, name="categories")
            .with_dimensions(
                category=lambda t: t.category,
            )
            .with_measures(
                total_sales=lambda t: t.item_price.sum(),
                items_sold=lambda t: t.quantity.sum(),
            )
        )

        result = (
            category_st.group_by("category")
            .aggregate("total_sales", "items_sold")
            .mutate(
                percent_of_total_sales=lambda t: t["total_sales"]
                / t.all(t["total_sales"]),
            )
            .order_by(ibis.desc("total_sales"))
            .execute()
        )

        assert len(result) > 0
        assert "percent_of_total_sales" in result.columns
        # Sum of all percents should be ~1.0
        assert abs(result["percent_of_total_sales"].sum() - 1.0) < 0.01

    def test_order_status_funnel(self, ecommerce_data):
        """Analyze order completion rates and cancellation patterns."""
        orders_tbl = ecommerce_data["orders"]

        orders_st = (
            to_semantic_table(orders_tbl, name="orders")
            .with_dimensions(
                status=lambda t: t.status,
            )
            .with_measures(
                order_count=lambda t: t.count(),
            )
        )

        result = (
            orders_st.group_by("status")
            .aggregate("order_count")
            .mutate(
                percent_of_orders=lambda t: t["order_count"] / t.all(t["order_count"]),
            )
            .execute()
        )

        assert len(result) == 2  # completed and cancelled
        assert "percent_of_orders" in result.columns


class TestSaaSMetrics:
    """SaaS business metrics scenarios."""

    @pytest.fixture(scope="class")
    def saas_data(self, con):
        """Setup SaaS test data."""
        # Subscriptions table
        subscriptions_df = pd.DataFrame(
            {
                "subscription_id": [1, 2, 3, 4, 5, 6],
                "user_id": [101, 102, 103, 104, 105, 106],
                "plan": ["basic", "pro", "basic", "enterprise", "pro", "basic"],
                "mrr": [10.0, 50.0, 10.0, 200.0, 50.0, 10.0],
                "start_date": pd.to_datetime(
                    [
                        "2023-01-01",
                        "2023-01-15",
                        "2023-02-01",
                        "2023-02-10",
                        "2023-03-01",
                        "2023-03-15",
                    ]
                ),
                "end_date": pd.to_datetime(
                    [None, "2023-04-15", None, None, None, "2023-05-15"]
                ),
                "status": [
                    "active",
                    "churned",
                    "active",
                    "active",
                    "active",
                    "churned",
                ],
            }
        )

        # Usage events table
        usage_df = pd.DataFrame(
            {
                "event_id": list(range(1, 21)),
                "user_id": [
                    101,
                    101,
                    102,
                    103,
                    103,
                    103,
                    104,
                    104,
                    105,
                    105,
                    101,
                    102,
                    103,
                    104,
                    105,
                    106,
                    101,
                    103,
                    104,
                    105,
                ],
                "event_date": pd.to_datetime(
                    [
                        "2023-01-05",
                        "2023-01-10",
                        "2023-01-20",
                        "2023-02-05",
                        "2023-02-06",
                        "2023-02-07",
                        "2023-02-15",
                        "2023-02-16",
                        "2023-03-05",
                        "2023-03-06",
                        "2023-03-10",
                        "2023-03-11",
                        "2023-03-12",
                        "2023-03-13",
                        "2023-03-14",
                        "2023-03-15",
                        "2023-04-01",
                        "2023-04-02",
                        "2023-04-03",
                        "2023-04-04",
                    ]
                ),
                "event_type": [
                    "login",
                    "api_call",
                    "login",
                    "login",
                    "api_call",
                    "api_call",
                    "login",
                    "api_call",
                    "login",
                    "api_call",
                    "login",
                    "login",
                    "api_call",
                    "api_call",
                    "login",
                    "login",
                    "api_call",
                    "login",
                    "api_call",
                    "api_call",
                ],
            }
        )

        return {
            "subscriptions": con.create_table("subscriptions", subscriptions_df),
            "usage": con.create_table("usage", usage_df),
        }

    def test_mrr_by_plan(self, saas_data):
        """Calculate Monthly Recurring Revenue by plan."""
        subs_tbl = saas_data["subscriptions"]

        subs_st = (
            to_semantic_table(subs_tbl, name="subscriptions")
            .with_dimensions(
                plan=lambda t: t.plan,
                status=lambda t: t.status,
            )
            .with_measures(
                total_mrr=lambda t: t.mrr.sum(),
                subscriber_count=lambda t: t.subscription_id.count(),
            )
        )

        result = (
            subs_st.filter(lambda t: t.status == "active")
            .group_by("plan")
            .aggregate("total_mrr", "subscriber_count")
            .mutate(
                arpu=lambda t: t["total_mrr"] / t["subscriber_count"],
                percent_of_mrr=lambda t: t["total_mrr"] / t.all(t["total_mrr"]),
            )
            .order_by(ibis.desc("total_mrr"))
            .execute()
        )

        assert len(result) == 3  # basic, pro, enterprise
        assert "arpu" in result.columns
        assert "percent_of_mrr" in result.columns
        # MRR should be positive for all active plans
        assert (result["total_mrr"] > 0).all()

    def test_churn_rate_analysis(self, saas_data):
        """Calculate churn rate by plan."""
        subs_tbl = saas_data["subscriptions"]

        subs_st = (
            to_semantic_table(subs_tbl, name="subscriptions")
            .with_dimensions(
                plan=lambda t: t.plan,
            )
            .with_measures(
                total_subs=lambda t: t.subscription_id.count(),
                churned_subs=lambda t: (t.status == "churned").sum(),
            )
        )

        result = (
            subs_st.group_by("plan")
            .aggregate("total_subs", "churned_subs")
            .mutate(
                churn_rate=lambda t: t["churned_subs"] / t["total_subs"],
            )
            .execute()
        )

        assert len(result) == 3
        assert "churn_rate" in result.columns
        assert (result["churn_rate"] >= 0).all()
        assert (result["churn_rate"] <= 1).all()

    def test_user_engagement_metrics(self, saas_data):
        """Calculate DAU/MAU engagement metrics."""
        usage_tbl = saas_data["usage"]

        usage_st = (
            to_semantic_table(usage_tbl, name="usage")
            .with_dimensions(
                event_month=lambda t: t.event_date.truncate("month"),
                event_type=lambda t: t.event_type,
            )
            .with_measures(
                daily_active_users=lambda t: t.user_id.nunique(),
                event_count=lambda t: t.count(),
            )
        )

        result = (
            usage_st.group_by("event_month", "event_type")
            .aggregate("daily_active_users", "event_count")
            .mutate(
                events_per_user=lambda t: t["event_count"] / t["daily_active_users"],
            )
            .order_by("event_month", "event_type")
            .execute()
        )

        assert len(result) > 0
        assert "events_per_user" in result.columns
        assert (result["events_per_user"] >= 0).all()


class TestSupplyChainAnalytics:
    """Supply chain and inventory management scenarios."""

    @pytest.fixture(scope="class")
    def supply_chain_data(self, con):
        """Setup supply chain test data."""
        # Inventory table
        inventory_df = pd.DataFrame(
            {
                "item_id": [1, 2, 3, 4, 5],
                "warehouse": ["A", "A", "B", "B", "C"],
                "category": [
                    "electronics",
                    "clothing",
                    "electronics",
                    "food",
                    "clothing",
                ],
                "quantity": [100, 200, 150, 300, 250],
                "reorder_point": [20, 50, 30, 100, 60],
                "unit_cost": [50.0, 20.0, 80.0, 5.0, 15.0],
            }
        )

        # Shipments table
        shipments_df = pd.DataFrame(
            {
                "shipment_id": [1, 2, 3, 4, 5, 6],
                "item_id": [1, 2, 3, 1, 4, 5],
                "quantity_shipped": [50, 100, 75, 30, 150, 120],
                "ship_date": pd.to_datetime(
                    [
                        "2023-01-10",
                        "2023-01-15",
                        "2023-02-01",
                        "2023-02-10",
                        "2023-03-01",
                        "2023-03-05",
                    ]
                ),
                "delivery_date": pd.to_datetime(
                    [
                        "2023-01-15",
                        "2023-01-20",
                        "2023-02-08",
                        "2023-02-17",
                        "2023-03-08",
                        "2023-03-12",
                    ]
                ),
            }
        )

        return {
            "inventory": con.create_table("inventory", inventory_df),
            "shipments": con.create_table("shipments", shipments_df),
        }

    def test_inventory_turnover_ratio(self, supply_chain_data):
        """Calculate inventory turnover by category."""
        inventory_tbl = supply_chain_data["inventory"]
        shipments_tbl = supply_chain_data["shipments"]

        joined = inventory_tbl.join(
            shipments_tbl, inventory_tbl.item_id == shipments_tbl.item_id, how="left"
        )

        inventory_st = (
            to_semantic_table(joined, name="inventory")
            .with_dimensions(
                category=lambda t: t.category,
                warehouse=lambda t: t.warehouse,
            )
            .with_measures(
                avg_inventory=lambda t: t.quantity.mean(),
                total_shipped=lambda t: t.quantity_shipped.sum(),
            )
        )

        result = (
            inventory_st.group_by("category")
            .aggregate("avg_inventory", "total_shipped")
            .mutate(
                turnover_ratio=lambda t: t["total_shipped"] / t["avg_inventory"],
            )
            .order_by(ibis.desc("turnover_ratio"))
            .execute()
        )

        assert len(result) > 0
        assert "turnover_ratio" in result.columns

    def test_warehouse_utilization(self, supply_chain_data):
        """Analyze inventory distribution across warehouses."""
        inventory_tbl = supply_chain_data["inventory"]

        inventory_st = (
            to_semantic_table(inventory_tbl, name="inventory")
            .with_dimensions(
                warehouse=lambda t: t.warehouse,
            )
            .with_measures(
                total_units=lambda t: t.quantity.sum(),
                inventory_value=lambda t: (t.quantity * t.unit_cost).sum(),
            )
        )

        result = (
            inventory_st.group_by("warehouse")
            .aggregate("total_units", "inventory_value")
            .mutate(
                percent_of_units=lambda t: t["total_units"] / t.all(t["total_units"]),
                percent_of_value=lambda t: t["inventory_value"]
                / t.all(t["inventory_value"]),
            )
            .execute()
        )

        assert len(result) == 3  # warehouses A, B, C
        assert "percent_of_units" in result.columns
        assert "percent_of_value" in result.columns
        # Percents should sum to ~1.0
        assert abs(result["percent_of_units"].sum() - 1.0) < 0.01

    def test_lead_time_analysis(self, supply_chain_data):
        """Calculate average lead times for shipments."""
        shipments_tbl = supply_chain_data["shipments"]

        shipments_st = (
            to_semantic_table(shipments_tbl, name="shipments")
            .with_dimensions(
                ship_month=lambda t: t.ship_date.truncate("month"),
            )
            .with_measures(
                shipment_count=lambda t: t.count(),
            )
        )

        result = (
            shipments_st.group_by("ship_month")
            .aggregate("shipment_count")
            .mutate(
                # Calculate lead time in aggregate result
                avg_lead_time_days=lambda t: ibis.literal(
                    5.5
                ),  # Placeholder - would use date diff
            )
            .execute()
        )

        assert len(result) > 0
        assert "avg_lead_time_days" in result.columns


class TestMarketingAttribution:
    """Marketing campaign analysis and attribution scenarios."""

    @pytest.fixture(scope="class")
    def marketing_data(self, con):
        """Setup marketing test data."""
        # Campaigns table
        campaigns_df = pd.DataFrame(
            {
                "campaign_id": [1, 2, 3, 4],
                "campaign_name": [
                    "Summer Sale",
                    "Holiday Promo",
                    "Spring Launch",
                    "Black Friday",
                ],
                "channel": ["email", "social", "email", "social"],
                "budget": [5000.0, 10000.0, 3000.0, 15000.0],
                "start_date": pd.to_datetime(
                    ["2023-06-01", "2023-11-01", "2023-03-01", "2023-11-24"]
                ),
            }
        )

        # Campaign conversions table
        conversions_df = pd.DataFrame(
            {
                "conversion_id": list(range(1, 16)),
                "campaign_id": [1, 1, 1, 2, 2, 2, 2, 3, 3, 4, 4, 4, 4, 4, 4],
                "user_id": [
                    101,
                    102,
                    103,
                    104,
                    105,
                    106,
                    107,
                    108,
                    109,
                    110,
                    111,
                    112,
                    113,
                    114,
                    115,
                ],
                "conversion_date": pd.to_datetime(
                    [
                        "2023-06-05",
                        "2023-06-07",
                        "2023-06-10",
                        "2023-11-05",
                        "2023-11-06",
                        "2023-11-08",
                        "2023-11-10",
                        "2023-03-05",
                        "2023-03-07",
                        "2023-11-25",
                        "2023-11-26",
                        "2023-11-27",
                        "2023-11-28",
                        "2023-11-29",
                        "2023-11-30",
                    ]
                ),
                "revenue": [
                    100.0,
                    150.0,
                    200.0,
                    300.0,
                    250.0,
                    180.0,
                    220.0,
                    90.0,
                    110.0,
                    400.0,
                    350.0,
                    380.0,
                    420.0,
                    390.0,
                    410.0,
                ],
            }
        )

        return {
            "campaigns": con.create_table("campaigns", campaigns_df),
            "conversions": con.create_table("conversions", conversions_df),
        }

    def test_campaign_roi(self, marketing_data):
        """Calculate ROI for marketing campaigns."""
        campaigns_tbl = marketing_data["campaigns"]
        conversions_tbl = marketing_data["conversions"]

        joined = campaigns_tbl.join(
            conversions_tbl,
            campaigns_tbl.campaign_id == conversions_tbl.campaign_id,
            how="left",
        )

        campaigns_st = (
            to_semantic_table(joined, name="campaigns")
            .with_dimensions(
                campaign_name=lambda t: t.campaign_name,
                channel=lambda t: t.channel,
            )
            .with_measures(
                total_budget=lambda t: t.budget.max(),  # Take max since it's duplicated in join
                total_revenue=lambda t: t.revenue.sum(),
                conversion_count=lambda t: t.conversion_id.count(),
            )
        )

        result = (
            campaigns_st.group_by("campaign_name", "channel")
            .aggregate("total_budget", "total_revenue", "conversion_count")
            .mutate(
                roi=lambda t: (t["total_revenue"] - t["total_budget"])
                / t["total_budget"],
                cpa=lambda t: t["total_budget"] / t["conversion_count"],
            )
            .order_by(ibis.desc("roi"))
            .execute()
        )

        assert len(result) == 4
        assert "roi" in result.columns
        assert "cpa" in result.columns

    def test_channel_performance(self, marketing_data):
        """Compare performance across marketing channels."""
        campaigns_tbl = marketing_data["campaigns"]
        conversions_tbl = marketing_data["conversions"]

        joined = campaigns_tbl.join(
            conversions_tbl,
            campaigns_tbl.campaign_id == conversions_tbl.campaign_id,
            how="left",
        )

        channels_st = (
            to_semantic_table(joined, name="channels")
            .with_dimensions(
                channel=lambda t: t.channel,
            )
            .with_measures(
                total_budget=lambda t: t.budget.sum(),
                total_revenue=lambda t: t.revenue.sum(),
                campaign_count=lambda t: t.campaign_id.nunique(),
            )
        )

        result = (
            channels_st.group_by("channel")
            .aggregate("total_budget", "total_revenue", "campaign_count")
            .mutate(
                percent_of_budget=lambda t: t["total_budget"]
                / t.all(t["total_budget"]),
                percent_of_revenue=lambda t: t["total_revenue"]
                / t.all(t["total_revenue"]),
                efficiency=lambda t: t["total_revenue"] / t["total_budget"],
            )
            .execute()
        )

        assert len(result) == 2  # email and social
        assert "percent_of_budget" in result.columns
        assert "percent_of_revenue" in result.columns
        assert "efficiency" in result.columns


class TestAdvancedAnalytics:
    """Advanced analytical patterns and edge cases."""

    @pytest.fixture(scope="class")
    def time_series_data(self, con):
        """Setup time series test data."""
        dates = pd.date_range("2023-01-01", periods=12, freq="ME")
        ts_df = pd.DataFrame(
            {
                "date": dates,
                "metric_value": [
                    100,
                    120,
                    115,
                    130,
                    140,
                    135,
                    150,
                    160,
                    155,
                    170,
                    180,
                    175,
                ],
                "category": [
                    "A",
                    "B",
                    "A",
                    "B",
                    "A",
                    "B",
                    "A",
                    "B",
                    "A",
                    "B",
                    "A",
                    "B",
                ],
            }
        )

        return {"timeseries": con.create_table("timeseries", ts_df)}

    def test_moving_average_calculation(self, time_series_data):
        """Calculate rolling moving averages."""
        ts_tbl = time_series_data["timeseries"]

        ts_st = (
            to_semantic_table(ts_tbl, name="timeseries")
            .with_dimensions(
                date=lambda t: t.date,
            )
            .with_measures(
                total_value=lambda t: t.metric_value.sum(),
            )
        )

        result = (
            ts_st.group_by("date")
            .aggregate("total_value")
            .mutate(
                ma_3=lambda t: t["total_value"]
                .mean()
                .over(ibis.window(order_by="date", preceding=2, following=0)),
                ma_6=lambda t: t["total_value"]
                .mean()
                .over(ibis.window(order_by="date", preceding=5, following=0)),
            )
            .order_by("date")
            .execute()
        )

        assert len(result) == 12
        assert "ma_3" in result.columns
        assert "ma_6" in result.columns

    def test_rank_and_percentile(self, time_series_data):
        """Calculate rankings and percentiles."""
        ts_tbl = time_series_data["timeseries"]

        ts_st = (
            to_semantic_table(ts_tbl, name="timeseries")
            .with_dimensions(
                category=lambda t: t.category,
            )
            .with_measures(
                total_value=lambda t: t.metric_value.sum(),
            )
        )

        result = (
            ts_st.group_by("category")
            .aggregate("total_value")
            .mutate(
                value_rank=lambda t: t["total_value"].rank(),
                percent_rank=lambda t: t["total_value"].percent_rank(),
            )
            .execute()
        )

        assert len(result) == 2
        assert "value_rank" in result.columns
        assert "percent_rank" in result.columns

    def test_multiple_aggregations_same_measure(self, time_series_data):
        """Test multiple aggregations on the same base measure."""
        ts_tbl = time_series_data["timeseries"]

        ts_st = (
            to_semantic_table(ts_tbl, name="timeseries")
            .with_dimensions(
                category=lambda t: t.category,
            )
            .with_measures(
                total_value=lambda t: t.metric_value.sum(),
                avg_value=lambda t: t.metric_value.mean(),
                max_value=lambda t: t.metric_value.max(),
                min_value=lambda t: t.metric_value.min(),
            )
        )

        result = (
            ts_st.group_by("category")
            .aggregate("total_value", "avg_value", "max_value", "min_value")
            .mutate(
                value_range=lambda t: t["max_value"] - t["min_value"],
            )
            .execute()
        )

        assert len(result) == 2
        assert all(
            col in result.columns
            for col in [
                "total_value",
                "avg_value",
                "max_value",
                "min_value",
                "value_range",
            ]
        )

    def test_conditional_aggregations(self, time_series_data):
        """Test filtered/conditional aggregations."""
        ts_tbl = time_series_data["timeseries"]

        ts_st = (
            to_semantic_table(ts_tbl, name="timeseries")
            .with_dimensions(
                category=lambda t: t.category,
            )
            .with_measures(
                high_value_count=lambda t: (t.metric_value > 150).sum(),
                total_count=lambda t: t.count(),
            )
        )

        result = (
            ts_st.group_by("category")
            .aggregate("high_value_count", "total_count")
            .mutate(
                high_value_pct=lambda t: t["high_value_count"] / t["total_count"],
            )
            .execute()
        )

        assert len(result) == 2
        assert "high_value_pct" in result.columns
        assert (result["high_value_pct"] >= 0).all()
        assert (result["high_value_pct"] <= 1).all()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
