"""Tests for OSI (Open Semantic Interchange) support.

Import tests use ``from_config`` (the native entry point) to verify that OSI
format is auto-detected and parsed correctly — no separate ``from_osi`` call
needed.  Export tests use ``to_osi`` / ``to_osi_yaml``.
"""

import json

import ibis
import pytest

from boring_semantic_layer import (
    Dimension,
    Measure,
    from_config,
    to_semantic_table,
)
from boring_semantic_layer.osi import (
    OSI_VERSION,
    _deferred_to_sql,
    _ibis_string_to_sql,
    to_osi,
    to_osi_yaml,
)
from boring_semantic_layer.yaml import (
    _is_osi_config,
    _sql_to_deferred,
    _strip_dataset_prefix,
)


# ---------------------------------------------------------------------------
# Expression conversion tests
# ---------------------------------------------------------------------------


class TestIbisStringToSql:
    def test_simple_column(self):
        assert _ibis_string_to_sql("_.column_name") == "column_name"

    def test_count(self):
        assert _ibis_string_to_sql("_.count()") == "COUNT(*)"

    def test_sum(self):
        assert _ibis_string_to_sql("_.amount.sum()") == "SUM(amount)"

    def test_mean(self):
        assert _ibis_string_to_sql("_.amount.mean()") == "AVG(amount)"

    def test_max(self):
        assert _ibis_string_to_sql("_.amount.max()") == "MAX(amount)"

    def test_min(self):
        assert _ibis_string_to_sql("_.amount.min()") == "MIN(amount)"

    def test_nunique(self):
        assert _ibis_string_to_sql("_.customer_id.nunique()") == "COUNT(DISTINCT customer_id)"


class TestDeferredToSql:
    def test_simple_column(self):
        assert _deferred_to_sql(ibis._.column_name) == "column_name"

    def test_count(self):
        assert _deferred_to_sql(ibis._.count()) == "COUNT(*)"

    def test_sum(self):
        assert _deferred_to_sql(ibis._.amount.sum()) == "SUM(amount)"

    def test_mean(self):
        assert _deferred_to_sql(ibis._.amount.mean()) == "AVG(amount)"


class TestSqlToDeferred:
    def test_simple_column(self):
        d = _sql_to_deferred("column_name")
        assert "column_name" in str(d)

    def test_count_star(self):
        d = _sql_to_deferred("COUNT(*)")
        assert "count" in str(d).lower()

    def test_sum(self):
        d = _sql_to_deferred("SUM(amount)")
        assert "sum" in str(d).lower() or "amount" in str(d).lower()

    def test_avg(self):
        d = _sql_to_deferred("AVG(price)")
        assert "mean" in str(d).lower() or "price" in str(d).lower()


class TestStripDatasetPrefix:
    def test_sum_with_prefix(self):
        assert _strip_dataset_prefix("SUM(flights.distance)") == "SUM(distance)"

    def test_count_star(self):
        assert _strip_dataset_prefix("COUNT(*)") == "COUNT(*)"

    def test_count_distinct_with_prefix(self):
        assert _strip_dataset_prefix("COUNT(DISTINCT customers.id)") == "COUNT(DISTINCT id)"

    def test_no_prefix(self):
        assert _strip_dataset_prefix("SUM(distance)") == "SUM(distance)"


# ---------------------------------------------------------------------------
# Format detection
# ---------------------------------------------------------------------------


class TestFormatDetection:
    def test_detects_osi(self):
        assert _is_osi_config({"version": "0.1.1", "semantic_model": []})

    def test_rejects_bsl(self):
        assert not _is_osi_config({"flights": {"table": "flights_tbl"}})

    def test_rejects_partial(self):
        assert not _is_osi_config({"version": "0.1.1"})
        assert not _is_osi_config({"semantic_model": []})


# ---------------------------------------------------------------------------
# Export tests: BSL -> OSI
# ---------------------------------------------------------------------------


@pytest.fixture
def simple_model():
    """A simple BSL model with dimensions and measures."""
    table = ibis.table(
        {"order_id": "int64", "customer_id": "int64", "amount": "float64", "created_at": "timestamp"},
        name="orders",
    )
    model = to_semantic_table(table, name="orders", description="Order transactions")
    model = model.with_dimensions(
        order_id=Dimension(expr=ibis._.order_id, description="Order ID", is_entity=True),
        customer_id=Dimension(expr=ibis._.customer_id, description="Customer ID"),
        created_at=Dimension(
            expr=ibis._.created_at,
            description="Order creation timestamp",
            is_time_dimension=True,
            smallest_time_grain="TIME_GRAIN_DAY",
        ),
    )
    model = model.with_measures(
        order_count=Measure(expr=ibis._.count(), description="Total orders"),
        total_amount=Measure(expr=ibis._.amount.sum(), description="Total order amount"),
        avg_amount=Measure(expr=ibis._.amount.mean(), description="Average order amount"),
    )
    return model


@pytest.fixture
def model_with_ai_context():
    """A BSL model with ai_context on dimensions and measures."""
    table = ibis.table(
        {"product_id": "int64", "name": "string", "price": "float64"},
        name="products",
    )
    model = to_semantic_table(table, name="products", description="Product catalog")
    model = model.with_dimensions(
        product_id=Dimension(
            expr=ibis._.product_id,
            description="Product identifier",
            is_entity=True,
            ai_context={"synonyms": ["SKU", "item ID"]},
        ),
        name=Dimension(
            expr=ibis._.name,
            description="Product name",
            ai_context="Product display name shown to customers",
        ),
    )
    model = model.with_measures(
        avg_price=Measure(
            expr=ibis._.price.mean(),
            description="Average product price",
            ai_context={"synonyms": ["mean price", "price average"]},
        ),
    )
    return model


class TestToOsi:
    def test_basic_structure(self, simple_model):
        osi = to_osi(simple_model, name="test_model")
        assert osi["version"] == OSI_VERSION
        assert len(osi["semantic_model"]) == 1
        sm = osi["semantic_model"][0]
        assert sm["name"] == "test_model"
        assert "datasets" in sm

    def test_dataset_fields(self, simple_model):
        osi = to_osi(simple_model)
        ds = osi["semantic_model"][0]["datasets"][0]
        assert ds["name"] == "orders"
        assert ds["description"] == "Order transactions"
        field_names = {f["name"] for f in ds["fields"]}
        assert {"order_id", "customer_id", "created_at"} <= field_names

    def test_primary_key_from_entity(self, simple_model):
        osi = to_osi(simple_model)
        ds = osi["semantic_model"][0]["datasets"][0]
        assert "order_id" in ds["primary_key"]

    def test_time_dimension(self, simple_model):
        osi = to_osi(simple_model)
        ds = osi["semantic_model"][0]["datasets"][0]
        created_at = next(f for f in ds["fields"] if f["name"] == "created_at")
        assert created_at["dimension"]["is_time"] is True

    def test_non_time_dimension(self, simple_model):
        osi = to_osi(simple_model)
        ds = osi["semantic_model"][0]["datasets"][0]
        customer = next(f for f in ds["fields"] if f["name"] == "customer_id")
        assert customer["dimension"]["is_time"] is False

    def test_metrics(self, simple_model):
        osi = to_osi(simple_model)
        metric_names = {m["name"] for m in osi["semantic_model"][0]["metrics"]}
        assert {"order_count", "total_amount", "avg_amount"} <= metric_names

    def test_metric_expressions(self, simple_model):
        osi = to_osi(simple_model)
        count_metric = next(m for m in osi["semantic_model"][0]["metrics"] if m["name"] == "order_count")
        assert count_metric["expression"]["dialects"][0]["dialect"] == "ANSI_SQL"

    def test_field_expression_format(self, simple_model):
        osi = to_osi(simple_model)
        ds = osi["semantic_model"][0]["datasets"][0]
        f = next(f for f in ds["fields"] if f["name"] == "order_id")
        assert f["expression"]["dialects"][0] == {"dialect": "ANSI_SQL", "expression": "order_id"}

    def test_custom_extensions_for_bsl_metadata(self, simple_model):
        osi = to_osi(simple_model)
        ds = osi["semantic_model"][0]["datasets"][0]
        f = next(f for f in ds["fields"] if f["name"] == "order_id")
        data = json.loads(f["custom_extensions"][0]["data"])
        assert data["is_entity"] is True

    def test_time_grain_in_custom_extensions(self, simple_model):
        osi = to_osi(simple_model)
        ds = osi["semantic_model"][0]["datasets"][0]
        f = next(f for f in ds["fields"] if f["name"] == "created_at")
        data = json.loads(f["custom_extensions"][0]["data"])
        assert data["smallest_time_grain"] == "TIME_GRAIN_DAY"

    def test_ai_context_on_dimensions(self, model_with_ai_context):
        osi = to_osi(model_with_ai_context)
        ds = osi["semantic_model"][0]["datasets"][0]
        f = next(f for f in ds["fields"] if f["name"] == "product_id")
        assert f["ai_context"]["synonyms"] == ["SKU", "item ID"]

    def test_ai_context_on_metrics(self, model_with_ai_context):
        osi = to_osi(model_with_ai_context)
        m = next(m for m in osi["semantic_model"][0]["metrics"] if m["name"] == "avg_price")
        assert m["ai_context"]["synonyms"] == ["mean price", "price average"]

    def test_with_description_and_ai_context(self, simple_model):
        osi = to_osi(simple_model, name="my_model", description="A test", ai_context={"instructions": "test"})
        sm = osi["semantic_model"][0]
        assert sm["description"] == "A test"
        assert sm["ai_context"]["instructions"] == "test"

    def test_multiple_models(self):
        m1 = to_semantic_table(ibis.table({"id": "int64", "amount": "float64"}, name="o"), name="orders")
        m1 = m1.with_dimensions(id=Dimension(expr=ibis._.id))
        m1 = m1.with_measures(total=Measure(expr=ibis._.amount.sum()))
        m2 = to_semantic_table(ibis.table({"id": "int64", "name": "string"}, name="c"), name="customers")
        m2 = m2.with_dimensions(id=Dimension(expr=ibis._.id))
        osi = to_osi({"orders": m1, "customers": m2}, name="ecommerce")
        assert len(osi["semantic_model"][0]["datasets"]) == 2


class TestToOsiYaml:
    def test_yaml_output(self, simple_model):
        s = to_osi_yaml(simple_model, name="test")
        assert "version:" in s
        assert "semantic_model:" in s
        assert "datasets:" in s


# ---------------------------------------------------------------------------
# Import tests: OSI parsed natively via from_config
# ---------------------------------------------------------------------------


@pytest.fixture
def osi_config():
    """A minimal OSI config dict."""
    return {
        "version": "0.1.1",
        "semantic_model": [
            {
                "name": "test_model",
                "datasets": [
                    {
                        "name": "orders",
                        "source": "orders_table",
                        "primary_key": ["order_id"],
                        "description": "Order transactions",
                        "fields": [
                            {
                                "name": "order_id",
                                "expression": {"dialects": [{"dialect": "ANSI_SQL", "expression": "order_id"}]},
                                "dimension": {"is_time": False},
                                "description": "Order identifier",
                                "custom_extensions": [{"vendor_name": "COMMON", "data": json.dumps({"is_entity": True})}],
                            },
                            {
                                "name": "created_at",
                                "expression": {"dialects": [{"dialect": "ANSI_SQL", "expression": "created_at"}]},
                                "dimension": {"is_time": True},
                                "description": "Creation timestamp",
                            },
                            {
                                "name": "amount",
                                "expression": {"dialects": [{"dialect": "ANSI_SQL", "expression": "amount"}]},
                                "dimension": {"is_time": False},
                            },
                        ],
                    },
                ],
                "metrics": [
                    {
                        "name": "total_amount",
                        "expression": {"dialects": [{"dialect": "ANSI_SQL", "expression": "SUM(orders.amount)"}]},
                        "description": "Total order amount",
                    },
                    {
                        "name": "order_count",
                        "expression": {"dialects": [{"dialect": "ANSI_SQL", "expression": "COUNT(*)"}]},
                        "description": "Number of orders",
                    },
                ],
            }
        ],
    }


class TestFromConfig_OSI:
    """Test that from_config auto-detects and parses OSI format."""

    def test_basic_import(self, osi_config):
        models = from_config(osi_config)
        assert "orders" in models

    def test_model_description(self, osi_config):
        models = from_config(osi_config)
        assert models["orders"].op().description == "Order transactions"

    def test_dimensions_imported(self, osi_config):
        dims = from_config(osi_config)["orders"].op().get_dimensions()
        assert {"order_id", "created_at", "amount"} == set(dims.keys())

    def test_dimension_descriptions(self, osi_config):
        dims = from_config(osi_config)["orders"].op().get_dimensions()
        assert dims["order_id"].description == "Order identifier"
        assert dims["created_at"].description == "Creation timestamp"

    def test_time_dimension_flag(self, osi_config):
        dims = from_config(osi_config)["orders"].op().get_dimensions()
        assert dims["created_at"].is_time_dimension is True
        assert dims["order_id"].is_time_dimension is False

    def test_entity_from_custom_extensions(self, osi_config):
        dims = from_config(osi_config)["orders"].op().get_dimensions()
        assert dims["order_id"].is_entity is True

    def test_measures_imported(self, osi_config):
        measures = from_config(osi_config)["orders"].op().get_measures()
        assert {"total_amount", "order_count"} == set(measures.keys())

    def test_measure_descriptions(self, osi_config):
        measures = from_config(osi_config)["orders"].op().get_measures()
        assert measures["total_amount"].description == "Total order amount"

    def test_with_real_table(self, osi_config):
        con = ibis.duckdb.connect()
        con.raw_sql("CREATE TABLE orders_table (order_id INT, created_at TIMESTAMP, amount DOUBLE)")
        models = from_config(osi_config, tables={"orders": con.table("orders_table")})
        assert "orders" in models

    def test_ai_context_preserved(self):
        config = {
            "version": "0.1.1",
            "semantic_model": [{
                "name": "test",
                "datasets": [{
                    "name": "items",
                    "source": "items",
                    "fields": [{
                        "name": "item_id",
                        "expression": {"dialects": [{"dialect": "ANSI_SQL", "expression": "item_id"}]},
                        "ai_context": {"synonyms": ["SKU", "product_id"]},
                    }],
                }],
            }],
        }
        dims = from_config(config)["items"].op().get_dimensions()
        assert dims["item_id"].ai_context == {"synonyms": ["SKU", "product_id"]}


# ---------------------------------------------------------------------------
# Round-trip tests
# ---------------------------------------------------------------------------


class TestRoundTrip:
    def test_bsl_to_osi_to_bsl(self, simple_model):
        """BSL -> OSI -> BSL preserves key semantics."""
        osi = to_osi(simple_model, name="round_trip_test")
        models = from_config(osi)  # auto-detects OSI
        assert "orders" in models

        orig_dims = simple_model.op().get_dimensions()
        new_dims = models["orders"].op().get_dimensions()
        assert set(orig_dims.keys()) == set(new_dims.keys())

        for name in orig_dims:
            assert orig_dims[name].description == new_dims[name].description

        assert new_dims["created_at"].is_time_dimension is True
        assert new_dims["order_id"].is_entity is True

    def test_bsl_to_osi_to_bsl_measures(self, simple_model):
        osi = to_osi(simple_model)
        models = from_config(osi)

        orig = simple_model.op().get_measures()
        new = models["orders"].op().get_measures()
        assert set(orig.keys()) == set(new.keys())
        for name in orig:
            assert orig[name].description == new[name].description

    def test_osi_to_bsl_to_osi(self, osi_config):
        models = from_config(osi_config)
        osi_out = to_osi(models, name="round_trip")

        assert osi_out["version"] == OSI_VERSION
        ds_out = osi_out["semantic_model"][0]["datasets"][0]
        assert ds_out["name"] == "orders"
        assert ds_out["description"] == "Order transactions"
        ds_in = osi_config["semantic_model"][0]["datasets"][0]
        assert len(ds_out["fields"]) == len(ds_in["fields"])

    def test_ai_context_round_trip(self, model_with_ai_context):
        osi = to_osi(model_with_ai_context)
        models = from_config(osi)

        dims = models["products"].op().get_dimensions()
        assert dims["product_id"].ai_context == {"synonyms": ["SKU", "item ID"]}
        assert dims["name"].ai_context == "Product display name shown to customers"

        measures = models["products"].op().get_measures()
        assert measures["avg_price"].ai_context == {"synonyms": ["mean price", "price average"]}
