"""Tests for the ``metadata`` field on Dimension and Measure."""

from pathlib import Path
import tempfile

import ibis

from boring_semantic_layer import from_yaml, to_semantic_table
from boring_semantic_layer.ops import Dimension, Measure


def _build_table():
    con = ibis.duckdb.connect(":memory:")
    data = ibis.memtable(
        {
            "country": ["SE", "DE", "GB"],
            "sales": [10.0, 20.0, 30.0],
        }
    )
    return con.create_table("orders", data)


def test_dimension_metadata_defaults_empty():
    dim = Dimension(expr=lambda t: t.country, description="Country")
    assert dim.metadata == {}
    assert dim.to_json() == {"description": "Country"}


def test_measure_metadata_defaults_empty():
    meas = Measure(expr=lambda t: t.sales.sum(), description="Total sales")
    assert meas.metadata == {}
    assert meas.to_json() == {"description": "Total sales"}


def test_dimension_metadata_flows_into_to_json():
    dim = Dimension(
        expr=lambda t: t.country,
        description="Country ISO code",
        is_entity=True,
        metadata={
            "entity_type": "market",
            "format": "iso_country",
            "example_values": ["SE", "DE", "GB"],
        },
    )
    payload = dim.to_json()
    assert payload["description"] == "Country ISO code"
    assert payload["is_entity"] is True
    assert payload["entity_type"] == "market"
    assert payload["format"] == "iso_country"
    assert payload["example_values"] == ["SE", "DE", "GB"]


def test_measure_metadata_flows_into_to_json():
    meas = Measure(
        expr=lambda t: t.sales.sum(),
        description="Total sales in EUR",
        metadata={"format": "currency_eur", "unit": "EUR", "is_additive": True},
    )
    payload = meas.to_json()
    assert payload["description"] == "Total sales in EUR"
    assert payload["format"] == "currency_eur"
    assert payload["unit"] == "EUR"
    assert payload["is_additive"] is True


def test_metadata_appears_in_json_definition():
    tbl = _build_table()
    model = (
        to_semantic_table(tbl, name="orders")
        .with_dimensions(
            country=Dimension(
                expr=lambda t: t.country,
                description="Country",
                is_entity=True,
                metadata={"entity_type": "market", "format": "iso_country"},
            )
        )
        .with_measures(
            total_sales=Measure(
                expr=lambda t: t.sales.sum(),
                description="Total sales",
                metadata={"format": "currency_eur", "unit": "EUR"},
            )
        )
    )
    json_def = model.json_definition
    country = json_def["dimensions"]["country"]
    assert country["entity_type"] == "market"
    assert country["format"] == "iso_country"
    total = json_def["measures"]["total_sales"]
    assert total["format"] == "currency_eur"
    assert total["unit"] == "EUR"


def test_metadata_equality_ignored():
    a = Dimension(expr=lambda t: t.country, metadata={"format": "iso_country"})
    b = Dimension(expr=a.expr, metadata={"format": "something_else"})
    assert a == b
    assert hash(a) == hash(b)


def test_metadata_overrides_base_fields():
    """Metadata is merged after base fields, so matching keys win."""
    dim = Dimension(
        expr=lambda t: t.country,
        description="real",
        metadata={"description": "override"},
    )
    assert dim.to_json()["description"] == "override"


def test_yaml_dimension_metadata_round_trip():
    yaml_content = """
orders:
  table: orders_tbl
  dimensions:
    country:
      expr: _.country
      description: Country ISO code
      is_entity: true
      metadata:
        entity_type: market
        format: iso_country
        example_values: [SE, DE, GB]
  measures:
    total_sales:
      expr: _.sales.sum()
      description: Total sales
      metadata:
        format: currency_eur
        unit: EUR
        is_additive: true
"""
    con = ibis.duckdb.connect(":memory:")
    data = ibis.memtable(
        {"country": ["SE", "DE"], "sales": [1.0, 2.0]}
    )
    tbl = con.create_table("orders_tbl", data)

    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "models.yml"
        path.write_text(yaml_content)
        models = from_yaml(path, tables={"orders_tbl": tbl})

    json_def = models["orders"].json_definition
    assert json_def["dimensions"]["country"]["entity_type"] == "market"
    assert json_def["dimensions"]["country"]["format"] == "iso_country"
    assert json_def["dimensions"]["country"]["example_values"] == ["SE", "DE", "GB"]
    assert json_def["measures"]["total_sales"]["format"] == "currency_eur"
    assert json_def["measures"]["total_sales"]["unit"] == "EUR"
    assert json_def["measures"]["total_sales"]["is_additive"] is True
