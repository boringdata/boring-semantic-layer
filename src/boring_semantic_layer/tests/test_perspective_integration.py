"""Tests for Perspective.js Arrow artifact helpers."""

import json
from decimal import Decimal

import ibis
import pandas as pd
import pytest

from boring_semantic_layer import to_semantic_table
from boring_semantic_layer.integrations.perspective import (
    infer_perspective_schema,
    perspective_viewer_config,
    write_perspective_artifact,
)


def test_infer_schema_from_arrow_backed_ibis_expression():
    expr = ibis.memtable(
        pd.DataFrame(
            {
                "region": ["EU"],
                "revenue": [Decimal("10.50")],
                "orders": pd.Series([1], dtype="Int64"),
                "day": pd.to_datetime(["2024-01-01"]),
            }
        ),
        schema={
            "region": "string",
            "revenue": "decimal(10, 2)",
            "orders": "int64",
            "day": "date",
        },
    )

    schema = infer_perspective_schema(expr)

    assert [column.to_dict() for column in schema] == [
        {"name": "region", "type": "string", "role": "dimension"},
        {"name": "revenue", "type": "float", "role": "measure"},
        {"name": "orders", "type": "integer", "role": "measure"},
        {"name": "day", "type": "date", "role": "time"},
    ]


def test_write_perspective_arrow_artifact(tmp_path):
    pyarrow = pytest.importorskip("pyarrow")
    ipc = pytest.importorskip("pyarrow.ipc")
    expr = ibis.memtable(
        pd.DataFrame({"region": ["EU"], "revenue": [Decimal("10.50")]}),
        schema={"region": "string", "revenue": "decimal(10, 2)"},
    )

    artifact = write_perspective_artifact(
        expr,
        tmp_path,
        id="sales",
        viewer=perspective_viewer_config(group_by=["region"]),
    )

    assert artifact.manifest_path.name == "sales.perspective.json"
    assert artifact.data_path.name == "sales.arrow"
    manifest = json.loads(artifact.manifest_path.read_text())
    assert manifest == artifact.manifest
    assert manifest["kind"] == "bsl.perspective.artifact"
    assert manifest["data_ref"] == {"format": "arrow", "path": "sales.arrow"}
    assert manifest["viewer"] == {"plugin": "Datagrid", "group_by": ["region"]}
    assert manifest["schema"] == [
        {"name": "region", "type": "string", "role": "dimension"},
        {"name": "revenue", "type": "float", "role": "measure"},
    ]
    with artifact.data_path.open("rb") as handle:
        arrow_schema = ipc.open_file(handle).schema
    assert pyarrow.types.is_decimal(arrow_schema.field("revenue").type)


def test_write_perspective_artifact_normalizes_viewer_values(tmp_path):
    expr = ibis.memtable(
        pd.DataFrame({"region": ["EU"], "revenue": [Decimal("10.50")]}),
        schema={"region": "string", "revenue": "decimal(10, 2)"},
    )

    artifact = write_perspective_artifact(
        expr,
        tmp_path,
        id="sales",
        viewer={"plugin": "Datagrid", "filter": [["revenue", ">", Decimal("1.5")]]},
    )

    assert artifact.manifest["viewer"] == {
        "plugin": "Datagrid",
        "filter": [["revenue", ">", 1.5]],
    }


def test_write_perspective_artifact_rejects_non_finite_viewer_values(tmp_path):
    expr = ibis.memtable(
        pd.DataFrame({"region": ["EU"], "revenue": [10.5]}),
        schema={"region": "string", "revenue": "float64"},
    )

    with pytest.raises(ValueError, match="Non-finite float"):
        write_perspective_artifact(
            expr,
            tmp_path,
            id="sales",
            viewer={"plugin": "Datagrid", "filter": [["revenue", ">", float("nan")]]},
        )

    assert not (tmp_path / "sales.arrow").exists()


def test_write_perspective_artifact_rejects_non_finite_decimal_viewer_values(tmp_path):
    expr = ibis.memtable(
        pd.DataFrame({"region": ["EU"], "revenue": [10.5]}),
        schema={"region": "string", "revenue": "float64"},
    )

    with pytest.raises(ValueError, match="Non-finite decimal"):
        write_perspective_artifact(
            expr,
            tmp_path,
            id="sales_decimal",
            viewer={"plugin": "Datagrid", "filter": [["revenue", ">", Decimal("NaN")]]},
        )

    assert not (tmp_path / "sales_decimal.arrow").exists()


def test_write_perspective_artifact_uses_to_pyarrow_without_execute(tmp_path):
    pyarrow = pytest.importorskip("pyarrow")

    class ArrowBackedQuery:
        def to_pyarrow(self):
            return pyarrow.table({"region": ["EU"], "revenue": [10.5]})

        def execute(self):
            raise AssertionError("Perspective artifact export should prefer Arrow")

    artifact = write_perspective_artifact(ArrowBackedQuery(), tmp_path, id="sales")

    assert artifact.manifest["schema"] == [
        {"name": "region", "type": "string", "role": "dimension"},
        {"name": "revenue", "type": "float", "role": "measure"},
    ]


def test_write_perspective_artifact_rejects_unsafe_id(tmp_path):
    df = pd.DataFrame({"region": ["EU"], "revenue": [10]})

    with pytest.raises(ValueError, match="safe file stem"):
        write_perspective_artifact(df, tmp_path, id="../outside")


def test_semantic_query_artifact_includes_roles(tmp_path):
    con = ibis.duckdb.connect(":memory:")
    table = con.create_table(
        "orders_perspective",
        pd.DataFrame({"year": [2024, 2024, 2025], "revenue": [10, 20, 7]}),
        overwrite=True,
    )
    orders = (
        to_semantic_table(table, name="orders")
        .with_dimensions(year=lambda t: t.year)
        .with_measures(revenue=lambda t: t.revenue.sum())
    )
    result = orders.group_by("year").aggregate("revenue")

    artifact = write_perspective_artifact(result, tmp_path, id="orders_by_year")

    assert artifact.manifest["schema"] == [
        {"name": "year", "type": "integer", "role": "dimension"},
        {"name": "revenue", "type": "integer", "role": "measure"},
    ]
    assert artifact.manifest["id"] == "orders_by_year"
