"""
OSI (Open Semantic Interchange) support for Boring Semantic Layer.

Export: ``to_osi`` / ``to_osi_yaml`` convert BSL models to OSI v0.1.1 YAML.
Import: ``from_yaml`` / ``from_config`` natively detect and parse OSI YAML
        (no separate import step needed).  ``from_osi`` / ``from_osi_yaml``
        are kept as convenience aliases.

See: https://github.com/open-semantic-interchange/OSI
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Any

from ibis.common.deferred import Deferred

from .expr import SemanticModel, SemanticTable
from .ops import Dimension, Measure, SemanticTableOp, _is_deferred
from .utils import expr_to_ibis_string

OSI_VERSION = "0.1.1"


# ---------------------------------------------------------------------------
# Expression helpers  (BSL -> SQL for OSI export)
# ---------------------------------------------------------------------------


def _deferred_to_sql(expr: Deferred) -> str:
    """Convert an Ibis Deferred expression to a simple SQL-like string."""
    return _ibis_string_to_sql(str(expr))


def _ibis_string_to_sql(s: str) -> str:
    """Convert an Ibis deferred string repr to SQL expression."""
    s = s.strip()

    if s == "_.count()":
        return "COUNT(*)"

    agg_map = {"sum": "SUM", "mean": "AVG", "max": "MAX", "min": "MIN"}
    for ibis_fn, sql_fn in agg_map.items():
        m = re.match(rf"^_\.(.+)\.{ibis_fn}\(\)$", s)
        if m:
            return f"{sql_fn}({m.group(1)})"

    m = re.match(r"^_\.(.+)\.nunique\(\)$", s)
    if m:
        return f"COUNT(DISTINCT {m.group(1)})"

    m = re.match(r"^_\.(\w+)$", s)
    if m:
        return m.group(1)

    if s.startswith("_."):
        return s[2:]
    return s


def _expr_to_sql_string(expr: Any) -> str | None:
    """Best-effort conversion of a BSL expression to a SQL string."""
    if _is_deferred(expr):
        return _deferred_to_sql(expr)

    from returns.result import Success

    result = expr_to_ibis_string(expr)
    if isinstance(result, Success):
        val = result.unwrap()
        if val is not None:
            return _ibis_string_to_sql(val)

    return None


# ---------------------------------------------------------------------------
# Export helpers
# ---------------------------------------------------------------------------


def _json_dumps(obj: Any) -> str:
    import json
    return json.dumps(obj)


def _make_osi_expression(sql_expr: str, dialect: str = "ANSI_SQL") -> dict:
    return {"dialects": [{"dialect": dialect, "expression": sql_expr}]}


def _dimension_to_osi_field(name: str, dim: Dimension) -> dict:
    """Convert a BSL Dimension to an OSI field dict."""
    sql = _expr_to_sql_string(dim.expr)
    field: dict[str, Any] = {
        "name": name,
        "expression": _make_osi_expression(sql or name),
    }

    field["dimension"] = {"is_time": bool(dim.is_time_dimension or dim.is_event_timestamp)}

    if dim.description:
        field["description"] = dim.description
    if dim.ai_context:
        field["ai_context"] = dim.ai_context

    bsl_data: dict[str, Any] = {}
    if dim.is_entity:
        bsl_data["is_entity"] = True
    if dim.is_event_timestamp:
        bsl_data["is_event_timestamp"] = True
    if dim.smallest_time_grain:
        bsl_data["smallest_time_grain"] = dim.smallest_time_grain
    if dim.derived_dimensions:
        bsl_data["derived_dimensions"] = list(dim.derived_dimensions)

    if bsl_data:
        field["custom_extensions"] = [
            {"vendor_name": "COMMON", "data": _json_dumps(bsl_data)}
        ]

    return field


def _prefix_columns_in_sql(sql: str, dataset: str) -> str:
    """``SUM(amount)`` -> ``SUM(dataset.amount)``"""
    if sql == "COUNT(*)":
        return sql

    def _prefix_match(m: re.Match) -> str:
        fn, inner = m.group(1), m.group(2).strip()
        if inner.upper().startswith("DISTINCT "):
            col = inner[9:].strip()
            if "." not in col:
                return f"{fn}(DISTINCT {dataset}.{col})"
            return m.group(0)
        if "." not in inner and inner != "*":
            return f"{fn}({dataset}.{inner})"
        return m.group(0)

    return re.sub(r"(\w+)\(([^)]+)\)", _prefix_match, sql)


def _measure_to_osi_metric(name: str, measure: Measure, dataset_name: str | None = None) -> dict:
    sql = _expr_to_sql_string(measure.expr)
    if sql and dataset_name:
        sql = _prefix_columns_in_sql(sql, dataset_name)

    metric: dict[str, Any] = {
        "name": name,
        "expression": _make_osi_expression(sql or name),
    }
    if measure.description:
        metric["description"] = measure.description
    if measure.ai_context:
        metric["ai_context"] = measure.ai_context
    return metric


def _extract_join_info(model: SemanticModel) -> list[dict]:
    """Extract relationship info from a model's join chain."""
    from .ops import SemanticJoinOp

    relationships: list[dict] = []
    op = model.op()

    def _name(node) -> str:
        if isinstance(node, SemanticTableOp):
            return node.name or "unnamed"
        if hasattr(node, "name") and node.name:
            return node.name
        if hasattr(node, "table"):
            inner = node.table if not hasattr(node.table, "op") else node.table.op()
            return _name(inner)
        return "unnamed"

    def _walk(node):
        if isinstance(node, SemanticJoinOp):
            rel: dict[str, Any] = {
                "name": f"{_name(node.left)}_{_name(node.right)}",
                "from": _name(node.left),
                "to": _name(node.right),
                "from_columns": ["unknown"],
                "to_columns": ["unknown"],
            }
            if hasattr(node, "cardinality"):
                rel["custom_extensions"] = [
                    {"vendor_name": "COMMON", "data": _json_dumps({"cardinality": node.cardinality})}
                ]
            relationships.append(rel)
            _walk(node.left)
            _walk(node.right)
        elif hasattr(node, "table"):
            _walk(node.table if not hasattr(node.table, "op") else node.table.op())

    _walk(op)
    return relationships


# ---------------------------------------------------------------------------
# Public: Export BSL -> OSI
# ---------------------------------------------------------------------------


def to_osi(
    models: dict[str, SemanticModel] | SemanticModel,
    name: str = "semantic_model",
    description: str | None = None,
    ai_context: str | dict | None = None,
) -> dict[str, Any]:
    """Convert BSL SemanticModel(s) to an OSI-compliant dict.

    Args:
        models: A single SemanticModel or dict of name -> SemanticModel
        name: Name for the OSI semantic model
        description: Optional description
        ai_context: Optional AI context

    Returns:
        Dict that can be serialized to OSI YAML via ``yaml.dump()``

    Example::

        >>> from boring_semantic_layer import from_yaml
        >>> from boring_semantic_layer.osi import to_osi
        >>> models = from_yaml("flights.yml")
        >>> osi = to_osi(models, name="flights_analytics")
    """
    if isinstance(models, (SemanticModel, SemanticTable)):
        op = models.op()
        model_name = op.name or "model"
        models = {model_name: models}

    datasets = []
    all_metrics: list[dict] = []
    all_relationships: list[dict] = []
    seen_relationship_names: set[str] = set()

    for model_name, model in models.items():
        op = model.op()

        dataset: dict[str, Any] = {"name": model_name}

        try:
            source_table = op.to_untagged()
            if hasattr(source_table, "get_name"):
                dataset["source"] = source_table.get_name()
            elif hasattr(source_table, "op") and hasattr(source_table.op(), "name"):
                dataset["source"] = source_table.op().name or model_name
            else:
                dataset["source"] = model_name
        except Exception:
            dataset["source"] = model_name

        pk_cols = []
        dims = op.get_dimensions()
        for dim_name, dim in dims.items():
            if dim.is_entity:
                sql = _expr_to_sql_string(dim.expr)
                pk_cols.append(sql or dim_name)
        if pk_cols:
            dataset["primary_key"] = pk_cols

        if op.description:
            dataset["description"] = op.description

        fields = [_dimension_to_osi_field(n, d) for n, d in dims.items()]
        if fields:
            dataset["fields"] = fields

        datasets.append(dataset)

        for meas_name, meas in op.get_measures().items():
            all_metrics.append(_measure_to_osi_metric(meas_name, meas, model_name))

        for cm_name, cm_fn in op.get_calculated_measures().items():
            metric: dict[str, Any] = {
                "name": cm_name,
                "expression": _make_osi_expression(cm_name),
            }
            if isinstance(cm_fn, Measure) and cm_fn.description:
                metric["description"] = cm_fn.description
            metric.setdefault("custom_extensions", []).append(
                {"vendor_name": "COMMON", "data": _json_dumps({"bsl_type": "calculated_measure"})}
            )
            all_metrics.append(metric)

        for rel in _extract_join_info(model):
            if rel["name"] not in seen_relationship_names:
                all_relationships.append(rel)
                seen_relationship_names.add(rel["name"])

    semantic_model: dict[str, Any] = {"name": name, "datasets": datasets}
    if description:
        semantic_model["description"] = description
    if ai_context:
        semantic_model["ai_context"] = ai_context
    if all_relationships:
        semantic_model["relationships"] = all_relationships
    if all_metrics:
        semantic_model["metrics"] = all_metrics

    return {"version": OSI_VERSION, "semantic_model": [semantic_model]}


def to_osi_yaml(
    models: dict[str, SemanticModel] | SemanticModel,
    name: str = "semantic_model",
    description: str | None = None,
    ai_context: str | dict | None = None,
) -> str:
    """Convert BSL models to an OSI YAML string."""
    import yaml
    return yaml.dump(
        to_osi(models, name=name, description=description, ai_context=ai_context),
        sort_keys=False,
        default_flow_style=False,
    )


# ---------------------------------------------------------------------------
# Convenience aliases — import delegates to from_config (which auto-detects)
# ---------------------------------------------------------------------------


def from_osi(
    osi_config: dict[str, Any],
    tables: Mapping[str, Any] | None = None,
) -> dict[str, SemanticModel]:
    """Parse an OSI config dict into BSL models.

    This is a convenience alias for ``from_config(osi_config, tables=tables)``.
    You can also call ``from_config`` or ``from_yaml`` directly — they
    auto-detect OSI format.
    """
    from .yaml import from_config
    return from_config(osi_config, tables=tables)


def from_osi_yaml(
    yaml_path: str,
    tables: Mapping[str, Any] | None = None,
) -> dict[str, SemanticModel]:
    """Load BSL models from an OSI YAML file.

    This is a convenience alias for ``from_yaml(yaml_path, tables=tables)``.
    """
    from .yaml import from_yaml
    return from_yaml(yaml_path, tables=tables)
