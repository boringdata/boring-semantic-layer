"""
OSI (Open Semantic Interchange) converter for Boring Semantic Layer.

Provides bidirectional conversion between BSL's semantic model format
and the OSI v0.1.1 YAML specification.

See: https://github.com/open-semantic-interchange/OSI
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Any

from ibis import _
from ibis.common.deferred import Deferred

from .api import to_semantic_table
from .expr import SemanticModel, SemanticTable
from .ops import Dimension, Measure, SemanticTableOp, _is_deferred
from .utils import expr_to_ibis_string, safe_eval

OSI_VERSION = "0.1.1"
BSL_VENDOR = "BSL"


# ---------------------------------------------------------------------------
# Expression helpers
# ---------------------------------------------------------------------------


def _deferred_to_sql(expr: Deferred) -> str:
    """Convert an Ibis Deferred expression to a simple SQL-like string.

    Handles common patterns:
      _.column_name           -> "column_name"
      _.column.sum()          -> "SUM(column)"
      _.column.mean()         -> "AVG(column)"
      _.column.max()          -> "MAX(column)"
      _.column.min()          -> "MIN(column)"
      _.column.nunique()      -> "COUNT(DISTINCT column)"
      _.count()               -> "COUNT(*)"
    """
    s = str(expr)  # e.g. "_.column_name" or "_.column.sum()"
    return _ibis_string_to_sql(s)


def _ibis_string_to_sql(s: str) -> str:
    """Convert an Ibis deferred string repr to SQL expression."""
    s = s.strip()

    # _.count() -> COUNT(*)
    if s == "_.count()":
        return "COUNT(*)"

    # _.col.agg() patterns
    agg_map = {
        "sum": "SUM",
        "mean": "AVG",
        "max": "MAX",
        "min": "MIN",
    }
    for ibis_fn, sql_fn in agg_map.items():
        pattern = rf"^_\.(.+)\.{ibis_fn}\(\)$"
        m = re.match(pattern, s)
        if m:
            return f"{sql_fn}({m.group(1)})"

    # _.col.nunique() -> COUNT(DISTINCT col)
    m = re.match(r"^_\.(.+)\.nunique\(\)$", s)
    if m:
        return f"COUNT(DISTINCT {m.group(1)})"

    # Simple column reference: _.col -> col
    m = re.match(r"^_\.(\w+)$", s)
    if m:
        return m.group(1)

    # Fallback: strip leading "_." and return as-is
    if s.startswith("_."):
        return s[2:]
    return s


def _expr_to_sql_string(expr: Any) -> str | None:
    """Best-effort conversion of a BSL expression to a SQL string."""
    if _is_deferred(expr):
        return _deferred_to_sql(expr)

    # Try the ibis string extraction utility
    from returns.result import Success

    result = expr_to_ibis_string(expr)
    if isinstance(result, Success):
        val = result.unwrap()
        if val is not None:
            return _ibis_string_to_sql(val)

    return None


def _sql_to_deferred(sql: str) -> Deferred:
    """Convert a simple SQL expression back to an Ibis Deferred.

    Handles:
      "column_name"                -> _.column_name
      "SUM(column)"                -> _.column.sum()
      "AVG(column)"                -> _.column.mean()
      "COUNT(*)"                   -> _.count()
      "COUNT(DISTINCT column)"     -> _.column.nunique()
    """
    sql = sql.strip()

    if sql == "COUNT(*)":
        return safe_eval("_.count()", context={"_": _}).unwrap()

    # COUNT(DISTINCT col)
    m = re.match(r"^COUNT\(DISTINCT\s+(\w+)\)$", sql, re.IGNORECASE)
    if m:
        return safe_eval(f"_.{m.group(1)}.nunique()", context={"_": _}).unwrap()

    # AGG(col) patterns
    sql_to_ibis = {"SUM": "sum", "AVG": "mean", "MAX": "max", "MIN": "min"}
    for sql_fn, ibis_fn in sql_to_ibis.items():
        m = re.match(rf"^{sql_fn}\((\w+)\)$", sql, re.IGNORECASE)
        if m:
            return safe_eval(f"_.{m.group(1)}.{ibis_fn}()", context={"_": _}).unwrap()

    # Simple column reference
    if re.match(r"^\w+$", sql):
        return safe_eval(f"_.{sql}", context={"_": _}).unwrap()

    # Fallback: try eval as-is with underscore prefix
    try:
        return safe_eval(f"_.{sql}", context={"_": _}).unwrap()
    except Exception:
        # Last resort: return as deferred column access
        return safe_eval(f"_.{sql.split('.')[0] if '.' in sql else sql}", context={"_": _}).unwrap()


# ---------------------------------------------------------------------------
# Export: BSL -> OSI
# ---------------------------------------------------------------------------


def _make_osi_expression(sql_expr: str, dialect: str = "ANSI_SQL") -> dict:
    """Create an OSI expression object."""
    return {"dialects": [{"dialect": dialect, "expression": sql_expr}]}


def _dimension_to_osi_field(name: str, dim: Dimension) -> dict:
    """Convert a BSL Dimension to an OSI field dict."""
    sql = _expr_to_sql_string(dim.expr)
    field: dict[str, Any] = {
        "name": name,
        "expression": _make_osi_expression(sql or name),
    }

    if dim.is_time_dimension or dim.is_event_timestamp:
        field["dimension"] = {"is_time": True}
    else:
        field["dimension"] = {"is_time": False}

    if dim.description:
        field["description"] = dim.description

    if dim.ai_context:
        field["ai_context"] = dim.ai_context

    # Store BSL-specific metadata in custom_extensions
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


def _measure_to_osi_metric(
    name: str, measure: Measure, dataset_name: str | None = None
) -> dict:
    """Convert a BSL Measure to an OSI metric dict."""
    sql = _expr_to_sql_string(measure.expr)
    if sql and dataset_name:
        # Prefix column references in aggregate functions with dataset name
        # e.g. SUM(amount) -> SUM(dataset.amount)
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


def _prefix_columns_in_sql(sql: str, dataset: str) -> str:
    """Add dataset prefix to bare column references inside aggregate functions.

    SUM(amount) -> SUM(dataset.amount)
    COUNT(*) stays as COUNT(*)
    COUNT(DISTINCT col) -> COUNT(DISTINCT dataset.col)
    """
    if sql == "COUNT(*)":
        return sql

    # Handle aggregate functions: FN(col) -> FN(dataset.col)
    def _prefix_match(m: re.Match) -> str:
        fn = m.group(1)
        inner = m.group(2).strip()
        # Handle DISTINCT keyword
        if inner.upper().startswith("DISTINCT "):
            col = inner[9:].strip()
            if "." not in col:
                return f"{fn}(DISTINCT {dataset}.{col})"
            return m.group(0)
        if "." not in inner and inner != "*":
            return f"{fn}({dataset}.{inner})"
        return m.group(0)

    return re.sub(r"(\w+)\(([^)]+)\)", _prefix_match, sql)


def _json_dumps(obj: Any) -> str:
    """Serialize to JSON string for custom_extensions."""
    import json

    return json.dumps(obj)


def _extract_join_info(model: SemanticModel) -> list[dict]:
    """Extract relationship info from a model's join chain.

    Returns list of OSI relationship dicts by inspecting the model's
    SemanticJoinOp chain.
    """
    relationships = []
    op = model.op()

    # Walk up the op tree looking for SemanticJoinOp nodes
    from .ops import SemanticJoinOp

    def _walk_joins(node, parent_name: str | None = None):
        if isinstance(node, SemanticJoinOp):
            rel: dict[str, Any] = {
                "name": f"{_get_model_name(node.left)}_{_get_model_name(node.right)}",
                "from": _get_model_name(node.left),
                "to": _get_model_name(node.right),
            }
            # Try to extract join columns from the predicate
            left_cols, right_cols = _extract_join_columns(node)
            if left_cols and right_cols:
                rel["from_columns"] = left_cols
                rel["to_columns"] = right_cols
            else:
                rel["from_columns"] = ["unknown"]
                rel["to_columns"] = ["unknown"]

            # Store cardinality in custom_extensions
            if hasattr(node, "cardinality"):
                rel["custom_extensions"] = [
                    {
                        "vendor_name": "COMMON",
                        "data": _json_dumps({"cardinality": node.cardinality}),
                    }
                ]

            relationships.append(rel)
            _walk_joins(node.left, _get_model_name(node.left))
            _walk_joins(node.right, _get_model_name(node.right))
        elif isinstance(node, SemanticTableOp):
            pass  # Base case
        elif hasattr(node, "table"):
            _walk_joins(node.table if not hasattr(node.table, "op") else node.table.op())

    _walk_joins(op)
    return relationships


def _get_model_name(node) -> str:
    """Extract model name from an op node."""
    if isinstance(node, SemanticTableOp):
        return node.name or "unnamed"
    if hasattr(node, "name") and node.name:
        return node.name
    if hasattr(node, "table"):
        inner = node.table if not hasattr(node.table, "op") else node.table.op()
        return _get_model_name(inner)
    return "unnamed"


def _extract_join_columns(join_op) -> tuple[list[str], list[str]]:
    """Try to extract column names from a join predicate. Returns (left_cols, right_cols)."""
    # This is best-effort; join predicates are lambdas and hard to introspect
    return [], []


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
        Dict that can be serialized to OSI YAML via yaml.dump()

    Example:
        >>> from boring_semantic_layer import from_yaml
        >>> from boring_semantic_layer.osi import to_osi
        >>> models = from_yaml("flights.yml")
        >>> osi = to_osi(models, name="flights_analytics")
        >>> import yaml
        >>> print(yaml.dump(osi, sort_keys=False))
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

        # --- Dataset ---
        dataset: dict[str, Any] = {"name": model_name}

        # Source: try to get table name
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

        # Primary key from entity dimensions
        pk_cols = []
        dims = op.get_dimensions()
        for dim_name, dim in dims.items():
            if dim.is_entity:
                sql = _expr_to_sql_string(dim.expr)
                pk_cols.append(sql or dim_name)
        if pk_cols:
            dataset["primary_key"] = pk_cols

        # Description
        if op.description:
            dataset["description"] = op.description

        # Fields from dimensions
        fields = []
        for dim_name, dim in dims.items():
            fields.append(_dimension_to_osi_field(dim_name, dim))
        if fields:
            dataset["fields"] = fields

        datasets.append(dataset)

        # --- Metrics from measures ---
        measures = op.get_measures()
        for meas_name, meas in measures.items():
            all_metrics.append(_measure_to_osi_metric(meas_name, meas, model_name))

        # --- Metrics from calculated measures ---
        calc_measures = op.get_calculated_measures()
        for cm_name, cm_fn in calc_measures.items():
            metric: dict[str, Any] = {
                "name": cm_name,
                "expression": _make_osi_expression(cm_name),
            }
            if isinstance(cm_fn, Measure) and cm_fn.description:
                metric["description"] = cm_fn.description
            metric.setdefault("custom_extensions", []).append(
                {
                    "vendor_name": "COMMON",
                    "data": _json_dumps({"bsl_type": "calculated_measure"}),
                }
            )
            all_metrics.append(metric)

        # --- Relationships from joins ---
        rels = _extract_join_info(model)
        for rel in rels:
            if rel["name"] not in seen_relationship_names:
                all_relationships.append(rel)
                seen_relationship_names.add(rel["name"])

    # Build the OSI document
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
    """Convert BSL models to an OSI YAML string.

    Convenience wrapper around to_osi() that returns a formatted YAML string.
    """
    import yaml

    osi_dict = to_osi(models, name=name, description=description, ai_context=ai_context)
    return yaml.dump(osi_dict, sort_keys=False, default_flow_style=False)


# ---------------------------------------------------------------------------
# Import: OSI -> BSL
# ---------------------------------------------------------------------------


def _parse_osi_expression(expr_obj: dict, prefer_dialect: str = "ANSI_SQL") -> str:
    """Extract the SQL expression string from an OSI expression object.

    Prefers the specified dialect, falls back to the first available.
    """
    dialects = expr_obj.get("dialects", [])
    if not dialects:
        raise ValueError("OSI expression has no dialects")

    # Try preferred dialect first
    for d in dialects:
        if d.get("dialect") == prefer_dialect:
            return d["expression"]

    # Fallback to first
    return dialects[0]["expression"]


def _osi_field_to_dimension(field: dict) -> tuple[str, Dimension]:
    """Convert an OSI field dict to a (name, BSL Dimension) tuple."""
    name = field["name"]
    sql_expr = _parse_osi_expression(field["expression"])
    deferred = _sql_to_deferred(sql_expr)

    kwargs: dict[str, Any] = {
        "expr": deferred,
        "description": field.get("description"),
    }

    # Dimension metadata
    dim_meta = field.get("dimension", {})
    if dim_meta.get("is_time"):
        kwargs["is_time_dimension"] = True

    # AI context
    if "ai_context" in field:
        kwargs["ai_context"] = field["ai_context"]

    # BSL-specific from custom_extensions
    import json

    for ext in field.get("custom_extensions", []):
        if ext.get("vendor_name") == "COMMON":
            try:
                data = json.loads(ext["data"])
                if data.get("is_entity"):
                    kwargs["is_entity"] = True
                if data.get("is_event_timestamp"):
                    kwargs["is_event_timestamp"] = True
                if data.get("smallest_time_grain"):
                    kwargs["smallest_time_grain"] = data["smallest_time_grain"]
                if data.get("derived_dimensions"):
                    kwargs["derived_dimensions"] = tuple(data["derived_dimensions"])
            except (json.JSONDecodeError, KeyError):
                pass

    return name, Dimension(**kwargs)


def _osi_metric_to_measure(metric: dict) -> tuple[str, Measure]:
    """Convert an OSI metric dict to a (name, BSL Measure) tuple."""
    name = metric["name"]
    sql_expr = _parse_osi_expression(metric["expression"])

    # Strip dataset prefix from column refs for BSL (which scopes per-model)
    # e.g., "SUM(flights.distance)" -> "SUM(distance)" via deferred
    sql_expr = _strip_dataset_prefix(sql_expr)
    deferred = _sql_to_deferred(sql_expr)

    kwargs: dict[str, Any] = {
        "expr": deferred,
        "description": metric.get("description"),
    }

    if "ai_context" in metric:
        kwargs["ai_context"] = metric["ai_context"]

    return name, Measure(**kwargs)


def _strip_dataset_prefix(sql: str) -> str:
    """Remove dataset.column prefixes from SQL, keeping just column name.

    SUM(flights.distance) -> SUM(distance)
    COUNT(DISTINCT customers.id) -> COUNT(DISTINCT id)
    """

    def _strip_match(m: re.Match) -> str:
        fn = m.group(1)
        inner = m.group(2).strip()
        # Handle DISTINCT
        if inner.upper().startswith("DISTINCT "):
            rest = inner[9:].strip()
            if "." in rest:
                col = rest.split(".")[-1]
                return f"{fn}(DISTINCT {col})"
            return m.group(0)
        if "." in inner and inner != "*":
            col = inner.split(".")[-1]
            return f"{fn}({col})"
        return m.group(0)

    return re.sub(r"(\w+)\(([^)]+)\)", _strip_match, sql)


def from_osi(
    osi_config: dict[str, Any],
    tables: Mapping[str, Any] | None = None,
) -> dict[str, SemanticModel]:
    """Convert an OSI YAML dict to BSL SemanticModel instances.

    Args:
        osi_config: Parsed OSI YAML dict (as returned by yaml.safe_load)
        tables: Optional mapping of dataset names to ibis table expressions.
                If not provided, models are created without backing tables
                (metadata-only, useful for inspection but not query execution).

    Returns:
        Dict mapping model names to SemanticModel instances

    Example:
        >>> import yaml
        >>> from boring_semantic_layer.osi import from_osi
        >>> with open("model.osi.yaml") as f:
        ...     osi = yaml.safe_load(f)
        >>> models = from_osi(osi, tables={"flights": con.table("flights")})
    """
    tables = dict(tables) if tables else {}

    semantic_models = osi_config.get("semantic_model", [])
    if not semantic_models:
        raise ValueError("No semantic_model found in OSI config")

    result: dict[str, SemanticModel] = {}

    for sm in semantic_models:
        datasets = sm.get("datasets", [])
        metrics = sm.get("metrics", [])
        relationships = sm.get("relationships", [])

        # Build a mapping of dataset_name -> metrics that reference it
        # (by checking column prefixes in metric expressions)
        dataset_names = {ds["name"] for ds in datasets}

        for ds in datasets:
            ds_name = ds["name"]

            # Get or create table
            if ds_name in tables:
                table = tables[ds_name]
            elif ds.get("source") and ds["source"] in tables:
                table = tables[ds["source"]]
            else:
                # Create a dummy table from field schema for metadata-only use
                table = _create_placeholder_table(ds)
                if table is None:
                    continue

            # Create semantic model
            model = to_semantic_table(table, name=ds_name, description=ds.get("description"))

            # Parse fields into dimensions
            dimensions: dict[str, Dimension] = {}
            for field in ds.get("fields", []):
                dim_name, dim = _osi_field_to_dimension(field)
                dimensions[dim_name] = dim

            if dimensions:
                model = model.with_dimensions(**dimensions)

            # Find metrics that belong to this dataset
            ds_measures: dict[str, Measure] = {}
            for metric in metrics:
                sql_expr = _parse_osi_expression(metric["expression"])
                # Check if metric references this dataset (or has no prefix)
                if f"{ds_name}." in sql_expr or not any(
                    f"{other}." in sql_expr for other in dataset_names if other != ds_name
                ):
                    meas_name, meas = _osi_metric_to_measure(metric)
                    ds_measures[meas_name] = meas

            if ds_measures:
                model = model.with_measures(**ds_measures)

            result[ds_name] = model

        # Apply relationships as joins (if tables are provided)
        if tables and relationships:
            for rel in relationships:
                from_ds = rel.get("from", "")
                to_ds = rel.get("to", "")
                if from_ds in result and to_ds in result:
                    from_cols = rel.get("from_columns", [])
                    to_cols = rel.get("to_columns", [])
                    if from_cols and to_cols and from_cols[0] != "unknown":
                        left_col = from_cols[0]
                        right_col = to_cols[0]

                        def make_join_cond(lc, rc):
                            return lambda left, right: getattr(left, lc) == getattr(right, rc)

                        result[from_ds] = result[from_ds].join_one(
                            result[to_ds],
                            on=make_join_cond(left_col, right_col),
                        )

    return result


def from_osi_yaml(
    yaml_path: str,
    tables: Mapping[str, Any] | None = None,
) -> dict[str, SemanticModel]:
    """Load BSL models from an OSI YAML file.

    Convenience wrapper around from_osi() that reads and parses the YAML file.
    """
    from .utils import read_yaml_file

    osi_config = read_yaml_file(yaml_path)
    return from_osi(osi_config, tables=tables)


def _create_placeholder_table(dataset: dict) -> Any:
    """Create a placeholder ibis table from OSI field definitions.

    This allows metadata inspection without a real database connection.
    Returns None if no fields are defined.
    """
    import ibis

    fields = dataset.get("fields", [])
    if not fields:
        return None

    # Create a simple schema from field names (all as string type as placeholder)
    schema = {f["name"]: "string" for f in fields}
    try:
        return ibis.table(schema, name=dataset["name"])
    except Exception:
        return None
