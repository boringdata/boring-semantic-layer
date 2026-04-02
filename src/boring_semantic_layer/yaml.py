"""
YAML loader for Boring Semantic Layer models using the semantic API.

Supports both BSL native YAML format and OSI (Open Semantic Interchange)
v0.1.1 format. The format is auto-detected based on the presence of
``version`` and ``semantic_model`` keys.
"""

import json
import re
from collections.abc import Mapping
from typing import Any

from ibis import _

from .api import to_semantic_table
from .expr import SemanticModel, SemanticTable
from .ops import Dimension, Measure
from .profile import get_connection
from .utils import read_yaml_file, safe_eval


# ---------------------------------------------------------------------------
# Format detection
# ---------------------------------------------------------------------------


def _is_osi_config(config: Mapping[str, Any]) -> bool:
    """Return True if *config* looks like an OSI YAML document."""
    return "semantic_model" in config and "version" in config


# ---------------------------------------------------------------------------
# OSI expression helpers (SQL <-> Ibis Deferred)
# ---------------------------------------------------------------------------


def _sql_to_deferred(sql: str):
    """Convert a simple SQL expression to an Ibis Deferred.

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
        return safe_eval(
            f"_.{sql.split('.')[0] if '.' in sql else sql}", context={"_": _}
        ).unwrap()


def _parse_osi_expression(expr_obj: dict, prefer_dialect: str = "ANSI_SQL") -> str:
    """Extract the SQL expression string from an OSI expression object."""
    dialects = expr_obj.get("dialects", [])
    if not dialects:
        raise ValueError("OSI expression has no dialects")
    for d in dialects:
        if d.get("dialect") == prefer_dialect:
            return d["expression"]
    return dialects[0]["expression"]


def _strip_dataset_prefix(sql: str) -> str:
    """Remove dataset.column prefixes from SQL aggregates.

    ``SUM(flights.distance)`` -> ``SUM(distance)``
    """

    def _strip_match(m: re.Match) -> str:
        fn = m.group(1)
        inner = m.group(2).strip()
        if inner.upper().startswith("DISTINCT "):
            rest = inner[9:].strip()
            if "." in rest:
                return f"{fn}(DISTINCT {rest.split('.')[-1]})"
            return m.group(0)
        if "." in inner and inner != "*":
            return f"{fn}({inner.split('.')[-1]})"
        return m.group(0)

    return re.sub(r"(\w+)\(([^)]+)\)", _strip_match, sql)


# ---------------------------------------------------------------------------
# OSI field / metric -> BSL Dimension / Measure
# ---------------------------------------------------------------------------


def _osi_field_to_dimension(field: dict) -> tuple[str, Dimension]:
    """Convert an OSI field dict to a ``(name, Dimension)`` pair."""
    name = field["name"]
    sql_expr = _parse_osi_expression(field["expression"])
    deferred = _sql_to_deferred(sql_expr)

    kwargs: dict[str, Any] = {
        "expr": deferred,
        "description": field.get("description"),
    }

    dim_meta = field.get("dimension", {})
    if dim_meta.get("is_time"):
        kwargs["is_time_dimension"] = True

    if "ai_context" in field:
        kwargs["ai_context"] = field["ai_context"]

    # Recover BSL-specific metadata stored in custom_extensions
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
    """Convert an OSI metric dict to a ``(name, Measure)`` pair."""
    name = metric["name"]
    sql_expr = _parse_osi_expression(metric["expression"])
    sql_expr = _strip_dataset_prefix(sql_expr)
    deferred = _sql_to_deferred(sql_expr)

    kwargs: dict[str, Any] = {
        "expr": deferred,
        "description": metric.get("description"),
    }
    if "ai_context" in metric:
        kwargs["ai_context"] = metric["ai_context"]

    return name, Measure(**kwargs)


# ---------------------------------------------------------------------------
# OSI config -> BSL models  (called from from_config when OSI is detected)
# ---------------------------------------------------------------------------


def _create_placeholder_table(dataset: dict):
    """Create a placeholder ibis table from OSI field definitions."""
    import ibis

    fields = dataset.get("fields", [])
    if not fields:
        return None
    schema = {f["name"]: "string" for f in fields}
    try:
        return ibis.table(schema, name=dataset["name"])
    except Exception:
        return None


def _from_osi_config(
    config: Mapping[str, Any],
    tables: Mapping[str, Any] | None = None,
    profile: str | None = None,
    profile_path: str | None = None,
) -> dict[str, SemanticModel]:
    """Parse an OSI config dict into BSL SemanticModel instances.

    This is an internal entry-point invoked by :func:`from_config` when it
    detects OSI format.  Users should call ``from_config`` / ``from_yaml``
    directly — those work for *both* BSL and OSI files.
    """
    tables = dict(tables) if tables else {}

    # Load tables from profile if not provided
    if not tables:
        profile_config = profile or config.get("profile")
        if profile_config or profile_path:
            connection = get_connection(
                profile_config or profile_path,
                profile_file=profile_path if profile_config else None,
            )
            tables = {name: connection.table(name) for name in connection.list_tables()}

    semantic_models = config.get("semantic_model", [])
    if not semantic_models:
        raise ValueError("No semantic_model found in OSI config")

    result: dict[str, SemanticModel] = {}

    for sm in semantic_models:
        datasets = sm.get("datasets", [])
        metrics = sm.get("metrics", [])
        relationships = sm.get("relationships", [])
        dataset_names = {ds["name"] for ds in datasets}

        for ds in datasets:
            ds_name = ds["name"]

            # Resolve backing table
            if ds_name in tables:
                table = tables[ds_name]
            elif ds.get("source") and ds["source"] in tables:
                table = tables[ds["source"]]
            else:
                table = _create_placeholder_table(ds)
                if table is None:
                    continue

            model = to_semantic_table(
                table, name=ds_name, description=ds.get("description")
            )

            # Fields -> Dimensions
            dimensions: dict[str, Dimension] = {}
            for field in ds.get("fields", []):
                dim_name, dim = _osi_field_to_dimension(field)
                dimensions[dim_name] = dim
            if dimensions:
                model = model.with_dimensions(**dimensions)

            # Metrics -> Measures (assign to the dataset they reference)
            ds_measures: dict[str, Measure] = {}
            for metric in metrics:
                sql_expr = _parse_osi_expression(metric["expression"])
                if f"{ds_name}." in sql_expr or not any(
                    f"{other}." in sql_expr
                    for other in dataset_names
                    if other != ds_name
                ):
                    meas_name, meas = _osi_metric_to_measure(metric)
                    ds_measures[meas_name] = meas
            if ds_measures:
                model = model.with_measures(**ds_measures)

            result[ds_name] = model

        # Relationships -> Joins
        if tables and relationships:
            for rel in relationships:
                from_ds = rel.get("from", "")
                to_ds = rel.get("to", "")
                if from_ds in result and to_ds in result:
                    from_cols = rel.get("from_columns", [])
                    to_cols = rel.get("to_columns", [])
                    if from_cols and to_cols and from_cols[0] != "unknown":

                        def _make_join_cond(lc, rc):
                            return lambda left, right: getattr(left, lc) == getattr(
                                right, rc
                            )

                        result[from_ds] = result[from_ds].join_one(
                            result[to_ds],
                            on=_make_join_cond(from_cols[0], to_cols[0]),
                        )

    return result


# ---------------------------------------------------------------------------
# BSL native YAML helpers
# ---------------------------------------------------------------------------


def _parse_expression_config(name: str, config: str | dict, metric_type: str):
    """Extract expression string, description, and extra kwargs from config."""
    if isinstance(config, str):
        return config, None, {}
    elif isinstance(config, dict):
        if "expr" not in config:
            raise ValueError(
                f"{metric_type.capitalize()} '{name}' must specify 'expr' field when using dict format"
            )
        extra_kwargs = {}
        if metric_type == "dimension":
            extra_kwargs["is_entity"] = config.get("is_entity", False)
            extra_kwargs["is_event_timestamp"] = config.get("is_event_timestamp", False)
            extra_kwargs["is_time_dimension"] = config.get("is_time_dimension", False)
            extra_kwargs["smallest_time_grain"] = config.get("smallest_time_grain")
            extra_kwargs["derived_dimensions"] = tuple(config.get("derived_dimensions") or ())
        if "ai_context" in config:
            extra_kwargs["ai_context"] = config["ai_context"]
        return config["expr"], config.get("description"), extra_kwargs
    else:
        raise ValueError(f"Invalid {metric_type} format for '{name}'. Must be a string or dict")


def _parse_dimension_or_measure(
    name: str, config: str | dict, metric_type: str
) -> Dimension | Measure:
    """Parse a single dimension or measure configuration.

    Supports two formats:
    1. Simple format (backwards compatible): name: expression_string
    2. Extended format with descriptions and metadata:
        name:
          expr: expression_string
          description: "description text"
          is_entity: true/false (dimensions only)
          is_event_timestamp: true/false (dimensions only)
          is_time_dimension: true/false (dimensions only)
          smallest_time_grain: "TIME_GRAIN_DAY" (dimensions only)
          derived_dimensions: ["year", "month", "day"] (dimensions only)
    """
    expr_str, description, extra_kwargs = _parse_expression_config(name, config, metric_type)
    deferred = safe_eval(expr_str, context={"_": _}).unwrap()
    base_kwargs = {"expr": deferred, "description": description}
    return (
        Dimension(**base_kwargs, **extra_kwargs)
        if metric_type == "dimension"
        else Measure(**base_kwargs)
    )


def _parse_calc_measure(name: str, config: str | dict) -> Measure:
    """Parse a calculated measure that references other measures by name.

    Unlike regular measures which use ibis Deferred (``_``), calculated measures
    are evaluated at runtime against a MeasureScope, allowing them to reference
    other measures by name and use ``.all()`` for window aggregations.

    Example YAML::

        calculated_measures:
          fraud_rate:
            expr: _.fraud_volume / _.transaction_volume
          pct_of_total:
            expr: _.distance_sum / _.all(_.distance_sum) * 100
    """
    expr_str, description, _ = _parse_expression_config(name, config, "measure")

    def _make_calc_fn(source: str):
        def calc_fn(scope):
            return safe_eval(source, context={"_": scope}).unwrap()
        return calc_fn

    return Measure(expr=_make_calc_fn(expr_str), description=description)


def _parse_filter(filter_expr: str) -> callable:
    """Parse a filter expression from YAML.

    Example YAML:
        flights:
          table: flights_tbl
          filter: _.origin.isin(['SFO', 'LAX', 'JFK'])
    """
    from ibis import _

    deferred = safe_eval(filter_expr, context={"_": _}, allowed_names={"_"}).unwrap()
    return lambda t, d=deferred: d.resolve(t)


def _resolve_join_model(
    alias: str,
    join_model_name: str,
    tables: Mapping[str, Any],
    yaml_configs: Mapping[str, Any],
    models: dict[str, SemanticModel],
) -> SemanticModel:
    """Look up and return the model to join."""
    if join_model_name in models:
        return models[join_model_name]
    elif join_model_name in tables:
        table = tables[join_model_name]
        if isinstance(table, SemanticModel | SemanticTable):
            return table
        else:
            raise TypeError(
                f"Join '{alias}' references '{join_model_name}' which is not a semantic model/table"
            )
    elif join_model_name in yaml_configs:
        raise ValueError(
            f"Model '{join_model_name}' in join '{alias}' not yet loaded. Check model order."
        )
    else:
        available = sorted(
            list(models.keys())
            + [k for k in tables if isinstance(tables.get(k), SemanticModel | SemanticTable)]
        )
        raise KeyError(
            f"Model '{join_model_name}' in join '{alias}' not found. Available: {', '.join(available)}"
        )


def _create_aliased_model(model: SemanticModel, alias: str) -> SemanticModel:
    """Create an aliased copy of a model with a different name for join prefixing.

    For self-joins (same model joined multiple times), also creates a distinct
    table reference via ``.view()`` to avoid ambiguous column errors.
    """
    base_table = model.op().to_untagged()

    # Create a distinct table reference for self-joins
    try:
        aliased_table = base_table.view()
    except Exception:
        aliased_table = base_table

    aliased_model = to_semantic_table(aliased_table, name=alias)

    dims = model.get_dimensions()
    if dims:
        aliased_model = aliased_model.with_dimensions(**dims)

    measures = model.get_measures()
    if measures:
        aliased_model = aliased_model.with_measures(**measures)

    calc_measures = model.get_calculated_measures()
    if calc_measures:
        aliased_model = aliased_model.with_measures(**calc_measures)

    return aliased_model


def _parse_joins(
    joins_config: dict[str, Mapping[str, Any]],
    tables: Mapping[str, Any],
    yaml_configs: Mapping[str, Any],
    current_model_name: str,
    models: dict[str, SemanticModel],
) -> SemanticModel:
    """Parse join configuration and apply joins to a semantic model."""
    result_model = models[current_model_name]

    # Track which models have been joined to detect self-joins
    joined_model_names: dict[str, int] = {}

    # Process each join definition
    for alias, join_config in joins_config.items():
        join_model_name = join_config.get("model")
        if not join_model_name:
            raise ValueError(f"Join '{alias}' must specify 'model' field")

        join_model = _resolve_join_model(alias, join_model_name, tables, yaml_configs, models)

        # Create an aliased copy when the alias differs from the model name,
        # or for self-joins (same model joined multiple times).
        # This ensures dimension prefixes match the YAML alias (e.g., "origin_airport.city")
        # rather than the underlying model name (e.g., "airports.city").
        join_count = joined_model_names.get(join_model_name, 0)
        needs_alias = (
            alias != join_model_name
            or join_count > 0
            or join_model_name == current_model_name
        )
        if needs_alias:
            join_model = _create_aliased_model(join_model, alias)
        joined_model_names[join_model_name] = join_count + 1

        # Apply the join based on type
        join_type = join_config.get("type", "one")  # Default to one-to-one
        how = join_config.get("how")  # Optional join method override

        if join_type == "cross":
            # Cross join - no keys needed
            result_model = result_model.join_cross(join_model)
        elif join_type == "one":
            left_on = join_config.get("left_on")
            right_on = join_config.get("right_on")
            if not left_on or not right_on:
                raise ValueError(
                    f"Join '{alias}' of type 'one' must specify 'left_on' and 'right_on' fields",
                )

            # Convert left_on/right_on to lambda condition
            def make_join_condition(left_col, right_col):
                return lambda left, right: getattr(left, left_col) == getattr(right, right_col)

            on_condition = make_join_condition(left_on, right_on)
            result_model = result_model.join_one(
                join_model,
                on=on_condition,
                how=how if how else "inner",
            )
        elif join_type == "many":
            left_on = join_config.get("left_on")
            right_on = join_config.get("right_on")
            if not left_on or not right_on:
                raise ValueError(
                    f"Join '{alias}' of type 'many' must specify 'left_on' and 'right_on' fields",
                )

            # Convert left_on/right_on to lambda condition
            def make_join_condition(left_col, right_col):
                return lambda left, right: getattr(left, left_col) == getattr(right, right_col)

            on_condition = make_join_condition(left_on, right_on)
            result_model = result_model.join_many(
                join_model,
                on=on_condition,
                how=how if how else "left",
            )
        else:
            raise ValueError(f"Invalid join type '{join_type}'. Must be 'one', 'many', or 'cross'")

    return result_model


def _load_tables_from_references(
    table_refs: dict[str, tuple[str, str] | tuple[str, str, str] | Any],
) -> dict[str, Any]:
    """Load tables from tuples (profile, table) or pass through table objects."""
    resolved = {}
    for name, ref in table_refs.items():
        if isinstance(ref, tuple) and len(ref) in (2, 3):
            profile_name, remote_table = ref[0], ref[1]
            profile_file = ref[2] if len(ref) == 3 else None
            con = get_connection(profile_name, profile_file=profile_file)
            resolved[name] = con.table(remote_table)
        else:
            resolved[name] = ref
    return resolved


def _load_table_for_yaml_model(
    model_config: dict[str, Any],
    existing_tables: dict[str, Any],
    table_name: str,
) -> tuple[dict[str, Any], Any]:
    """Load table from model config profile if specified, verify it exists.

    Supports optional 'database' kwarg in model_config which is passed to
    connection.table(). The database can be a string or list for multi-part
    identifiers (e.g., ["catalog", "schema"] for catalog.schema.table).

    Returns:
        A tuple of (updated_tables_dict, table_for_this_model).
        - updated_tables_dict: Only modified when loading from a profile (persisted)
        - table_for_this_model: The specific table for this model (may be database-overridden)
    """
    tables = existing_tables.copy()

    # Get optional database kwarg for connection.table()
    database = model_config.get("database")
    # Convert list to tuple for ibis (which expects tuple for multi-part identifiers)
    if isinstance(database, list):
        database = tuple(database)

    # Load table from model-specific profile if needed
    if "profile" in model_config:
        profile_config = model_config["profile"]
        connection = get_connection(profile_config)
        if table_name in tables:
            raise ValueError(f"Table name conflict: {table_name} already exists")
        table = connection.table(table_name, database=database)
        tables[table_name] = table
        return tables, table
    elif database is not None:
        # database specified without profile - reload from existing connection
        # This table is NOT persisted to avoid affecting other models
        if table_name not in tables:
            available = ", ".join(sorted(tables.keys()))
            raise KeyError(
                f"Table '{table_name}' not found. When using 'database' without 'profile', "
                f"provide the table via the 'tables' parameter. Available: {available}"
            )
        existing_table = tables[table_name]
        connection = existing_table.op().source
        table = connection.table(table_name, database=database)
        # Return original tables (unmodified) but with the database-specific table for this model
        return tables, table

    # Verify table exists
    if table_name not in tables:
        available = ", ".join(sorted(tables.keys()))
        raise KeyError(f"Table '{table_name}' not found. Available: {available}")

    return tables, tables[table_name]


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def from_config(
    config: Mapping[str, Any],
    tables: Mapping[str, Any] | None = None,
    profile: str | None = None,
    profile_path: str | None = None,
) -> dict[str, SemanticModel]:
    """
    Load semantic tables from a configuration dictionary.

    Accepts **both** BSL native YAML format and OSI (Open Semantic Interchange)
    v0.1.1 format.  The format is auto-detected: if the dict contains
    ``version`` and ``semantic_model`` keys it is treated as OSI, otherwise
    as BSL native.

    Args:
        config: Configuration dictionary (BSL or OSI format)
        tables: Optional mapping of table names to ibis table expressions
        profile: Optional profile name to load tables from
        profile_path: Optional path to profile file

    Returns:
        Dict mapping model names to SemanticModel instances
    """
    # ---- Auto-detect OSI format ----
    if _is_osi_config(config):
        return _from_osi_config(
            config, tables=tables, profile=profile, profile_path=profile_path
        )

    # ---- BSL native format ----
    tables = _load_tables_from_references(dict(tables) if tables else {})

    # Load tables from profile if not provided
    if not tables:
        profile_config = profile or config.get("profile")
        if profile_config or profile_path:
            connection = get_connection(
                profile_config or profile_path,
                profile_file=profile_path if profile_config else None,
            )
            tables = {name: connection.table(name) for name in connection.list_tables()}

    # Filter to only model definitions (exclude 'profile' key and non-dict values)
    model_configs = {
        name: cfg for name, cfg in config.items() if name != "profile" and isinstance(cfg, dict)
    }

    models: dict[str, SemanticModel] = {}

    # First pass: create models
    for name, model_config in model_configs.items():
        table_name = model_config.get("table")
        if not table_name:
            raise ValueError(f"Model '{name}' must specify 'table' field")

        # Load table if needed and verify it exists
        tables, table = _load_table_for_yaml_model(model_config, tables, table_name)

        # Parse dimensions and measures
        dimensions = {
            dim_name: _parse_dimension_or_measure(dim_name, dim_cfg, "dimension")
            for dim_name, dim_cfg in model_config.get("dimensions", {}).items()
        }
        measures = {
            measure_name: _parse_dimension_or_measure(measure_name, measure_cfg, "measure")
            for measure_name, measure_cfg in model_config.get("measures", {}).items()
        }

        calc_measures = {
            cm_name: _parse_calc_measure(cm_name, cm_cfg)
            for cm_name, cm_cfg in model_config.get("calculated_measures", {}).items()
        }

        # Create the semantic table and add dimensions/measures
        semantic_table = to_semantic_table(table, name=name)
        if dimensions:
            semantic_table = semantic_table.with_dimensions(**dimensions)
        if measures:
            semantic_table = semantic_table.with_measures(**measures)
        if calc_measures:
            semantic_table = semantic_table.with_measures(**calc_measures)

        # Apply filter if specified
        if "filter" in model_config:
            filter_predicate = _parse_filter(model_config["filter"])
            semantic_table = semantic_table.filter(filter_predicate)

        models[name] = semantic_table

    # Second pass: add joins now that all models exist
    for name, model_config in model_configs.items():
        if "joins" in model_config and model_config["joins"]:
            models[name] = _parse_joins(
                model_config["joins"],
                tables,
                config,
                name,
                models,
            )

    return models


def from_yaml(
    yaml_path: str,
    tables: Mapping[str, Any] | None = None,
    profile: str | None = None,
    profile_path: str | None = None,
) -> dict[str, SemanticModel]:
    """
    Load semantic tables from a YAML file.

    Accepts **both** BSL native YAML format and OSI (Open Semantic Interchange)
    v0.1.1 format.  The format is auto-detected.

    Args:
        yaml_path: Path to the YAML configuration file (BSL or OSI format)
        tables: Optional mapping of table names to ibis table expressions
        profile: Optional profile name to load tables from
        profile_path: Optional path to profile file

    Returns:
        Dict mapping model names to SemanticModel instances

    Examples:
        Load a BSL native YAML file::

            models = from_yaml("flights.yml")

        Load an OSI YAML file::

            models = from_yaml("flights_osi.yaml", tables=tables)
    """
    yaml_configs = read_yaml_file(yaml_path)
    return from_config(yaml_configs, tables=tables, profile=profile, profile_path=profile_path)
