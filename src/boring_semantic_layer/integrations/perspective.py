"""Perspective.js Arrow artifact helpers.

This integration intentionally has one job: write BSL/Ibis query results as a
static Arrow artifact plus a small Perspective-compatible manifest. Host
applications decide how to serve the files and render Perspective in the
frontend.
"""

from __future__ import annotations

import json
import math
import re
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any, Literal

PerspectiveType = Literal["string", "integer", "float", "boolean", "date", "datetime"]
ColumnRole = Literal["dimension", "measure", "time", "unknown"]


@dataclass(frozen=True)
class PerspectiveColumn:
    """Column metadata for a Perspective-compatible artifact manifest."""

    name: str
    type: PerspectiveType
    role: ColumnRole = "unknown"

    def to_dict(self) -> dict[str, str]:
        return {"name": self.name, "type": self.type, "role": self.role}


@dataclass(frozen=True)
class PerspectiveArtifact:
    """Files written for a Perspective-compatible Arrow artifact."""

    manifest_path: Path
    data_path: Path
    manifest: dict[str, Any]


def _json_safe(value: Any) -> Any:
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise ValueError(
                f"Non-finite decimal is not valid JSON for Perspective manifest: {value!r}"
            )
        return float(value)
    if hasattr(value, "isoformat"):
        return value.isoformat()
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError(
                f"Non-finite float is not valid JSON for Perspective manifest: {value!r}"
            )
        return value
    if value is None or isinstance(value, str | int | bool):
        return value
    raise TypeError(f"Value is not JSON serializable for Perspective manifest: {value!r}")


def _json_safe_tree(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_safe_tree(item) for key, item in value.items()}
    if isinstance(value, list | tuple):
        return [_json_safe_tree(item) for item in value]
    return _json_safe(value)


def _import_pyarrow() -> tuple[Any, Any]:
    try:
        import pyarrow as pa
        import pyarrow.ipc as ipc
    except ImportError as exc:
        raise ImportError(
            "Perspective artifact export requires pyarrow. Install with "
            "`pip install 'boring-semantic-layer[viz-perspective]'`."
        ) from exc
    return pa, ipc


def _perspective_type(pa: Any, arrow_type: Any) -> PerspectiveType:
    if pa.types.is_boolean(arrow_type):
        return "boolean"
    if pa.types.is_integer(arrow_type):
        return "integer"
    if pa.types.is_floating(arrow_type) or pa.types.is_decimal(arrow_type):
        return "float"
    if pa.types.is_date(arrow_type):
        return "date"
    if pa.types.is_timestamp(arrow_type):
        return "datetime"
    return "string"


def _role_for_type(column_type: PerspectiveType, role: ColumnRole = "unknown") -> ColumnRole:
    if role != "unknown":
        return role
    if column_type in {"integer", "float"}:
        return "measure"
    if column_type in {"date", "datetime"}:
        return "time"
    return "dimension"


def _metadata_roles(query: Any) -> dict[str, ColumnRole]:
    """Best-effort semantic roles from a BSL aggregate."""
    if not hasattr(query, "op"):
        return {}
    try:
        from boring_semantic_layer.chart.utils import (
            detect_time_dimension,
            extract_aggregate_metadata,
        )

        dimensions, measures, *_ = extract_aggregate_metadata(query)
        roles: dict[str, ColumnRole] = {dim: "dimension" for dim in dimensions}
        roles.update({measure: "measure" for measure in measures})
        if time_dimension := detect_time_dimension(query, dimensions):
            roles[time_dimension] = "time"
        return roles
    except Exception:
        return {}


def _manifest_schema(arrow_schema: Any, roles: dict[str, ColumnRole]) -> list[PerspectiveColumn]:
    pa, _ = _import_pyarrow()
    return [
        PerspectiveColumn(
            field.name,
            column_type := _perspective_type(pa, field.type),
            _role_for_type(column_type, roles.get(field.name, "unknown")),
        )
        for field in arrow_schema
    ]


def infer_perspective_schema(query_or_table: Any) -> list[PerspectiveColumn]:
    """Return the Perspective manifest schema from an Arrow-backed query/table."""
    return _manifest_schema(_to_arrow_table(query_or_table).schema, _metadata_roles(query_or_table))


def _to_arrow_table(query_or_table: Any, *, execute_kwargs: dict[str, Any] | None = None) -> Any:
    pa, _ = _import_pyarrow()
    execute_kwargs = execute_kwargs or {}

    if isinstance(query_or_table, pa.Table):
        return query_or_table

    to_pyarrow = getattr(query_or_table, "to_pyarrow", None)
    if callable(to_pyarrow):
        return to_pyarrow(**execute_kwargs)

    raise TypeError(
        "Expected an Arrow table or BSL/Ibis expression with to_pyarrow() "
        "for Perspective artifact export; "
        f"got {type(query_or_table).__name__}"
    )


def perspective_viewer_config(
    *,
    plugin: str = "Datagrid",
    columns: list[str] | None = None,
    group_by: list[str] | None = None,
    split_by: list[str] | None = None,
    sort: list[list[str]] | None = None,
    filter: list[list[Any]] | None = None,
    aggregates: dict[str, str] | None = None,
    settings: bool | None = None,
    expressions: list[str] | None = None,
    **extra: Any,
) -> dict[str, Any]:
    """Build optional Perspective viewer hints for the artifact manifest."""
    viewer: dict[str, Any] = {"plugin": plugin}
    viewer.update(
        {
            key: value
            for key, value in {
                "columns": columns,
                "group_by": group_by,
                "split_by": split_by,
                "sort": sort,
                "filter": filter,
                "aggregates": aggregates,
                "settings": settings,
                "expressions": expressions,
            }.items()
            if value is not None
        }
    )
    viewer.update(extra)
    return viewer


_SAFE_ARTIFACT_ID = re.compile(r"^[A-Za-z0-9_.-]+$")


def _validate_artifact_id(id: str) -> None:
    if not _SAFE_ARTIFACT_ID.fullmatch(id) or id in {".", ".."}:
        raise ValueError(
            "Perspective artifact id must be a safe file stem containing only "
            "letters, numbers, '.', '_' and '-'"
        )


def write_perspective_artifact(
    query_or_table: Any,
    output_dir: str | Path,
    *,
    id: str,
    viewer: dict[str, Any] | None = None,
    execute_kwargs: dict[str, Any] | None = None,
) -> PerspectiveArtifact:
    """Write ``<id>.arrow`` plus ``<id>.perspective.json``.

    The manifest is host-generic. It tells a frontend where the Arrow file is,
    what Perspective-compatible schema to expect, and which viewer hints a host
    may choose to apply.
    """
    _validate_artifact_id(id)
    _, ipc = _import_pyarrow()
    table = _to_arrow_table(query_or_table, execute_kwargs=execute_kwargs)
    roles = _metadata_roles(query_or_table)
    schema = _manifest_schema(table.schema, roles)

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    data_path = output_path / f"{id}.arrow"
    manifest_path = output_path / f"{id}.perspective.json"
    manifest = {
        "kind": "bsl.perspective.artifact",
        "version": 1,
        "id": id,
        "schema": [column.to_dict() for column in schema],
        "viewer": _json_safe_tree(viewer or perspective_viewer_config()),
        "data_ref": {"format": "arrow", "path": data_path.name},
    }
    manifest_json = json.dumps(manifest, indent=2, allow_nan=False) + "\n"

    with data_path.open("wb") as handle, ipc.new_file(handle, table.schema) as writer:
        writer.write_table(table)
    manifest_path.write_text(manifest_json)

    return PerspectiveArtifact(manifest_path=manifest_path, data_path=data_path, manifest=manifest)
