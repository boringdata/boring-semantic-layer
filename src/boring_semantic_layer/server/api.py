"""FastAPI transport for boring-semantic-layer semantic models."""

from __future__ import annotations

import json
import logging
import os
import re
from collections.abc import Mapping, Sequence
from contextlib import asynccontextmanager
from typing import Any

import ibis
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, model_validator

from boring_semantic_layer.agents.utils.chart_handler import generate_chart_with_data
from boring_semantic_layer.ops import Dimension
from boring_semantic_layer.query import (
    TIME_GRAIN_TRANSFORMATIONS,
    TimeGrain,
    _find_time_dimension,
    _validate_time_grain,
)

from .loader import load_models

logger = logging.getLogger(__name__)


class QueryRequest(BaseModel):
    """HTTP request body for the query endpoint."""

    model_name: str
    dimensions: list[str] | None = None
    measures: list[str] | None = None
    filters: list[dict[str, Any]] | None = None
    order_by: list[tuple[str, str]] | None = None
    limit: int | None = Field(default=None, ge=1, le=100_000)
    time_grain: TimeGrain | None = None
    time_grains: dict[str, TimeGrain] | None = None
    time_range: dict[str, str] | None = None
    get_records: bool = True
    records_limit: int | None = Field(default=None, ge=1)
    get_chart: bool = True
    chart_backend: str | None = None
    chart_format: str | None = None
    chart_spec: dict[str, Any] | None = None

    @model_validator(mode="after")
    def _check_grain_fields(self) -> "QueryRequest":
        if self.time_grain is not None and self.time_grains is not None:
            raise ValueError(
                "Cannot specify both 'time_grain' and 'time_grains'. "
                "Use 'time_grain' to apply a single grain to all time dimensions, "
                "or 'time_grains' to specify per-dimension grains."
            )
        return self


def _default_cors_origins() -> list[str]:
    raw = os.environ.get("BSL_CORS_ORIGINS")
    if not raw:
        return ["*"]
    origins = [origin.strip() for origin in raw.split(",") if origin.strip()]
    return origins or ["*"]


def _get_models(request: Request) -> Mapping[str, Any]:
    return request.app.state.models


def _get_model_or_404(models: Mapping[str, Any], model_name: str) -> Any:
    model = models.get(model_name)
    if model is None:
        raise HTTPException(status_code=404, detail=f"Model '{model_name}' not found")
    return model


def _build_model_response(model: Any) -> dict[str, Any]:
    dimensions = {}
    for name, dim in model.get_dimensions().items():
        dimensions[name] = {
            "description": dim.description,
            "is_time_dimension": dim.is_time_dimension,
            "smallest_time_grain": dim.smallest_time_grain,
        }

    measures = {}
    for name, measure in model.get_measures().items():
        measures[name] = {"description": measure.description}

    response = {
        "name": model.name or "unnamed",
        "dimensions": dimensions,
        "measures": measures,
        "calculated_measures": list(model.get_calculated_measures().keys()),
    }
    if model.description:
        response["description"] = model.description
    return response


def _get_time_range_response(model: Any, model_name: str) -> dict[str, str]:
    time_dim_name = _find_time_dimension(model, list(model.dimensions))
    if not time_dim_name:
        raise HTTPException(status_code=400, detail=f"Model '{model_name}' has no time dimension")

    tbl = model.table
    col_name = time_dim_name.split(".")[-1] if "." in time_dim_name else time_dim_name
    time_col = tbl[col_name]
    result = tbl.aggregate(start=time_col.min(), end=time_col.max()).execute()

    return {
        "start": result["start"].iloc[0].isoformat(),
        "end": result["end"].iloc[0].isoformat(),
    }


def _search_dimension_values_response(
    model: Any,
    model_name: str,
    dimension_name: str,
    search_term: str | None = None,
    limit: int = 20,
) -> dict[str, Any]:
    dims = model.get_dimensions()
    if dimension_name not in dims:
        raise HTTPException(
            status_code=404,
            detail=(
                f"Dimension '{dimension_name}' not found in model '{model_name}'. "
                f"Available dimensions: {list(dims.keys())}"
            ),
        )

    dim = dims[dimension_name]
    tbl = model.table
    col_expr = dim(tbl)
    agg = (
        tbl.select(col_expr.name("_value"))
        .filter(lambda t: t["_value"].notnull())
        .group_by("_value")
        .aggregate(frequency=lambda t: t.count())
    )
    total_distinct = int(agg.count().execute())

    def to_value_list(df) -> list[dict[str, Any]]:
        return [
            {"value": str(row["_value"]), "count": int(row["frequency"])}
            for _, row in df.iterrows()
        ]

    def fetch(base_agg, n: int) -> tuple[list[dict[str, Any]], bool]:
        df = base_agg.order_by(ibis.desc("frequency")).limit(n + 1).execute()
        return to_value_list(df.head(n)), len(df) <= n

    separator_pattern = r"[\s\-_.,]+"

    if search_term:
        search_normalized = re.sub(separator_pattern, " ", search_term.lower()).strip()
        filtered_agg = agg.filter(
            lambda t: (
                t["_value"].cast("string").lower()
                .re_replace(separator_pattern, " ")
                .strip()
                .contains(search_normalized)
            )
        )
        values, is_complete = fetch(filtered_agg, limit)
        if not values:
            fallback_values, fallback_complete = fetch(agg, limit)
            return {
                "total_distinct": total_distinct,
                "is_complete": fallback_complete,
                "values": [],
                "fallback_top_values": fallback_values,
                "note": (
                    f"No matches found for '{search_term}'. "
                    "Showing top values for reference; use one of these exact spellings."
                ),
            }
    else:
        values, is_complete = fetch(agg, limit)

    return {
        "total_distinct": total_distinct,
        "is_complete": is_complete,
        "values": values,
    }


def create_app(
    *,
    models: Mapping[str, Any] | None = None,
    config_path: str | None = None,
    cors_origins: Sequence[str] | None = None,
) -> FastAPI:
    """Create the FastAPI app for the BSL HTTP server."""

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        app.state.models = models if models is not None else load_models(config_path)
        yield

    app = FastAPI(title="Boring Semantic Layer HTTP API", version="0.1.0", lifespan=lifespan)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=list(cors_origins or _default_cors_origins()),
        allow_methods=["*"],
        allow_headers=["*"],
    )

    @app.exception_handler(Exception)
    async def unhandled_exception_handler(_request: Request, exc: Exception) -> JSONResponse:
        logger.exception("Unhandled HTTP API error")
        return JSONResponse(status_code=500, content={"detail": str(exc)})

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/models")
    def list_models(request: Request) -> dict[str, str]:
        return {
            name: model.description or f"Semantic model: {name}"
            for name, model in _get_models(request).items()
        }

    @app.get("/models/{model_name}/schema")
    def get_model(model_name: str, request: Request) -> dict[str, Any]:
        model = _get_model_or_404(_get_models(request), model_name)
        return _build_model_response(model)

    @app.get("/models/{model_name}/time-range")
    def get_time_range(model_name: str, request: Request) -> dict[str, str]:
        model = _get_model_or_404(_get_models(request), model_name)
        return _get_time_range_response(model, model_name)

    @app.get("/models/{model_name}/dimensions/{dimension_name}/values")
    def search_dimension_values(
        model_name: str,
        dimension_name: str,
        request: Request,
        search_term: str | None = None,
        limit: int = Query(default=20, ge=1, le=1_000),
    ) -> dict[str, Any]:
        model = _get_model_or_404(_get_models(request), model_name)
        return _search_dimension_values_response(model, model_name, dimension_name, search_term, limit)

    @app.post("/query")
    def query_model(payload: QueryRequest, request: Request) -> dict[str, Any]:
        model = _get_model_or_404(_get_models(request), payload.model_name)

        # When per-dimension time_grains are specified, apply grain
        # transformations at the server layer (the core query() only
        # supports a single time_grain for all dimensions).
        effective_time_grain = payload.time_grain
        if payload.time_grains:
            dims_dict = model.get_dimensions()
            transformed: dict[str, Dimension] = {}
            for dim_name, grain in payload.time_grains.items():
                if dim_name not in dims_dict:
                    raise HTTPException(
                        status_code=400,
                        detail=f"Dimension '{dim_name}' not found in model '{payload.model_name}'.",
                    )
                dim_obj = dims_dict[dim_name]
                if not dim_obj.is_time_dimension:
                    raise HTTPException(
                        status_code=400,
                        detail=f"Dimension '{dim_name}' is not a time dimension.",
                    )
                if grain not in TIME_GRAIN_TRANSFORMATIONS:
                    raise HTTPException(
                        status_code=400,
                        detail=(
                            f"Invalid time grain '{grain}' for dimension '{dim_name}'. "
                            f"Must be one of {list(TIME_GRAIN_TRANSFORMATIONS.keys())}."
                        ),
                    )
                _validate_time_grain(grain, dim_obj.smallest_time_grain, dim_name)
                truncate_unit = TIME_GRAIN_TRANSFORMATIONS[grain]
                transformed[dim_name] = Dimension(
                    expr=lambda t, dim=dim_obj, unit=truncate_unit: dim(t).truncate(unit),
                    description=dim_obj.description,
                    is_time_dimension=dim_obj.is_time_dimension,
                    smallest_time_grain=dim_obj.smallest_time_grain,
                )
            if transformed:
                model = model.with_dimensions(**transformed)
            # Grain already applied; don't pass time_grain to query()
            effective_time_grain = None

        query_result = model.query(
            dimensions=payload.dimensions,
            measures=payload.measures,
            filters=payload.filters or [],
            order_by=payload.order_by,
            limit=payload.limit,
            time_grain=effective_time_grain,
            time_range=payload.time_range,
        )
        response = json.loads(
            generate_chart_with_data(
                query_result,
                get_records=payload.get_records,
                records_limit=payload.records_limit,
                get_chart=payload.get_chart,
                chart_backend=payload.chart_backend,
                chart_format=payload.chart_format,
                chart_spec=payload.chart_spec,
                default_backend="altair",
            )
        )
        if "error" in response:
            raise HTTPException(status_code=400, detail=response["error"])
        return response

    return app


app = create_app()
