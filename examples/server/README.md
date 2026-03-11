# BSL HTTP API Server — Example

This example uses the **TPC-H** dataset (orders, lineitems, customers, etc.) to demonstrate the BSL FastAPI server.

---

## 1. Install dependencies

```bash
pip install 'boring-semantic-layer[server]'
# also needed for the demo database:
pip install duckdb
```

---

## 2. Create a `semantic_config.py`

The server loads models from a Python file that must expose a top-level `MODELS` dict. Each value is a `SemanticModel` or `SemanticTable` built with the BSL API.

**Minimal example (single table):**

```python
import ibis
from ibis import _
import boring_semantic_layer as bsl

conn = ibis.duckdb.connect("my_database.duckdb")

sales = (
    bsl.to_semantic_table(conn.table("sales"), name="sales")
    .with_dimensions(
        region=_.region,
        product=_.product_name,
        sale_date=_.sale_date,
    )
    .with_measures(
        revenue=_.amount.sum(),
        order_count=_.count(),
        avg_order=_.amount.mean(),
    )
)

MODELS = {
    "sales": sales,
}
```

Key rules:
- `MODELS` must be a `dict[str, SemanticModel]` at module level — the server reads nothing else.
- Dimension names and measure names become the field names used in API requests.
- Any ibis-compatible backend works (DuckDB, PostgreSQL, BigQuery, etc.).

The `semantic_config.py` in this folder uses the TPC-H demo database. See `scripts/generate_demo_db.py` to generate it.

---

## 3. Generate the demo database (optional)

Skip this if you have your own database. Run from this folder:

```bash
python scripts/generate_demo_db.py
```

This creates `data/tpch.duckdb` (~12 MB at the default scale factor of 0.1).
Increase `SF` in the script for more data (1.0 → ~120 MB).

---

## 4. Start the server

From any directory, point `--config` at your `semantic_config.py`:

```bash
bsl serve --config ./examples/server/semantic_config.py
```

Additional options:

```bash
bsl serve --config ./semantic_config.py --host 0.0.0.0 --port 8000 --reload
```

| Flag | Default | Description |
|------|---------|-------------|
| `--config` | `./semantic_config.py` | Path to your config file |
| `--host` | `0.0.0.0` | Bind address |
| `--port` | `8000` | Port |
| `--reload` | off | Auto-reload on code changes (dev only) |

You can also set the config path via environment variable instead of `--config`:

```bash
export BSL_CONFIG_PATH=/path/to/semantic_config.py
bsl serve
```

The interactive API docs are available at [http://localhost:8000/docs](http://localhost:8000/docs) once the server is running.

---

## 5. Send requests

### List available models

```bash
curl http://localhost:8000/models
```

```json
{"models": ["orders", "lineitem", "customer", "supplier", "part", "partsupp", "nation", "region", "region_to_nation_to_customer_to_orders"]}
```

### Inspect a model's schema

```bash
curl http://localhost:8000/models/orders/schema
```

```json
{
  "model": "orders",
  "dimensions": [
    {"name": "orderkey", "type": "integer"},
    {"name": "custkey",  "type": "integer"},
    {"name": "order_date", "type": "date"},
    {"name": "status",   "type": "string"},
    {"name": "priority", "type": "string"},
    {"name": "clerk",    "type": "string"}
  ],
  "measures": ["total_price", "order_count", "avg_price"]
}
```

### Run a query — dimensions + measures

```bash
curl -X POST http://localhost:8000/query \
  -H "Content-Type: application/json" \
  -d '{
    "model": "orders",
    "dimensions": ["status"],
    "measures": ["order_count", "total_price"],
    "limit": 100
  }'
```

```json
{
  "columns": ["status", "order_count", "total_price"],
  "rows": [
    ["F", 730160, 1082282971.33],
    ["O", 733534, 1088816017.57],
    ["P",   3817,    5765021.67]
  ]
}
```

### Filter results

```bash
curl -X POST http://localhost:8000/query \
  -H "Content-Type: application/json" \
  -d '{
    "model": "orders",
    "dimensions": ["status", "priority"],
    "measures": ["order_count", "total_price"],
    "filters": [
      {"dimension": "status", "op": "eq", "value": "O"}
    ]
  }'
```

Supported filter operators: `eq`, `neq`, `gt`, `gte`, `lt`, `lte`, `contains`

### Sort results

```bash
curl -X POST http://localhost:8000/query \
  -H "Content-Type: application/json" \
  -d '{
    "model": "orders",
    "dimensions": ["status"],
    "measures": ["order_count"],
    "sort_by": [{"field": "order_count", "direction": "desc"}]
  }'
```

### Time grain truncation

Use `grains` to truncate date dimensions to year / quarter / month / date.
The output column is renamed to `<dimension>.<grain>`.

```bash
curl -X POST http://localhost:8000/query \
  -H "Content-Type: application/json" \
  -d '{
    "model": "orders",
    "dimensions": ["order_date"],
    "measures": ["order_count", "total_price"],
    "grains": {"order_date": "month"},
    "sort_by": [{"field": "order_date.month", "direction": "asc"}],
    "limit": 12
  }'
```

```json
{
  "columns": ["order_date.month", "order_count", "total_price"],
  "rows": [
    ["1992-01-01", 5765, 8534221.10],
    ["1992-02-01", 5244, 7762340.88],
    ...
  ]
}
```

### Health check

```bash
curl http://localhost:8000/health
```

```json
{"status": "ok"}
```

---

## Query request reference

`POST /query`

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `model` | string | yes | Name from `MODELS` in your config |
| `dimensions` | string[] | no | Dimension names to select/group by |
| `measures` | string[] | no | Measure names to aggregate |
| `filters` | FilterClause[] | no | Row-level filters |
| `sort_by` | SortClause[] | no | Output sort order |
| `grains` | dict | no | Time grain per dimension (`year`/`quarter`/`month`/`date`) |
| `limit` | int | no | Max rows returned (default 1000, max 100 000) |

**FilterClause:** `{"dimension": "<name>", "op": "<op>", "value": "<value>"}`

**SortClause:** `{"field": "<dimension-or-measure>", "direction": "asc" | "desc"}`
