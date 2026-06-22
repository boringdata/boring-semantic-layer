# Perspective.js Integration

BSL can write semantic query results as Perspective.js-compatible Arrow artifacts. This integration is intentionally narrow: BSL prepares a static backend artifact, and host applications decide how to serve the files and instantiate `@finos/perspective` or `<perspective-viewer>`.

## Why Perspective artifacts

Perspective is useful for BI-style tables and exploratory dashboard panels:

- interactive sorting, filtering, grouping, and pivoting
- browser-side WebAssembly execution
- Arrow-friendly client/server replication
- embeddable web component runtime

BSL does not depend on Perspective's frontend packages. It writes a small host-generic manifest next to an Arrow file.

## Backend/frontend model

The intended integration boundary is expression-first and artifact-focused:

```text
LLM or application intent
  -> BSL semantic query / Ibis expression
  -> backend writes Arrow + Perspective manifest
  -> host serves those files
  -> frontend loads Arrow into Perspective and applies viewer hints
```

Agents and application code should describe semantic intent in BSL terms: models, dimensions, measures, filters, and optional viewer hints. They should not generate raw Perspective runtime objects or frontend-specific table setup code.

The backend should own:

- compiling semantic intent into a BSL/Ibis expression
- materializing the expression as Arrow
- writing the Arrow artifact and manifest

The frontend should own:

- fetching the manifest and Arrow file
- loading Arrow into Perspective
- instantiating the viewer component
- applying allowed viewer hints such as plugin, columns, grouping, sorting, and filters

## Write a Perspective artifact

```python
result = orders.group_by("customer", "region").aggregate("revenue", "order_count")

from boring_semantic_layer.integrations.perspective import write_perspective_artifact

artifact = write_perspective_artifact(
    result,
    "./dashboard-data",
    id="orders_by_customer",
    viewer={"plugin": "Datagrid", "group_by": ["region"]},
)

print(artifact.manifest_path)
print(artifact.data_path)
```

This writes:

```text
dashboard-data/
  orders_by_customer.perspective.json
  orders_by_customer.arrow
```

The manifest uses `kind: "bsl.perspective.artifact"` and stores the external file reference in `data_ref`:

```json
{
  "kind": "bsl.perspective.artifact",
  "version": 1,
  "id": "orders_by_customer",
  "schema": [
    {"name": "customer", "type": "string", "role": "dimension"},
    {"name": "region", "type": "string", "role": "dimension"},
    {"name": "revenue", "type": "float", "role": "measure"}
  ],
  "viewer": {"plugin": "Datagrid", "group_by": ["region"]},
  "data_ref": {"format": "arrow", "path": "orders_by_customer.arrow"}
}
```

Arrow export requires `pyarrow`; install it with `pip install 'boring-semantic-layer[viz-perspective]'` or your project's equivalent extra installation.

## Use with host applications

A host application can load the manifest, fetch the referenced Arrow file, and pass the Arrow bytes into a Perspective table or viewer. Hosts should expose high-level application components instead of asking agents to generate raw Perspective options.

BSL owns query execution and artifact writing; the host owns transport, UI rendering, and component mapping.
