Maximum number of rows to display in the table output (plotext backend only). Use `0` to show all rows.

**IMPORTANT:** This only limits the **display**, not query execution. Full dataset is still processed.

**With window functions:** Use this parameter, NOT `.limit()` in the query (which breaks calculations).

For window function details: `get_documentation(topic="windowing")`
