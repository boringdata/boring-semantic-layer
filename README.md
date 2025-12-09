# Boring Semantic Layer (BSL)

[![Run in Smithery](https://smithery.ai/badge/skills/boringdata)](https://smithery.ai/skills?ns=boringdata&utm_source=github&utm_medium=badge)


The Boring Semantic Layer (BSL) is a lightweight semantic layer based on [Ibis](https://ibis-project.org/).

**Key Features:**
- **Lightweight**: `pip install boring-semantic-layer`
- **Ibis-powered**: Built on top of [Ibis](https://ibis-project.org/), supporting any database engine that Ibis integrates with (DuckDB, Snowflake, BigQuery, PostgreSQL, and more)
- **MCP-friendly**: Perfect for connecting LLMs to structured data sources

## Quick Start

```bash
pip install 'boring-semantic-layer[examples]'
```

**1. Define your ibis input table**

```python
import ibis

# Create a simple in-memory table
flights_tbl = ibis.memtable({
    "origin": ["JFK", "LAX", "JFK", "ORD", "LAX"],
    "carrier": ["AA", "UA", "AA", "UA", "AA"]
})
```

**2. Define a semantic table**

```python
from boring_semantic_layer import to_semantic_table
flights = (
    to_semantic_table(flights_tbl, name="flights")
    .with_dimensions(origin=lambda t: t.origin)
    .with_measures(flight_count=lambda t: t.count())
)
```

**3. Query it**

```python
result_df = flights.group_by("origin").aggregate("flight_count").execute()
```

---

## 📚 Documentation

**[→ View the full documentation](https://boringdata.github.io/boring-semantic-layer/)**

---

*This project is a joint effort by [xorq-labs](https://github.com/xorq-labs/xorq) and [boringdata](https://www.boringdata.io/).*

*We welcome feedback and contributions!*

---

*Freely inspired by the awesome [Malloy](https://github.com/malloydata/malloy) project. We loved the vision, just took the Python route.*
