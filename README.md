# Boring Semantic Layer (BSL)

**A lightweight, Ibis-powered semantic layer that makes your data queryable by both humans and AI.**

BSL lets you define your data model once - dimensions, measures, and relationships - then query it with a simple, fluent API. Built on [Ibis](https://ibis-project.org/), it works with any database that Ibis supports (DuckDB, Snowflake, BigQuery, PostgreSQL, and more).

## Why BSL?

- **Define once, query anywhere**: Create semantic tables that abstract away SQL complexity
- **Built for AI agents**: Native [MCP (Model Context Protocol)](https://modelcontextprotocol.io/) support lets LLMs query your data directly
- **Pure Python**: No DSL to learn - just Python and Ibis expressions
- **Instant visualization**: Built-in charting with Altair and Plotly backends

## Quick Start

```bash
pip install 'boring-semantic-layer[examples]'
```

```python
import ibis
from boring_semantic_layer import to_semantic_table

# 1. Define your semantic model
flights = (
    to_semantic_table(flights_tbl, name="flights")
    .with_dimensions(origin=lambda t: t.origin)
    .with_measures(flight_count=lambda t: t.count())
)

# 2. Query it
result = flights.group_by("origin").aggregate("flight_count").execute()
```

## Installation

```bash
# Basic installation
pip install boring-semantic-layer

# With DuckDB support (for examples)
pip install 'boring-semantic-layer[examples]'

# With MCP integration
pip install 'boring-semantic-layer[fastmcp]'

# With visualization support
pip install 'boring-semantic-layer[viz-altair]'  # or viz-plotly
```

---

## 📚 Documentation

**[→ View the full documentation](https://xorq-labs.github.io/boring-semantic-layer/)**

Learn about:
- Getting started guide
- Defining semantic tables with dimensions and measures
- Advanced query patterns (filters, joins, window functions)
- MCP integration for AI agents
- Chart visualization with Altair and Plotly
- YAML configuration
- Complete API reference

---

*This project is a joint effort by [xorq-labs](https://github.com/xorq-labs/xorq) and [boringdata](https://www.boringdata.io/).*

*We welcome feedback and contributions!*
