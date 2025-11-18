# BSL vs Malloy vs dbt Semantic Layer

Comprehensive comparison of three semantic layer solutions to help you choose the right tool for your needs.

## Comparison Table

| Aspect | BSL | Malloy | dbt Semantic Layer |
|--------|-----|--------|-------------------|
| **Language Type** | Pure Python API | Custom DSL (programming language) | YAML Configuration |
| **Query Style** | Python fluent API with lambdas | Malloy query language with `->` operator | CLI commands or API calls |
| **Learning Curve** | Python + Ibis expressions | New language (approachable syntax) | YAML + dbt + MetricFlow concepts |
| **Backend Support** | 20+ databases via Ibis | 7 databases (BigQuery, Snowflake, PostgreSQL, MySQL, Trino, Presto, DuckDB) | dbt-supported warehouses |
| **License** | Open Source | MIT (fully open source) | Hybrid (MetricFlow open, full Layer requires paid dbt Cloud) |
| **Cost** | ✅ Free | ✅ Free | ⚠️ Free for local use, paid for cloud APIs & BI integrations |
| **Visualization** | Built-in (Altair/Plotly) | Built-in (basic in VSCode/Composer) | Via BI tool integrations |
| **AI/LLM Support** | ✅ MCP protocol native | Not explicit | ✅ Native (MetricFlow open-sourced for AI) |
| **IDE Support** | Standard Python IDEs | ✅ Excellent VSCode extension | dbt Cloud IDE |
| **Programming Languages** | Python | Python (`malloy-py`), JavaScript/TypeScript | Python (`dbt-metricflow`) |
| **Dimensions** | Lambda functions with Ibis | Native `dimension:` syntax | YAML `dimensions:` definitions |
| **Measures** | Lambda functions with aggregations | Native `measure:` syntax | YAML `measures:` with aggregation types |
| **Joins** | Ibis join system | ✅ Graph-based, automatic safety (prevents fan-out) | Entity-based, dynamic at query time |
| **Nested Queries** | Via Ibis subqueries | ✅ Native, infinite nesting | Limited |
| **Time Operations** | Ibis time functions (`.year()`, `.month()`) | ✅ Built-in syntax (`@2003`, `.year`, `.month`) | Time dimensions with granularity |
| **Window Functions** | ✅ Full Ibis support | ✅ Supported | ✅ Supported |
| **Calculated Fields** | Python expressions with Ibis | Built-in expressions with `pick` statements | SQL expressions in YAML |
| **Metric Types** | Custom via Python | Dimensions + Measures | 5 types: Simple, Ratio, Cumulative, Derived, Conversion |
| **Performance** | Depends on Ibis + backend | ✅ Optimized SQL generation (faster than hand-written) | Depends on warehouse + caching |
| **BI Integrations** | Export to DataFrames for any tool | Export + growing ecosystem | ✅ Extensive (Tableau, Power BI, Looker, Mode, Hex, etc.) |
| **CLI Tools** | Standard Python | `malloy-cli` (run, compile, connections) | `dbt sl` (query, list, validate) |
| **Setup Complexity** | ⚡ Simple (`pip install`) | Medium (npm/VSCode extension) | Complex (requires dbt project) |
| **Community Size** | Growing | Medium (Google-backed) | ✅ Large (dbt ecosystem) |
| **Primary Use Case** | Python-first analytics, AI agents | Complex analytical queries, BigQuery | Enterprise metrics governance |
| **Target Audience** | Data scientists, Python developers | Data analysts, anyone wanting "better SQL" | Analytics engineers, enterprise teams |
| **Enterprise Features** | Basic | Basic | ✅ Governance, validation, centralized metrics |
| **Documentation** | Growing | ✅ Comprehensive | ✅ Extensive |
| **Query Syntax Example** | `.filter(lambda t: t.year > 1900)` | `where: year > 1900` | `--where "year > 1900"` |
| **Join Syntax** | Explicit Ibis joins | `join_one: users with user_id` | Entity relationships in YAML |
| **Inspired By** | Malloy + Ibis portability | Looker (created by Looker founder) | LookML + metrics-as-code |


## Resources

### BSL
- GitHub: [boring-semantic-layer](https://github.com/boringdata/boring-semantic-layer)
- Docs: [boringdata.github.io/boring-semantic-layer](https://boringdata.github.io/boring-semantic-layer/)
- Install: `pip install boring-semantic-layer`

### Malloy
- Website: [malloydata.dev](https://www.malloydata.dev/)
- Docs: [docs.malloydata.dev](https://docs.malloydata.dev/)
- GitHub: [malloydata/malloy](https://github.com/malloydata/malloy)
- Install: `npm install -g @malloydata/malloy-cli` or VSCode extension

### dbt Semantic Layer
- Website: [getdbt.com/product/semantic-layer](https://www.getdbt.com/product/semantic-layer)
- Docs: [docs.getdbt.com](https://docs.getdbt.com/docs/use-dbt-semantic-layer/dbt-sl)
- GitHub: [dbt-labs/metricflow](https://github.com/dbt-labs/metricflow)
- Install: `pip install dbt-metricflow`
