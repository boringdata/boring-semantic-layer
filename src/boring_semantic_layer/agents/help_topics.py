"""Progressive-disclosure help topics for `bsl docs`.

Structure: TOPICS[topic] = { "summary": ..., "content": ..., "subtopics": { name: { "summary": ..., "content": ... } } }

Level 0: `bsl docs`         → list topics with summaries
Level 1: `bsl docs <topic>` → topic content + subtopic list
Level 2: `bsl docs <topic> <subtopic>` → subtopic content
"""

TOPICS = {
    "overview": {
        "summary": "What BSL is, why it matters, quick start",
        "content": """\
BSL (Boring Semantic Layer) — deterministic analytics for LLM agents.

Define metrics once, query them consistently everywhere. No raw SQL.

WHY:
  LLMs generating SQL is non-deterministic: same question, different SQL,
  different results. BSL makes metrics declarative and reusable:
  - Measures defined once, same calculation every time
  - Type-safe joins (join_one vs join_many) prevent fan-out
  - Agents discover metrics via list_models() / get_model()
  - YAML models are version-controlled and tested

INSTALL:
  pip install boring-semantic-layer          # core
  pip install 'boring-semantic-layer[mcp]'   # + MCP server
  pip install 'boring-semantic-layer[agent]' # + chat agent

QUICK START:
  from boring_semantic_layer import to_semantic_table

  model = (
      to_semantic_table(table, name="orders")
      .with_dimensions(region=lambda t: t.region)
      .with_measures(
          count=lambda t: t.count(),
          revenue=lambda t: t.amount.sum(),
      )
  )
  result = model.group_by("region").aggregate("count", "revenue").execute()

NEXT:
  bsl docs build       → building models
  bsl docs query       → querying data
  bsl docs yaml        → YAML configuration
  bsl docs mcp         → MCP server for LLM tools""",
    },
    "build": {
        "summary": "Build semantic models (dimensions, measures, joins)",
        "content": """\
Building Semantic Models

A SemanticTable wraps an Ibis table with typed dimensions and measures:

  from boring_semantic_layer import to_semantic_table

  model = (
      to_semantic_table(ibis_table, name="orders")
      .with_dimensions(...)
      .with_measures(...)
  )

Three expression syntaxes:
  lambda:    region=lambda t: t.region
  unbound:   region=_.region                  (from ibis import _)
  class:     region=Dimension(expr=..., description="...")

SUBTOPICS:
  bsl docs build dimensions  → groupable attributes, time, derived
  bsl docs build measures    → aggregations, composed, percent-of-total
  bsl docs build joins       → join_one, join_many, prefixing""",
        "subtopics": {
            "dimensions": {
                "summary": "Define groupable attributes",
                "content": """\
Dimensions — groupable attributes (categorical data)

BASIC:
  model.with_dimensions(
      region=lambda t: t.region,       # lambda
      status=_.status,                  # unbound (from ibis import _)
      carrier=Dimension(                # with description (LLM-friendly)
          expr=lambda t: t.carrier,
          description="Airline carrier code"
      ),
  )

TIME DIMENSIONS:
  model.with_dimensions(
      year=lambda t: t.created_at.truncate("Y"),
      month=lambda t: t.created_at.truncate("M"),
      day=lambda t: t.created_at.truncate("D"),
  )
  Truncate units: "Y" "Q" "M" "W" "D" "h" "m" "s"

DERIVED (bucketing):
  model.with_dimensions(
      size=lambda t: ibis.cases(        # PLURAL: cases(), not case()
          (t.amount < 100, "small"),
          (t.amount < 1000, "medium"),
          else_="large"
      ),
  )

CRITICAL:
  - In with_dimensions lambdas, access columns directly: t.column
  - Do NOT use model prefix: t.orders.column  → ERROR
  - Always add descriptions for LLM-powered agents""",
            },
            "measures": {
                "summary": "Define aggregations and calculations",
                "content": """\
Measures — aggregations and calculations (quantitative data)

BASIC:
  model.with_measures(
      order_count=lambda t: t.count(),
      total_revenue=lambda t: t.amount.sum(),
      avg_value=lambda t: t.amount.mean(),
      max_delay=lambda t: t.delay.max(),
  )

COMPOSED (reference other measures):
  model.with_measures(
      count=lambda t: t.count(),
      revenue=lambda t: t.amount.sum(),
      avg_revenue=lambda t: t.revenue / t.count,   # uses count + revenue
  )

PERCENT OF TOTAL (t.all()):
  model.with_measures(
      count=lambda t: t.count(),
      market_share=lambda t: t.count / t.all(t.count) * 100,
  )

WITH DESCRIPTION:
  from boring_semantic_layer import Measure
  model.with_measures(
      revenue=Measure(
          expr=lambda t: t.amount.sum(),
          description="Total revenue in USD"
      ),
  )

RATIO PATTERNS:
  model.with_measures(
      total=lambda t: t.count(),
      delayed=lambda t: (t.delay > 0).sum(),
      delay_rate=lambda t: t.delayed / t.total * 100,
  )

CRITICAL:
  - In with_measures lambdas, access columns directly: t.column
  - Do NOT use model prefix: t.orders.column  → ERROR""",
            },
            "joins": {
                "summary": "Compose models with type-safe joins",
                "content": """\
Joins — compose models with fan-out safety

join_one  → each left row has exactly one right match (LEFT JOIN, no fan-out)
join_many → one left row has many right matches (handles aggregation correctly)
join_cross → cartesian product

  # Each order has one customer
  orders_joined = orders.join_one(
      customers, lambda o, c: o.customer_id == c.id
  )

  # One customer has many orders
  customers_joined = customers.join_many(
      orders, lambda c, o: c.id == o.customer_id
  )

  # Custom join type
  model.join(other, lambda a, b: a.key == b.key, how="left")

AFTER JOINS — fields are prefixed:
  orders_joined.group_by("customers.country").aggregate("orders.revenue")

  # In filters, use the prefix:
  orders_joined.filter(lambda t: t.customers.country == "US")

MULTIPLE JOINS TO SAME TABLE — use .view():
  pickup = to_semantic_table(locs_tbl.view(), "pickup")
  dropoff = to_semantic_table(locs_tbl.view(), "dropoff")

YAML JOINS:
  orders:
    joins:
      customers:
        model: customers
        type: one           # one | many | cross
        left_on: customer_id
        right_on: id""",
            },
        },
    },
    "query": {
        "summary": "Query semantic models (group_by, aggregate, filter, sort)",
        "content": """\
Querying Semantic Models

CORE PATTERN:
  model.group_by(<dimensions>).aggregate(<measures>)
  # Both take STRING names, not expressions

METHOD ORDER:
  model → filter → with_dimensions → with_measures → group_by → aggregate → order_by → mutate → limit

FULL EXAMPLE:
  result = (
      model
      .filter(lambda t: t.status == "delivered")
      .group_by("region", "year")
      .aggregate("order_count", "total_revenue")
      .order_by(ibis.desc("total_revenue"))
      .limit(10)
      .execute()    # → pandas DataFrame
  )
  print(result.sql())  # view SQL without executing

SUBTOPICS:
  bsl docs query filter     → predicates, .isin(), timestamps, HAVING
  bsl docs query aggregate  → string names, on-the-fly measures
  bsl docs query mutate     → post-aggregation transforms
  bsl docs query window     → rolling averages, rank, cumulative
  bsl docs query percent    → percent-of-total with t.all()
  bsl docs query nest       → arrays of structs, hierarchical data""",
        "subtopics": {
            "filter": {
                "summary": "Filter rows with predicates",
                "content": """\
Filtering

BASIC:
  model.filter(lambda t: t.status == "active")

MULTIPLE CONDITIONS — ibis.and_() / ibis.or_():
  model.filter(lambda t: ibis.and_(t.amount > 100, t.year >= 2023))
  model.filter(lambda t: ibis.or_(t.status == "a", t.status == "b"))

IN OPERATOR — MUST use .isin():
  model.filter(lambda t: t.region.isin(["US", "EU"]))     # CORRECT
  model.filter(lambda t: t.region in ["US", "EU"])         # ERROR!

POST-AGGREGATE (SQL HAVING):
  model.group_by("carrier").aggregate("count").filter(lambda t: t.count > 1000)

JOINED COLUMNS — use exact prefixed name from get_model():
  model.filter(lambda t: t.customers.country == "US")      # CORRECT
  model.filter(lambda t: t.customer_id.country())          # ERROR!

TIMESTAMPS — match types:
  model.filter(lambda t: t.date.year() >= 2023)            # .year() → int
  model.filter(lambda t: t.yr >= '2023-01-01')             # .truncate() → timestamp

NEVER GUESS FILTER VALUES:
  Data uses codes/IDs you don't expect ("California" might be "CA").
  Always discover first:
    Step 1: model.group_by("region").aggregate("count")  → see actual values
    Step 2: model.filter(lambda t: t.region.isin(["CA","NY"])) → use real values""",
            },
            "aggregate": {
                "summary": "Aggregate measures by dimensions",
                "content": """\
Aggregation

BASIC — string names only:
  model.group_by("origin").aggregate("flight_count", "avg_duration")

  CRITICAL: aggregate() takes measure NAMES as strings, NOT lambdas!
    model.aggregate("flight_count")                     # CORRECT
    model.aggregate(total=lambda t: t.sum())            # ERROR

ON-THE-FLY measures (without modifying model):
  model.group_by("origin").aggregate(
      "flight_count",                                    # pre-defined
      total_miles=lambda t: t.distance.sum(),            # on-the-fly
  )

NO GROUPING (grand total):
  model.group_by().aggregate("count", "revenue")

TABLE COLUMNS in aggregate:
  - Any column from underlying table works, not just pre-defined measures
  - But MUST be aggregated: t.distance.sum(), not just t.distance

SORTING & LIMITING:
  model.group_by("cat").aggregate("rev").order_by(ibis.desc("rev")).limit(10)""",
            },
            "mutate": {
                "summary": "Post-aggregation transforms",
                "content": """\
Mutate — add computed columns AFTER aggregation

  result = (
      model
      .group_by("origin")
      .aggregate("count", "total_distance")
      .mutate(
          avg_dist=lambda t: t.total_distance / t.count,
          category=lambda t: ibis.cases(
              (t.count >= 3, "high"),
              (t.count >= 2, "medium"),
              else_="low"
          ),
      )
  )

KEY DIFFERENCE:
  .aggregate() computes from raw data
  .mutate() transforms already-aggregated results""",
            },
            "window": {
                "summary": "Window functions (rolling, cumulative, rank)",
                "content": """\
Window Functions — calculations across ordered rows

Must come AFTER .order_by(), applied via .mutate():

ROLLING AVERAGE:
  model.group_by("week").aggregate("count").order_by("week").mutate(
      rolling_avg=lambda t: t.count.mean().over(
          ibis.window(rows=(-9, 0), order_by="week")
      )
  )

CUMULATIVE SUM:
  .mutate(running_total=lambda t: t.revenue.cumsum())

RANK:
  .mutate(rank=lambda t: ibis.rank().over(
      ibis.window(order_by=ibis.desc(t.revenue))
  ))

LAG / LEAD:
  .mutate(
      prev_count=lambda t: t.count.lag(1),
      next_count=lambda t: t.count.lead(1),
  )

See: bsl docs query mutate""",
            },
            "percent": {
                "summary": "Percent-of-total with t.all()",
                "content": """\
Percent of Total

Use t.all(measure) in with_measures() for grand total reference:

SIMPLE:
  model.with_measures(
      count=lambda t: t.count(),
      pct=lambda t: t.count / t.all(t.count) * 100,
  ).group_by("category").aggregate("count", "pct")

WITH FILTER + JOINED COLUMNS:
  orders.filter(
      lambda t: t.customers.country.isin(["US", "CA"])
  ).with_measures(
      pct=lambda t: t.order_count / t.all(t.order_count) * 100
  ).group_by("region").aggregate("order_count", "pct")""",
            },
            "nest": {
                "summary": "Create nested data structures",
                "content": """\
Nesting — arrays of structs within aggregated results

CREATE NESTED DATA:
  result = (
      model
      .group_by("origin")
      .aggregate(
          "flight_count",
          nest={"flights": lambda t: t.group_by(["carrier", "distance"])}
      )
  )

UNNEST (re-group to access nested fields):
  result.group_by("origin").aggregate(
      total=lambda t: t.flight_count.sum(),
      unique_carriers=lambda t: t.flights.carrier.nunique(),
  )

USE CASES:
  - API responses (JSON hierarchical structures)
  - Drill-down analysis
  - Preserving parent-child relationships""",
            },
        },
    },
    "yaml": {
        "summary": "YAML model configuration and profiles",
        "content": """\
YAML Configuration

Define models in YAML for version control and team collaboration.
YAML uses unbound syntax only (_.field), not lambdas.

MINIMAL EXAMPLE:
  orders:
    table: orders_tbl
    dimensions:
      region: _.region
      status: _.status
    measures:
      count: _.count()
      revenue: _.amount.sum()

LOADING:
  from boring_semantic_layer import from_yaml
  models = from_yaml("models.yml")
  orders = models["orders"]

  # With explicit tables (no profile)
  models = from_yaml("models.yml", tables={"orders_tbl": my_ibis_table})

SUBTOPICS:
  bsl docs yaml models    → full model syntax (descriptions, time dims)
  bsl docs yaml profiles  → database connection configuration
  bsl docs yaml joins     → defining joins in YAML""",
        "subtopics": {
            "models": {
                "summary": "Full YAML model syntax",
                "content": """\
YAML Model Syntax

FULL EXAMPLE:
  orders:
    table: orders_tbl
    description: "Order transactions"

    dimensions:
      # Simple
      region: _.region

      # With description (LLM-friendly)
      status:
        expr: _.status
        description: "Order status (pending, shipped, delivered)"

      # Time dimension
      order_date:
        expr: _.created_at
        is_time_dimension: true
        smallest_time_grain: TIME_GRAIN_DAY

      # Derived
      year: _.created_at.truncate("Y")

    measures:
      # Simple
      count: _.count()

      # With description
      revenue:
        expr: _.amount.sum()
        description: "Total revenue in USD"

      avg_value: _.amount.mean()

TIME GRAIN VALUES (short form preferred):
  second, minute, hour, day, week, month, quarter, year
  Long form also accepted: TIME_GRAIN_MONTH, TIME_GRAIN_YEAR, etc.

PER-DIMENSION GRAINS:
  Use time_grains to set different grains per time dimension:
    model.query(
        dimensions=["order_date", "ship_date"],
        measures=["total_sales"],
        time_grains={"order_date": "month", "ship_date": "quarter"}
    )

BEST PRACTICES:
  - Always add descriptions (LLMs use them to pick the right field)
  - Use business names: total_revenue not sum_amt
  - Mark time dimensions for time-grain query support""",
            },
            "profiles": {
                "summary": "Database connection profiles",
                "content": """\
Profiles — database connection configuration

INLINE PROFILE (in model YAML):
  profile:
    type: duckdb
    database: ":memory:"
    tables:
      orders_tbl: "s3://bucket/orders.parquet"
      users_tbl: "path/to/users.csv"

SEPARATE PROFILES FILE (profiles.yml):
  production:
    type: postgres
    host: db.example.com
    port: 5432
    database: analytics
    tables:
      orders_tbl: public.orders

  development:
    type: duckdb
    database: ":memory:"
    tables:
      orders_tbl: "data/orders.parquet"

LOADING WITH PROFILE:
  models = from_yaml("models.yml", profile="production")
  models = from_yaml("models.yml", profile="dev", profile_path="profiles.yml")

ENV VAR:
  BSL_PROFILE_FILE=profiles.yml  (auto-discovered)""",
            },
            "joins": {
                "summary": "Defining joins in YAML",
                "content": """\
YAML Joins

  orders:
    table: orders_tbl
    dimensions:
      region: _.region
    measures:
      count: _.count()
    joins:
      customers:              # join name (becomes field prefix)
        model: customers      # target model name
        type: one             # one | many | cross
        left_on: customer_id  # join column on this model
        right_on: id          # join column on target model

  customers:
    table: customers_tbl
    dimensions:
      country: _.country
      segment: _.segment
    measures:
      customer_count: _.count()

AFTER LOADING:
  orders = models["orders"]
  # Joined fields are auto-prefixed:
  orders.group_by("customers.country").aggregate("orders.count")

JOIN TYPES:
  one   → LEFT JOIN, no fan-out (each left row → one right row)
  many  → handles aggregation correctly (one left → many right)
  cross → cartesian product""",
            },
        },
    },
    "mcp": {
        "summary": "Serve models via MCP for LLM tools",
        "content": """\
MCP Server — serve semantic models to LLM tools (Claude, Cursor, etc.)

SETUP:
  from boring_semantic_layer import MCPSemanticModel, from_yaml

  models = from_yaml("models.yml")
  server = MCPSemanticModel(models=models, name="Analytics Server")

  if __name__ == "__main__":
      server.run()

CLI:
  bsl chat --sm models.yml

AVAILABLE TOOLS (for LLM agents):
  list_models()       → discover available model names
  get_model(name)     → inspect dimensions, measures, descriptions
  query_model(query)  → execute query, auto-display results
  get_time_range()    → available time range for time dimensions

SUBTOPICS:
  bsl docs mcp workflow  → agent query workflow and multi-hop pattern
  bsl docs mcp params    → query_model parameters reference""",
        "subtopics": {
            "workflow": {
                "summary": "Agent query workflow and multi-hop pattern",
                "content": """\
MCP Agent Workflow

THE DETERMINISTIC PATTERN:
  1. list_models()                      → discover models
  2. get_model("orders")                → inspect schema
  3. get_documentation("query-methods") → learn syntax (first query only)
  4. query_model(query=...)             → execute
  5. Brief summary (1-2 sentences)

RULES:
  - Execute immediately — don't show code to user
  - Never stop after listing models — proceed to query
  - Charts/tables auto-display — don't print inline
  - Reuse context — don't re-call tools if already loaded
  - If query fails → call get_documentation("query-methods") then retry

MULTI-HOP (discover-then-filter):
  When you don't know the actual values in the data:

  Step 1 (discover):
    query_model(
        query="model.group_by('region').aggregate('count')",
        records_limit=50,
        get_chart=false
    )

  Step 2 (filter with real values):
    query_model(
        query="model.filter(lambda t: t.region.isin(['CA','NY'])).group_by('region').aggregate('count')",
        get_records=false
    )

EXPLORATION VS FINAL:
  - Exploring data values → get_chart=false
  - Final answer for user → get_chart=true (default)""",
            },
            "params": {
                "summary": "query_model parameters reference",
                "content": """\
query_model Parameters

  query           (str)   BSL query string
  get_records     (bool)  Return data to LLM (default: true)
  get_chart       (bool)  Show chart (default: true)
  records_limit   (int)   Max records returned to LLM
  chart_spec      (dict)  Override chart type: {"chart_type": "bar"}
  chart_backend   (str)   "altair" | "plotly" | "plotext"

COMMON PATTERNS:
  # Discovery query (data to LLM, no chart)
  query_model(query=..., records_limit=50, get_chart=false)

  # Display-only (chart for user, no data to LLM)
  query_model(query=..., get_records=false)

  # Force chart type
  query_model(query=..., chart_spec={"chart_type": "line"})

IMPORTANT:
  - Charts require group_by + aggregate results
  - Filter-only queries (raw Ibis Table) → set get_chart=false
  - Omit chart_spec for auto-detection (handles most cases)""",
            },
        },
    },
    "pitfalls": {
        "summary": "Common mistakes and how to avoid them",
        "content": """\
Common Pitfalls

1. PYTHON 'in' vs .isin():
   model.filter(lambda t: t.region in ["US", "EU"])     # WRONG — runtime error
   model.filter(lambda t: t.region.isin(["US", "EU"]))  # CORRECT

2. ibis.case() vs ibis.cases():
   ibis.case(...)                                        # WRONG — singular
   ibis.cases((cond, val), (cond, val), else_=default)  # CORRECT — plural

3. MODEL PREFIX IN LAMBDAS:
   model.with_dimensions(x=lambda t: t.orders.region)   # WRONG
   model.with_dimensions(x=lambda t: t.region)           # CORRECT
   (model prefix only works in .filter() for joined models)

4. GUESSING COLUMN NAMES:
   model.filter(lambda t: t.country == "US")             # WRONG
   model.filter(lambda t: t.customers.country == "US")   # CORRECT
   Always call get_model() first to see exact field names.

5. GUESSING FILTER VALUES:
   model.filter(lambda t: t.state == "California")       # WRONG — might be "CA"
   Always discover actual values with a group_by query first.

6. RAW SQL GENERATION:
   sql = f"SELECT ... WHERE status = '{status}'"         # WRONG — injection risk
   model.filter(lambda t: t.status == status)            # CORRECT — deterministic

7. AGGREGATE WITH LAMBDAS:
   model.aggregate(total=lambda t: t.sum())              # WRONG
   model.aggregate("total_revenue")                      # CORRECT — string name

8. UNAGGREGATED TABLE COLUMNS:
   model.aggregate(x=lambda t: t.distance)               # WRONG — must aggregate
   model.aggregate(x=lambda t: t.distance.sum())         # CORRECT

9. LIMIT POSITION:
   model.limit(10).group_by("x").aggregate("y")          # WRONG — limits raw data
   model.group_by("x").aggregate("y").limit(10)          # CORRECT — limits results

10. WINDOW WITHOUT ORDER_BY:
    .mutate(avg=lambda t: t.x.mean().over(window))       # WRONG — no ordering
    .order_by("date").mutate(avg=...)                     # CORRECT — ordered first""",
    },
    "examples": {
        "summary": "Quick recipes for common patterns",
        "content": """\
Quick Recipes

TOP N:
  model.group_by("carrier").aggregate("revenue")
      .order_by(ibis.desc("revenue")).limit(5)

YEAR-OVER-YEAR:
  model.with_dimensions(year=lambda t: t.date.truncate("Y"))
      .group_by("year").aggregate("revenue")
      .order_by("year")

MARKET SHARE:
  model.with_measures(
      pct=lambda t: t.revenue / t.all(t.revenue) * 100
  ).group_by("region").aggregate("revenue", "pct")

FILTER + JOINED COLUMNS:
  model.filter(lambda t: t.customers.country.isin(["US", "CA"]))
      .group_by("customers.segment").aggregate("order_count")

ROLLING AVERAGE:
  model.group_by("week").aggregate("sales")
      .order_by("week")
      .mutate(ma4=lambda t: t.sales.mean().over(
          ibis.window(rows=(-3, 0), order_by="week")
      ))

HAVING (post-aggregate filter):
  model.group_by("carrier").aggregate("count")
      .filter(lambda t: t.count > 100)

BUCKETING:
  model.with_dimensions(
      tier=lambda t: ibis.cases(
          (t.amount >= 1000, "premium"),
          (t.amount >= 100, "standard"),
          else_="basic"
      )
  ).group_by("tier").aggregate("count")

MCP SETUP:
  from boring_semantic_layer import MCPSemanticModel, from_yaml
  models = from_yaml("models.yml")
  MCPSemanticModel(models=models, name="My Data").run()

YAML QUICK START:
  orders:
    table: orders_tbl
    dimensions:
      region:
        expr: _.region
        description: "Sales region"
    measures:
      count:
        expr: _.count()
        description: "Order count"

  → from_yaml("models.yml")""",
    },
}


def format_topic_list():
    """Level 0: show all topics with summaries."""
    lines = [
        "BSL — Boring Semantic Layer",
        "Deterministic analytics for LLM agents.",
        "",
        "Usage: bsl docs <topic> [subtopic]",
        "",
        "Topics:",
    ]
    for name, topic in TOPICS.items():
        lines.append(f"  {name:<12} {topic['summary']}")
    lines.append("")
    lines.append("Run: bsl docs <topic>")
    return "\n".join(lines)


def format_topic(name):
    """Level 1: show topic content + subtopic list."""
    topic = TOPICS.get(name)
    if not topic:
        suggestions = [t for t in TOPICS if t.startswith(name[:3])]
        msg = f"Unknown topic: {name}\n"
        if suggestions:
            msg += f"Did you mean: {', '.join(suggestions)}?\n"
        msg += f"\nAvailable topics: {', '.join(TOPICS.keys())}"
        return msg
    return topic["content"]


def format_subtopic(topic_name, subtopic_name):
    """Level 2: show subtopic content."""
    topic = TOPICS.get(topic_name)
    if not topic:
        return format_topic(topic_name)  # will show error

    subtopics = topic.get("subtopics", {})
    if not subtopics:
        return f"No subtopics for '{topic_name}'.\n\n{topic['content']}"

    sub = subtopics.get(subtopic_name)
    if not sub:
        suggestions = [s for s in subtopics if s.startswith(subtopic_name[:3])]
        msg = f"Unknown subtopic: {subtopic_name}\n"
        if suggestions:
            msg += f"Did you mean: {', '.join(suggestions)}?\n"
        msg += f"\nAvailable subtopics for '{topic_name}': {', '.join(subtopics.keys())}"
        return msg
    return sub["content"]
