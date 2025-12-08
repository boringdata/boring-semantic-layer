# Getting Started with BSL

BSL (Boring Semantic Layer) is a lightweight semantic layer built on top of Ibis. It allows you to define your data models once and query them anywhere.

## Installation

```bash
pip install boring-semantic-layer
```

## Quick Start

Let's create your first Semantic Table using synthetic data in Ibis.

```setup_flights
import ibis
from boring_semantic_layer import to_semantic_table

# Create sample flight data
flights_tbl = ibis.memtable({
    "origin": ["NYC", "LAX", "NYC", "SFO", "LAX", "NYC", "SFO", "LAX"],
    "destination": ["LAX", "NYC", "SFO", "NYC", "SFO", "LAX", "LAX", "SFO"],
    "distance": [2789, 2789, 2902, 2902, 347, 2789, 347, 347],
    "duration": [330, 330, 360, 360, 65, 330, 65, 65],
})
```

You can then convert these tables in Semantic Tables that contains dimensios and measures definitions:

```define_semantic_table
# Define semantic table with dimensions and measures
flights_st = (
    to_semantic_table(flights_tbl, name="flights")
    .with_dimensions(
        origin=lambda t: t.origin,
        destination=lambda t: t.destination,
    )
    .with_measures(
        flight_count=lambda t: t.count(),
        total_distance=lambda t: t.distance.sum(),
        avg_duration=lambda t: t.duration.mean(),
    )
)
```

## Query Your Data

Now let's query the semantic table by grouping flights by origin:

```query_by_origin
# Group flights by origin airport
result = flights_st.group_by("origin").aggregate(
    "flight_count",
    "total_distance",
    "avg_duration"
)
```

<bslquery code-block="query_by_origin"></bslquery>

You can also group by destination:

```query_by_destination
# Group flights by destination airport
result = flights_st.group_by("destination").aggregate(
    "flight_count",
    "total_distance"
)
```

<bslquery code-block="query_by_destination"></bslquery>

## Chat with Your Data

BSL includes a built-in chat interface to query your semantic models using natural language.

### 1. Install the agent extra

```bash
pip install 'boring-semantic-layer[agent]'

# Install your LLM provider
pip install langchain-anthropic  # or langchain-openai, langchain-google-genai
```

### 2. Set your API key

Create a `.env` file:

```bash
ANTHROPIC_API_KEY=sk-ant-...  # or OPENAI_API_KEY, GOOGLE_API_KEY
```

### 3. Start chatting

Try the built-in flights demo model (loads remote data automatically):

```bash
# Interactive mode
bsl chat --sm https://raw.githubusercontent.com/boringdata/boring-semantic-layer/main/examples/flights.yml

# Or pass a question directly
bsl chat --sm https://raw.githubusercontent.com/boringdata/boring-semantic-layer/main/examples/flights.yml \
  "What are the top 5 origins by flight count?"

```

### Create your own YAML model

Here's a minimal example showing how to define your own semantic model:

```yaml
# my_model.yaml - Minimal BSL semantic model

# Database profile - loads remote parquet into in-memory DuckDB
profile:
  type: duckdb
  database: ":memory:"
  tables:
    orders_tbl: "path/to/orders.parquet"

# Semantic model definition
orders:
  table: orders_tbl
  description: "Order data with categories and metrics"

  dimensions:
    category:
      expr: _.category
      description: "Product category"
    region:
      expr: _.region
      description: "Sales region"
    status: _.status

  measures:
    order_count:
      expr: _.count()
      description: "Total number of orders"
    total_sales:
      expr: _.amount.sum()
      description: "Total sales amount"
    avg_order_value:
      expr: _.amount.mean()
      description: "Average order value"
```

Then run:

```bash
bsl chat --sm my_model.yaml
```

See [Query Agent Chat](/examples/query-agent-chat) for full documentation on YAML models with joins and advanced features.

## Next Steps

- [Chat with your data](/examples/query-agent-chat) using natural language
- Define models in [YAML configuration](/examples/yaml-config)
- Configure database connections with [Profiles](/examples/profile)
- Learn how to [Build Semantic Tables](/examples/semantic-table) with dimensions, measures, and joins
- Explore [Query Methods](/examples/query-methods) for retrieving data
- Discover how to [Compose Models](/examples/compose) together
