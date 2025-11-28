# Query Agent: LLM Tool

LLM tools are Python functions that a language model can call during a conversation. When the model needs data, it invokes a tool, receives the result, and continues reasoning.

The advantage of this approach is that the LLM can directly execute Ibis-style chained queries—unlike MCP, which requires passing JSON payloads through a separate server.

**Benefits:**
- No additional server to run
- Full access to native BSL features without an intermediate DSL

## Reference implementation

We provide a LangChain-based chat agent as a reference:

👉 [`langchain.py`](https://github.com/boringdata/boring-semantic-layer/blob/main/src/boring_semantic_layer/agents/backends/langchain.py)

This implementation powers the [BSL CLI demo chat](/agents/chat) and can be adapted to any AI framework (PydanticAI, AI SDK).

## Integrating with your own agent ([LangChain](https://www.langchain.com/))

### Installation

Install the agent dependency group:

```bash
pip install boring-semantic-layer[agent]
```

Then install the LLM provider you want to use:

```bash
# OpenAI
pip install langchain-openai

# Anthropic
pip install langchain-anthropic

# Google
pip install langchain-google-genai
```

### Usage

The `LangChainAgent` loads semantic models from a [YAML config file](/building/yaml):

```python
from pathlib import Path
from boring_semantic_layer.agents.backends.langchain import LangChainAgent

agent = LangChainAgent(
    model_path=Path("flights.yaml"),      # Semantic model YAML
    llm_model="gpt-4o",                   # LLM to use
    chart_backend="plotext",              # plotext, altair, or plotly
    profile="dev",                        # Profile name (optional)
    profile_file=Path("profiles.yml"),    # Profile file path (optional)
)

tool_output, response = agent.query("What are the top 10 origins by flight count?")
```

See [YAML Config](/building/yaml) for the semantic model format and [Backend Profiles](/building/profile) for connection setup.

## Available tools

The LLM has access to three tools, similar to the MCP approach:

### `get_documentation`

Returns BSL documentation split into topics (query syntax, methods, charting, etc.). The LLM can explore relevant topics on demand to learn how to construct valid queries and charts.

### `list_models`

Lists all available semantic models by name. Useful when multiple models are loaded and the LLM needs to pick the right one.

### `query_model`

Executes a BSL query and returns results. The LLM passes an Ibis-style query string:

```python
sm.group_by("origin").aggregate("flight_count")
```

The tool executes this query against your semantic model and returns the result.

**Parameters:**
- `query` — The BSL query string to execute
- `show_chart` — Return a chart visualization
- `show_table` — Return a DataFrame
- `chart_spec` — Custom Vega-Lite or Plotly chart specification
- `limit` — Cap the number of rows returned
