# Query Agent: LLM Tool

LLM tools are Python functions that a language model can call during a conversation. When the model needs data, it invokes a tool, receives the result, and continues reasoning.

The advantage of this approach is that the LLM can directly execute Ibis-style chained queries—unlike MCP, which requires passing JSON payloads through a separate server.

**Benefits:**
- No additional server to run
- Full access to native BSL features without an intermediate DSL

## BSLTools: Framework-Agnostic Tool Layer

`BSLTools` provides tool definitions in OpenAI JSON Schema format (the de facto standard), making it compatible with any LLM provider:

- **OpenAI**: `client.chat.completions.create(tools=bsl.tools)`
- **LangChain**: `llm.bind_tools(bsl.tools)`
- **Anthropic**, **PydanticAI**, **AI SDK**, etc.

### Installation

```bash
pip install boring-semantic-layer[agent]
```

### Usage

```python
import json
from pathlib import Path
from openai import OpenAI
from boring_semantic_layer.agents.tools import BSLTools

# Initialize BSLTools with your semantic model
bsl = BSLTools(
    model_path=Path("flights.yml"),
    profile="dev",                        # Profile name (optional)
    profile_file=Path("profiles.yml"),    # Profile file path (optional)
    chart_backend="plotext",              # plotext, altair, or plotly
)

# Use with OpenAI SDK
client = OpenAI()
response = client.chat.completions.create(
    model="gpt-4o",
    messages=[
        {"role": "system", "content": bsl.system_prompt},
        {"role": "user", "content": "Show top 5 carriers by flight count"},
    ],
    tools=bsl.tools,
)

# Execute tool calls
for tool_call in response.choices[0].message.tool_calls or []:
    result = bsl.execute(
        tool_call.function.name,
        json.loads(tool_call.function.arguments)
    )
    print(result)
```

See [YAML Config](/building/yaml) for the semantic model format and [Backend Profiles](/building/profile) for connection setup.

### What BSLTools Provides

| Attribute | Description |
|-----------|-------------|
| `bsl.tools` | Tool definitions in OpenAI JSON Schema format |
| `bsl.system_prompt` | System prompt teaching the LLM how to write BSL queries |
| `bsl.execute(name, args)` | Execute a tool and return the result |

### Available Tools

The LLM has access to three tools:

#### `list_models`

Lists all available semantic models with their dimensions and measures. Useful when multiple models are loaded and the LLM needs to pick the right one.

#### `query_model`

Executes a BSL query and returns results. The LLM passes an Ibis-style query string:

```python
sm.group_by("origin").aggregate("flight_count")
```

**Parameters:**
- `query` — The BSL query string to execute
- `chart_spec` — Chart specification (backend, format, and visualization options)

#### `get_documentation`

Returns BSL documentation split into topics (query syntax, methods, charting, etc.). The LLM can explore relevant topics on demand to learn how to construct valid queries and charts.

