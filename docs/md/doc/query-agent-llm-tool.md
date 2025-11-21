# Query Agent: BSL as an LLM Tool

If you already have a LangChain workflow (CLI, Slack, custom app), you can drop the Query Agent in as a toolset. Everything routes through [`LangChainAgent`](../../src/boring_semantic_layer/agents/backends/langchain.py), so table discovery and query execution stay identical no matter the host.

## Tool surface

- `list_models()` -> returns a friendly description of every semantic model plus its dimensions/measures.
- `query_model(query, show_chart, show_table, chart_spec, limit)` -> executes a literal BSL pipeline string and optionally renders a chart.

Use these helpers from any LangChain-compatible LLM (OpenAI, Claude, Gemini). The agent will automatically keep the conversation history, enforce the `.with_dimensions()` ordering, and cap row counts unless a user explicitly asks for more.

## LangChain + BSL Chat CLI

The CLI is the fastest way to get hands-on with the Query Agent:

```bash
pip install boring-semantic-layer

bsl chat \
  --sm examples/flights/flights.yaml \
  --model claude-3-5-sonnet-20241022 \
  --chart-backend plotext
```

Key flags:

- `--sm` -> semantic model YAML path (same file you load in notebooks).
- `--model` -> OpenAI, Anthropic, or Google model; the CLI auto-detects provider if you omit it.
- `--chart-backend` -> `plotext` (TTY by default) or `altair`/`plotly` for richer renderers.
- `--profile` / `--profile-file` -> point to connection info if your YAML references `profiles.yml`.

Behind the scenes the CLI spins up `LangChainAgent` from [`boring_semantic_layer.agents.backends.langchain`](../../src/boring_semantic_layer/agents/backends/langchain.py), prints tool calls for transparency, and streams chart/table output inline.

## LangChain + Slack

[`BSLSlackBot`](../../src/boring_semantic_layer/agents/chats/slack.py) wraps the same agent logic inside a Socket Mode Slack bot:

```python
from boring_semantic_layer.agents.chats.slack import BSLSlackBot

bot = BSLSlackBot(
    semantic_model_path="examples/flights/flights.yaml",
    llm_model="gpt-4o-mini",
)

bot.start()
```

Provide `SLACK_BOT_TOKEN` and `SLACK_APP_TOKEN` via environment variables or constructor args. When a user mentions the bot, it uses the LangChain agent to query BSL models and reply with results.
