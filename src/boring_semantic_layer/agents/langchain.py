"""
LangChain Backend for Boring Semantic Layer

This module provides the core LangChain agent logic for querying semantic models.
It's backend-agnostic and can be used by different frontends (CLI, Slack, etc.).
"""

from collections.abc import Callable
from pathlib import Path

from langchain.tools import tool

from boring_semantic_layer.agents.tools import BSLTools

# Global state for tools instance
_TOOLS: BSLTools | None = None


def _initialize_tools(
    model_path: Path,
    chart_backend: str = "plotext",
    profile: str | None = None,
    profile_file: Path | str | None = None,
) -> BSLTools:
    """Initialize the BSL tools instance."""
    global _TOOLS
    _TOOLS = BSLTools(model_path, chart_backend, profile, profile_file)
    return _TOOLS


@tool
def list_models() -> str:
    """List all available semantic models with their dimensions and measures."""
    global _TOOLS

    if _TOOLS is None:
        return "Error: No models loaded. Please check the configuration."

    return _TOOLS.list_models()


@tool
def query_model(
    query: str,
    show_chart: bool = True,
    show_table: bool = True,
    chart_spec: dict | None = None,
    limit: int = 10,
) -> str:
    """
    Execute a semantic model query and return results with optional chart.

    Args:
        query: A query string in the format:
               model_name.group_by("dim1", "dim2").aggregate("measure1", "measure2")
               Also supports .with_dimensions() for time-based transformations:
               model.with_dimensions(month=lambda t: t.time_col.truncate("M")).group_by("month").aggregate("measure")
        show_chart: Whether to display a chart visualization (default: True)
        show_table: Whether to display the data table (default: True)
        chart_spec: Optional chart specification dict with keys:
                   - chart_type: "bar", "line", "scatter", "table"
                   - theme: "pro", "clear", "dark", "default" (plotext themes)
                   - height: chart height in terminal lines (default: 20)
                   - grid: show grid lines (default: true)
                   - title: custom chart title
                   - marker: marker style (single character like "●", "■", "▲")
        limit: Maximum number of rows to display in the table (default: 10, use 0 for all rows)

    Returns:
        Formatted string with query results and optional terminal chart.
    """
    global _TOOLS

    if _TOOLS is None:
        return "Error: No models loaded. Please check the configuration."

    return _TOOLS.query_model(
        query, show_chart=show_chart, show_table=show_table, chart_spec=chart_spec, limit=limit
    )


def create_system_message() -> str:
    """Create the system message for the LangChain agent."""
    # Load the BSL Query Expert skill from claude-code directory
    skill_path = Path(__file__).parent / "claude-code" / "bsl-query-expert" / "SKILL.md"
    try:
        bsl_guide = skill_path.read_text()
        # Remove the frontmatter (---...---) if present
        if bsl_guide.startswith("---"):
            parts = bsl_guide.split("---", 2)
            if len(parts) >= 3:
                bsl_guide = parts[2].strip()
    except FileNotFoundError:
        # Fallback to basic instructions if guide not found
        bsl_guide = """
## Basic Query Syntax

All BSL queries follow this pattern:
```python
model_name.group_by(<dimensions>).aggregate(<measures>)
```

### Key Rules:
1. group_by() only accepts dimension names as strings
2. Use .with_dimensions() for time transformations
3. Always quote dimension and measure names
"""

    return f"""You are a data analytics agent specialized in querying semantic models using the Boring Semantic Layer.

Your role is to:
1. Understand natural language questions about data
2. Translate them into semantic model queries
3. Use the available tools to execute queries
4. Present results clearly with appropriate visualizations

Available Tools:
1. list_models() - Lists all available models, dimensions, and measures
2. query_model(query, show_chart, show_table, chart_spec, limit) - Executes a query and returns results

## Display Options

You have full control over how results are presented:

**Table Display:**
- `show_table`: Whether to show the data table (default: true)
- `limit`: Number of rows to display (default: 10)
  - Keep default of 10 for most queries to avoid overwhelming output
  - Use smaller values (3-5) only when user asks for "top N" or "just a few"
  - Avoid using 0 (all rows) unless user specifically asks to "show all data"

**Chart Display:**
- `show_chart`: Whether to show a chart visualization (default: true)
- `chart_spec`: Customize chart appearance with:
  - `chart_type`: "bar", "line", "scatter", "table"
  - `theme`: "pro", "clear", "dark", "default"
  - `height`: Terminal lines (default: 40, range: 20-60 for better resolution)
  - `width`: Terminal width in characters (default: auto-detect from terminal)
  - `grid`: Show grid lines (default: true)
  - `title`: Custom chart title
  - `marker`: Marker style like "●", "■", "▲"

**Common Scenarios:**
- Chart only: `show_table=False, show_chart=True`
- Table only: `show_table=True, show_chart=False`
- Top N results: `show_chart=True, limit=5`
- Extra high-res chart: `chart_spec={{"height": 50}}`
- Custom styled chart: `chart_spec={{"theme": "dark", "height": 45, "title": "My Chart"}}`

When asked about data:
1. **CRITICAL**: Do NOT show code or examples to the user - EXECUTE queries directly using the tools
2. Only call list_models() if you genuinely don't know what dimensions/measures are available
3. Construct the appropriate BSL query following the syntax below
4. **Immediately execute** the query using query_model() tool with appropriate display options
5. Use default limit=10 unless user specifically asks for different row count
6. After execution, provide a brief natural language summary of the results

**NEVER do this:**
- Show Python code examples with `query = '''...'''`
- Show `functions.query_model()` or similar pseudo-code
- Explain how to run queries

**ALWAYS do this:**
- Call the query_model() tool directly with the query string
- Let the tool display the results (table/chart)
- Provide a brief summary after results are shown

**Example of correct behavior:**

User asks: "Show me flights per week with 10-week rolling average"

❌ WRONG Response:
"Here's how to calculate it:
```python
query = 'flights.with_dimensions(...)'
result = query_model({{"query": query}})
```"

✅ CORRECT Response:
*Agent immediately calls query_model() tool with the query*
*Tool displays results (table + chart)*
"The chart shows weekly flight counts (blue) and the 10-week rolling average (green) which smooths out short-term fluctuations. The rolling average reveals an upward trend from 2000 to 2005."

## Post-Aggregation Transformations with .mutate()

**IMPORTANT**: BSL supports advanced post-aggregation operations using `.mutate()` with window functions.

**Common operations you can perform with .mutate():**
- Rolling averages/sums (moving windows)
- Cumulative calculations (running totals, cumsum)
- Period-over-period comparisons (lag/lead)
- Ranking (rank, row_number, dense_rank)
- Percentage changes
- Computed columns from aggregated results

### Rolling Averages (Moving Windows)

For a 3-week rolling average, use `.mutate()` with `.over()`:

```python
# First: aggregate to weekly level with time dimension
weekly = flights.with_dimensions(
    arr_week=lambda t: t.arr_time.truncate("W")
).group_by("arr_week").aggregate("flight_count").order_by("arr_week")

# Then: apply 3-week rolling average using window function
window_3wk = ibis.window(rows=(-2, 0), order_by="arr_week")
result = weekly.mutate(
    rolling_avg_3wk=_.flight_count.mean().over(window_3wk).round(2)
)
```

**Window specification:**
- `rows=(-2, 0)` means "2 rows before current row through current row" (3 total rows)
- `rows=(-6, 0)` for 7-day moving average
- Must include `order_by` in window for proper ordering

### Cumulative Calculations

For running totals:

```python
result = weekly.mutate(
    cumulative_flights=_.flight_count.cumsum()
)
```

### Period-over-Period Comparisons

For week-over-week changes:

```python
result = weekly.mutate(
    prev_week_count=_.flight_count.lag(),
    wow_change=_.flight_count - _.flight_count.lag(),
    wow_pct_change=((_.flight_count - _.flight_count.lag()) / _.flight_count.lag() * 100).round(2)
)
```

**Key points:**
- `.lag()` accesses previous row value
- `.lead()` accesses next row value
- First row's lag will be null

### Important Guidelines

1. **Order matters**: ALWAYS use `.order_by()` before `.mutate()` with window functions
2. **Structure**: `aggregate() → order_by() → mutate(window_function)`
3. **Table vs Chart**: Use default `limit=10` for table display - the chart automatically uses all data for accurate visualization
4. **Use ibis constructs**: `ibis.window()`, `ibis._` (underscore) syntax

When providing your response, keep it concise since the full results with charts are shown to the user.

---

{bsl_guide}"""


def process_query(
    llm,  # Type: ChatOpenAI | ChatAnthropic | ChatGoogleGenerativeAI
    user_input: str,
    conversation_history: list,
    on_tool_call: Callable[[str, dict], None] | None = None,
) -> tuple[str, str]:
    """
    Process a user query using the LLM and tools.

    Args:
        llm: The LLM instance (ChatOpenAI, ChatAnthropic, or ChatGoogleGenerativeAI)
        user_input: User's query
        conversation_history: List of previous messages
        on_tool_call: Optional callback for tool calls (function_name, function_args)

    Returns:
        (tool_output, agent_response) tuple
    """
    # Add system message and user input to conversation
    messages = (
        [
            {"role": "system", "content": create_system_message()},
        ]
        + conversation_history
        + [{"role": "user", "content": user_input}]
    )

    # Bind tools to the LLM using the modern LangChain interface
    tools = [list_models, query_model]
    llm_with_tools = llm.bind_tools(tools)

    # Allow multiple rounds of function calling
    max_iterations = 5
    all_tool_outputs = []

    for _ in range(max_iterations):
        # Get LLM response with tool calling
        response = llm_with_tools.invoke(messages)

        # Check if the LLM wants to call a tool
        if not response.tool_calls:
            # No more tool calls, return final response
            tool_output = "\n\n".join(all_tool_outputs) if all_tool_outputs else ""
            return tool_output, response.content

        # Process each tool call
        messages.append(response)

        for tool_call in response.tool_calls:
            function_name = tool_call["name"]
            function_args = tool_call["args"]

            # Notify frontend about tool call
            if on_tool_call:
                on_tool_call(function_name, function_args)

            if function_name == "list_models":
                tool_output = list_models.invoke({})
                # Don't add list_models to display outputs - it's just for agent context
            elif function_name == "query_model":
                tool_output = query_model.invoke(function_args)
                # Only add query results to display outputs
                all_tool_outputs.append(tool_output)
            else:
                tool_output = f"Unknown function: {function_name}"
                all_tool_outputs.append(tool_output)

            # Add tool result to messages
            messages.append(
                {
                    "role": "tool",
                    "content": tool_output,
                    "tool_call_id": tool_call["id"],
                }
            )

    # If we hit max iterations, return what we have
    tool_output = "\n\n".join(all_tool_outputs) if all_tool_outputs else ""
    return tool_output, "Reached maximum function call iterations."


class LangChainAgent:
    """LangChain agent for semantic model querying."""

    def __init__(
        self,
        model_path: Path,
        llm_model: str = "gpt-4",
        openai_api_key: str | None = None,
        chart_backend: str = "plotext",
        profile: str | None = None,
        profile_file: Path | str | None = None,
    ):
        """
        Initialize the LangChain agent.

        Args:
            model_path: Path to YAML semantic model definition
            llm_model: LLM model to use. Supports:
                      - OpenAI: gpt-4, gpt-4-turbo, gpt-3.5-turbo
                      - Anthropic: claude-3-5-sonnet-20241022, claude-3-opus-20240229
                      - Google: gemini-1.5-pro, gemini-1.5-flash
                      (default: gpt-4)
            openai_api_key: OpenAI API key (defaults to env var OPENAI_API_KEY)
                           For other providers, set ANTHROPIC_API_KEY or GOOGLE_API_KEY
            chart_backend: Chart backend to use (default: plotext)
            profile: Optional profile name to use for database connection
            profile_file: Optional path to profiles.yml file
        """
        self.model_path = model_path
        self.llm_model = llm_model
        self.chart_backend = chart_backend
        self.profile = profile
        self.profile_file = profile_file
        self.conversation_history = []

        # Set API key if provided
        if openai_api_key:
            import os

            os.environ["OPENAI_API_KEY"] = openai_api_key

        # Initialize tools with profile support
        _initialize_tools(model_path, chart_backend, profile, profile_file)

        # Create the LLM based on model name
        self.llm = self._create_llm(llm_model)

    def _create_llm(self, model_name: str):
        """Create LLM instance based on model name."""
        # Common models by provider (for helpful error messages)
        provider_examples = {
            "openai": ["gpt-4", "gpt-4-turbo", "gpt-4o", "gpt-3.5-turbo"],
            "anthropic": [
                "claude-3-5-sonnet-20241022",
                "claude-3-opus-20240229",
                "claude-3-sonnet-20240229",
            ],
            "google": ["gemini-1.5-pro", "gemini-1.5-flash", "gemini-pro"],
        }

        # Check if user provided just a provider name
        model_lower = model_name.lower()
        if model_lower in ["openai", "anthropic", "claude", "google", "gemini"]:
            provider = model_lower
            if provider in ["claude", "anthropic"]:
                provider = "anthropic"
                examples = provider_examples["anthropic"]
            elif provider == "gemini":
                provider = "google"
                examples = provider_examples["google"]
            else:
                examples = provider_examples.get(provider, [])

            examples_str = "\n  - ".join(examples)
            raise ValueError(
                f"Please specify a model name, not just '{model_name}'.\n\n"
                f"Common {provider.title()} models:\n  - {examples_str}\n\n"
                f"Example: --model {examples[0]}"
            )

        # Detect provider from model name
        if model_name.startswith("claude"):
            try:
                from langchain_anthropic import ChatAnthropic

                return ChatAnthropic(model=model_name, temperature=0)
            except ImportError as e:
                raise ImportError(
                    "langchain-anthropic not installed. Install with: pip install langchain-anthropic"
                ) from e
        elif model_name.startswith("gemini") or model_name.startswith("models/gemini"):
            try:
                from langchain_google_genai import ChatGoogleGenerativeAI

                return ChatGoogleGenerativeAI(model=model_name, temperature=0)
            except ImportError as e:
                raise ImportError(
                    "langchain-google-genai not installed. Install with: pip install langchain-google-genai"
                ) from e
        else:
            # Default to OpenAI (gpt-4, gpt-3.5-turbo, etc.)
            try:
                from langchain_openai import ChatOpenAI

                return ChatOpenAI(model=model_name, temperature=0)
            except ImportError as e:
                raise ImportError(
                    "langchain-openai not installed. Install with: pip install langchain-openai"
                ) from e

    def query(
        self, user_input: str, on_tool_call: Callable[[str, dict], None] | None = None
    ) -> tuple[str, str]:
        """
        Process a user query.

        Args:
            user_input: User's query
            on_tool_call: Optional callback for tool calls (function_name, function_args)

        Returns:
            (tool_output, agent_response) tuple
        """
        tool_output, agent_response = process_query(
            self.llm, user_input, self.conversation_history, on_tool_call
        )

        # Update conversation history
        self.conversation_history.append({"role": "user", "content": user_input})
        self.conversation_history.append({"role": "assistant", "content": agent_response})

        # Keep history limited to last 10 exchanges
        if len(self.conversation_history) > 20:
            self.conversation_history = self.conversation_history[-20:]

        return tool_output, agent_response

    def reset_history(self):
        """Clear conversation history."""
        self.conversation_history = []
