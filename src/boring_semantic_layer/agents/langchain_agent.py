"""
LangChain Agent Integration for Boring Semantic Layer

This module provides a LangChain-powered chat interface for querying
semantic models using natural language with OpenAI function calling.
"""

import json
import os
from pathlib import Path

from dotenv import load_dotenv
from langchain.tools import tool
from langchain_openai import ChatOpenAI

from boring_semantic_layer.agents.tools import BSLTools

# Global state for tools instance
_TOOLS: BSLTools | None = None


def _initialize_tools(model_path: Path, chart_backend: str = "plotext") -> BSLTools:
    """Initialize the BSL tools instance."""
    global _TOOLS
    _TOOLS = BSLTools(model_path, chart_backend)
    return _TOOLS


@tool
def list_models() -> str:
    """List all available semantic models with their dimensions and measures."""
    global _TOOLS

    if _TOOLS is None:
        return "Error: No models loaded. Please check the configuration."

    return _TOOLS.list_models()


@tool
def query_model(query: str) -> str:
    """
    Execute a semantic model query and return results with a chart.

    Args:
        query: A query string in the format:
               model_name.group_by("dim1", "dim2").aggregate("measure1", "measure2")

    Returns:
        Formatted string with query results and terminal chart.
    """
    global _TOOLS

    if _TOOLS is None:
        return "Error: No models loaded. Please check the configuration."

    # Always show chart for langchain backend
    return _TOOLS.query_model(query, show_chart=True)


def create_system_message() -> str:
    """Create the system message for the LangChain agent."""
    from pathlib import Path

    # Load the BSL Agent Guide
    guide_path = Path(__file__).parent / "BSL_AGENT_GUIDE.md"
    try:
        bsl_guide = guide_path.read_text()
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
4. Present results clearly

Available Tools:
1. list_models() - Lists all available models, dimensions, and measures
2. query_model(query) - Executes a query and returns results with terminal charts

When asked about data:
1. Only call list_models() if you genuinely don't know what dimensions/measures are available
2. Construct the appropriate query following the BSL syntax below
3. Execute it using query_model()
4. The results will include a chart visualization
5. Provide a brief summary of the results

When providing your response, keep it concise since the full results with charts are shown to the user.

---

{bsl_guide}"""


def process_query(llm, user_input: str, conversation_history: list) -> tuple[str, str]:
    """Process a user query using the LLM and tools.

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

    # Define available functions
    functions = [
        {
            "name": "list_models",
            "description": "Lists all available semantic models with their dimensions and measures",
            "parameters": {
                "type": "object",
                "properties": {},
            },
        },
        {
            "name": "query_model",
            "description": "Executes a semantic model query and returns results with terminal charts. Query format: model_name.group_by('dim1').aggregate('measure1')",
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "The query to execute, e.g., flights.group_by('origin').aggregate('flight_count')",
                    },
                },
                "required": ["query"],
            },
        },
    ]

    # Allow multiple rounds of function calling
    max_iterations = 5
    all_tool_outputs = []

    for _ in range(max_iterations):
        # Get LLM response with function calling
        response = llm.invoke(messages, functions=functions, function_call="auto")

        # Check if the LLM wants to call a function
        if not response.additional_kwargs.get("function_call"):
            # No more function calls, return final response
            tool_output = "\n\n".join(all_tool_outputs) if all_tool_outputs else ""
            return tool_output, response.content

        # Execute the function
        function_call = response.additional_kwargs["function_call"]
        function_name = function_call["name"]
        function_args = json.loads(function_call["arguments"])

        if function_name == "list_models":
            tool_output = list_models.invoke({})
        elif function_name == "query_model":
            tool_output = query_model.invoke(function_args)
        else:
            tool_output = f"Unknown function: {function_name}"

        all_tool_outputs.append(tool_output)

        # Add function call and result to messages for next iteration
        messages.append(
            {"role": "assistant", "content": response.content or "", "function_call": function_call}
        )
        messages.append({"role": "function", "name": function_name, "content": tool_output})

    # If we hit max iterations, return what we have
    tool_output = "\n\n".join(all_tool_outputs) if all_tool_outputs else ""
    return tool_output, "Reached maximum function call iterations."


def start_langchain_agent(
    model_path: Path,
    llm_model: str = "gpt-4",
    openai_api_key: str | None = None,
    chart_backend: str = "plotext",
):
    """
    Start an interactive LangChain agent session for querying semantic models.

    Args:
        model_path: Path to YAML semantic model definition
        llm_model: OpenAI model to use (default: gpt-4)
        openai_api_key: OpenAI API key (defaults to env var)
        chart_backend: Chart backend to use (default: plotext)
    """
    # Load environment variables
    load_dotenv()

    # Set API key
    if openai_api_key:
        os.environ["OPENAI_API_KEY"] = openai_api_key

    # Check for API key
    if not os.getenv("OPENAI_API_KEY"):
        print("\n❌ Error: OPENAI_API_KEY not found.")
        print("Please set it as an environment variable or in your .env file.")
        return

    # Initialize tools
    print(f"📊 Loading semantic models from {model_path}...")
    tools = _initialize_tools(model_path, chart_backend)
    models = tools._load_models()
    print(f"✅ Loaded {len(models)} model(s): {', '.join(models.keys())}\n")

    # Create the LLM
    try:
        llm = ChatOpenAI(model=llm_model, temperature=0)
    except Exception as e:
        print(f"❌ Error creating LLM: {e}")
        return

    # Print welcome message
    print("=" * 80)
    print("  Boring Semantic Layer - LangChain Agent")
    print("=" * 80)
    print(f"\nModel: {llm_model}")
    print("Charts: Enabled (plotext)")
    print("\nAsk questions about your data in natural language!")
    print("Type 'quit' or 'exit' to stop.\n")
    print("Example questions:")
    print("  - What data is available?")
    print("  - Show me counts by category")
    print("  - What is the average value by group?")
    print("-" * 80)

    # Conversation history
    conversation_history = []

    # Interactive loop
    while True:
        try:
            # Get user input
            user_input = input("\n💬 You: ").strip()

            if not user_input:
                continue

            if user_input.lower() in ["quit", "exit", "q"]:
                print("\n👋 Goodbye!")
                break

            # Process the query
            tool_output, agent_response = process_query(llm, user_input, conversation_history)

            # Display tool output (with charts) if available
            if tool_output:
                print("\n" + "=" * 80)
                print(tool_output)
                print("=" * 80)

            # Display the agent's summary
            print(f"\n🤖 Agent: {agent_response}")

            # Update conversation history
            conversation_history.append({"role": "user", "content": user_input})
            conversation_history.append({"role": "assistant", "content": agent_response})

            # Keep history limited to last 10 exchanges
            if len(conversation_history) > 20:
                conversation_history = conversation_history[-20:]

        except KeyboardInterrupt:
            print("\n\n👋 Goodbye!")
            break
        except Exception as e:
            print(f"\n❌ Error: {e}")
            print("Please try again.\n")
