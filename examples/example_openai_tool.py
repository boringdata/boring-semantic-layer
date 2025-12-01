#!/usr/bin/env python3
"""
Standalone OpenAI Tool Call Example for Boring Semantic Layer

Demonstrates a single OpenAI API call using BSLTools.
With plotext backend, charts render directly to terminal.

Run from project root:
    uv run python examples/example_openai_tool.py
"""

import json
from pathlib import Path

from openai import OpenAI

from boring_semantic_layer.agents.tools import BSLTools

bsl = BSLTools(
    model_path=Path(__file__).parent / "flights.yml",
    profile="example_db",
    profile_file=Path(__file__).parent / "profiles.yml",
    chart_backend="plotext",  # ASCII charts in terminal
)

client = OpenAI()
response = client.chat.completions.create(
    model="gpt-4o",
    messages=[
        {"role": "system", "content": bsl.system_prompt},
        {"role": "user", "content": "Show top 5 carriers by flight count"},
    ],
    tools=bsl.tools,
)

print(response.choices[0].message.tool_calls)
for tc in response.choices[0].message.tool_calls or []:
    bsl.execute(tc.function.name, json.loads(tc.function.arguments))
