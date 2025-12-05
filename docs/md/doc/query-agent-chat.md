# Query Agent: Demo Chat

The BSL CLI includes a built-in chat interface to interact with your semantic models using natural language. It uses LangChain under the hood and supports multiple LLM providers.

<img src="/bsl-chat-demo.png" alt="BSL Chat Demo" width="600" />

## Installation

Install BSL with the agent extra:

```bash
pip install 'boring-semantic-layer[agent]'
```

Then install the LLM provider package for your preferred model:

```bash
# For Claude models
pip install langchain-anthropic

# For GPT models
pip install langchain-openai

# For Gemini models
pip install langchain-google-genai
```

See the LangChain docs for available models: [Anthropic](https://python.langchain.com/docs/integrations/chat/anthropic/) | [OpenAI](https://python.langchain.com/docs/integrations/chat/openai/) | [Google](https://python.langchain.com/docs/integrations/chat/google_generative_ai/)

## Configuration

Set your API key and optionally the semantic model path in a `.env` file or as environment variables:

```bash
# .env

# LLM API key (pick one)
ANTHROPIC_API_KEY=sk-ant-...
OPENAI_API_KEY=sk-...
GOOGLE_API_KEY=...

# Optional: default semantic model path (avoids --sm flag)
BSL_MODEL_PATH=path/to/your/model.yaml

# Optional: default profile name (avoids --profile flag)
BSL_PROFILE=my_profile

# Optional: default profile file path (avoids --profile-file flag)
BSL_PROFILE_FILE=path/to/profiles.yml
```

## Starting the chat

```bash
bsl chat --sm path/to/your/model.yaml
```

You can also pass a prompt directly to skip interactive mode:

```bash
bsl chat --sm path/to/your/model.yaml "What are the top 5 origins by flight count?"
```

## Required flags

| Flag | Description |
|------|-------------|
| `--sm` | Path to your semantic model YAML file |
| `--model` | LLM model to use (OpenAI, Anthropic, or Google) |

## Optional flags

| Flag | Description |
|------|-------------|
| `--chart-backend` | Chart renderer: `plotext` (terminal, default), `altair` (opens in browser), or `plotly` (opens in browser) |
| `--profile` | Profile name to use |
| `--profile-file` | Path to a custom profiles file |

