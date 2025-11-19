# list_models Tool

List all available semantic model names.

## Usage

This should be your FIRST call when exploring a new semantic layer.
It returns the names of all models you can query.

## Arguments

None

## Returns

Dictionary mapping model names to descriptions

## Example Output

```json
{
    "flights": "Semantic model: flights",
    "carriers": "Semantic model: carriers"
}
```

## Next Step

Call get_model(model_name) for each model to see its dimensions and measures.
