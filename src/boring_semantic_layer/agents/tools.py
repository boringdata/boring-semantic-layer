"""
Shared tools for BSL agent backends (aichat, langchain, etc).

This module provides unified tool functions that can be used across different
agent backends, eliminating code duplication.
"""

from pathlib import Path

from boring_semantic_layer import from_yaml


class BSLTools:
    """Shared tools for querying semantic models across different agent backends."""

    def __init__(self, model_path: Path | str, chart_backend: str = "plotext"):
        """
        Initialize BSL tools with a semantic model.

        Args:
            model_path: Path to YAML semantic model definition
            chart_backend: Chart backend to use (plotext, altair, plotly)
        """
        self.model_path = Path(model_path)
        self.chart_backend = chart_backend
        self._models = None

    def _load_models(self):
        """Load models from YAML file, using catalog if defined."""
        if self._models is not None:
            return self._models

        # Load from YAML - catalog will be handled automatically
        self._models = from_yaml(str(self.model_path))
        return self._models

    def list_models(self) -> str:
        """
        List all available semantic models with their dimensions and measures.

        Returns:
            Formatted string listing all models, dimensions, and measures.
        """
        try:
            models = self._load_models()

            output = []
            output.append("=" * 80)
            output.append("AVAILABLE SEMANTIC MODELS")
            output.append("=" * 80)

            for model_name, model in models.items():
                output.append(f"\n📊 Model: {model_name}")
                output.append("-" * 80)

                output.append("\n  Dimensions:")
                for dim in model.dimensions:
                    output.append(f"    - {dim}")

                output.append("\n  Measures:")
                for measure in model.measures:
                    output.append(f"    - {measure}")

                # List joins if available
                if hasattr(model, "joins") and model.joins:
                    output.append("\n  Joins:")
                    for join_name in model.joins:
                        output.append(f"    - {join_name}")

            output.append("\n" + "=" * 80)
            output.append("QUERY SYNTAX")
            output.append("=" * 80)
            output.append("model_name.group_by(<dimensions>).aggregate(<measures>)")
            output.append("")

            return "\n".join(output)

        except Exception as e:
            import traceback

            return f"❌ Error loading model: {e}\n\n{traceback.format_exc()}"

    def query_model(self, query: str, show_chart: bool | None = None) -> str:
        """
        Execute a BSL query and optionally display results with a chart.

        Args:
            query: BSL query string (e.g., 'model.group_by("dim").aggregate("measure")')
            show_chart: Whether to display a chart (True/False). If None, only returns data table.

        Returns:
            Formatted string with query results and optional chart.
        """
        from boring_semantic_layer.chart import chart

        try:
            # Load models
            models = self._load_models()

            # Make models available for eval
            exec_globals = {**models}

            # Execute query
            result = eval(query, exec_globals)
            df = result.execute()

            # Build output
            output = []
            output.append("\n" + "=" * 80)
            output.append("QUERY RESULTS")
            output.append("=" * 80)
            output.append(df.to_string(index=False))

            # Show chart if requested
            if show_chart is True:
                output.append("")
                output.append("=" * 80)
                output.append("VISUALIZATION")
                output.append("=" * 80)
                print("\n".join(output))
                chart(result, backend=self.chart_backend, format="static")
                print("")
                return f"✅ Query executed successfully. Returned {len(df)} rows with chart."
            else:
                output.append("")
                print("\n".join(output))
                if show_chart is False:
                    return (
                        f"✅ Query executed successfully. Returned {len(df)} rows (chart disabled)."
                    )
                else:
                    return f"✅ Query executed successfully. Returned {len(df)} rows. Use show_chart=True for visualization."

        except Exception as e:
            import traceback

            return f"❌ Error executing query: {str(e)}\n\n{traceback.format_exc()}"


# Global instance for use by agents
_tools_instance: BSLTools | None = None


def initialize_tools(model_path: Path | str, chart_backend: str = "plotext") -> BSLTools:
    """
    Initialize the global tools instance.

    Args:
        model_path: Path to YAML semantic model definition
        chart_backend: Chart backend to use

    Returns:
        BSLTools instance
    """
    global _tools_instance
    _tools_instance = BSLTools(model_path, chart_backend)
    return _tools_instance


def get_tools() -> BSLTools:
    """
    Get the current tools instance.

    Raises:
        RuntimeError: If tools haven't been initialized

    Returns:
        BSLTools instance
    """
    if _tools_instance is None:
        raise RuntimeError("BSL tools not initialized. Call initialize_tools() first.")
    return _tools_instance
