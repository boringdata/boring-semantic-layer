"""
Shared tools for BSL agent backends (aichat, langchain, etc).

This module provides unified tool functions that can be used across different
agent backends, eliminating code duplication.
"""

from pathlib import Path

from boring_semantic_layer import from_yaml


class BSLTools:
    """Shared tools for querying semantic models across different agent backends."""

    def __init__(
        self,
        model_path: Path | str,
        chart_backend: str = "plotext",
        profile: str | None = None,
        profile_path: str | Path | None = None,
    ):
        """
        Initialize BSL tools with a semantic model.

        Args:
            model_path: Path to YAML semantic model definition
            chart_backend: Chart backend to use (plotext, altair, plotly)
            profile: Optional profile name to load tables from (e.g., 'my_flights_db')
            profile_path: Optional path to a profile YAML file
        """
        self.model_path = Path(model_path)
        self.chart_backend = chart_backend
        self.profile = profile
        self.profile_path = Path(profile_path) if profile_path else None
        self._models = None

    def _load_models(self):
        """Load models from YAML file, using profile if specified."""
        if self._models is not None:
            return self._models

        # Load from YAML - profile will be handled automatically if specified
        self._models = from_yaml(
            str(self.model_path),
            profile=self.profile,
            profile_path=str(self.profile_path) if self.profile_path else None,
        )
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

    def query_model(
        self,
        query: str,
        show_chart: bool | None = None,
        show_table: bool = True,
        chart_spec: dict | None = None,
        limit: int = 10,
    ) -> str:
        """
        Execute a BSL query and optionally display results with a chart.

        Args:
            query: BSL query string (e.g., 'model.group_by("dim").aggregate("measure")')
            show_chart: Whether to display a chart (True/False). If None, only returns data table.
            show_table: Whether to display the data table (default: True)
            chart_spec: Optional chart specification dict with keys:
                       - chart_type: "bar", "line", "scatter", "table"
                       - theme: "pro", "clear", "dark", "default" (plotext themes)
                       - height: chart height in terminal lines (default: 40)
                       - width: chart width in characters (default: auto-detect from terminal)
                       - grid: show grid lines (default: true)
                       - title: custom chart title
                       - marker: marker style (single character like "●", "■", "▲")
            limit: Maximum number of rows to display in the table (default: 10, use 0 for all rows)

        Returns:
            Formatted string with query results and optional chart.
        """
        from boring_semantic_layer.chart import chart

        try:
            # Load models
            models = self._load_models()

            # Make models and ibis available for eval
            import ibis

            exec_globals = {**models, "ibis": ibis}

            # Execute query
            result = eval(query, exec_globals)
            df = result.execute()

            # Build output
            output = []

            # Show table if requested
            if show_table:
                output.append("\n📊 Results:")

                # Try to use rich for better formatting, fall back to plain text
                try:
                    from rich.console import Console
                    from rich.table import Table

                    # Apply row limit (0 means show all)
                    if limit > 0 and len(df) > limit:
                        display_df = df.head(limit)
                        show_more = True
                    else:
                        display_df = df
                        show_more = False

                    # Create rich table
                    table = Table(show_header=True, header_style="bold cyan")

                    # Add columns
                    for col in display_df.columns:
                        table.add_column(str(col))

                    # Add rows
                    for _, row in display_df.iterrows():
                        table.add_row(*[str(val) for val in row])

                    # Print table directly
                    console = Console()
                    console.print(table)

                    if show_more:
                        print(f"\n... ({len(df) - limit} more rows)")

                except ImportError:
                    # Fall back to plain pandas string output
                    if limit > 0 and len(df) > limit:
                        display_df = df.head(limit)
                        show_more = True
                    else:
                        display_df = df
                        show_more = False

                    print(display_df.to_string(index=False))
                    if show_more:
                        print(f"\n... ({len(df) - limit} more rows)")

            # Show chart if requested
            if show_chart is True:
                print("\n📈 Chart:")
                try:
                    chart(result, spec=chart_spec, backend=self.chart_backend, format="static")
                except Exception as chart_error:
                    import traceback

                    from rich.console import Console

                    console = Console()
                    console.print(f"\n❌ Chart Error: {str(chart_error)}", style="bold red")
                    console.print("\nTraceback:", style="bold red")
                    console.print(traceback.format_exc(), style="red")

            # Return minimal summary for agent
            return f"Query executed successfully. Returned {len(df)} rows."

        except Exception as e:
            import traceback

            from rich.console import Console

            console = Console()
            console.print(f"\n❌ Query Error: {str(e)}", style="bold red")
            console.print("\nTraceback:", style="bold red")
            console.print(traceback.format_exc(), style="red")

            return f"❌ Error executing query: {str(e)}\n\n{traceback.format_exc()}"

    def query_model_with_chart_file(
        self, query: str, chart_spec: dict | None = None, output_path: str | None = None
    ) -> tuple[str, str | None]:
        """
        Execute a BSL query and generate a chart as an image file.

        This method is specifically designed for integrations like Slack that need
        to upload chart images as files.

        Args:
            query: BSL query string (e.g., 'model.group_by("dim").aggregate("measure")')
            chart_spec: Optional chart specification dict
            output_path: Optional path to save the chart. If None, uses a temp file.

        Returns:
            tuple: (summary_text, chart_file_path)
                - summary_text: Formatted text with query results
                - chart_file_path: Path to the generated chart image file, or None if chart generation failed

        Raises:
            Exception: If query execution fails
        """
        import tempfile

        try:
            # Load models
            models = self._load_models()

            # Make models and ibis available for eval
            import ibis

            exec_globals = {**models, "ibis": ibis}

            # Execute query
            result = eval(query, exec_globals)
            df = result.execute()

            # Build summary text
            summary_lines = [f"📊 Query executed successfully. Returned {len(df)} rows."]

            # Add a preview of the data (first 5 rows)
            if len(df) > 0:
                summary_lines.append("\n*Preview (first 5 rows):*")
                preview_df = df.head(5)

                # Format as markdown table for Slack
                summary_lines.append("```")
                summary_lines.append(preview_df.to_string(index=False))
                summary_lines.append("```")

                if len(df) > 5:
                    summary_lines.append(f"\n_({len(df) - 5} more rows not shown)_")

            summary_text = "\n".join(summary_lines)

            # Try to generate chart
            chart_file_path = None
            try:
                from boring_semantic_layer.chart import chart, list_backends

                # Determine which backend to use for image generation
                # Prefer plotly or altair for image generation (not plotext which is terminal-only)
                available_backends = list_backends()
                image_backend = None

                if "plotly" in available_backends:
                    image_backend = "plotly"
                elif "altair" in available_backends:
                    image_backend = "altair"

                if image_backend:
                    # Generate chart
                    chart_obj = chart(result, spec=chart_spec, backend=image_backend, format="json")

                    # Determine output path
                    if output_path is None:
                        # Create temp file with appropriate extension
                        suffix = ".png"
                        with tempfile.NamedTemporaryFile(
                            suffix=suffix, delete=False, mode="wb"
                        ) as temp_file:
                            output_path = temp_file.name

                    # Save chart to file
                    if image_backend == "plotly":
                        # Plotly: convert to static image
                        try:
                            import plotly.io as pio

                            pio.write_image(chart_obj, output_path, width=800, height=600)
                            chart_file_path = output_path
                        except Exception:
                            # If kaleido not installed, try HTML export
                            try:
                                import plotly

                                html_path = output_path.replace(".png", ".html")
                                plotly.offline.plot(chart_obj, filename=html_path, auto_open=False)
                                chart_file_path = html_path
                            except Exception:
                                # Chart generation failed, continue without chart
                                pass

                    elif image_backend == "altair":
                        # Altair: save as PNG or SVG
                        try:
                            chart_obj.save(output_path, format="png")
                            chart_file_path = output_path
                        except Exception:
                            try:
                                # Fallback to SVG if PNG fails
                                svg_path = output_path.replace(".png", ".svg")
                                chart_obj.save(svg_path, format="svg")
                                chart_file_path = svg_path
                            except Exception:
                                # Chart generation failed, continue without chart
                                pass

            except Exception as chart_error:
                # Chart generation failed, but we still have the data
                # Just log it and continue
                import logging

                logging.debug(f"Chart generation failed: {chart_error}")

            return summary_text, chart_file_path

        except Exception as e:
            import traceback

            error_msg = f"❌ Error executing query: {str(e)}\n\n```\n{traceback.format_exc()}\n```"
            raise Exception(error_msg) from e


# Global instance for use by agents
_tools_instance: BSLTools | None = None


def initialize_tools(
    model_path: Path | str,
    chart_backend: str = "plotext",
    profile: str | None = None,
    profile_path: str | Path | None = None,
) -> BSLTools:
    """
    Initialize the global tools instance.

    Args:
        model_path: Path to YAML semantic model definition
        chart_backend: Chart backend to use
        profile: Optional profile name to load tables from
        profile_path: Optional path to a profile YAML file

    Returns:
        BSLTools instance
    """
    global _tools_instance
    _tools_instance = BSLTools(model_path, chart_backend, profile, profile_path)
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
