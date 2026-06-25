"""
Unit tests for dependency group error messages.

Verifies that when users try to use features requiring optional dependencies,
they receive clear error messages indicating which dependency group to install.

Dependency groups in pyproject.toml:
- xorq: For tagged-expression serialization (to_tagged/from_tagged), caching
- mcp: For MCP semantic model functionality (MCPSemanticModel)
- agent: For LangChain-based query agents
- viz-altair: For Altair visualization (chart with backend="altair")
- viz-plotly: For Plotly visualization (chart with backend="plotly")
- examples: For running examples (includes xorq and duckdb)

Note: xorq is an optional dependency; core BSL works without it.
"""

import subprocess
import sys
import textwrap
from pathlib import Path

import pytest


class TestDependencyGroupDocumentation:
    """Test that dependency groups are properly documented in pyproject.toml."""

    def test_pyproject_has_all_optional_dependencies(self):
        """Verify all optional dependency groups exist in pyproject.toml."""
        # Use tomllib (Python 3.11+) or tomli (Python 3.10)
        if sys.version_info >= (3, 11):
            import tomllib
        else:
            try:
                import tomli as tomllib
            except ImportError:
                pytest.skip("tomli not available for Python < 3.11")

        # Read pyproject.toml - go up from test file to project root
        # test file is at: src/boring_semantic_layer/tests/test_dependency_groups.py
        # pyproject.toml is at project root
        test_file = Path(__file__)
        project_root = test_file.parent.parent.parent.parent
        pyproject_path = project_root / "pyproject.toml"
        with open(pyproject_path, "rb") as f:
            pyproject = tomllib.load(f)

        optional_deps = pyproject["project"]["optional-dependencies"]

        # Verify all expected groups exist
        assert "xorq" in optional_deps, "xorq dependency group missing"
        assert "mcp" in optional_deps, "mcp dependency group missing"
        assert "agent" in optional_deps, "agent dependency group missing"
        assert "viz-altair" in optional_deps, "viz-altair dependency group missing"
        assert "viz-plotly" in optional_deps, "viz-plotly dependency group missing"
        assert "examples" in optional_deps, "examples dependency group missing"

        # Verify key dependencies in each group
        assert any("xorq" in dep for dep in optional_deps["xorq"])
        assert any("fastmcp" in dep for dep in optional_deps["mcp"])
        assert any("langchain" in dep for dep in optional_deps["agent"])
        assert any("altair" in dep for dep in optional_deps["viz-altair"])
        assert any("plotly" in dep for dep in optional_deps["viz-plotly"])
        assert any("xorq" in dep for dep in optional_deps["examples"])

    def test_all_dependency_groups_in_dev(self):
        """Verify dev dependency group contains developer tooling (not a self-referential bundle)."""
        # Use tomllib (Python 3.11+) or tomli (Python 3.10)
        if sys.version_info >= (3, 11):
            import tomllib
        else:
            try:
                import tomli as tomllib
            except ImportError:
                pytest.skip("tomli not available for Python < 3.11")

        # Read pyproject.toml - go up from test file to project root
        test_file = Path(__file__)
        project_root = test_file.parent.parent.parent.parent
        pyproject_path = project_root / "pyproject.toml"
        with open(pyproject_path, "rb") as f:
            pyproject = tomllib.load(f)

        dev_deps = pyproject["project"]["optional-dependencies"]["dev"]

        # Dev should NOT use a self-referential boring-semantic-layer[...] bundle;
        # optional extras (xorq, mcp, etc.) are installed separately via --all-extras.
        dev_with_self_ref = [dep for dep in dev_deps if "boring-semantic-layer[" in dep]
        assert len(dev_with_self_ref) == 0, (
            "Dev should not use a self-referential bundle; install extras via --all-extras"
        )

        # Dev should contain developer tooling
        assert any("ruff" in dep for dep in dev_deps), "Dev should include ruff"
        assert any("pre-commit" in dep for dep in dev_deps), "Dev should include pre-commit"
        assert any("langchain-anthropic" in dep or "langchain-openai" in dep for dep in dev_deps), (
            "Dev should include LLM provider clients for testing"
        )


class TestXorqErrorMessages:
    """Test that xorq functions have proper error handling."""

    def test_serialization_module_has_error_handling(self):
        """Verify serialization module has ImportError handling."""
        import inspect

        from boring_semantic_layer import serialization

        # Check that to_tagged raises ImportError with helpful message if xorq is not available
        source = inspect.getsource(serialization.to_tagged)
        assert "ImportError" in source
        # xorq is optional, so the message should name the install extra.
        assert "boring-semantic-layer[xorq]" in source

    def test_tagged_api_errors_are_helpful_without_xorq(self):
        """Verify tagged APIs fail cleanly when xorq is not installed."""
        test_file = Path(__file__)
        project_root = test_file.parent.parent.parent.parent
        code = textwrap.dedent(
            """
            import importlib.abc
            import sys

            sys.path.insert(0, "src")

            class BlockXorq(importlib.abc.MetaPathFinder):
                def find_spec(self, fullname, path=None, target=None):
                    if fullname == "xorq" or fullname.startswith("xorq."):
                        raise ImportError("blocked xorq for optional dependency test")
                    return None

            sys.meta_path.insert(0, BlockXorq())

            import ibis

            from boring_semantic_layer import to_semantic_table
            from boring_semantic_layer._xorq import HAS_XORQ
            from boring_semantic_layer.serialization import from_tagged

            assert HAS_XORQ is False
            table = ibis.table({"a": "int64"}, name="t")
            model = to_semantic_table(table, name="t").with_dimensions(a=lambda t: t.a)

            errors = []
            for call in (model.to_tagged, lambda: from_tagged(object())):
                try:
                    call()
                except ImportError as exc:
                    errors.append(str(exc))
                else:
                    raise AssertionError("tagged API unexpectedly succeeded without xorq")

            assert len(errors) == 2
            for message in errors:
                assert "xorq" in message.lower()
                assert "optional dependency" in message.lower()
                assert "boring-semantic-layer[xorq]" in message
            """
        )
        subprocess.run(
            [sys.executable, "-c", code],
            cwd=project_root,
            text=True,
            capture_output=True,
            check=True,
        )


class TestMCPErrorMessages:
    """Test that MCP functions have proper error handling."""

    def test_main_module_getattr_handles_mcp(self):
        """Verify __init__.py __getattr__ handles MCPSemanticModel imports."""
        import inspect

        import boring_semantic_layer

        # Check __getattr__ implementation
        source = inspect.getsource(boring_semantic_layer.__getattr__)
        assert "MCPSemanticModel" in source
        assert "boring-semantic-layer[mcp]" in source or "mcp" in source


class TestChartErrorMessages:
    """Test that chart functions have proper error handling for missing viz dependencies."""

    def test_chart_module_imports_altair_conditionally(self):
        """Verify Altair backend imports altair only when needed."""
        import inspect

        from boring_semantic_layer.chart import altair_chart

        # Altair backend should import altair inside methods, not at module level
        source = inspect.getsource(altair_chart.AltairBackend.create_chart)
        assert "import altair" in source

    def test_chart_module_imports_plotly_conditionally(self):
        """Verify Plotly backend imports plotly only when needed."""
        import inspect

        from boring_semantic_layer.chart import plotly_chart

        # Plotly backend should import plotly inside methods
        source = inspect.getsource(plotly_chart.PlotlyBackend.create_chart)
        assert "import plotly" in source

    def test_chart_png_export_has_error_handling(self):
        """Verify chart backends have error handling for PNG export dependencies."""
        import inspect

        from boring_semantic_layer.chart import altair_chart

        # Altair backend should have error handling for image export
        source = inspect.getsource(altair_chart.AltairBackend.format_output)
        # Should have try/except for image formats
        assert "ImportError" in source or "Exception" in source


class TestErrorMessageQuality:
    """Test that error messages are clear and actionable."""

    def test_init_has_import_error_messages(self):
        """Verify __init__.py has clear import error messages."""
        import inspect

        import boring_semantic_layer

        source = inspect.getsource(boring_semantic_layer.__getattr__)

        # Should mention features and how to install
        assert "MCPSemanticModel" in source
        assert "mcp" in source

        # Should have install instructions
        assert "pip install" in source or "Install with" in source

    def test_serialization_has_clear_error_messages(self):
        """Verify serialization module has clear error messages."""
        import inspect

        from boring_semantic_layer import serialization

        # Check to_tagged function
        source = inspect.getsource(serialization.to_tagged)
        assert "ImportError" in source
        # Should mention how to install
        assert "pip install" in source or "Install with" in source or "xorq" in source


class TestDependencyGroupCoverage:
    """Test that all features requiring optional dependencies are documented."""

    def test_all_features_with_optional_deps_documented(self):
        """Verify this test file documents all features with optional dependencies."""
        # This is a meta-test to ensure we've covered all the dependency groups
        # Read this test file and verify it tests all groups
        test_file_content = Path(__file__).read_text()

        # Should test all dependency groups
        assert "xorq" in test_file_content
        assert "mcp" in test_file_content
        assert "agent" in test_file_content
        assert "viz-altair" in test_file_content or "altair" in test_file_content
        assert "viz-plotly" in test_file_content or "plotly" in test_file_content

    def test_pyproject_dev_group_is_comprehensive(self):
        """Verify all optional dependency groups are declared in pyproject.toml."""
        # Use tomllib (Python 3.11+) or tomli (Python 3.10)
        if sys.version_info >= (3, 11):
            import tomllib
        else:
            try:
                import tomli as tomllib
            except ImportError:
                pytest.skip("tomli not available for Python < 3.11")

        test_file = Path(__file__)
        project_root = test_file.parent.parent.parent.parent
        pyproject_path = project_root / "pyproject.toml"
        with open(pyproject_path, "rb") as f:
            pyproject = tomllib.load(f)

        optional_deps = pyproject["project"]["optional-dependencies"]

        # All user-facing extras must exist as top-level optional-dependencies keys.
        # (Extras are no longer bundled into dev; they are installed via --all-extras in CI.)
        expected_extras = {"xorq", "mcp", "agent", "viz-altair", "viz-plotly", "viz-plotext"}
        for group in expected_extras:
            assert group in optional_deps, (
                f"Optional dependency group '{group}' missing from pyproject.toml"
            )


class TestIntegrationWithRealDependencies:
    """Integration tests that verify behavior with real (installed) dependencies."""

    def test_xorq_available_if_installed(self):
        """Verify xorq functions work when xorq is installed."""
        try:
            import xorq  # noqa: F401

            xorq_available = True
        except ImportError:
            xorq_available = False

        if xorq_available:
            # xorq is installed, verify it can be imported and used
            # Note: to_tagged and from_tagged are internal functions, not public API
            from boring_semantic_layer.serialization import from_tagged, to_tagged

            assert callable(to_tagged)
            assert callable(from_tagged)
        else:
            # xorq not installed, verify we get helpful error with a
            # schema-only table (avoids pandas/pyarrow dependency)
            with pytest.raises(ImportError) as exc_info:
                import ibis

                from boring_semantic_layer import SemanticModel
                from boring_semantic_layer.serialization import to_tagged

                table = ibis.table({"a": "int64"}, "test")
                model = SemanticModel(table=table, dimensions={}, measures={})
                to_tagged(model)

            assert "xorq" in str(exc_info.value).lower()

    def test_mcp_available_if_installed(self):
        """Verify MCPSemanticModel works when fastmcp is installed."""
        try:
            import fastmcp  # noqa: F401

            mcp_available = True
        except ImportError:
            mcp_available = False

        if mcp_available:
            # fastmcp is installed, verify it can be imported
            from boring_semantic_layer import MCPSemanticModel

            assert MCPSemanticModel is not None
        else:
            # fastmcp not installed, verify we get helpful error
            with pytest.raises((ImportError, AttributeError)) as exc_info:
                from boring_semantic_layer import MCPSemanticModel  # noqa: F401

            # Should mention fastmcp in the error
            assert "fastmcp" in str(exc_info.value).lower() or "MCPSemanticModel" in str(
                exc_info.value
            )

    def test_altair_available_if_installed(self):
        """Verify chart with altair backend works when altair is installed."""
        try:
            import altair  # noqa: F401

            altair_available = True
        except ImportError:
            altair_available = False

        if altair_available:
            import ibis

            from boring_semantic_layer import Dimension, Measure, SemanticModel
            from boring_semantic_layer.chart import chart

            # Create a simple model and chart
            table = ibis.memtable({"x": [1, 2], "y": [3, 4]})
            model = SemanticModel(
                table=table,
                dimensions={"x": Dimension(expr=lambda t: t.x)},
                measures={"y_sum": Measure(expr=lambda t: t.y.sum())},
            )
            result = model.group_by("x").aggregate("y_sum")
            chart_obj = chart(result, backend="altair")
            assert chart_obj is not None

    def test_plotly_available_if_installed(self):
        """Verify chart with plotly backend works when plotly is installed."""
        try:
            import plotly  # noqa: F401

            plotly_available = True
        except ImportError:
            plotly_available = False

        if plotly_available:
            import ibis

            from boring_semantic_layer import Dimension, Measure, SemanticModel
            from boring_semantic_layer.chart import chart

            # Create a simple model and chart
            table = ibis.memtable({"x": [1, 2], "y": [3, 4]})
            model = SemanticModel(
                table=table,
                dimensions={"x": Dimension(expr=lambda t: t.x)},
                measures={"y_sum": Measure(expr=lambda t: t.y.sum())},
            )
            result = model.group_by("x").aggregate("y_sum")
            chart_obj = chart(result, backend="plotly")
            assert chart_obj is not None
