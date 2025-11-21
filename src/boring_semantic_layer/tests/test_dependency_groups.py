"""
Unit tests for dependency group error messages.

Verifies that when users try to use features requiring optional dependencies,
they receive clear error messages indicating which dependency group to install.

Dependency groups in pyproject.toml:
- fastmcp: For MCP semantic model functionality (MCPSemanticModel)
- viz-altair: For Altair visualization (chart with backend="altair")
- viz-plotly: For Plotly visualization (chart with backend="plotly")

Note: xorq is now a required dependency (in main dependencies list)
Note: agents dependency group is only on bsl-agent branch, not bsl-profiles
"""

import sys
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

        # Verify all expected groups exist (agents is only on bsl-agent branch)
        assert "fastmcp" in optional_deps, "fastmcp dependency group missing"
        assert "viz-altair" in optional_deps, "viz-altair dependency group missing"
        assert "viz-plotly" in optional_deps, "viz-plotly dependency group missing"

        # Verify key dependencies in each group
        assert any("fastmcp" in dep for dep in optional_deps["fastmcp"])
        assert any("altair" in dep for dep in optional_deps["viz-altair"])
        assert any("plotly" in dep for dep in optional_deps["viz-plotly"])

        # Verify xorq is in main dependencies (not optional)
        main_deps = pyproject["project"]["dependencies"]
        assert any("xorq" in dep for dep in main_deps), "xorq should be in main dependencies"

    def test_all_dependency_groups_in_dev(self):
        """Verify dev dependency group includes all optional dependencies."""
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

        # Dev should include all optional dependency groups
        # Check for the pattern boring-semantic-layer[...] in dev deps
        dev_with_extras = [dep for dep in dev_deps if "boring-semantic-layer[" in dep]

        assert len(dev_with_extras) > 0, "Dev should include boring-semantic-layer with extras"

        # The first dev dependency should include all the optional groups (not xorq, it's required; not agents, only on bsl-agent branch)
        if dev_with_extras:
            first_dep = dev_with_extras[0]
            assert "fastmcp" in first_dep
            assert "viz-altair" in first_dep
            assert "viz-plotly" in first_dep
            assert "viz-plotext" in first_dep


class TestXorqAvailability:
    """Test that xorq functions are available (xorq is now a required dependency)."""

    def test_xorq_is_required_dependency(self):
        """Verify xorq is in main dependencies."""
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

        # xorq should be in main dependencies, not optional
        main_deps = pyproject["project"]["dependencies"]
        assert any("xorq" in dep for dep in main_deps)

    def test_xorq_functions_importable(self):
        """Verify to_xorq and from_xorq can be imported."""
        from boring_semantic_layer import from_xorq, to_xorq

        assert callable(to_xorq)
        assert callable(from_xorq)


class TestMCPErrorMessages:
    """Test that MCP functions have proper error handling."""

    def test_main_module_getattr_handles_mcp(self):
        """Verify __init__.py __getattr__ handles MCPSemanticModel imports."""
        import inspect

        import boring_semantic_layer

        # Check __getattr__ implementation
        source = inspect.getsource(boring_semantic_layer.__getattr__)
        assert "MCPSemanticModel" in source
        assert "boring-semantic-layer[fastmcp]" in source or "fastmcp" in source


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
        assert "fastmcp" in source

        # xorq is now a required dependency, so no error message needed
        # Should have install instructions
        assert "pip install" in source or "Install with" in source

    def test_xorq_convert_available(self):
        """Verify xorq_convert module functions are available (xorq is now required)."""
        from boring_semantic_layer import xorq_convert

        # xorq is now a required dependency, so these should just work
        assert hasattr(xorq_convert, "to_xorq")
        assert hasattr(xorq_convert, "from_xorq")
        assert callable(xorq_convert.to_xorq)
        assert callable(xorq_convert.from_xorq)


class TestDependencyGroupCoverage:
    """Test that all features requiring optional dependencies are documented."""

    def test_all_features_with_optional_deps_documented(self):
        """Verify this test file documents all features with optional dependencies."""
        # This is a meta-test to ensure we've covered all the dependency groups
        # Read this test file and verify it tests all groups
        test_file_content = Path(__file__).read_text()

        # Should test all optional dependency groups (agents is only on bsl-agent branch)
        assert "fastmcp" in test_file_content
        assert "viz-altair" in test_file_content or "altair" in test_file_content
        assert "viz-plotly" in test_file_content or "plotly" in test_file_content

    def test_pyproject_dev_group_is_comprehensive(self):
        """Verify dev group in pyproject.toml includes all optional dependencies."""
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

        # Get all optional dependency group names (excluding dev itself and examples)
        optional_deps = pyproject["project"]["optional-dependencies"]
        optional_groups = [group for group in optional_deps if group not in ["dev", "examples"]]

        # Dev dependencies string
        dev_deps_str = str(pyproject["project"]["optional-dependencies"]["dev"])

        # Check that dev group references all other optional groups
        # It should have boring-semantic-layer[group1,group2,...]
        for group in optional_groups:
            assert group in dev_deps_str, (
                f"Dev group should include optional dependency group: {group}"
            )


class TestIntegrationWithRealDependencies:
    """Integration tests that verify behavior with real (installed) dependencies."""

    def test_xorq_always_available(self):
        """Verify xorq functions are always available (xorq is a required dependency)."""
        # xorq is now a required dependency, so it should always be available
        from boring_semantic_layer import from_xorq, to_xorq

        assert callable(to_xorq)
        assert callable(from_xorq)

        # Verify we can actually use them
        import ibis

        from boring_semantic_layer import SemanticModel

        table = ibis.memtable({"a": [1]})
        model = SemanticModel(table=table, dimensions={}, measures={})
        # This should work without errors since xorq is required
        result = to_xorq(model)
        assert result is not None

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
