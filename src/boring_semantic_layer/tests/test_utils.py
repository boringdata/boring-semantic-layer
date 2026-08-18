from __future__ import annotations

import ibis
from ibis import _
from returns.result import Failure, Success

from boring_semantic_layer.utils import (
    _is_url,
    safe_eval,
)


def test_safe_eval_simple_expression():
    result = safe_eval("1 + 2")
    assert isinstance(result, Success)
    assert result.unwrap() == 3


def test_safe_eval_with_context():
    result = safe_eval("x + y", context={"x": 10, "y": 20})
    assert isinstance(result, Success)
    assert result.unwrap() == 30


def test_safe_eval_unsafe_import():
    result = safe_eval("__import__('os')")
    assert isinstance(result, Failure)


def test_safe_eval_unsafe_exec():
    result = safe_eval("exec('print(1)')")
    assert isinstance(result, Failure)


def test_safe_eval_allowed_names():
    result = safe_eval("x + 1", context={"x": 5}, allowed_names={"x"})
    assert isinstance(result, Success)
    assert result.unwrap() == 6


def test_safe_eval_disallowed_names():
    result = safe_eval("x + y", context={"x": 5, "y": 10}, allowed_names={"x"})
    assert isinstance(result, Failure)


def test_safe_eval_blocks_dunder_attribute_escape():
    result = safe_eval("().__class__.__mro__[1].__subclasses__()")
    assert isinstance(result, Failure)


def test_safe_eval_blocks_dunder_escape_even_with_allowed_names():
    result = safe_eval(
        "().__class__.__mro__[1].__subclasses__()",
        allowed_names={"_"},
    )
    assert isinstance(result, Failure)


def test_safe_eval_blocks_dunder_inside_lambda_body():
    result = safe_eval("lambda t: t.__class__")
    assert isinstance(result, Failure)


def test_safe_eval_blocks_private_names_from_context():
    result = safe_eval("__import__('os')", context={"__import__": __import__})
    assert isinstance(result, Failure)


def test_safe_eval_blocks_effectful_method_calls():
    class MockModel:
        def execute(self):
            raise AssertionError("execute should not be called")

    result = safe_eval("model.execute()", context={"model": MockModel()})
    assert isinstance(result, Failure)


def test_safe_eval_blocks_arbitrary_model_attribute_chains():
    class MockModel:
        table = object()

    result = safe_eval("model.table", context={"model": MockModel()})
    assert isinstance(result, Failure)


def test_safe_eval_allows_query_metadata_attributes():
    class MockModel:
        dimensions = ("origin", "destination")
        measures = ("flight_count",)

    dimensions = safe_eval("model.dimensions", context={"model": MockModel()})
    measures = safe_eval("model.measures", context={"model": MockModel()})

    assert dimensions.unwrap() == ("origin", "destination")
    assert measures.unwrap() == ("flight_count",)


def test_safe_eval_allows_curated_ibis_helpers():
    result = safe_eval("ibis.literal(1)", context={"ibis": ibis}, allowed_names={"ibis"})
    assert isinstance(result, Success)


def test_safe_eval_blocks_ibis_io_helpers():
    result = safe_eval(
        "ibis.duckdb.connect(':memory:')",
        context={"ibis": ibis},
        allowed_names={"ibis"},
    )
    assert isinstance(result, Failure)


def test_safe_eval_ibis_column_access():
    result = safe_eval("_.column_name", context={"_": _}, allowed_names={"_"})
    assert isinstance(result, Success)
    deferred = result.unwrap()
    assert hasattr(deferred, "resolve")


def test_safe_eval_ibis_method_call():
    result = safe_eval("_.distance.mean()", context={"_": _}, allowed_names={"_"})
    assert isinstance(result, Success)
    deferred = result.unwrap()
    assert hasattr(deferred, "resolve")


def test_safe_eval_ibis_complex_expression():
    result = safe_eval("_.col1 + _.col2", context={"_": _}, allowed_names={"_"})
    assert isinstance(result, Success)


def test_no_file_access():
    result = safe_eval("open('/etc/passwd')")
    assert isinstance(result, Failure)


def test_safe_operations_allowed():
    result = safe_eval("1 + 2 * 3 - 4 / 2")
    assert isinstance(result, Success)
    assert result.unwrap() == 5.0


def test_safe_comparisons():
    result = safe_eval("5 > 3 and 2 < 10")
    assert isinstance(result, Success)
    assert result.unwrap() is True


def test_safe_list_operations():
    result = safe_eval("[1, 2, 3]")
    assert isinstance(result, Success)
    assert result.unwrap() == [1, 2, 3]


def test_safe_dict_operations():
    result = safe_eval("{'a': 1, 'b': 2}")
    assert isinstance(result, Success)
    assert result.unwrap() == {"a": 1, "b": 2}


def test_safe_eval_lambda_expression():
    """Test that lambda expressions are allowed for BSL dynamic dimensions."""
    result = safe_eval("lambda t: t.column")
    assert isinstance(result, Success)
    fn = result.unwrap()
    assert callable(fn)


def test_safe_eval_lambda_with_method_call():
    """Test lambda with method calls for time truncation."""

    # Create a mock object with a truncate method
    class MockColumn:
        def truncate(self, unit):
            return f"truncated_{unit}"

    class MockTable:
        def __init__(self):
            self.arr_time = MockColumn()

    result = safe_eval("lambda t: t.arr_time.truncate('Y')")
    assert isinstance(result, Success)
    fn = result.unwrap()
    assert callable(fn)

    # Test that it works with a mock table
    mock_table = MockTable()
    output = fn(mock_table)
    assert output == "truncated_Y"


def test_safe_eval_lambda_in_context():
    """Test lambda with ibis _ in context (realistic BSL use case)."""
    result = safe_eval("lambda t: t.distance.mean()", context={"t": _}, allowed_names={"t"})
    assert isinstance(result, Success)
    fn = result.unwrap()
    assert callable(fn)


def test_is_url_http():
    """Test _is_url detects http URLs."""
    assert _is_url("http://example.com/file.yml") is True


def test_is_url_https():
    """Test _is_url detects https URLs."""
    assert _is_url("https://example.com/file.yml") is True


def test_is_url_local_path():
    """Test _is_url returns False for local paths."""
    assert _is_url("/local/path/file.yml") is False
    assert _is_url("relative/path.yml") is False


def test_is_url_none():
    """Test _is_url handles None."""
    assert _is_url(None) is False


def test_is_url_other_schemes():
    """Test _is_url rejects non-http schemes."""
    assert _is_url("ftp://example.com/file.yml") is False
    assert _is_url("file:///local/file.yml") is False


def test_safe_eval_multiline_method_chaining():
    """Test that multiline method chaining works (LLM-generated queries).

    LLMs often generate queries like:
        model.filter(...)
        .group_by(...)
        .aggregate(...)

    This should work by wrapping in parentheses automatically.
    """

    class MockModel:
        def filter(self, fn):
            return self

        def with_dimensions(self, **kwargs):
            return self

        def group_by(self, *args):
            return self

        def aggregate(self, *args):
            return "SUCCESS"

    multiline_query = """model.filter(lambda t: t.origin == 'NYC')
.with_dimensions(arr_week=lambda t: t.arr_time.truncate('W'))
.group_by('arr_week')
.aggregate('flight_count')"""

    result = safe_eval(multiline_query, context={"model": MockModel()})
    assert isinstance(result, Success)
    assert result.unwrap() == "SUCCESS"


def test_safe_eval_multiline_with_leading_newline():
    """Test multiline query starting with the model name on first line."""

    class MockModel:
        def filter(self, fn):
            return self

        def group_by(self, *args):
            return self

        def aggregate(self, *args):
            return "DONE"

    # Query that starts with model on its own line
    query = """model
.filter(lambda t: t.x > 0)
.group_by('y')
.aggregate('z')"""

    result = safe_eval(query, context={"model": MockModel()})
    assert isinstance(result, Success)
    assert result.unwrap() == "DONE"
