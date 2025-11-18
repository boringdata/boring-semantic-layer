from __future__ import annotations

from ibis import _
from returns.result import Failure, Success

from boring_semantic_layer.utils import (
    expr_to_ibis_string,
    ibis_string_to_expr,
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


def test_expr_to_ibis_string():
    fn = lambda t: t.distance.mean()  # noqa: E731
    result = expr_to_ibis_string(fn)
    assert isinstance(result, Success)
    ibis_str = result.unwrap()
    assert ibis_str == "_.distance.mean()"


def test_expr_to_ibis_string_simple():
    fn = lambda t: t.origin  # noqa: E731
    result = expr_to_ibis_string(fn)
    assert isinstance(result, Success)
    ibis_str = result.unwrap()
    assert ibis_str == "_.origin"


def test_ibis_string_to_expr():
    result = ibis_string_to_expr("_.distance.mean()")
    assert isinstance(result, Success)
    fn = result.unwrap()
    assert callable(fn)


def test_ibis_string_to_expr_simple():
    result = ibis_string_to_expr("_.origin")
    assert isinstance(result, Success)
    fn = result.unwrap()
    assert callable(fn)


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
