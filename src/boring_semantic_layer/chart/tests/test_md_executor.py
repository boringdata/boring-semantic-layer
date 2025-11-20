"""Unit tests for BSL query executor module."""

from boring_semantic_layer.chart.md_parser import QueryExecutor


class TestQueryExecutor:
    """Test QueryExecutor functionality."""

    def test_simple_expression_evaluation(self):
        """Test evaluation of simple expressions."""
        executor = QueryExecutor(capture_output=False)
        result = executor.execute("2 + 2")
        # Should capture output from print or last expression
        assert result["output"] == "4" or "output" not in result

    def test_print_output_capture(self):
        """Test capturing print output."""
        executor = QueryExecutor(capture_output=True)
        result = executor.execute('print("hello world")')
        assert result["output"] == "hello world"

    def test_context_persistence(self):
        """Test that variables persist across executions."""
        executor = QueryExecutor()
        executor.execute("x = 42")
        executor.execute("y = x * 2")
        assert executor.context["x"] == 42
        assert executor.context["y"] == 84

    def test_bsl_query_execution(self):
        """Test execution of BSL query with .execute()."""
        executor = QueryExecutor()
        code = """
import ibis
from boring_semantic_layer import to_semantic_table

t = ibis.memtable({"x": [1, 2, 3], "y": [10, 20, 30]})
result = to_semantic_table(t)
"""
        result = executor.execute(code)
        assert "table" in result
        assert "sql" in result
        assert result["table"]["columns"] == ["x", "y"]

    def test_error_handling(self):
        """Test error handling for invalid code."""
        executor = QueryExecutor()
        result = executor.execute("invalid syntax !!!")
        assert "error" in result
        assert "traceback" in result

    def test_tuple_output(self):
        """Test handling of tuple expressions."""
        executor = QueryExecutor(capture_output=True)
        result = executor.execute('"a", "b", "c"')
        # Should return list of strings
        assert result.get("output") == ["a", "b", "c"]

    def test_last_line_evaluation(self):
        """Test that last line is evaluated as expression."""
        executor = QueryExecutor(capture_output=True)
        code = """
x = 10
y = 20
x + y
"""
        result = executor.execute(code)
        assert "30" in str(result.get("output", ""))

    def test_no_result_found(self):
        """Test handling when no result is found."""
        executor = QueryExecutor(capture_output=False)
        result = executor.execute("x = 5")
        # Should either find x in context or return error
        assert "error" in result or executor.context.get("x") == 5

    def test_multiline_statement(self):
        """Test execution of multiline statements."""
        executor = QueryExecutor()
        code = """
def add(a, b):
    return a + b

result = add(5, 3)
"""
        executor.execute(code)
        assert executor.context.get("result") == 8
