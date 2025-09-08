"""
Unit tests for the query module.
Tests the query() function and semantic table query operations.
"""

import pytest

try:
    import xorq.vendor.ibis as ibis_mod
except ImportError:
    pass

from boring_semantic_layer.semantic_api.query import query, _parse_expression


class MockSemanticTableExpr:
    """Mock SemanticTableExpr for testing."""

    def __init__(self, name="test_table"):
        self.name = name
        self.operations = []
        self.last_operation = None

    def group_by(self, *args, **kwargs):
        self.operations.append(("group_by", args, kwargs))
        self.last_operation = ("group_by", args, kwargs)
        return self

    def aggregate(self, **kwargs):
        self.operations.append(("aggregate", (), kwargs))
        self.last_operation = ("aggregate", (), kwargs)
        return self

    def mutate(self, **kwargs):
        self.operations.append(("mutate", (), kwargs))
        self.last_operation = ("mutate", (), kwargs)
        return self

    def filter(self, func):
        self.operations.append(("filter", (func,), {}))
        self.last_operation = ("filter", (func,), {})
        return self

    def select(self, *args):
        self.operations.append(("select", args, {}))
        self.last_operation = ("select", args, {})
        return self

    def order_by(self, *args):
        self.operations.append(("order_by", args, {}))
        self.last_operation = ("order_by", args, {})
        return self

    def limit(self, n):
        self.operations.append(("limit", (n,), {}))
        self.last_operation = ("limit", (n,), {})
        return self


class TestParseExpression:
    """Test the _parse_expression function."""

    def test_parse_simple_unbound_expression(self):
        """Test parsing simple unbound expressions like '_.count()'."""
        expr_func = _parse_expression("_.count()")
        assert callable(expr_func)

    def test_parse_lambda_expression(self):
        """Test parsing lambda expressions."""
        expr_func = _parse_expression("lambda t: t.count()")
        assert callable(expr_func)

    def test_parse_field_access_expression(self):
        """Test parsing field access expressions."""
        expr_func = _parse_expression("_.field_name")
        assert callable(expr_func)

    def test_parse_method_call_expression(self):
        """Test parsing method call expressions."""
        expr_func = _parse_expression("_.field_name.mean()")
        assert callable(expr_func)

    def test_parse_complex_lambda_expression(self):
        """Test parsing complex lambda expressions."""
        expr_func = _parse_expression("lambda t: (t.field > 10).sum()")
        assert callable(expr_func)


class TestQuery:
    """Test the main query() function."""

    def test_empty_operations_list(self):
        """Test query with empty operations list."""
        mock_table = MockSemanticTableExpr()
        operations = []

        result = query(mock_table, operations)

        assert result == mock_table
        assert len(mock_table.operations) == 0

    def test_single_group_by_operation_list_format(self):
        """Test single group_by operation with list format."""
        mock_table = MockSemanticTableExpr()
        operations = [{"group_by": ["carrier", "origin"]}]

        result = query(mock_table, operations)

        assert result == mock_table
        assert len(mock_table.operations) == 1
        assert mock_table.operations[0][0] == "group_by"
        assert mock_table.operations[0][1] == ("carrier", "origin")

    def test_single_group_by_operation_dict_format(self):
        """Test single group_by operation with dict format (inline dimensions)."""
        mock_table = MockSemanticTableExpr()
        operations = [{"group_by": {"departure_hour": "_.departure_time.hour()"}}]

        result = query(mock_table, operations)

        assert result == mock_table
        assert len(mock_table.operations) == 1
        assert mock_table.operations[0][0] == "group_by"
        # Should have inline dimensions
        assert len(mock_table.operations[0][2]) == 1  # kwargs
        assert "departure_hour" in mock_table.operations[0][2]

    def test_single_aggregate_operation(self):
        """Test single aggregate operation."""
        mock_table = MockSemanticTableExpr()
        operations = [
            {
                "aggregate": {
                    "flight_count": "_.count()",
                    "avg_delay": "_.dep_delay.mean()",
                }
            }
        ]

        result = query(mock_table, operations)

        assert result == mock_table
        assert len(mock_table.operations) == 1
        assert mock_table.operations[0][0] == "aggregate"
        assert len(mock_table.operations[0][2]) == 2  # Two aggregations
        assert "flight_count" in mock_table.operations[0][2]
        assert "avg_delay" in mock_table.operations[0][2]

    def test_single_mutate_operation(self):
        """Test single mutate operation."""
        mock_table = MockSemanticTableExpr()
        operations = [{"mutate": {"calculated_field": "_.field1 + _.field2"}}]

        result = query(mock_table, operations)

        assert result == mock_table
        assert len(mock_table.operations) == 1
        assert mock_table.operations[0][0] == "mutate"
        assert "calculated_field" in mock_table.operations[0][2]

    def test_single_filter_operation(self):
        """Test single filter operation."""
        mock_table = MockSemanticTableExpr()
        operations = [{"filter": "_.field > 10"}]

        result = query(mock_table, operations)

        assert result == mock_table
        assert len(mock_table.operations) == 1
        assert mock_table.operations[0][0] == "filter"

    def test_single_select_operation(self):
        """Test single select operation."""
        mock_table = MockSemanticTableExpr()
        operations = [{"select": ["field1", "field2", "field3"]}]

        result = query(mock_table, operations)

        assert result == mock_table
        assert len(mock_table.operations) == 1
        assert mock_table.operations[0][0] == "select"
        assert mock_table.operations[0][1] == ("field1", "field2", "field3")

    def test_single_order_by_operation(self):
        """Test single order_by operation."""
        mock_table = MockSemanticTableExpr()
        operations = [{"order_by": ["field1", "field2 desc"]}]

        result = query(mock_table, operations)

        assert result == mock_table
        assert len(mock_table.operations) == 1
        assert mock_table.operations[0][0] == "order_by"

    def test_single_limit_operation(self):
        """Test single limit operation."""
        mock_table = MockSemanticTableExpr()
        operations = [{"limit": 10}]

        result = query(mock_table, operations)

        assert result == mock_table
        assert len(mock_table.operations) == 1
        assert mock_table.operations[0][0] == "limit"
        assert mock_table.operations[0][1] == (10,)

    def test_multiple_operations_sequential(self):
        """Test multiple operations applied sequentially."""
        mock_table = MockSemanticTableExpr()
        operations = [
            {"group_by": ["carrier"]},
            {"aggregate": {"flight_count": "_.count()"}},
            {"filter": "_.flight_count > 10"},
            {"order_by": ["flight_count desc"]},
            {"limit": 5},
        ]

        result = query(mock_table, operations)

        assert result == mock_table
        assert len(mock_table.operations) == 5

        # Check operations applied in correct order
        op_names = [op[0] for op in mock_table.operations]
        expected_order = ["group_by", "aggregate", "filter", "order_by", "limit"]
        assert op_names == expected_order

    def test_complex_real_world_query(self):
        """Test complex real-world query with all operation types."""
        mock_table = MockSemanticTableExpr()
        operations = [
            {"filter": "_.dep_delay.notnull()"},
            {"group_by": ["carrier", "origin"]},
            {
                "aggregate": {
                    "flight_count": "_.count()",
                    "avg_delay": "_.dep_delay.mean()",
                    "total_distance": "_.distance.sum()",
                }
            },
            {
                "mutate": {
                    "delay_per_flight": "_.avg_delay",
                    "efficiency_score": "lambda t: 100 - (t.avg_delay / 10)",
                }
            },
            {"filter": "_.flight_count >= 10"},
            {"select": ["carrier", "origin", "flight_count", "efficiency_score"]},
            {"order_by": ["efficiency_score desc"]},
            {"limit": 15},
        ]

        result = query(mock_table, operations)

        assert result == mock_table
        assert len(mock_table.operations) == 8

        # Verify all operations were applied
        op_names = [op[0] for op in mock_table.operations]
        expected_order = [
            "filter",
            "group_by",
            "aggregate",
            "mutate",
            "filter",
            "select",
            "order_by",
            "limit",
        ]
        assert op_names == expected_order

    def test_invalid_operation_multiple_keys(self):
        """Test error handling for operations with multiple keys."""
        mock_table = MockSemanticTableExpr()
        operations = [{"group_by": ["carrier"], "aggregate": {"count": "_.count()"}}]

        with pytest.raises(
            ValueError, match="Each operation must have exactly one key-value pair"
        ):
            query(mock_table, operations)

    def test_invalid_operation_unknown_key(self):
        """Test error handling for unknown operation keys."""
        mock_table = MockSemanticTableExpr()
        operations = [{"unknown_operation": "some_value"}]

        with pytest.raises(ValueError, match="Unknown operation: unknown_operation"):
            query(mock_table, operations)

    def test_invalid_group_by_value_type(self):
        """Test error handling for invalid group_by value type."""
        mock_table = MockSemanticTableExpr()
        operations = [{"group_by": "invalid_string"}]

        with pytest.raises(ValueError, match="group_by value must be list or dict"):
            query(mock_table, operations)

    def test_invalid_aggregate_value_type(self):
        """Test error handling for invalid aggregate value type."""
        mock_table = MockSemanticTableExpr()
        operations = [{"aggregate": "invalid_string"}]

        with pytest.raises(ValueError, match="aggregate value must be dict"):
            query(mock_table, operations)

    def test_invalid_mutate_value_type(self):
        """Test error handling for invalid mutate value type."""
        mock_table = MockSemanticTableExpr()
        operations = [{"mutate": "invalid_string"}]

        with pytest.raises(ValueError, match="mutate value must be dict"):
            query(mock_table, operations)

    def test_invalid_filter_value_type(self):
        """Test error handling for invalid filter value type."""
        mock_table = MockSemanticTableExpr()
        operations = [{"filter": ["invalid_list"]}]

        with pytest.raises(ValueError, match="filter value must be string"):
            query(mock_table, operations)

    def test_invalid_select_value_type(self):
        """Test error handling for invalid select value type."""
        mock_table = MockSemanticTableExpr()
        operations = [{"select": "invalid_string"}]

        with pytest.raises(ValueError, match="select value must be list"):
            query(mock_table, operations)

    def test_invalid_order_by_value_type(self):
        """Test error handling for invalid order_by value type."""
        mock_table = MockSemanticTableExpr()
        operations = [{"order_by": "invalid_string"}]

        with pytest.raises(ValueError, match="order_by value must be list"):
            query(mock_table, operations)

    def test_invalid_limit_value_negative(self):
        """Test error handling for negative limit value."""
        mock_table = MockSemanticTableExpr()
        operations = [{"limit": -5}]

        with pytest.raises(ValueError, match="limit value must be positive integer"):
            query(mock_table, operations)

    def test_invalid_limit_value_zero(self):
        """Test error handling for zero limit value."""
        mock_table = MockSemanticTableExpr()
        operations = [{"limit": 0}]

        with pytest.raises(ValueError, match="limit value must be positive integer"):
            query(mock_table, operations)

    def test_order_by_with_desc_suffix(self):
        """Test order_by operations with 'desc' suffix parsing."""
        mock_table = MockSemanticTableExpr()
        operations = [{"order_by": ["field1 desc", "field2", "field3 desc"]}]

        result = query(mock_table, operations)

        assert result == mock_table
        assert len(mock_table.operations) == 1
        assert mock_table.operations[0][0] == "order_by"

        # Check that desc parsing was attempted (we can't fully test without ibis)
        assert len(mock_table.operations[0][1]) == 3


class TestQueryIntegration:
    """Integration tests for the query functionality."""

    def test_query_with_semantic_table_expr_method(self):
        """Test that SemanticTableExpr.query() method works correctly."""
        # This would be an integration test with actual SemanticTableExpr
        # For now, we just test that the method exists and can be called

        # Create a mock semantic table expr
        mock_table = MockSemanticTableExpr()

        # Mock the query method to use our query function
        def mock_query_method(operations):
            from boring_semantic_layer.semantic_api.query import query

            return query(mock_table, operations)

        mock_table.query = mock_query_method

        operations = [{"group_by": ["carrier"]}, {"aggregate": {"count": "_.count()"}}]

        result = mock_table.query(operations)

        assert result == mock_table
        assert len(mock_table.operations) == 2
        assert mock_table.operations[0][0] == "group_by"
        assert mock_table.operations[1][0] == "aggregate"


class TestQueryExpressionTypes:
    """Test different expression types in query operations."""

    def test_unbound_expressions(self):
        """Test various unbound expression formats."""
        mock_table = MockSemanticTableExpr()
        operations = [
            {
                "aggregate": {
                    "count": "_.count()",
                    "avg_field": "_.field.mean()",
                    "sum_field": "_.field.sum()",
                    "max_field": "_.field.max()",
                }
            }
        ]

        result = query(mock_table, operations)

        assert result == mock_table
        assert len(mock_table.operations) == 1
        assert mock_table.operations[0][0] == "aggregate"
        assert len(mock_table.operations[0][2]) == 4

    def test_lambda_expressions(self):
        """Test various lambda expression formats."""
        mock_table = MockSemanticTableExpr()
        operations = [
            {
                "aggregate": {
                    "count": "lambda t: t.count()",
                    "conditional_sum": "lambda t: (t.field > 10).sum()",
                    "complex_calc": "lambda t: t.field1 / t.field2",
                }
            }
        ]

        result = query(mock_table, operations)

        assert result == mock_table
        assert len(mock_table.operations) == 1
        assert mock_table.operations[0][0] == "aggregate"
        assert len(mock_table.operations[0][2]) == 3

    def test_mixed_expressions(self):
        """Test mixing unbound and lambda expressions in same operation."""
        mock_table = MockSemanticTableExpr()
        operations = [
            {
                "aggregate": {
                    "simple_count": "_.count()",
                    "lambda_count": "lambda t: t.count()",
                    "avg_delay": "_.delay.mean()",
                    "delayed_flights": "lambda t: (t.delay > 15).sum()",
                }
            }
        ]

        result = query(mock_table, operations)

        assert result == mock_table
        assert len(mock_table.operations) == 1
        assert mock_table.operations[0][0] == "aggregate"
        assert len(mock_table.operations[0][2]) == 4


if __name__ == "__main__":
    pytest.main([__file__])
