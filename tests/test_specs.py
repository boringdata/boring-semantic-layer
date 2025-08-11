from unittest.mock import MagicMock, Mock

import pytest

from boring_semantic_layer.specs import DimensionSpec, MeasureSpec
from xorq import desc

class TestDimensionSpec:
    def test_dimension_spec_creation(self):
        expr_func = lambda table: table.column_name
        description = "Test Dimension"

        dim_spec = DimensionSpec(expr_func, description)

        assert dim_spec.expr == expr_func
        assert dim_spec.description == description

    def test_dimension_spec_is_frozen(self):
        expr_func = lambda table: table.column_name
        description = "Test Dimension"

        dim_spec = DimensionSpec(expr_func, description)

        with pytest.raises(AttributeError):
            dim_spec.expr = lambda table: table.column_name + 1

        with pytest.raises(AttributeError):
            dim_spec.description = "New Description"

    def test_dimension_spec_call(self):
        mock_table = Mock()
        mock_result = Mock()
        expr_func = Mock(return_value=mock_result)

        dim_spec = DimensionSpec(expr=expr_func, description="Test Dimension")
        result = dim_spec(mock_table)

        expr_func.assert_called_once_with(mock_table)
        assert result == mock_result

    def test_dimension_spec_equality(self):
        expr_func = lambda table: table.column_name
        description = "Test Dimension"

        dim_spec1 = DimensionSpec(expr_func, description)
        dim_spec2 = DimensionSpec(expr_func, description)

        assert dim_spec1 == dim_spec2

class TestMeasureSpec:
    def test_measure_spec_creation(self):
        expr_func = lambda table: table.column_name
        description = "Test Measure"

        measure_spec = MeasureSpec(expr_func, description)

        assert measure_spec.expr == expr_func
        assert measure_spec.description == description

    def test_measure_spec_is_frozen(self):
        expr_func = lambda table: table.column_name
        description = "Test Measure"

        measure_spec = MeasureSpec(expr_func, description)

        with pytest.raises(AttributeError):
            measure_spec.expr = lambda table: table.column_name + 1

        with pytest.raises(AttributeError):
            measure_spec.description = "New Description"

    def test_measure_spec_call(self):
        mock_table = Mock()
        mock_result = Mock()
        expr_func = Mock(return_value=mock_result)

        measure_spec = MeasureSpec(expr=expr_func, description="Test Measure")
        result = measure_spec(mock_table)

        expr_func.assert_called_once_with(mock_table)
        assert result == mock_result

    def test_measure_spec_equality(self):
        expr_func = lambda table: table.column_name
        description = "Test Measure"

        measure_spec1 = MeasureSpec(expr_func, description)
        measure_spec2 = MeasureSpec(expr_func, description)

        assert measure_spec1 == measure_spec2

class TestSpecInteraction:
    def test_specs_work_with_ibis_expressions(self):
        mock_table = MagicMock()
        mock_column = MagicMock()
        mock_table.customer_id = mock_column

        dim_spec = DimensionSpec(expr=lambda table: table.customer_id, description="Customer ID")
        result = dim_spec(mock_table)

        assert result == mock_column
