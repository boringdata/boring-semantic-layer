"""SQL to semantic layer translation.

Translates SQL queries with semantic dimension/measure names to BSL method chains.
"""

import sqlglot
from sqlglot import exp


class SemanticSQLTranslator:
    """Translates SQL queries to BSL method chains.

    Example:
        SQL: SELECT airline, total_distance FROM flights GROUP BY airline
        →
        flights.group_by("airline").aggregate("total_distance")
    """

    def __init__(self, semantic_model):
        """Initialize translator with semantic model.

        Args:
            semantic_model: SemanticModel instance
        """
        self.model = semantic_model
        self.dimensions = set(semantic_model.get_dimensions().keys())
        self.measures = set(semantic_model.get_measures().keys())

    def can_translate(self, sql: str) -> bool:
        """Check if SQL can be translated to semantic query.

        Returns False for:
        - JOINs, subqueries, window functions
        - SELECT * (raw SQL feature)
        - Raw aggregate functions like COUNT(*), AVG(column)
        - Columns not in dimensions or measures

        Args:
            sql: SQL query string

        Returns:
            bool: True if translatable to semantic query
        """
        try:
            ast = sqlglot.parse_one(sql)

            # Check for unsupported features
            if ast.find(exp.Join):
                return False
            if ast.find(exp.Subquery):
                return False
            if ast.find(exp.Window):
                return False

            # Check for SELECT * - indicates raw SQL
            if ast.find(exp.Star):
                return False

            # Check for raw aggregate functions - indicates raw SQL
            for select_expr in ast.find_all(exp.Select):
                for col_expr in select_expr.expressions:
                    if isinstance(col_expr, exp.Alias) and isinstance(
                        col_expr.this, (exp.AggFunc, exp.Anonymous)
                    ):
                        return False

            # Check if query uses ANY semantic columns
            # If it uses at least one semantic dimension/measure, we consider it semantic SQL
            # (even if it has unknown columns - translate() will validate and raise ValueError)
            columns = self._extract_all_columns(ast)
            if not columns:
                return False  # No columns at all

            # Check if ANY column is semantic
            has_semantic_column = any(
                col in self.dimensions or col in self.measures for col in columns
            )

            return has_semantic_column

        except Exception:
            return False

    def translate(self, sql: str):
        """Translate SQL to BSL method chain.

        Args:
            sql: SQL query string

        Returns:
            BSL query object (SemanticFilter, SemanticAggregate, etc.)

        Raises:
            ValueError: If SQL cannot be translated
        """
        # Parse SQL
        ast = sqlglot.parse_one(sql)

        # Extract components
        dimensions = self._extract_dimensions(ast)
        measures = self._extract_measures(ast)
        filters = self._extract_filters(ast)
        order_by = self._extract_order_by(ast)
        limit = self._extract_limit(ast)

        # Validate - check all SELECT columns are recognized
        self._validate_select_columns(ast)

        # Validate extracted components
        self._validate(dimensions, measures)

        # Build method chain
        result = self.model

        # Apply filters
        for filter_expr in filters:
            result = result.filter(filter_expr)

        # Handle different query patterns
        if measures:
            # Aggregation query: SELECT dimensions, measures GROUP BY dimensions
            if dimensions:
                result = result.group_by(*dimensions)
            result = result.aggregate(*measures)
        elif dimensions:
            # Dimensional query: SELECT dimensions (no aggregation)
            # This returns distinct dimension values, like a cube slice
            # Build Ibis select expression with only the requested dimensions
            t = result.to_ibis()
            all_dims = result.get_dimensions()

            # Select only requested dimensions
            dim_exprs = {}
            for dim_name in dimensions:
                dim_obj = all_dims[dim_name]
                dim_exprs[dim_name] = dim_obj.expr(t)

            # Create a new table with only selected dimensions
            result_table = t.select(**dim_exprs)

            # For dimensional queries, return distinct combinations
            # (like SELECT DISTINCT in SQL)
            from boring_semantic_layer.expr import SemanticTable

            result = SemanticTable(result_table.distinct())

        # Order by
        if order_by:
            from ibis import _

            for col_name, direction in order_by:
                col_ref = getattr(_, col_name)
                result = result.order_by(col_ref.desc() if direction == "desc" else col_ref.asc())

        # Limit
        if limit:
            result = result.limit(limit)

        return result

    def _extract_dimensions(self, ast) -> list[str]:
        """Extract dimension names from SELECT and GROUP BY."""
        dimensions = []

        # From SELECT
        for select_expr in ast.find_all(exp.Select):
            for col_expr in select_expr.expressions:
                col_name = self._get_column_name(col_expr)
                if col_name in self.dimensions:
                    dimensions.append(col_name)

        # From GROUP BY
        for group_expr in ast.find_all(exp.Group):
            for col_expr in group_expr.expressions:
                col_name = col_expr.alias_or_name
                if col_name in self.dimensions and col_name not in dimensions:
                    dimensions.append(col_name)

        return dimensions

    def _extract_measures(self, ast) -> list[str]:
        """Extract measure names from SELECT."""
        measures = []

        for select_expr in ast.find_all(exp.Select):
            for col_expr in select_expr.expressions:
                col_name = self._get_column_name(col_expr)
                if col_name in self.measures:
                    measures.append(col_name)

        return measures

    def _extract_filters(self, ast) -> list:
        """Extract filters from WHERE clause."""
        filters = []

        for where_expr in ast.find_all(exp.Where):
            filter_lambda = self._where_to_lambda(where_expr.this)
            if filter_lambda:
                filters.append(filter_lambda)

        return filters

    def _extract_order_by(self, ast) -> list[tuple[str, str]]:
        """Extract ORDER BY expressions."""
        order_by = []

        for order_expr in ast.find_all(exp.Order):
            for ordered in order_expr.expressions:
                col_name = ordered.this.alias_or_name
                direction = "desc" if ordered.args.get("desc") else "asc"
                order_by.append((col_name, direction))

        return order_by

    def _extract_limit(self, ast) -> int | None:
        """Extract LIMIT value."""
        if ast.args.get("limit"):
            limit_expr = ast.args["limit"]
            if hasattr(limit_expr, "expression") and limit_expr.expression:
                return int(limit_expr.expression.this)
        return None

    def _extract_all_columns(self, ast) -> set[str]:
        """Extract all column names referenced in query."""
        columns = set()
        for col in ast.find_all(exp.Column):
            columns.add(col.alias_or_name)
        return columns

    def _get_column_name(self, col_expr) -> str:
        """Get column name from SELECT expression."""
        if col_expr.alias:
            return col_expr.alias
        if isinstance(col_expr, exp.Alias):
            return col_expr.alias
        if isinstance(col_expr, exp.Column):
            return col_expr.name
        if hasattr(col_expr, "this") and isinstance(col_expr.this, exp.Column):
            return col_expr.this.name
        return str(col_expr)

    def _where_to_lambda(self, where_expr):
        """Convert WHERE expression to lambda function."""
        if isinstance(where_expr, exp.GT):
            field = where_expr.this.alias_or_name
            value = self._extract_value(where_expr.expression)
            return lambda t: getattr(t, field) > value

        elif isinstance(where_expr, exp.GTE):
            field = where_expr.this.alias_or_name
            value = self._extract_value(where_expr.expression)
            return lambda t: getattr(t, field) >= value

        elif isinstance(where_expr, exp.LT):
            field = where_expr.this.alias_or_name
            value = self._extract_value(where_expr.expression)
            return lambda t: getattr(t, field) < value

        elif isinstance(where_expr, exp.LTE):
            field = where_expr.this.alias_or_name
            value = self._extract_value(where_expr.expression)
            return lambda t: getattr(t, field) <= value

        elif isinstance(where_expr, exp.EQ):
            field = where_expr.this.alias_or_name
            value = self._extract_value(where_expr.expression)
            return lambda t: getattr(t, field) == value

        elif isinstance(where_expr, exp.NEQ):
            field = where_expr.this.alias_or_name
            value = self._extract_value(where_expr.expression)
            return lambda t: getattr(t, field) != value

        elif isinstance(where_expr, exp.And):
            # Compound AND - need to build combined lambda
            left_lambda = None
            right_lambda = None

            # Get left and right conditions
            if hasattr(where_expr, "left"):
                left_lambda = self._where_to_lambda(where_expr.left)
            if hasattr(where_expr, "right"):
                right_lambda = self._where_to_lambda(where_expr.right)

            if left_lambda and right_lambda:
                return lambda t: left_lambda(t) & right_lambda(t)
            elif left_lambda:
                return left_lambda
            elif right_lambda:
                return right_lambda

        elif isinstance(where_expr, exp.Or):
            # Compound OR
            left_lambda = None
            right_lambda = None

            if hasattr(where_expr, "left"):
                left_lambda = self._where_to_lambda(where_expr.left)
            if hasattr(where_expr, "right"):
                right_lambda = self._where_to_lambda(where_expr.right)

            if left_lambda and right_lambda:
                return lambda t: left_lambda(t) | right_lambda(t)
            elif left_lambda:
                return left_lambda
            elif right_lambda:
                return right_lambda

        return None

    def _extract_value(self, expr):
        """Extract literal value from expression."""
        if isinstance(expr, exp.Literal):
            val = expr.this
            if expr.is_int:
                return int(val)
            elif expr.is_number:
                return float(val)
            elif expr.is_string:
                return str(val)
            return val
        return str(expr)

    def _validate_select_columns(self, ast):
        """Validate all SELECT columns are dimensions or measures.

        Raises:
            ValueError: If SQL contains unknown semantic column names
        """
        for select_expr in ast.find_all(exp.Select):
            for col_expr in select_expr.expressions:
                col_name = self._get_column_name(col_expr)
                if col_name not in self.dimensions and col_name not in self.measures:
                    raise ValueError(
                        f"Unknown column '{col_name}'. "
                        f"Available dimensions: {sorted(self.dimensions)}, "
                        f"measures: {sorted(self.measures)}"
                    )

    def _validate(self, dimensions: list[str], measures: list[str]):
        """Validate extracted components."""
        # Check all dimensions are valid
        for dim in dimensions:
            if dim not in self.dimensions:
                raise ValueError(f"Unknown dimension '{dim}'. Available: {sorted(self.dimensions)}")

        # Check all measures are valid
        for meas in measures:
            if meas not in self.measures:
                raise ValueError(f"Unknown measure '{meas}'. Available: {sorted(self.measures)}")
