"""
Example: Using join_many for One-to-Many Joins in Boring Semantic Layer

This example demonstrates how to use the `join_many` helper and `SemanticModel` to perform a one-to-many join between two tables: departments and employees. It shows how to define semantic models, set up a join, and query aggregated measures (such as employee counts per department).

Tables:

Department table (`dept_tbl`):

| dept_id | dept_name |
| ------- | --------- |
|   10    |    HR     |
|   20    |    Eng    |

Employee table (`emp_tbl`):

| emp_id | dept_id |
| ------ | ------- |
|   1    |   10    |
|   2    |   10    |
|   3    |   20    |

The example computes the number of employees in each department using a semantic join.

Expected Output:

| dept_name | emp_child_count |
|-----------|----------------|
|    Eng    |        1       |
|    HR     |        2       |
"""

import pandas as pd
import ibis
from boring_semantic_layer.semantic_model import SemanticModel, Join

# Create a DuckDB connection for in-memory table creation
con = ibis.duckdb.connect()

# Sample data for departments and employees
dept_df = pd.DataFrame({"dept_id": [10, 20], "dept_name": ["HR", "Eng"]})
emp_df = pd.DataFrame({"emp_id": [1, 2, 3], "dept_id": [10, 10, 20]})

# Register the dataframes as DuckDB tables
dept_tbl = con.create_table("dept_tbl", dept_df)
emp_tbl = con.create_table("emp_tbl", emp_df)

# Define the employee semantic model
# - Primary key: dept_id (for one-to-many join from department)
# - Dimensions: emp_id, dept_id
# - Measures: child_count (number of employees per department)
emp_model = SemanticModel(
    table=emp_tbl,
    dimensions={"emp_id": lambda t: t.emp_id, "dept_id": lambda t: t.dept_id},
    measures={"child_count": lambda t: t.emp_id.count()},
    primary_key="dept_id",
)

# Define the department semantic model
# - Dimensions: dept_name
# - Joins: join_many to employees on dept_id
#   (alias 'emp' for referencing employee measures/dimensions)
dept_model = SemanticModel(
    table=dept_tbl,
    dimensions={"dept_name": lambda t: t.dept_name},
    measures={},
    joins={"emp": Join.many(alias="emp", model=emp_model, with_=lambda t: t.dept_id)},
)

# Query: Get department name and employee count per department
emp_counts_df = (
    dept_model.query(dimensions=["dept_name"], measures=["emp.child_count"])
    .execute()
    .sort_values("dept_name")
    .reset_index(drop=True)
)

print("Employee counts by department:")
print(emp_counts_df)
