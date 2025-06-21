import pandas as pd
import ibis
from boring_semantic_layer.semantic_model import SemanticModel, join_many

# Create a connection for table creation
con = ibis.duckdb.connect()

# Sample department and employee data
dept_df = pd.DataFrame({"dept_id": [10, 20], "dept_name": ["HR", "Eng"]})
emp_df = pd.DataFrame({"emp_id": [1, 2, 3], "dept_id": [10, 10, 20]})

dept_tbl = con.create_table("dept_tbl", dept_df)
emp_tbl = con.create_table("emp_tbl", emp_df)

# Define employee model with primary key on dept_id for one-to-many join
emp_model = SemanticModel(
    table=emp_tbl,
    dimensions={"emp_id": lambda t: t.emp_id, "dept_id": lambda t: t.dept_id},
    measures={"child_count": lambda t: t.emp_id.count()},
    primary_key="dept_id",
)

# Define department model with join_many to employees
dept_model = SemanticModel(
    table=dept_tbl,
    dimensions={"dept_name": lambda t: t.dept_name},
    measures={},
    joins={"emp": join_many(alias="emp", model=emp_model, with_=lambda t: t.dept_id)},
)

emp_counts_df = (
    dept_model.query(dims=["dept_name"], measures=["emp.child_count"])
    .execute()
    .sort_values("dept_name")
    .reset_index(drop=True)
)
print("Employee counts by department:")
print(emp_counts_df)
