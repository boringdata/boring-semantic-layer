"""
Advanced Features Example: Demonstrating calculated columns, calculated measures,
hierarchies, and semantic level security improvements over Azure Analysis Services.

This example shows how to implement complex business logic that would typically
require DAX expressions in AAS, but using Python and Ibis expressions.
"""

import xorq as xo
from boring_semantic_layer.semantic_model import Join, SemanticModel, Filter
from typing import Dict, Any, Optional
import datetime


# Path to Malloy sample data
DATA_DIR = "../malloy-samples/data"
con = xo.connect()

# Load data
carriers_tbl = xo.deferred_read_parquet(path=f"{DATA_DIR}/carriers.parquet", con=con)
flights_tbl = xo.deferred_read_parquet(path=f"{DATA_DIR}/flights.parquet", con=con)
airports_tbl = xo.deferred_read_parquet(path=f"{DATA_DIR}/airports.parquet", con=con)

# Define carriers model with calculated columns
carriers_sm = SemanticModel(
    table=carriers_tbl,
    dimensions={
        "code": lambda t: t.code,
        "name": lambda t: t.name,
        "nickname": lambda t: t.nickname,
    },
    calculated_columns={
        # Calculated column: Full carrier name
        "full_name": lambda t: t.name + " (" + t.code + ")",
        # Calculated column: Carrier type based on name
        "carrier_type": lambda t: t.name.case()
            .when(t.name.like("%Airlines%"), "Major Airline")
            .when(t.name.like("%Express%"), "Regional")
            .when(t.name.like("%Jet%"), "Low Cost")
            .else_("Other")
            .end(),
    },
    measures={
        "carrier_count": lambda t: t.count(),
    },
    primary_key="code",
)

# Define airports model with hierarchies
airports_sm = SemanticModel(
    table=airports_tbl,
    dimensions={
        "code": lambda t: t.code,
        "name": lambda t: t.name,
        "city": lambda t: t.city,
        "state": lambda t: t.state,
        "country": lambda t: t.country,
    },
    calculated_columns={
        # Calculated column: Full location
        "location": lambda t: t.city + ", " + t.state + ", " + t.country,
        # Calculated column: Region based on state
        "region": lambda t: t.state.case()
            .when(t.state.isin(["CA", "OR", "WA"]), "West Coast")
            .when(t.state.isin(["NY", "NJ", "PA", "MA"]), "Northeast")
            .when(t.state.isin(["TX", "FL", "GA"]), "South")
            .when(t.state.isin(["IL", "MI", "OH"]), "Midwest")
            .else_("Other")
            .end(),
    },
    hierarchies={
        "geography": ["country", "state", "city", "code"],
        "region_hierarchy": ["region", "state", "city"],
    },
    measures={
        "airport_count": lambda t: t.count(),
    },
    primary_key="code",
)

# Define flights model with complex calculated measures
flights_sm = SemanticModel(
    table=flights_tbl,
    dimensions={
        "origin": lambda t: t.origin,
        "destination": lambda t: t.destination,
        "carrier": lambda t: t.carrier,
        "tail_num": lambda t: t.tail_num,
        "flight_num": lambda t: t.flight_num,
    },
    calculated_columns={
        # Calculated column: Route identifier
        "route": lambda t: t.origin + " → " + t.destination,
        # Calculated column: Flight status
        "flight_status": lambda t: t.dep_delay.case()
            .when(t.dep_delay <= 0, "On Time")
            .when(t.dep_delay <= 15, "Minor Delay")
            .when(t.dep_delay <= 60, "Major Delay")
            .else_("Severe Delay")
            .end(),
        # Calculated column: Time period
        "time_period": lambda t: t.dep_time.case()
            .when(t.dep_time.hour() < 6, "Early Morning")
            .when(t.dep_time.hour() < 12, "Morning")
            .when(t.dep_time.hour() < 18, "Afternoon")
            .else_("Evening")
            .end(),
        # Calculated column: Distance category
        "distance_category": lambda t: t.distance.case()
            .when(t.distance < 500, "Short Haul")
            .when(t.distance < 1500, "Medium Haul")
            .else_("Long Haul")
            .end(),
    },
    measures={
        "flight_count": lambda t: t.count(),
        "avg_distance": lambda t: t.distance.mean(),
        "total_distance": lambda t: t.distance.sum(),
        "avg_dep_delay": lambda t: t.dep_delay.mean(),
        "avg_arr_delay": lambda t: t.arr_delay.mean(),
    },
    calculated_measures={
        # Calculated measure: On-time percentage (like DAX CALCULATE)
        "on_time_percentage": lambda t: (
            t.filter(t.dep_delay <= 0).count() / t.count()
        ) * 100,
        
        # Calculated measure: Delay rate
        "delay_rate": lambda t: (
            t.filter(t.dep_delay > 0).count() / t.count()
        ) * 100,
        
        # Calculated measure: Average delay for delayed flights only
        "avg_delay_when_delayed": lambda t: (
            t.filter(t.dep_delay > 0).dep_delay.mean()
        ),
        
        # Calculated measure: Revenue per flight (assuming distance-based pricing)
        "revenue_per_flight": lambda t: t.distance * 0.15,  # $0.15 per mile
        
        # Calculated measure: Total revenue
        "total_revenue": lambda t: (t.distance * 0.15).sum(),
        
        # Calculated measure: Efficiency score (lower delays = higher score)
        "efficiency_score": lambda t: (
            (100 - t.dep_delay.mean()) / 100
        ) * 100,
        
        # Calculated measure: Route popularity rank
        "route_rank": lambda t: t.group_by("route").count().rank(),
    },
    joins={
        "carriers": Join.one(
            alias="carriers",
            model=carriers_sm,
            with_=lambda t: t.carrier,
        ),
        "origin_airports": Join.one(
            alias="origin_airports",
            model=airports_sm,
            with_=lambda t: t.origin,
        ),
        "dest_airports": Join.one(
            alias="dest_airports",
            model=airports_sm,
            with_=lambda t: t.destination,
        ),
    },
    time_dimension="date",
    smallest_time_grain="TIME_GRAIN_DAY",
)


# Semantic Level Security (Row-level security)
class SecurityContext:
    """Mock security context for demonstration"""
    
    def __init__(self, user_id: str, roles: list, regions: list = None):
        self.user_id = user_id
        self.roles = roles
        self.regions = regions or []
    
    def can_access_region(self, region: str) -> bool:
        return "admin" in self.roles or region in self.regions
    
    def can_access_carrier(self, carrier_code: str) -> bool:
        return "admin" in self.roles or "carrier_analyst" in self.roles


def apply_security_filters(model: SemanticModel, security_context: SecurityContext) -> list:
    """Apply semantic level security filters based on user context"""
    filters = []
    
    # Regional access control
    if security_context.regions and "admin" not in security_context.roles:
        filters.append({
            "operator": "OR",
            "conditions": [
                {"field": "origin_airports.region", "operator": "in", "values": security_context.regions},
                {"field": "dest_airports.region", "operator": "in", "values": security_context.regions},
            ]
        })
    
    # Carrier access control
    if "carrier_analyst" not in security_context.roles and "admin" not in security_context.roles:
        # Restrict to major carriers only
        filters.append({
            "field": "carriers.carrier_type", 
            "operator": "=", 
            "value": "Major Airline"
        })
    
    return filters


# Example usage
def demonstrate_advanced_features():
    """Demonstrate all advanced features"""
    
    print("=== Advanced Semantic Layer Features ===\n")
    
    # 1. Show available dimensions and measures
    print("Available Dimensions:", flights_sm.available_dimensions)
    print("Available Measures:", flights_sm.available_measures)
    print()
    
    # 2. Query with calculated columns and measures
    print("=== Query with Calculated Columns and Measures ===")
    query1 = flights_sm.query(
        dimensions=["carriers.carrier_type", "distance_category", "flight_status"],
        measures=["flight_count", "on_time_percentage", "total_revenue", "efficiency_score"],
        filters=[
            {"field": "date", "operator": ">=", "value": "2024-01-01"},
            {"field": "date", "operator": "<=", "value": "2024-01-31"},
        ],
        order_by=[("total_revenue", "desc")],
        limit=10
    )
    
    result1 = query1.execute()
    print("Top 10 revenue routes by carrier type and distance:")
    print(result1)
    print()
    
    # 3. Query with hierarchies
    print("=== Query with Geographic Hierarchy ===")
    query2 = flights_sm.query(
        dimensions=["origin_airports.region", "origin_airports.state", "origin_airports.city"],
        measures=["flight_count", "avg_distance"],
        order_by=[("flight_count", "desc")],
        limit=15
    )
    
    result2 = query2.execute()
    print("Flight counts by geographic hierarchy:")
    print(result2)
    print()
    
    # 4. Semantic Level Security
    print("=== Semantic Level Security Demo ===")
    
    # Admin user - full access
    admin_context = SecurityContext("admin1", ["admin"])
    admin_filters = apply_security_filters(flights_sm, admin_context)
    
    # Regional user - limited access
    regional_context = SecurityContext("user1", ["analyst"], ["West Coast", "Northeast"])
    regional_filters = apply_security_filters(flights_sm, regional_context)
    
    print(f"Admin filters: {admin_filters}")
    print(f"Regional user filters: {regional_filters}")
    
    # Query with security filters
    secure_query = flights_sm.query(
        dimensions=["carriers.name", "origin_airports.region"],
        measures=["flight_count", "on_time_percentage"],
        filters=regional_filters,
        order_by=[("flight_count", "desc")],
        limit=5
    )
    
    secure_result = secure_query.execute()
    print("Regional user query result:")
    print(secure_result)
    print()
    
    # 5. Complex calculated measures
    print("=== Complex Calculated Measures ===")
    query3 = flights_sm.query(
        dimensions=["carriers.name", "time_period"],
        measures=["flight_count", "delay_rate", "avg_delay_when_delayed", "efficiency_score"],
        filters=[
            {"field": "date", "operator": ">=", "value": "2024-01-01"},
        ],
        order_by=[("efficiency_score", "desc")],
        limit=10
    )
    
    result3 = query3.execute()
    print("Carrier efficiency by time period:")
    print(result3)
    print()
    
    # 6. Materialization with calculated columns
    print("=== Materialization with Calculated Columns ===")
    try:
        # Materialize the model with calculated columns
        materialized_model = flights_sm.materialize(
            time_grain="TIME_GRAIN_DAY",
            dimensions=["carriers.carrier_type", "distance_category", "flight_status"],
            storage="memory"  # In-memory storage
        )
        
        print("Materialized model created successfully!")
        print(f"Materialized dimensions: {materialized_model.available_dimensions}")
        print(f"Materialized measures: {materialized_model.available_measures}")
        
        # Query the materialized model
        mat_query = materialized_model.query(
            dimensions=["carriers.carrier_type"],
            measures=["flight_count"],
            order_by=[("flight_count", "desc")]
        )
        
        mat_result = mat_query.execute()
        print("Materialized query result:")
        print(mat_result)
        
    except Exception as e:
        print(f"Materialization not available: {e}")
    
    print("\n=== Feature Comparison with Azure Analysis Services ===")
    print("✅ Calculated Columns: Implemented")
    print("✅ Calculated Measures: Implemented")
    print("✅ Hierarchies: Basic support")
    print("✅ Semantic Level Security: Implemented")
    print("❌ Advanced Time Intelligence: Limited")
    print("❌ KPIs: Not implemented")
    print("❌ Perspectives: Not implemented")
    print("❌ Advanced DAX-like expressions: Limited")


if __name__ == "__main__":
    demonstrate_advanced_features() 