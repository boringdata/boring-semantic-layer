"""
Malloy Query Reference for BSL Comparison Tests

This file contains references to all the original Malloy queries that are being compared
with their BSL (Boring Semantic Layer) equivalents in test_malloy.py.

Each query is stored in a separate .malloynb file in the tests/malloy/ directory and is
documented with:
- Original Malloy syntax
- Description and purpose
- Source URL from official Malloy documentation or samples
- Corresponding BSL test function name

Sources:
- Basic queries: https://context7.com/malloydata/malloy/llms.txt
- Advanced patterns: https://github.com/malloydata/malloy-samples/tree/main/patterns
- Language documentation: https://github.com/malloydata/malloydata.github.io/tree/main/src/documentation/language
"""

from pathlib import Path

# Get the directory where this file is located
CURRENT_DIR = Path(__file__).parent
MALLOY_DIR = CURRENT_DIR / "malloy"


def _read_malloynb_file(filename: str) -> str:
    """Read a .malloynb file and return its content."""
    file_path = MALLOY_DIR / f"{filename}.malloynb"
    if not file_path.exists():
        raise FileNotFoundError(f"Malloy file not found: {file_path}")

    with open(file_path, "r", encoding="utf-8") as f:
        content = f.read()

    # Extract just the Malloy code (remove comments)
    lines = content.split("\n")
    malloy_lines = []
    in_comment_block = False

    for line in lines:
        stripped = line.strip()
        if stripped.startswith("/*"):
            in_comment_block = True
            continue
        elif stripped.endswith("*/"):
            in_comment_block = False
            continue
        elif not in_comment_block and stripped and not stripped.startswith("//"):
            malloy_lines.append(line)

    return "\n".join(malloy_lines).strip()


# Query Mappings Documentation
QUERY_MAPPINGS = {
    "basic_flight_query": {
        "description": "Basic filtering and aggregation with grouping",
        "test_function": "test_basic_flight_query_sfo_by_carrier",
        "source_url": "https://context7.com/malloydata/malloy/llms.txt",
        "malloynb_file": "basic_flight_query",
    },
    "select_all": {
        "description": "Simple select all records",
        "test_function": "test_airports_select_all",
        "source_url": "https://context7.com/malloydata/malloy/llms.txt",
        "malloynb_file": "select_all",
    },
    "filtered_select": {
        "description": "Select with WHERE clause filtering",
        "test_function": "test_airports_filter_by_district",
        "source_url": "https://context7.com/malloydata/malloy/llms.txt",
        "malloynb_file": "filtered_select",
    },
    "extended_measures": {
        "description": "Source extension with custom measures and views",
        "test_function": "test_airports_extended_measures",
        "source_url": "https://context7.com/malloydata/malloy/llms.txt",
        "malloynb_file": "extended_measures",
    },
    "by_state_view": {
        "description": "View definition with null filtering",
        "test_function": "test_airports_by_state_view",
        "source_url": "https://context7.com/malloydata/malloy/llms.txt",
        "malloynb_file": "by_state_view",
    },
    "carrier_grouping": {
        "description": "Simple grouping by dimension",
        "test_function": "test_flights_carrier_grouping",
        "source_url": "https://context7.com/malloydata/malloy/llms.txt",
        "malloynb_file": "carrier_grouping",
    },
    "complex_filtering": {
        "description": "Multiple filter conditions with AND logic",
        "test_function": "test_complex_filtering_combinations",
        "source_url": "https://context7.com/malloydata/malloy/llms.txt",
        "malloynb_file": "complex_filtering",
    },
    "time_grouping": {
        "description": "Time-based dimension grouping",
        "test_function": "test_time_based_grouping",
        "source_url": "https://context7.com/malloydata/malloy/llms.txt",
        "malloynb_file": "time_grouping",
    },
    "calculated_measures": {
        "description": "Multiple calculated aggregations",
        "test_function": "test_measure_calculations",
        "source_url": "https://context7.com/malloydata/malloy/llms.txt",
        "malloynb_file": "calculated_measures",
    },
    "conditional_aggregation": {
        "description": "Conditional counting with WHERE in measure",
        "test_function": "test_conditional_aggregation",
        "source_url": "https://context7.com/malloydata/malloy/llms.txt",
        "malloynb_file": "conditional_aggregation",
    },
    "multi_view_source": {
        "description": "Source definition with multiple views and complex measures",
        "test_function": "test_multi_view_source",
        "source_url": "https://context7.com/malloydata/malloy/llms.txt",
        "malloynb_file": "multi_view_source",
    },
    "bar_chart_examples": {
        "description": "Chart annotations and visualization directives",
        "test_function": "test_bar_chart_examples",
        "source_url": "https://github.com/malloydata/malloydata.github.io/tree/main/src/documentation/language",
        "malloynb_file": "bar_chart_examples",
    },
    "nested_query": {
        "description": "Nested queries with grouping hierarchy",
        "test_function": "test_nested_query",
        "source_url": "https://github.com/malloydata/malloydata.github.io/tree/main/src/documentation/language",
        "malloynb_file": "nested_query",
    },
    "advanced_parameterized": {
        "description": "Parameterized query with explore syntax",
        "test_function": "test_advanced_parameterized_query",
        "source_url": "https://context7.com/malloydata/malloy/llms.txt",
        "malloynb_file": "advanced_parameterized",
    },
    "percentage_calculation": {
        "description": "Percentage calculation using all() function",
        "test_function": "test_percentage_calculation",
        "source_url": "https://raw.githubusercontent.com/malloydata/malloy-samples/main/patterns/percent_of_total.malloynb",
        "malloynb_file": "percentage_calculation",
    },
    "cohort_analysis": {
        "description": "Cohort analysis with nested grouping and time-based filtering",
        "test_function": "test_cohort_analysis",
        "source_url": "https://raw.githubusercontent.com/malloydata/malloy-samples/main/patterns/cohorts.malloynb",
        "malloynb_file": "cohort_analysis",
    },
    "moving_average": {
        "description": "Moving average calculations with window functions",
        "test_function": "test_moving_average",
        "source_url": "https://raw.githubusercontent.com/malloydata/malloy-samples/main/patterns/moving_avg.malloynb",
        "malloynb_file": "moving_average",
    },
    "year_over_year": {
        "description": "Year-over-year comparison with filtered aggregates",
        "test_function": "test_year_over_year",
        "source_url": "https://raw.githubusercontent.com/malloydata/malloy-samples/main/patterns/yoy.malloynb",
        "malloynb_file": "year_over_year",
    },
    "sessionization": {
        "description": "Session analysis with row numbering and nested structures",
        "test_function": "test_sessionization",
        "source_url": "https://raw.githubusercontent.com/malloydata/malloy-samples/main/patterns/sessionize.malloynb",
        "malloynb_file": "sessionization",
    },
    "percent_of_total_detailed": {
        "description": "Detailed percentage calculations across multiple dimensions",
        "test_function": "test_percent_of_total_detailed",
        "source_url": "https://raw.githubusercontent.com/malloydata/malloy-samples/main/patterns/percent_of_total.malloynb",
        "malloynb_file": "percent_of_total",
    },
    "window_lag_calculation": {
        "description": "Window functions with lag calculations for growth analysis",
        "test_function": "test_window_lag_calculation",
        "source_url": "https://raw.githubusercontent.com/malloydata/malloy-samples/main/patterns/yoy.malloynb",
        "malloynb_file": "window_lag_calculation",
    },
}


def get_malloy_query(query_name: str) -> str:
    """Get a Malloy query by name."""
    mapping = QUERY_MAPPINGS.get(query_name)
    if not mapping:
        raise KeyError(f"Unknown query: {query_name}")

    return _read_malloynb_file(mapping["malloynb_file"])


def get_query_description(query_name: str) -> str:
    """Get the description for a query."""
    mapping = QUERY_MAPPINGS.get(query_name)
    if not mapping:
        raise KeyError(f"Unknown query: {query_name}")
    return mapping["description"]


def get_source_url(query_name: str) -> str:
    """Get the source URL for a query."""
    mapping = QUERY_MAPPINGS.get(query_name)
    if not mapping:
        raise KeyError(f"Unknown query: {query_name}")
    return mapping["source_url"]


def get_malloynb_file_path(query_name: str) -> Path:
    """Get the path to the .malloynb file for a query."""
    mapping = QUERY_MAPPINGS.get(query_name)
    if not mapping:
        raise KeyError(f"Unknown query: {query_name}")
    return MALLOY_DIR / f"{mapping['malloynb_file']}.malloynb"


def list_available_queries() -> list[str]:
    """List all available query names."""
    return list(QUERY_MAPPINGS.keys())


def get_query_info(query_name: str) -> dict:
    """Get complete information for a query including malloy code, description, and source URL."""
    mapping = QUERY_MAPPINGS.get(query_name)
    if not mapping:
        raise KeyError(f"Unknown query: {query_name}")

    return {
        "name": query_name,
        "malloy": get_malloy_query(query_name),
        "description": mapping["description"],
        "source_url": mapping["source_url"],
        "test_function": mapping["test_function"],
        "malloynb_file": get_malloynb_file_path(query_name),
    }


def list_all_malloynb_files() -> list[Path]:
    """List all .malloynb files in the malloy directory."""
    return list(MALLOY_DIR.glob("*.malloynb"))
