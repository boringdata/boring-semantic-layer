"""
Time grain definitions and transformations for Boring Semantic Layer.
"""

from typing import Literal, Dict, Callable


# Time grain type alias
TimeGrain = Literal[
    "TIME_GRAIN_YEAR",
    "TIME_GRAIN_QUARTER",
    "TIME_GRAIN_MONTH",
    "TIME_GRAIN_WEEK",
    "TIME_GRAIN_DAY",
    "TIME_GRAIN_HOUR",
    "TIME_GRAIN_MINUTE",
    "TIME_GRAIN_SECOND",
]

# Mapping of time grain identifiers to ibis truncate functions
TIME_GRAIN_TRANSFORMATIONS: Dict[str, Callable] = {
    "TIME_GRAIN_YEAR": lambda t: t.truncate("Y"),
    "TIME_GRAIN_QUARTER": lambda t: t.truncate("Q"),
    "TIME_GRAIN_MONTH": lambda t: t.truncate("M"),
    "TIME_GRAIN_WEEK": lambda t: t.truncate("W"),
    "TIME_GRAIN_DAY": lambda t: t.truncate("D"),
    "TIME_GRAIN_HOUR": lambda t: t.truncate("h"),
    "TIME_GRAIN_MINUTE": lambda t: t.truncate("m"),
    "TIME_GRAIN_SECOND": lambda t: t.truncate("s"),
}

# Order of grains from finest to coarsest for validation
TIME_GRAIN_ORDER = [
    "TIME_GRAIN_SECOND",
    "TIME_GRAIN_MINUTE",
    "TIME_GRAIN_HOUR",
    "TIME_GRAIN_DAY",
    "TIME_GRAIN_WEEK",
    "TIME_GRAIN_MONTH",
    "TIME_GRAIN_QUARTER",
    "TIME_GRAIN_YEAR",
]
