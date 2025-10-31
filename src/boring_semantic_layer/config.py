"""
Global configuration for Boring Semantic Layer.

This module provides configuration options that affect query optimization
and behavior across the library.

Note: Projection pushdown optimization is always enabled and built into the
Relation operations. It automatically filters out unused columns before joins
to reduce data scanned, which is especially beneficial for wide tables.
"""

from ibis.config import Config


class Options(Config):
    """Boring Semantic Layer configuration options.

    Currently, all optimizations are always enabled and built into the relation
    operations. This configuration class is reserved for future configuration options.
    """

    pass


# Global options instance
options = Options()
