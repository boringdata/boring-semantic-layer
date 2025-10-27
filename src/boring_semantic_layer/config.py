"""
Global configuration for Boring Semantic Layer.

This module provides configuration options that affect query optimization
and behavior across the library, following the Ibis config pattern.
"""

from ibis.config import Config


class Rewrites(Config):
    """Configuration for query rewrite optimizations.

    Attributes
    ----------
    enable_projection_pushdown : bool
        Enable projection pushdown optimization for joins.

        When enabled, BSL will filter out unused columns before joins to reduce
        data scanned. This is especially beneficial for wide tables with many
        unused columns.

        Default: True

        Performance impact when enabled:
        - Wide tables with 10+ unused columns: 50-70% reduction in columns scanned
        - Join-heavy queries: Multiplicative benefit across multiple tables

        Set to False to disable this optimization if you encounter issues or want
        to compare query performance.

        Example:
            >>> from boring_semantic_layer import options
            >>> options.rewrites.enable_projection_pushdown = False
            >>> # Run queries without projection pushdown...
            >>> options.rewrites.enable_projection_pushdown = True
    """

    enable_projection_pushdown: bool = True


class Options(Config):
    """Boring Semantic Layer configuration options.

    Attributes
    ----------
    rewrites : Rewrites
        Options controlling query rewrite optimizations.

    Example:
        >>> from boring_semantic_layer import options
        >>> # Disable projection pushdown
        >>> options.rewrites.enable_projection_pushdown = False
        >>> # Or use get/set methods
        >>> options.set("rewrites.enable_projection_pushdown", False)
        >>> options.get("rewrites.enable_projection_pushdown")
        False
    """

    rewrites: Rewrites = Rewrites()


# Global options instance
options = Options()
