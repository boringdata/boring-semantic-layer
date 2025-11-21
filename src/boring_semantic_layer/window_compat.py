"""Compatibility layer for window functions between regular and vendored ibis (9.5.0)"""

from __future__ import annotations

try:
    import ibis
    _IBIS_AVAILABLE = True
except ImportError:
    _IBIS_AVAILABLE = False

import xorq.vendor.ibis as xibis
from xorq.vendor.ibis.expr.types.generic import Value as XorqValue


def _extract_value(boundary):
    """Extract the numeric value from a window boundary object.

    Args:
        boundary: A window boundary object that may have nested .value attributes

    Returns:
        The numeric value or None if the boundary has no value
    """
    if not hasattr(boundary, 'value') or boundary.value is None:
        return None

    value = boundary.value
    # Handle nested value attribute (e.g., boundary.value.value)
    if hasattr(value, 'value'):
        value = value.value

    return value


def _process_rows_frame(window, params):
    """Process ROWS frame type window bounds.

    For ROWS frames:
    - start=0 means current row (no preceding needed)
    - start<0 means N rows before current (preceding=abs(N))
    - end=0 means current row (following=0)
    - end>0 means N rows after current (following=N)

    Args:
        window: The ibis window object
        params: Dictionary to populate with window parameters
    """
    start_val = _extract_value(window.start)
    if start_val is not None and start_val != 0:
        params['preceding'] = abs(start_val)

    end_val = _extract_value(window.end)
    if end_val is not None:
        params['following'] = end_val if end_val != 0 else 0


def _process_range_frame(window, params):
    """Process RANGE frame type window bounds.

    For RANGE frames:
    - bounds are relative to the current row's value in the ORDER BY expression
    - start/end=0 means current value (no preceding/following)

    Args:
        window: The ibis window object
        params: Dictionary to populate with window parameters
    """
    start_val = _extract_value(window.start)
    if start_val is not None and start_val != 0:
        params['preceding'] = abs(start_val)

    end_val = _extract_value(window.end)
    if end_val is not None and end_val != 0:
        params['following'] = end_val


def convert_window_to_xorq(window):
    """Convert a regular ibis window to a xorq-compatible window.

    This function handles conversion between regular ibis LegacyWindowBuilder
    objects and xorq's vendored ibis window objects. It preserves:
    - Group by columns
    - Order by columns
    - Frame type (ROWS vs RANGE)
    - Frame bounds (preceding/following)

    Args:
        window: An ibis or xorq window object

    Returns:
        A xorq-compatible window object, or the original if already compatible
        or if ibis is not available
    """
    # Already a xorq window - no conversion needed
    if isinstance(window, xibis.expr.builders.LegacyWindowBuilder):
        return window

    # Only convert if ibis is available and window is a regular ibis window
    if not (_IBIS_AVAILABLE and isinstance(window, ibis.expr.builders.LegacyWindowBuilder)):
        return window

    params = {}

    # Extract grouping and ordering columns
    if window.groupings:
        params['group_by'] = window.groupings

    if window.orderings:
        params['order_by'] = window.orderings

    # Process frame bounds based on frame type
    if window.how == 'rows':
        _process_rows_frame(window, params)
    elif window.how == 'range':
        _process_range_frame(window, params)

    return xibis.window(**params)


# Store the original method before patching
_original_over = XorqValue.over
_patch_installed = False


def _patched_over(self, window=None, *, rows=None, range=None, group_by=None, order_by=None):
    """Patched version of .over() that accepts both regular and xorq windows.

    This wrapper automatically converts regular ibis windows to xorq-compatible
    windows before calling the original over() method.

    Args:
        self: The Value expression instance
        window: A window specification (ibis or xorq)
        rows: Optional row-based window frame specification
        range: Optional range-based window frame specification
        group_by: Optional grouping columns
        order_by: Optional ordering columns

    Returns:
        The result of calling the original over() method with converted window
    """
    if window is not None:
        window = convert_window_to_xorq(window)

    return _original_over(
        self,
        window=window,
        rows=rows,
        range=range,
        group_by=group_by,
        order_by=order_by
    )


def install_window_compatibility():
    """Install the window compatibility monkey-patch.

    This patches xorq's vendored ibis Value.over() method to automatically
    convert regular ibis windows to xorq windows. This allows code written
    with regular ibis to work seamlessly with xorq's vendored ibis.

    The patch is only installed if regular ibis is available in the environment.
    If ibis is not available, the compatibility layer is not needed since there
    are no regular ibis windows to convert.

    Note:
        This function is idempotent - calling it multiple times has no effect
        after the first installation.
    """
    global _patch_installed

    if _IBIS_AVAILABLE and not _patch_installed:
        XorqValue.over = _patched_over
        _patch_installed = True


def uninstall_window_compatibility():
    """Uninstall the window compatibility monkey-patch.

    Restores the original .over() method to xorq's vendored ibis Value class.
    This is primarily useful for testing or cleanup purposes.

    The uninstall only occurs if regular ibis is available and the patch
    was previously installed.

    Note:
        This function is idempotent - calling it multiple times has no effect
        after the first uninstallation.
    """
    global _patch_installed

    if _IBIS_AVAILABLE and _patch_installed:
        XorqValue.over = _original_over
        _patch_installed = False
