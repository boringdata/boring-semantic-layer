""" TO BE REMOVED ONCE XORQ CATCHES UP
This compatibility layer is only needed for ibis version 10.0.0 and greater.
"""

from __future__ import annotations

import ibis
import xorq.vendor.ibis as xibis
from packaging.version import Version
from xorq.vendor.ibis.expr.types.generic import Value as XorqValue

_IBIS_VERSION = Version(ibis.__version__)
_NEEDS_COMPAT = Version("10.0.0") <= _IBIS_VERSION


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
    if hasattr(value, 'value'):
        value = value.value

    return value


def _process_rows_frame(window, params):
    start_val = _extract_value(window.start)
    if start_val is not None and start_val != 0:
        params['preceding'] = abs(start_val)

    end_val = _extract_value(window.end)
    if end_val is not None:
        params['following'] = end_val if end_val != 0 else 0


def _process_range_frame(window, params):
    start_val = _extract_value(window.start)
    if start_val is not None and start_val != 0:
        params['preceding'] = abs(start_val)

    end_val = _extract_value(window.end)
    if end_val is not None and end_val != 0:
        params['following'] = end_val


def convert_window_to_xorq(window):
    if isinstance(window, xibis.expr.builders.LegacyWindowBuilder):
        return window

    if not (_NEEDS_COMPAT and isinstance(window, ibis.expr.builders.LegacyWindowBuilder)):
        return window

    params = {}

    if window.groupings:
        params['group_by'] = window.groupings

    if window.orderings:
        params['order_by'] = window.orderings

    if window.how == 'rows':
        _process_rows_frame(window, params)
    elif window.how == 'range':
        _process_range_frame(window, params)

    return xibis.window(**params)


_original_xorq_over = XorqValue.over
_original_ibis_over = None
_patch_installed = False

_original_ibis_window = ibis.window
_original_ibis_desc = ibis.desc
_original_ibis_asc = ibis.asc


def _is_xorq_expr(expr):
    if not hasattr(expr, '__class__'):
        return False

    if isinstance(expr, XorqValue):
        return True

    import sys
    if hasattr(expr, '__module__'):
        expr_module_name = expr.__module__.split('.')[0]
        if expr_module_name == 'ibis':
            expr_module = sys.modules.get(expr.__module__)
            if expr_module:
                root_parts = expr.__module__.split('.')
                root_module = sys.modules.get(root_parts[0])
                return root_module is xibis

    return False


def _contains_xorq_exprs(*args, **kwargs):
    def check_value(val):
        if val is None:
            return False
        if _is_xorq_expr(val):
            return True
        if isinstance(val, (list, tuple)):
            return any(check_value(v) for v in val)
        return False

    return any(check_value(arg) for arg in args) or any(check_value(v) for v in kwargs.values())


def _patched_ibis_window(**kwargs):
    if _contains_xorq_exprs(**kwargs):
        return xibis.window(**kwargs)

    return _original_ibis_window(**kwargs)


def _patched_ibis_desc(expr):
    if _is_xorq_expr(expr):
        return xibis.desc(expr)
    return _original_ibis_desc(expr)


def _patched_ibis_asc(expr):
    if _is_xorq_expr(expr):
        return xibis.asc(expr)
    return _original_ibis_asc(expr)


def _is_xorq_window(window):
    if window is None:
        return False
    return isinstance(window, xibis.expr.builders.LegacyWindowBuilder)


def _patched_xorq_over(self, window=None, *, rows=None, range=None, group_by=None, order_by=None):
    if window is not None:
        window = convert_window_to_xorq(window)

    return _original_xorq_over(
        self,
        window=window,
        rows=rows,
        range=range,
        group_by=group_by,
        order_by=order_by
    )


def _patched_ibis_over(self, window=None, *, rows=None, range=None, group_by=None, order_by=None):
    if window is not None and _is_xorq_window(window):
        from xorq.common.utils.ibis_utils import from_ibis
        xorq_expr = from_ibis(self)
        return xorq_expr.over(
            window=window,
            rows=rows,
            range=range,
            group_by=group_by,
            order_by=order_by
        )

    return _original_ibis_over(
        self,
        window=window,
        rows=rows,
        range=range,
        group_by=group_by,
        order_by=order_by
    )


def install_window_compatibility():
    """Install the window compatibility monkey-patch.

    This patches:
    1. xorq's vendored ibis Value.over() method to accept regular ibis windows
       (converts them to xorq windows using convert_window_to_xorq)
    2. regular ibis.window() function to return xorq windows when xorq
       expressions are detected in the arguments
    3. regular ibis Value.over() method to accept xorq windows
       (converts the expression to xorq using from_ibis)
    4. regular ibis.desc() and ibis.asc() to accept xorq expressions

    Only installs the patch if ibis version is 10.0.0 or greater.
    """
    global _patch_installed, _original_ibis_over

    if _NEEDS_COMPAT and not _patch_installed:
        _original_ibis_over = ibis.expr.types.generic.Value.over

        XorqValue.over = _patched_xorq_over

        ibis.expr.types.generic.Value.over = _patched_ibis_over

        ibis.window = _patched_ibis_window
        ibis.desc = _patched_ibis_desc
        ibis.asc = _patched_ibis_asc

        _patch_installed = True


def uninstall_window_compatibility():
    """Uninstall the window compatibility monkey-patch.

    Restores the original methods:
    1. .over() methods to both xorq's and regular ibis Value classes
    2. window(), desc(), asc() functions to regular ibis module
    """
    global _patch_installed

    if _NEEDS_COMPAT and _patch_installed:
        XorqValue.over = _original_xorq_over
        ibis.expr.types.generic.Value.over = _original_ibis_over
        ibis.window = _original_ibis_window
        ibis.desc = _original_ibis_desc
        ibis.asc = _original_ibis_asc
        _patch_installed = False
