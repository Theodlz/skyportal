"""Unit tests for Boom API utility functions.

These are pure-Python unit tests with no database or SkyPortal-stack dependencies.
"""

import importlib.util
import os

import pytest

# Load boom/utils.py directly to avoid triggering boom/__init__.py,
# which imports run_filter.py (makes module-level network/config calls).
_utils_path = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "skyportal",
    "handlers",
    "api",
    "boom",
    "utils.py",
)
_spec = importlib.util.spec_from_file_location("boom_utils", _utils_path)
_boom_utils = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_boom_utils)

MAX_SAFE_INTEGER = _boom_utils.MAX_SAFE_INTEGER
convert_large_ints = _boom_utils.convert_large_ints


class TestConvertLargeInts:
    """Tests for convert_large_ints."""

    # --- scalars ---

    def test_small_positive_int_unchanged(self):
        assert convert_large_ints(42) == 42

    def test_zero_unchanged(self):
        assert convert_large_ints(0) == 0

    def test_small_negative_int_unchanged(self):
        assert convert_large_ints(-100) == -100

    def test_max_safe_integer_unchanged(self):
        assert convert_large_ints(MAX_SAFE_INTEGER) == MAX_SAFE_INTEGER

    def test_negative_max_safe_integer_unchanged(self):
        assert convert_large_ints(-MAX_SAFE_INTEGER) == -MAX_SAFE_INTEGER

    def test_exceeds_max_safe_integer_becomes_string(self):
        large = MAX_SAFE_INTEGER + 1
        result = convert_large_ints(large)
        assert result == str(large)
        assert isinstance(result, str)

    def test_exceeds_min_safe_integer_becomes_string(self):
        large_neg = -(MAX_SAFE_INTEGER + 1)
        result = convert_large_ints(large_neg)
        assert result == str(large_neg)
        assert isinstance(result, str)

    def test_typical_ztf_candid_becomes_string(self):
        # ZTF candid values are typically around 1e18, well above MAX_SAFE_INTEGER
        candid = 1234567890123456789
        result = convert_large_ints(candid)
        assert result == str(candid)

    def test_bool_not_converted(self):
        # bool is a subclass of int; must not be stringified
        assert convert_large_ints(True) is True
        assert convert_large_ints(False) is False

    def test_float_unchanged(self):
        assert convert_large_ints(3.14) == 3.14

    def test_string_unchanged(self):
        assert convert_large_ints("hello") == "hello"

    def test_none_unchanged(self):
        assert convert_large_ints(None) is None

    # --- dict ---

    def test_dict_small_values_unchanged(self):
        d = {"a": 1, "b": "x"}
        assert convert_large_ints(d) == {"a": 1, "b": "x"}

    def test_dict_large_value_converted(self):
        candid = 1234567890123456789
        d = {"candid": candid, "rb": 0.9}
        result = convert_large_ints(d)
        assert result["candid"] == str(candid)
        assert result["rb"] == 0.9

    def test_dict_keys_not_modified(self):
        # Keys are strings; they should never be touched
        d = {"1234567890123456789": 1}
        result = convert_large_ints(d)
        assert "1234567890123456789" in result

    # --- list ---

    def test_list_small_values_unchanged(self):
        lst = [1, 2, 3]
        assert convert_large_ints(lst) == [1, 2, 3]

    def test_list_large_value_converted(self):
        candid = 1234567890123456789
        lst = [42, candid]
        result = convert_large_ints(lst)
        assert result == [42, str(candid)]

    # --- nested structures ---

    def test_nested_dict_in_list(self):
        candid = 9999999999999999
        data = [{"candid": candid, "rb": 0.5}]
        result = convert_large_ints(data)
        assert result[0]["candid"] == str(candid)
        assert result[0]["rb"] == 0.5

    def test_deeply_nested(self):
        large = MAX_SAFE_INTEGER + 100
        obj = {"level1": {"level2": [{"value": large}]}}
        result = convert_large_ints(obj)
        assert result["level1"]["level2"][0]["value"] == str(large)

    def test_typical_boom_response_shape(self):
        """Simulate a Boom response document with a large candid field."""
        candid = 2475506410015010019
        doc = {
            "_id": "507f1f77bcf86cd799439011",  # already a string
            "candid": candid,
            "rb": 0.98,
            "annotations": {"period": 1.23},
        }
        result = convert_large_ints(doc)
        assert result["_id"] == "507f1f77bcf86cd799439011"
        assert result["candid"] == str(candid)
        assert result["rb"] == 0.98
        assert result["annotations"]["period"] == 1.23
