"""Lightweight utility functions for the Boom API handler.

Kept in a separate module with no SkyPortal model imports so that unit tests
can import these functions without needing a live database connection.
"""

# JavaScript's Number.MAX_SAFE_INTEGER (2^53 - 1)
MAX_SAFE_INTEGER = 2**53 - 1


def convert_large_ints(obj):
    """Recursively convert integers that exceed JS Number.MAX_SAFE_INTEGER to strings.

    JavaScript cannot represent integers larger than 2^53 - 1 without loss of
    precision. This function walks the response tree and converts any
    out-of-range integer to its string representation so the browser receives
    the exact value.
    """
    if isinstance(obj, bool):
        return obj
    if isinstance(obj, int):
        if obj > MAX_SAFE_INTEGER or obj < -MAX_SAFE_INTEGER:
            return str(obj)
        return obj
    if isinstance(obj, dict):
        return {k: convert_large_ints(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [convert_large_ints(item) for item in obj]
    return obj
