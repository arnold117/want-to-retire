"""Canonical JSON for hashing: sorted keys, no whitespace, UTF-8, integers exact, no floats.
Same as RFC 8785 for every value we use except integers above 2^53 (nanosecond timestamps), which RFC 8785 would round."""
import hashlib
import json


def _no_floats(x) -> None:
    if isinstance(x, float):
        raise ValueError(f"float in canonical JSON: {x!r}; use a decimal string")
    if isinstance(x, dict):
        for v in x.values():
            _no_floats(v)
    elif isinstance(x, list):
        for v in x:
            _no_floats(v)


def dumps(obj) -> bytes:
    _no_floats(obj)
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()


def sha256(obj) -> str:
    return hashlib.sha256(dumps(obj)).hexdigest()
