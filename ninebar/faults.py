"""Fault injection, so crash and retry behaviour can be demonstrated rather than claimed.

NINEBAR_CRASH_AT=<point>[:<match>]   hard-kill the process (os._exit) at that point,
                                     simulating OOM / node loss mid-transaction
NINEBAR_FAIL_ONCE=<point>[:<match>]  raise a TransientError the first time the point is hit
                                     in this process, to exercise the retry path
"""
from __future__ import annotations

import os
import sys

_failed: set[str] = set()


class TransientError(RuntimeError):
    """Errors worth retrying (I/O hiccups, lock contention)."""


def _matches(var: str, point: str, ctx: str) -> bool:
    spec = os.environ.get(var)
    if not spec:
        return False
    want_point, _, want_ctx = spec.partition(":")
    return want_point == point and (not want_ctx or want_ctx in ctx)


def checkpoint(point: str, ctx: str = "") -> None:
    if _matches("NINEBAR_CRASH_AT", point, ctx):
        print(f"!! injected crash at {point} [{ctx}]", file=sys.stderr, flush=True)
        os._exit(97)
    key = f"{point}|{ctx}"
    if _matches("NINEBAR_FAIL_ONCE", point, ctx) and key not in _failed:
        _failed.add(key)
        raise TransientError(f"injected transient failure at {point} [{ctx}]")
