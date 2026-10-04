"""Period anchors for the three cadences.

daily       Europe/Rome calendar day [00:00, 24:00). Same cut as the source files.
weekly      ISO week, starting Monday 00:00 Europe/Rome.
fortnightly 14-day billing cycle counted from a fixed anchor date (a Friday by
            default). Because the anchor weekday differs from the weekly anchor,
            a cycle always covers parts of three ISO weeks.

All period keys are local dates. Converting to UTC instants only happens where
we need slot counts (DST days).
"""
from __future__ import annotations

from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo


def week_start(d: date) -> date:
    return d - timedelta(days=d.weekday())


def week_days(ws: date) -> list[date]:
    return [ws + timedelta(days=i) for i in range(7)]


def cycle_start(d: date, anchor: date, length: int = 14) -> date:
    n = (d - anchor).days // length  # floor division also works for dates before the anchor
    return anchor + timedelta(days=n * length)


def cycle_days(cs: date, length: int = 14) -> list[date]:
    return [cs + timedelta(days=i) for i in range(length)]


def expected_slots(d: date, tz: str = "Europe/Rome") -> int:
    """Number of 10-minute slots in a local calendar day (144, or 138/150 on DST days)."""
    z = ZoneInfo(tz)
    start = datetime(d.year, d.month, d.day, tzinfo=z)
    nxt = d + timedelta(days=1)
    end = datetime(nxt.year, nxt.month, nxt.day, tzinfo=z)
    return int((end.timestamp() - start.timestamp()) // 600)


def daterange(a: date, b: date):
    d = a
    while d <= b:
        yield d
        d += timedelta(days=1)
