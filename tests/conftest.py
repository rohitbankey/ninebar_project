"""A miniature world: 3 cells per area, a few 10-minute slots per day, 4 days.

Same code paths as the real run, just small enough to run in seconds.
"""
from __future__ import annotations

import json
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest
import yaml

ROOT = Path(__file__).resolve().parent.parent
ROME = ZoneInfo("Europe/Rome")


def square(lon, lat, d=0.003):
    return {"type": "Polygon", "coordinates": [[[lon, lat], [lon + d, lat], [lon + d, lat - d], [lon, lat - d], [lon, lat]]]}


def write_grid(path: Path, ids, lon, lat):
    feats = [{"type": "Feature", "id": i, "geometry": square(lon + i * 0.003, lat), "properties": {"cellId": cid}}
             for i, cid in enumerate(ids)]
    path.write_text(json.dumps({"type": "FeatureCollection", "features": feats}))


def cdr_lines(d: date, cells, slots=(0, 1, 2), scale=1.0):
    out = []
    start = datetime(d.year, d.month, d.day, tzinfo=ROME)
    for s in slots:
        ts = int((start + timedelta(minutes=10 * s)).timestamp() * 1000)
        for c in cells:
            out.append(f"{c}\t{ts}\t39\t{0.5 * scale}\t{0.25 * scale}\t\t0.1\t{10.0 * scale + c / 100}")
            out.append(f"{c}\t{ts}\t49\t0.01\t\t\t\t{0.2 * scale}")
    return out


@pytest.fixture
def world(tmp_path, monkeypatch):
    raw = tmp_path / "raw_cache"
    (raw / "grid").mkdir(parents=True)
    write_grid(raw / "grid" / "milano-grid.geojson", [1, 2, 3], 9.18, 45.47)
    write_grid(raw / "grid" / "trentino-grid.geojson", [38, 39, 40], 11.10, 46.07)
    for area, cells in (("MI", [1, 2, 3]), ("TN", [38, 39, 40])):
        (raw / area).mkdir()
        for i in range(4):
            d = date(2013, 11, 1) + timedelta(days=i)
            (raw / area / f"sms-call-internet-{area.lower()}-{d}.txt").write_text("\n".join(cdr_lines(d, cells)) + "\n")
    cfg = yaml.safe_load((ROOT / "config" / "pipeline.yaml").read_text())
    cfg["window"] = {"start": "2013-11-01", "end": "2013-11-04"}
    cfg["paths"] = {"raw_cache": str(raw), "landing": str(tmp_path / "landing"),
                    "warehouse": str(tmp_path / "wh.duckdb"), "lineage_events": str(tmp_path / "lineage"),
                    "reports": str(tmp_path / "out")}
    cfg_path = tmp_path / "pipeline.yaml"
    cfg_path.write_text(yaml.safe_dump(cfg))
    monkeypatch.setenv("NINEBAR_CONFIG", str(cfg_path))
    for k in ("NINEBAR_WAREHOUSE", "NINEBAR_LANDING", "NINEBAR_LINEAGE_EVENTS", "NINEBAR_REPORTS", "NINEBAR_RAW_CACHE",
              "NINEBAR_CRASH_AT", "NINEBAR_FAIL_ONCE"):
        monkeypatch.delenv(k, raising=False)
    return tmp_path


@pytest.fixture
def pipe(world):
    from ninebar import config
    from ninebar.orchestrator import Pipeline
    p = Pipeline(config.load())
    yield p
    p.close()


def land_and_tick(p, d: date, skip=()):
    from ninebar.cli import land_day
    land_day(p, d, set(skip))
    p.tick(d + timedelta(days=1))
