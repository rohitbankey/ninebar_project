"""Generate the reference data I model myself: cell sites and tariff plans.

The open dataset has grid geometry but nothing about the network or tariffs, so
these are built deterministically (seeded by cell id / country code) and
delivered as full snapshots, exactly like an upstream system would send them.

cell_site    - one row per grid square. Geometry is real (centroid of the grid
               polygon). site_class comes from distance to the area centre.
               radio_band and site_manager_msisdn are invented. The msisdn is
               there on purpose so that the masking rule has something real to do.
               Snapshot 2 (2013-11-18) upgrades ~4% of sites to LTE800+1800 and
               swaps a handful of site managers -> SCD2 history.

tariff_plan  - interconnect/roaming rate per country-code prefix. Snapshot 2
               (2013-11-20) cuts the EU internet rate. That date sits in the
               middle of the second billing cycle, so the fortnightly mart has
               to price each day with the plan that was valid on that day.
"""
from __future__ import annotations

import csv
import hashlib
import json
import math
from datetime import date
from pathlib import Path

CELL_SNAPSHOTS = [date(2013, 11, 1), date(2013, 11, 18)]
PLAN_SNAPSHOTS = [date(2013, 11, 1), date(2013, 11, 20)]

GRID_FILES = {"MI": ("milano-grid.geojson", 235), "TN": ("trentino-grid.geojson", 1000)}

EU = [30, 31, 32, 33, 34, 351, 352, 353, 356, 357, 358, 359, 36, 370, 371, 372, 385, 386,
      40, 420, 421, 43, 44, 45, 46, 48, 49]
ROW = [1, 7, 20, 27, 41, 47, 51, 52, 53, 54, 55, 56, 57, 58, 60, 61, 62, 63, 64, 65, 66, 81, 82,
       84, 86, 90, 91, 92, 93, 94, 95, 98, 212, 213, 216, 218, 220, 221, 225, 233, 234, 237, 251,
       254, 255, 256, 260, 263, 351, 355, 373, 374, 375, 377, 378, 380, 381, 382, 387, 389, 852,
       853, 855, 880, 886, 960, 961, 962, 963, 964, 965, 966, 967, 968, 970, 971, 972, 973, 974,
       975, 976, 977, 992, 993, 994, 995, 996, 998]


def _h(*parts) -> int:
    return int(hashlib.sha1("|".join(map(str, parts)).encode()).hexdigest()[:12], 16)


def _centroid(geom: dict) -> tuple[float, float]:
    ring = geom["coordinates"][0][:-1]
    return (sum(p[1] for p in ring) / len(ring), sum(p[0] for p in ring) / len(ring))


def cell_rows(grid_dir: Path, snapshot: date) -> list[dict]:
    rows = []
    for area, (fname, size) in GRID_FILES.items():
        feats = json.loads((grid_dir / fname).read_text())["features"]
        cents = [(f["properties"]["cellId"], *_centroid(f["geometry"])) for f in feats]
        clat = sum(c[1] for c in cents) / len(cents)
        clon = sum(c[2] for c in cents) / len(cents)
        dist = {c[0]: math.hypot(c[1] - clat, (c[2] - clon) * math.cos(math.radians(clat))) for c in cents}
        ranked = sorted(dist, key=dist.get)
        pct = {sq: i / len(ranked) for i, sq in enumerate(ranked)}
        for sq, lat, lon in cents:
            p = pct[sq]
            site_class = "core" if p < 0.10 else "urban" if p < 0.35 else "suburban" if p < 0.70 else "rural"
            r = _h("band", area, sq) % 100
            band = "LTE1800" if r < 70 else "UMTS2100" if r < 90 else "LTE800+1800"
            manager = _h("mgr", area, sq) % 10**8
            if snapshot >= CELL_SNAPSHOTS[1]:
                if band != "LTE800+1800" and _h("upgrade", area, sq) % 100 < 4:
                    band = "LTE800+1800"
                if _h("mgr-change", area, sq) % 1000 < 5:
                    manager = _h("mgr2", area, sq) % 10**8
            rows.append({
                "cell_id": f"{area}-{sq:05d}", "area": area, "square_id": sq,
                "centroid_lat": round(lat, 6), "centroid_lon": round(lon, 6), "cell_size_m": size,
                "site_class": site_class, "radio_band": band,
                "site_manager_msisdn": f"+3933{manager:08d}",
            })
    return rows


def plan_rows(snapshot: date) -> list[dict]:
    rows = [
        {"country_code": 39, "zone": "DOMESTIC", "plan_name": "IT-national", "rate_sms": 0.01, "rate_call": 0.02, "rate_internet": 0.005},
        {"country_code": 0, "zone": "UNKNOWN", "plan_name": "unrated", "rate_sms": 0.0, "rate_call": 0.0, "rate_internet": 0.0},
    ]
    eu_internet = 0.20 if snapshot < PLAN_SNAPSHOTS[1] else 0.12
    for cc in sorted(set(EU)):
        rows.append({"country_code": cc, "zone": "EU", "plan_name": "EU-regulated", "rate_sms": 0.06,
                     "rate_call": 0.19, "rate_internet": eu_internet})
    for cc in sorted(set(ROW) - set(EU)):
        rows.append({"country_code": cc, "zone": "ROW", "plan_name": "world-standard", "rate_sms": 0.25,
                     "rate_call": 0.90, "rate_internet": 1.50})
    return rows


def _write(path: Path, rows) -> Path:
    # deterministic output, and landing hardlinks + chmods it read-only, so write once
    if path.exists():
        return path
    rows = rows()
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)
    return path


def generate(grid_dir: Path, out_dir: Path) -> list[tuple[str, date, Path]]:
    """Write snapshot CSVs. Returns (dataset, snapshot_date, path) in delivery order."""
    out = []
    for d in CELL_SNAPSHOTS:
        out.append(("cell_site", d, _write(out_dir / f"cell_site_{d.isoformat()}.csv", lambda d=d: cell_rows(grid_dir, d))))
    for d in PLAN_SNAPSHOTS:
        out.append(("tariff_plan", d, _write(out_dir / f"tariff_plan_{d.isoformat()}.csv", lambda d=d: plan_rows(d))))
    return sorted(out, key=lambda x: (x[1], x[0]))
