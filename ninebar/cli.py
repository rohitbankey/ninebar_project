"""Command line entry point: python -m ninebar <command> ...  (see README for the tour)."""
from __future__ import annotations

import argparse
import json
import shutil
import sys
from datetime import date
from pathlib import Path

from . import config, contracts, download, landing, lineage, recovery, reference, scenarios
from .orchestrator import Pipeline


def _d(s: str) -> date:
    return date.fromisoformat(s)


def raw_file(cfg, area: str, d: date) -> Path:
    return cfg.path("raw_cache") / area / f"sms-call-internet-{area.lower()}-{d.isoformat()}.txt"


def land_reference(p: Pipeline, d: date) -> None:
    cfg = p.cfg
    ref_dir = cfg.path("raw_cache") / "reference"
    for ds, snap, path in reference.generate(cfg.path("raw_cache") / "grid", ref_dir):
        if snap == d:
            aid, st = landing.land(p.con, cfg.path("landing"), path, ds, None, snap, "generated")
            print(f"   landed {ds} snapshot {snap} -> arrival {aid} {st}")


def land_day(p: Pipeline, d: date, skip: set[str] = frozenset()) -> None:
    cfg = p.cfg
    land_reference(p, d)
    for area in cfg.areas:
        if area in skip:
            continue
        f = raw_file(cfg, area, d)
        if not f.exists():
            print(f"   {f.name} not in raw cache - run `ninebar download` first")
            continue
        aid, st = landing.land(p.con, cfg.path("landing"), f, "cdr_activity", area, d, "dataverse")
        print(f"   landed {area} {d} -> arrival {aid} {st}")


def main(argv=None):
    ap = argparse.ArgumentParser(prog="ninebar")
    sub = ap.add_subparsers(dest="cmd", required=True)

    s = sub.add_parser("download", help="fetch daily files + grids from Harvard Dataverse into data/raw_cache")
    s.add_argument("--from", dest="start", type=_d)
    s.add_argument("--to", dest="end", type=_d)
    s.add_argument("--workers", type=int, default=4)

    sub.add_parser("reset", help="delete warehouse, landing zone, lineage events and outputs (keeps raw_cache)")

    s = sub.add_parser("land", help="land the source files for one day (and any reference snapshot dated that day)")
    s.add_argument("--date", type=_d, required=True)
    s.add_argument("--skip", nargs="*", default=[])
    s = sub.add_parser("land-file", help="land an arbitrary file as an arrival (e.g. a re-delivery)")
    s.add_argument("path", type=Path)
    s.add_argument("--area", required=True)
    s.add_argument("--date", type=_d, required=True)
    s.add_argument("--origin", default="manual")

    s = sub.add_parser("tick", help="run whatever is due on the morning of DATE")
    s.add_argument("--date", type=_d, required=True)
    s = sub.add_parser("run", help="run one cadence explicitly")
    s.add_argument("cadence", choices=["daily", "weekly", "fortnightly"])
    s.add_argument("--date", type=_d, required=True, help="day / week start / cycle start")

    s = sub.add_parser("simulate", help="land + tick day by day across a window")
    s.add_argument("--from", dest="start", type=_d)
    s.add_argument("--to", dest="end", type=_d)
    s.add_argument("--no-scenarios", action="store_true")

    s = sub.add_parser("replay", help="reprocess one (area, day) from landing without duplicating")
    s.add_argument("--area", required=True)
    s.add_argument("--date", type=_d, required=True)
    s.add_argument("--full", action="store_true", help="rewrite the partition instead of hash-merging")
    s = sub.add_parser("backfill", help="replay a date range and rebuild what changed")
    s.add_argument("--from", dest="start", type=_d, required=True)
    s.add_argument("--to", dest="end", type=_d, required=True)
    s.add_argument("--full", action="store_true")

    s = sub.add_parser("runs", help="run history")
    s.add_argument("--limit", type=int, default=30)
    s.add_argument("--tasks", type=int, help="show tasks of this run id")
    sub.add_parser("status", help="manifest, partitions, quarantine, late events at a glance")

    s = sub.add_parser("contract", help="contract registry")
    cs = s.add_subparsers(dest="sub", required=True)
    x = cs.add_parser("show")
    x.add_argument("dataset", nargs="?")
    x = cs.add_parser("migrate")
    x.add_argument("file", type=Path)
    x = cs.add_parser("check", help="dry-run drift detection on a file")
    x.add_argument("file", type=Path)
    x.add_argument("--dataset", default="cdr_activity")

    s = sub.add_parser("lineage", help="query the runtime lineage graph")
    ls = s.add_subparsers(dest="sub", required=True)
    for n in ("downstream", "upstream"):
        x = ls.add_parser(n)
        x.add_argument("target", help="dataset.column, e.g. landing.cdr_activity.internet")
        x.add_argument("--values-only", action="store_true", help="follow projection edges only")
    x = ls.add_parser("blast-radius")
    x.add_argument("target")
    x.add_argument("--change", default="rename or type change")
    ls.add_parser("export")
    x = ls.add_parser("sql")
    x.add_argument("query")

    s = sub.add_parser("trace", help="walk a gold metric back to the landing lines that produced it")
    s.add_argument("--model", required=True)
    s.add_argument("--key", required=True, help="e.g. cell_id=MI-05060,local_date=2013-11-07")
    s.add_argument("--metric", required=True)
    s.add_argument("--top", type=int, default=5)

    sub.add_parser("reconcile", help="row-count reconciliation landing -> silver -> gold")
    sub.add_parser("report", help="write representative outputs to outputs/")
    s = sub.add_parser("demo", help="everything: download window, reset, simulate with scenarios, report")
    s.add_argument("--skip-download", action="store_true")

    a = ap.parse_args(argv)
    cfg = config.load()

    if a.cmd == "download":
        start, end = cfg.window
        download.download(cfg.path("raw_cache"), a.start or start, a.end or end, tuple(cfg.areas), a.workers)
        return
    if a.cmd == "reset":
        for k in ("landing", "lineage_events"):
            _rmtree(cfg.path(k))
        wh = cfg.path("warehouse")
        for f in (wh, wh.with_suffix(".duckdb.wal")):
            f.unlink(missing_ok=True)
        rep = cfg.path("reports")
        if rep.exists():
            for f in rep.glob("*"):
                if f.is_file():
                    f.unlink()
        print("reset done (raw_cache kept)")
        return
    if a.cmd == "contract" and a.sub == "check":
        p = Pipeline(cfg)
        obs = contracts.observe(a.file, "\t" if a.dataset == "cdr_activity" else ",")
        r = contracts.resolve(p.con, a.dataset, obs)
        print(json.dumps({"kind": r.kind, "version": r.version.version if r.version else None,
                          "new_columns": r.new_columns, "problems": r.problems, "header": obs.header}, indent=2))
        return
    if a.cmd == "demo":
        return scenarios.demo(cfg, skip_download=a.skip_download)

    p = Pipeline(cfg)
    try:
        if a.cmd == "land":
            land_day(p, a.date, set(a.skip))
        elif a.cmd == "land-file":
            aid, st = landing.land(p.con, cfg.path("landing"), a.path, "cdr_activity", a.area, a.date, a.origin)
            print(f"landed {a.path} -> arrival {aid} {st}")
        elif a.cmd == "tick":
            p.tick(a.date)
        elif a.cmd == "run":
            {"daily": p.run_daily, "weekly": p.run_weekly, "fortnightly": p.run_fortnightly}[a.cadence](a.date)
        elif a.cmd == "simulate":
            start, end = cfg.window
            p.close()
            scenarios.simulate(cfg, a.start or start, a.end or end, with_scenarios=not a.no_scenarios)
            return
        elif a.cmd == "replay":
            p.replay(a.area, a.date, a.full)
        elif a.cmd == "backfill":
            p.backfill(a.start, a.end, a.full)
        elif a.cmd == "runs":
            recovery.print_runs(p.con, a.limit, a.tasks)
        elif a.cmd == "status":
            recovery.print_status(p.con)
        elif a.cmd == "contract" and a.sub == "show":
            for ds in ([a.dataset] if a.dataset else ["cdr_activity", "cell_site", "tariff_plan"]):
                for row in p.con.execute("SELECT version, source, registered_at, spec FROM ops.contract_registry "
                                         "WHERE dataset = ? ORDER BY registered_at", [ds]).fetchall():
                    spec = json.loads(row[3])
                    cols = ", ".join(f"{c['name']}:{c['type']}" for c in spec["columns"])
                    ren = f"  rename={spec['rename']}" if spec.get("rename") else ""
                    print(f"{ds} v{row[0]:<5} {row[1]:<40} {cols}{ren}")
        elif a.cmd == "contract" and a.sub == "migrate":
            mid, ver, n = contracts.apply_migration(p.con, a.file)
            print(f"applied {mid}: registered v{ver}, released {n} held arrival(s). They are picked up by the next run.")
        elif a.cmd == "lineage":
            if a.sub in ("downstream", "upstream", "blast-radius"):
                ds, col = a.target.rsplit(".", 1)
            if a.sub == "downstream":
                for r in lineage.downstream(p.con, ds, col, a.values_only):
                    print(f"  {r[2]} hop(s)  {r[0]}.{r[1]}")
            elif a.sub == "upstream":
                for r in lineage.upstream(p.con, ds, col):
                    print(f"  {r[2]} hop(s)  {r[0]}.{r[1]}")
            elif a.sub == "blast-radius":
                print(lineage.blast_radius(p.con, ds, col, a.change))
            elif a.sub == "export":
                lineage.export_graph(p.con, cfg.path("reports"))
                print(f"wrote {cfg.path('reports')}/lineage_current_edges.json and lineage_graph.mmd")
            elif a.sub == "sql":
                print(p.con.sql(a.query))
        elif a.cmd == "trace":
            key = dict(kv.split("=", 1) for kv in a.key.split(","))
            print(recovery.trace(p.con, a.model, key, a.metric, a.top))
        elif a.cmd == "reconcile":
            print(recovery.reconcile(p.con))
        elif a.cmd == "report":
            recovery.write_report(p.con, cfg)
    finally:
        p.close()


def _rmtree(path: Path):
    if not path.exists():
        return
    for f in path.rglob("*"):
        if f.is_file():
            f.chmod(0o644)
    shutil.rmtree(path)


if __name__ == "__main__":
    sys.exit(main())
