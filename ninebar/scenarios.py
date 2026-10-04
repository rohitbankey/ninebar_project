"""Day-by-day simulation of the window, with the failure scenarios injected.

Each scenario produces a *new arrival* derived from a real file and is recorded
in the manifest as origin 'scenario:<name>', so nothing synthetic can be confused
with what came from Dataverse. Timeline for the default window (Nov 1-28):

  Nov 01  reference snapshots (cell_site, tariff_plan) land before the first CDR file
  Nov 03  TN file arrives with ~0.1% garbage lines (bad types, negative value, unknown cell,
          wrong field count, duplicate keys)                    -> quarantine, run still green
  Nov 04  MI file arrives with ~2% garbage                      -> quality gate fails the arrival;
          the real file is re-sent the next day and goes through
  Nov 05  TN file held back by the "provider"                   -> Nov 5 published PARTIAL
  Nov 08  daily run is hard-killed in the middle of the silver merge (subprocess, os._exit)
          and re-run                                            -> no duplicates, counts match
  Nov 09  first attempt of a gold build hits a transient error  -> retried, run green
  Nov 12  TN Nov 5 finally lands (after week Nov 4-10 was published) -> late_file, restatements
  Nov 13  MI Nov 7 is re-delivered with 3 corrected rows and 1 bad spike -> hash diff touches 4 rows;
          later traced back from the gold mart to the exact landing line
  Nov 19  MI file arrives with a header and an extra column      -> additive, contract v1.1, flows to silver
  Nov 21  MI file arrives with `internet` renamed               -> breaking, held, blast radius written
  Nov 22  operator applies migration 0001                       -> contract v2.0, held file released and loaded
"""
from __future__ import annotations

import os
import random
import subprocess
import sys
from datetime import date, timedelta
from pathlib import Path

from . import calendar as cal
from . import landing, recovery
from .config import ROOT, Config
from .orchestrator import Pipeline

SCEN = "scenario_files"


def _src(cfg: Config, area: str, d: date) -> Path:
    return cfg.path("raw_cache") / area / f"sms-call-internet-{area.lower()}-{d.isoformat()}.txt"


def _out(cfg: Config, name: str) -> Path:
    p = cfg.path("raw_cache") / SCEN / name
    p.parent.mkdir(parents=True, exist_ok=True)
    return p


def make_garbage(cfg, area: str, d: date, share: float, name: str) -> Path:
    """Copy a real file and corrupt `share` of its lines in a few different ways."""
    out = _out(cfg, name)
    if out.exists():
        return out
    rnd = random.Random(f"{area}{d}{share}")
    with open(_src(cfg, area, d)) as f:
        lines = f.read().splitlines()
    n = int(len(lines) * share)
    picks = rnd.sample(range(len(lines)), n)
    for i, idx in enumerate(picks):
        parts = lines[idx].split("\t")
        kind = i % 6
        if kind == 0:
            parts[0] = "x" + parts[0]                      # bad_type:square_id
        elif kind == 1:
            parts[7] = "-" + (parts[7] or "1.0")            # out_of_range:internet
        elif kind == 2:
            parts[0] = "99999"                              # out_of_range + unknown_cell
        elif kind == 3:
            parts = parts[:5]                               # field_count
        elif kind == 4:
            parts[1] = str(int(parts[1]) + 123)             # slot_not_aligned
        else:
            parts[2] = ""                                   # null_required:country_code
        lines[idx] = "\t".join(parts)
    # plus a handful of duplicates: exact copies and same-key-different-value
    for idx in rnd.sample(range(len(lines)), 20):
        lines.append(lines[idx])
    for idx in rnd.sample(range(len(lines)), 5):
        p = lines[idx].split("\t")
        if len(p) == 8 and p[0].isdigit():
            p[3] = "0.123456"
            lines.append("\t".join(p))
    out.write_text("\n".join(lines) + "\n")
    return out


def make_correction(cfg, area: str, d: date, name: str) -> tuple[Path, str]:
    """Re-delivery of a real file with 3 rows corrected and 1 row given an implausible spike."""
    out = _out(cfg, name)
    with open(_src(cfg, area, d)) as f:
        lines = f.read().splitlines()
    # pick rows with internet traffic in a busy central cell so the spike is visible in the mart
    target_cell = "5060" if area == "MI" else "5200"
    cand = [i for i, ln in enumerate(lines) if ln.startswith(target_cell + "\t") and ln.split("\t")[7]]
    rnd = random.Random(f"corr{area}{d}")
    fix = rnd.sample(cand, 4)
    for k, idx in enumerate(fix):
        p = lines[idx].split("\t")
        p[7] = "4242.0" if k == 3 else f"{float(p[7]) * 1.05:.6f}"
        lines[idx] = "\t".join(p)
    if not out.exists():
        out.write_text("\n".join(lines) + "\n")
    return out, f"{'MI' if area == 'MI' else 'TN'}-{int(target_cell):05d}"


def make_header_file(cfg, area: str, d: date, name: str, header: list[str], extra=None) -> Path:
    out = _out(cfg, name)
    if out.exists():
        return out
    rnd = random.Random(name)
    with open(_src(cfg, area, d)) as src, open(out, "w") as dst:
        dst.write("\t".join(header) + "\n")
        for ln in src:
            ln = ln.rstrip("\n")
            if extra:
                ln += "\t" + (str(rnd.randint(0, 40)) if rnd.random() < 0.6 else "")
            dst.write(ln + "\n")
    return out


BASE_HEADER = ["square_id", "time_interval", "country_code", "sms_in", "sms_out", "call_in", "call_out", "internet"]


def land_ref(p: Pipeline, d: date):
    from .cli import land_reference
    land_reference(p, d)


def simulate(cfg: Config, start: date, end: date, with_scenarios: bool = True) -> None:
    rep = cfg.path("reports")
    rep.mkdir(parents=True, exist_ok=True)
    notes: list[str] = []
    p = Pipeline(cfg)

    def land(path: Path, area: str, d: date, origin: str):
        aid, st = landing.land(p.con, cfg.path("landing"), path, "cdr_activity", area, d, origin)
        print(f"   landed {area} {d} ({origin}) -> arrival {aid} {st}")
        return aid

    S = with_scenarios
    D = lambda m, dd: date(2013, m, dd)  # noqa: E731
    in_window = lambda x: start <= x <= end  # noqa: E731

    for d in cal.daterange(start, end):
        print(f"\n=== {d} ({d.strftime('%a')}) ===")
        land_ref(p, d)
        for area in cfg.areas:
            if S and (area, d) == ("TN", D(11, 5)):
                print("   [scenario] TN 2013-11-05 not delivered on time")
                continue
            if S and (area, d) == ("TN", D(11, 3)):
                land(make_garbage(cfg, "TN", d, 0.001, "tn-2013-11-03-dirty.txt"), area, d, "scenario:dirty_rows")
                continue
            if S and (area, d) == ("MI", D(11, 4)):
                land(make_garbage(cfg, "MI", d, 0.02, "mi-2013-11-04-broken.txt"), area, d, "scenario:quality_gate")
                continue
            if S and (area, d) == ("MI", D(11, 19)):
                land(make_header_file(cfg, "MI", d, "mi-2013-11-19-v1_1.txt", BASE_HEADER + ["roaming_sessions"], extra=True),
                     area, d, "scenario:additive_column")
                continue
            if S and (area, d) == ("MI", D(11, 21)):
                hdr = BASE_HEADER[:-1] + ["internet_activity", "roaming_sessions"]
                land(make_header_file(cfg, "MI", d, "mi-2013-11-21-v2.txt", hdr, extra=True), area, d, "scenario:renamed_column")
                continue
            f = _src(cfg, area, d)
            if f.exists():
                land(f, area, d, "dataverse")
            else:
                print(f"   missing {f.name} in raw cache")

        # ---- things that happen between landing and the morning run
        if S and d == D(11, 5) and in_window(D(11, 4)):
            print("   [scenario] provider re-sends the real MI 2013-11-04 file after the quality gate rejected the broken one")
            land(_src(cfg, "MI", D(11, 4)), "MI", D(11, 4), "dataverse:resend")
        if S and d == D(11, 12):
            print("   [scenario] TN 2013-11-05 finally arrives, a week late")
            land(_src(cfg, "TN", D(11, 5)), "TN", D(11, 5), "dataverse:late")
        if S and d == D(11, 13):
            path, cell = make_correction(cfg, "MI", D(11, 7), "mi-2013-11-07-correction.txt")
            print("   [scenario] corrected re-delivery of MI 2013-11-07 (4 rows differ)")
            land(path, "MI", D(11, 7), "scenario:correction")
            notes.append(f"correction_cell={cell}")
        if S and d == D(11, 22):
            from . import contracts
            mig = ROOT / "contracts" / "migrations" / "0001_cdr_internet_renamed.yaml"
            mid, ver, n = contracts.apply_migration(p.con, mig)
            print(f"   [operator] applied {mid}: cdr_activity v{ver}, released {n} held arrival(s)")

        today = d + timedelta(days=1)
        if S and d == D(11, 8):
            p.close()
            print("   [scenario] daily run killed mid-merge (NINEBAR_CRASH_AT=silver_merge:TN)")
            env = dict(os.environ, NINEBAR_CRASH_AT="silver_merge:TN:2013-11-08")
            r = subprocess.run([sys.executable, "-m", "ninebar", "tick", "--date", today.isoformat()], env=env, cwd=ROOT)
            print(f"   crashed run exited with code {r.returncode}")
            p = Pipeline(cfg)
            before = p.con.execute("SELECT count(*) FROM silver.cdr_activity WHERE _file_date = ? AND area = 'TN'", [d]).fetchone()[0]
            st = p.con.execute("SELECT status FROM ops.landing_manifest WHERE area='TN' AND file_date=? ORDER BY arrival_id DESC", [d]).fetchone()[0]
            print(f"   after crash: TN {d} rows in silver = {before}, manifest status = {st} -> re-running")
        if S and d == D(11, 9):
            os.environ["NINEBAR_FAIL_ONCE"] = "build:gold.daily_cell_usage"
        p.tick(today)
        os.environ.pop("NINEBAR_FAIL_ONCE", None)
        if S and d == D(11, 8):
            dup = p.con.execute("""SELECT count(*) FROM (SELECT cell_id, slot_start_utc, country_code FROM silver.cdr_activity
                                   WHERE local_date = ? GROUP BY ALL HAVING count(*) > 1)""", [d]).fetchone()[0]
            n = p.con.execute("SELECT count(*) FROM silver.cdr_activity WHERE _file_date = ? AND area = 'TN'", [d]).fetchone()[0]
            lines = p.con.execute("SELECT row_count FROM ops.landing_manifest WHERE area='TN' AND file_date=? AND status='PROCESSED'", [d]).fetchone()[0]
            msg = f"crash recovery: TN {d} silver rows {n:,} vs file lines {lines:,}, duplicate keys {dup}"
            print("   " + msg)
            notes.append(msg)

    # close out: weekly for the last full week if its Monday falls just after the window
    p.con.execute("CHECKPOINT")
    (rep / "simulation_notes.txt").write_text("\n".join(notes) + "\n")
    p.close()


def demo(cfg: Config, skip_download: bool = False) -> None:
    from . import download
    from .cli import main as cli
    start, end = cfg.window
    if not skip_download:
        download.download(cfg.path("raw_cache"), start, end, tuple(cfg.areas))
    cli(["reset"])
    simulate(cfg, start, end, with_scenarios=True)
    p = Pipeline(cfg)
    try:
        rep = cfg.path("reports")
        recovery.write_report(p.con, cfg)
        _demo_followups(p, cfg, rep)
    finally:
        p.close()


def _demo_followups(p: Pipeline, cfg: Config, rep: Path) -> None:
    """The questions a reviewer would ask, answered from the warehouse and written to outputs/."""
    from . import lineage

    # 1. if a bronze column changes, what breaks?
    cols = [r[0] for r in p.con.execute("SELECT DISTINCT src_column FROM lineage.current_edges "
                                        "WHERE src_dataset = 'landing.cdr_activity' ORDER BY 1").fetchall()]
    (rep / "lineage_impact_by_source_column.md").write_text(lineage.impact_table(p.con, "landing.cdr_activity", cols) + "\n")
    parts = [lineage.blast_radius(p.con, "landing.cdr_activity", c, "rename / type change")
             for c in ("internet", "country_code", "time_interval")]
    parts.append(lineage.blast_radius(p.con, "silver.dim_tariff_plan", "rate_internet", "semantic change"))
    (rep / "blast_radius_examples.md").write_text("\n\n".join(parts) + "\n")

    # 2. replay one partition twice: second time must change nothing
    out = ["# Replay of a single partition", ""]
    for i in (1, 2):
        before = p.con.execute("SELECT count(*) FROM silver.cdr_activity").fetchone()[0]
        p.replay("TN", date(2013, 11, 10))
        after = p.con.execute("SELECT count(*) FROM silver.cdr_activity").fetchone()[0]
        last = p.con.execute("SELECT inserted, updated, deleted, unchanged FROM ops.ingest_stats ORDER BY recorded_at DESC LIMIT 1").fetchone()
        out.append(f"* replay #{i} of TN 2013-11-10: silver rows {before:,} -> {after:,}; inserted/updated/deleted/unchanged = {last}")
    p.con.execute("DELETE FROM silver.cdr_activity WHERE area='TN' AND _file_date = DATE '2013-11-10' AND cell_id LIKE 'TN-05%'")
    damaged = p.con.execute("SELECT count(*) FROM silver.cdr_activity WHERE area='TN' AND _file_date = DATE '2013-11-10'").fetchone()[0]
    p.replay("TN", date(2013, 11, 10))
    healed = p.con.execute("SELECT count(*) FROM silver.cdr_activity WHERE area='TN' AND _file_date = DATE '2013-11-10'").fetchone()[0]
    last = p.con.execute("SELECT inserted, updated, deleted, unchanged FROM ops.ingest_stats ORDER BY recorded_at DESC LIMIT 1").fetchone()
    out.append(f"* deleted the TN-05xxx rows of TN 2013-11-10 by hand ({damaged:,} rows left), replayed: back to {healed:,}; "
               f"inserted/updated/deleted/unchanged = {last}")
    (rep / "replay_check.md").write_text("\n".join(out) + "\n")

    # 3. backfill a range twice
    out = ["# Backfill 2013-11-14 .. 2013-11-16, twice", ""]
    for i in (1, 2):
        before = p.con.execute("SELECT count(*) FROM silver.cdr_activity").fetchone()[0]
        p.backfill(date(2013, 11, 14), date(2013, 11, 16))
        after = p.con.execute("SELECT count(*) FROM silver.cdr_activity").fetchone()[0]
        out.append(f"* backfill #{i}: silver rows {before:,} -> {after:,}")
    (rep / "backfill_check.md").write_text("\n".join(out) + "\n")

    # 4. trace the spike from the correction scenario
    notes = (rep / "simulation_notes.txt").read_text() if (rep / "simulation_notes.txt").exists() else ""
    cell = next((ln.split("=")[1] for ln in notes.splitlines() if ln.startswith("correction_cell=")), "MI-05060")
    t = recovery.trace(p.con, "gold.daily_cell_usage", {"cell_id": cell, "local_date": "2013-11-07"}, "internet_total")
    (rep / "trace_wrong_metric.md").write_text(t + "\n")

    # 5. final reconciliation and run history
    recovery.write_report(p.con, cfg)
    print(f"\nDone. Representative outputs are in {rep}")
