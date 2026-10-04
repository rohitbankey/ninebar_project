"""End to end on the miniature world."""
from datetime import date, timedelta


from ninebar import landing, recovery

from .conftest import cdr_lines, land_and_tick

D1, D2, D3, D4 = (date(2013, 11, 1) + timedelta(days=i) for i in range(4))


def silver_count(p, **where):
    sql = "SELECT count(*) FROM silver.cdr_activity" + (" WHERE " + " AND ".join(f"{k} = ?" for k in where) if where else "")
    return p.con.execute(sql, list(where.values())).fetchone()[0]


def dupes(p):
    return p.con.execute("""SELECT count(*) FROM (SELECT cell_id, slot_start_utc, country_code FROM silver.cdr_activity
                            GROUP BY ALL HAVING count(*) > 1)""").fetchone()[0]


def test_daily_run_loads_and_publishes(pipe):
    land_and_tick(pipe, D1)
    assert silver_count(pipe) == 2 * 3 * 3 * 2   # areas * cells * slots * country codes
    st = pipe.con.execute("SELECT status, version FROM ops.partition_state WHERE model='gold.daily_cell_usage'").fetchall()
    assert st == [("PUBLISHED", 1)]
    # cell ids carry the area so Milano 1 and Trentino 1 could never collide
    assert pipe.con.execute("SELECT min(cell_id) FROM silver.cdr_activity WHERE area='MI'").fetchone()[0] == "MI-00001"


def test_same_file_twice_is_a_duplicate_arrival(pipe, world):
    land_and_tick(pipe, D1)
    f = world / "raw_cache" / "MI" / f"sms-call-internet-mi-{D1}.txt"
    aid, st = landing.land(pipe.con, pipe.cfg.path("landing"), f, "cdr_activity", "MI", D1, "test")
    assert st == "DUPLICATE"


def test_replay_is_idempotent_and_heals(pipe):
    land_and_tick(pipe, D1)
    n = silver_count(pipe)
    pipe.replay("MI", D1)
    assert silver_count(pipe) == n and dupes(pipe) == 0
    pipe.con.execute("DELETE FROM silver.cdr_activity WHERE cell_id = 'MI-00002'")
    pipe.replay("MI", D1)
    assert silver_count(pipe) == n and dupes(pipe) == 0


def test_correction_touches_only_changed_rows_and_restates(pipe, world):
    land_and_tick(pipe, D1)
    lines = cdr_lines(D1, [1, 2, 3])
    lines[0] = lines[0].replace("\t10.01", "\t99.0")
    f = world / "corr.txt"
    f.write_text("\n".join(lines) + "\n")
    landing.land(pipe.con, pipe.cfg.path("landing"), f, "cdr_activity", "MI", D1, "scenario:correction")
    land_and_tick(pipe, D2)
    s = pipe.con.execute("SELECT inserted, updated, deleted FROM ops.ingest_stats s JOIN ops.landing_manifest m USING (arrival_id) "
                         "WHERE m.origin = 'scenario:correction'").fetchone()
    assert s == (0, 1, 0)
    v = pipe.con.execute("SELECT version FROM ops.partition_state WHERE model='gold.daily_cell_usage' AND partition_key=?", [D1]).fetchone()[0]
    assert v == 2
    assert pipe.con.execute("SELECT count(*) FROM ops.late_events WHERE kind='correction'").fetchone()[0] == 1


def test_late_file_publishes_partial_then_restates(pipe, world):
    land_and_tick(pipe, D1, skip=["TN"])
    assert pipe.con.execute("SELECT status FROM ops.partition_state WHERE model='gold.daily_cell_usage'").fetchone()[0] == "PUBLISHED_PARTIAL"
    f = world / "raw_cache" / "TN" / f"sms-call-internet-tn-{D1}.txt"
    landing.land(pipe.con, pipe.cfg.path("landing"), f, "cdr_activity", "TN", D1, "late")
    land_and_tick(pipe, D2)
    row = pipe.con.execute("SELECT status, version FROM ops.partition_state WHERE model='gold.daily_cell_usage' AND partition_key=?", [D1]).fetchone()
    assert row == ("PUBLISHED", 2)
    assert pipe.con.execute("SELECT count(*) FROM ops.late_events WHERE kind='late_file'").fetchone()[0] >= 1


def test_bad_rows_are_quarantined_with_reasons(pipe, world):
    lines = cdr_lines(D1, [1, 2, 3])
    lines += ["x1\t1383260400000\t39\t1\t\t\t\t1", "999\t1383260400000\t39\t1\t\t\t\t1", "1\t2"]
    lines += [lines[0]]  # exact duplicate -> collapsed, counted
    f = world / "dirty.txt"
    f.write_text("\n".join(lines) + "\n")
    from ninebar.cli import land_reference
    land_reference(pipe, D1)
    landing.land(pipe.con, pipe.cfg.path("landing"), f, "cdr_activity", "MI", D1, "test")
    # contract threshold is 1%; 3 bad of 22 trips it -> nothing reaches silver for this arrival
    pipe.tick(D2)
    st = pipe.con.execute("SELECT status FROM ops.landing_manifest WHERE origin='test'").fetchone()[0]
    assert st == "FAILED_QUALITY"
    reasons = {r[0] for r in pipe.con.execute("SELECT unnest(reasons) FROM quarantine.records").fetchall()}
    assert {"bad_type:square_id", "unknown_cell"} <= reasons and any(r.startswith("field_count") for r in reasons)
    assert silver_count(pipe, area="MI") == 0


def test_transient_failure_is_retried(pipe, monkeypatch):
    monkeypatch.setenv("NINEBAR_FAIL_ONCE", "build:gold.daily_cell_usage")
    land_and_tick(pipe, D1)
    rows = pipe.con.execute("SELECT status FROM ops.tasks WHERE task LIKE 'build:gold.daily_cell_usage%' ORDER BY attempt").fetchall()
    assert [r[0] for r in rows] == ["RETRYING", "SUCCESS"]
    assert pipe.con.execute("SELECT status FROM ops.runs").fetchone()[0] == "SUCCESS"


def test_mid_merge_crash_leaves_nothing_behind(world):
    import os
    import subprocess
    import sys

    from ninebar import config
    from ninebar.orchestrator import Pipeline
    from .conftest import ROOT
    p = Pipeline(config.load())
    from ninebar.cli import land_day
    land_day(p, D1)
    p.close()
    env = dict(os.environ, NINEBAR_CRASH_AT="silver_merge:TN")
    r = subprocess.run([sys.executable, "-m", "ninebar", "tick", "--date", str(D2)], env=env, cwd=ROOT, capture_output=True)
    assert r.returncode == 97
    p = Pipeline(config.load())
    assert silver_count(p, area="TN") == 0
    assert p.con.execute("SELECT status FROM ops.landing_manifest WHERE area='TN'").fetchone()[0] == "PENDING"
    p.tick(D2)
    assert silver_count(p, area="TN") == 18 and dupes(p) == 0
    assert p.con.execute("SELECT count(*) FROM ops.runs WHERE status='CRASHED'").fetchone()[0] == 1
    p.close()


def test_cadences_and_reconciliation(pipe):
    for d in (D1, D2, D3, D4):
        land_and_tick(pipe, d)
    pipe.run_weekly(date(2013, 10, 28))
    pipe.run_fortnightly(date(2013, 11, 1))
    rec = pipe.con.execute("SELECT area, status, days_in_silver, days_in_weekly_controls FROM gold.fortnight_reconciliation ORDER BY area").fetchall()
    # the week of Nov 4 closes (Sunday Nov 10) before the cycle does (Thursday Nov 14), so it is covered
    assert rec == [("MI", "MATCH", 4, 4), ("TN", "MATCH", 4, 4)]
    assert "**0**" in recovery.reconcile(pipe.con).split("###")[0].split("\n")[2]


def test_lineage_is_emitted_and_answers_the_blast_radius_question(pipe):
    for d in (D1, D2, D3, D4):
        land_and_tick(pipe, d)
    pipe.run_weekly(date(2013, 10, 28))
    pipe.run_fortnightly(date(2013, 11, 1))
    from ninebar import lineage
    hit = {ds for ds, _, _ in lineage.downstream(pipe.con, "landing.cdr_activity", "internet")}
    assert {"silver.cdr_activity", "silver.usage_cell_day", "gold.daily_cell_usage", "gold.weekly_cell_rollup",
            "gold.fortnightly_settlement", "gold.fortnight_reconciliation"} <= hit
    up = {(ds, c) for ds, c, _ in lineage.upstream(pipe.con, "gold.daily_cell_usage", "internet_per_km2")}
    assert ("landing.cdr_activity", "internet") in up and ("silver.dim_cell_site", "cell_size_m") in up
    # the events are on disk too, one JSON per line
    files = list(pipe.cfg.path("lineage_events").glob("*.jsonl"))
    assert files and all(f.read_text().strip() for f in files)
