"""Runs, tasks, retries, and the three cadences.

There is no scheduler daemon. `ninebar tick --date T` answers "it is the morning
of T, what is due?" and runs it:

    daily        always, for T-1
    weekly       if T is a Monday, for the ISO week T-7 .. T-1
    fortnightly  if T is a cycle start, for the cycle that just closed

In production that one command goes in cron at 06:00 Europe/Rome. `simulate`
calls it once per day across the window, which is how the demo walks through
two months of history in one go.

Every invocation is a row in ops.runs and every step a row in ops.tasks
(one row per attempt). A run left in RUNNING is a crash; the next run marks it
CRASHED so the history reads honestly.
"""
from __future__ import annotations

import json
import time
import traceback
from datetime import date, datetime, timedelta, timezone

import duckdb

from . import calendar as cal
from . import contracts, db, ingest
from . import models as mdl
from .config import ROOT, Config
from .faults import TransientError
from .lineage import Emitter

RETRYABLE = (TransientError, duckdb.IOException)


class Pipeline:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.con = db.connect(cfg.path("warehouse"))
        contracts.seed(self.con, ROOT / "contracts")
        self.models = mdl.load_models(ROOT / "models")
        mdl.sync_registry(self.con, self.models)
        self.areas = list(cfg.areas)
        self.anchor = cfg.fortnight_anchor
        self.cycle_len = cfg.fortnight_days

    def close(self):
        self.con.close()

    # ------------------------------------------------------------ run bookkeeping

    def start_run(self, command: str, cadence: str | None = None, logical_date: date | None = None,
                  params: dict | None = None) -> tuple[int, Emitter]:
        crashed = self.con.execute("UPDATE ops.runs SET status = 'CRASHED', error = 'process exited without closing the run' "
                                   "WHERE status = 'RUNNING' RETURNING run_id").fetchall()
        for (rid,) in crashed:
            print(f"   note: run {rid} never finished -> marked CRASHED")
        # max()+1 rather than a sequence: after a hard kill DuckDB can hand out a sequence value
        # again (it is not logged until something commits), which would merge two runs' history
        run_id = self.con.execute("SELECT coalesce(max(run_id), 0) + 1 FROM ops.runs").fetchone()[0]
        self.con.execute("INSERT INTO ops.runs VALUES (?,?,?,?,?, now(), NULL, 'RUNNING', NULL)",
                         [run_id, command, cadence, logical_date, json.dumps(params or {}, default=str)])
        return run_id, Emitter(self.con, self.cfg.path("lineage_events"), run_id)

    def end_run(self, run_id: int, status: str, error: str | None = None):
        self.con.execute("UPDATE ops.runs SET ended_at = now(), status = ?, error = ? WHERE run_id = ?", [status, error, run_id])

    def task(self, run_id: int, name: str, fn, *args, **kw):
        max_attempts = self.cfg.raw["retries"]["max_attempts"]
        backoff = self.cfg.raw["retries"]["backoff_seconds"]
        for attempt in range(1, max_attempts + 1):
            started = datetime.now(timezone.utc)
            try:
                out = fn(*args, **kw)
                self._task_row(run_id, name, attempt, "SUCCESS", started, out)
                return out
            except RETRYABLE as e:
                last = attempt == max_attempts
                self._task_row(run_id, name, attempt, "FAILED" if last else "RETRYING", started, None, repr(e))
                if last:
                    raise
                print(f"   {name}: {e} -> retry {attempt + 1}/{max_attempts}")
                time.sleep(backoff * 2 ** (attempt - 1))
            except Exception as e:
                self._task_row(run_id, name, attempt, "FAILED", started, None, f"{type(e).__name__}: {e}")
                raise

    def _task_row(self, run_id, name, attempt, status, started, detail, error=None):
        self.con.execute("INSERT INTO ops.tasks VALUES (?,?,?,?,?, now(), ?, ?)",
                         [run_id, name, attempt, status, started, json.dumps(detail, default=str) if detail else None, error])

    # ------------------------------------------------------------ steps

    def pending(self) -> list[dict]:
        cols = ["arrival_id", "dataset", "area", "file_date", "path", "sha256"]
        rows = self.con.execute(f"""
            SELECT {', '.join(cols)} FROM ops.landing_manifest WHERE status = 'PENDING'
            ORDER BY CASE WHEN dataset = 'cdr_activity' THEN 1 ELSE 0 END, file_date, area, arrival_id
        """).fetchall()
        return [dict(zip(cols, r)) for r in rows]

    def ingest_pending(self, run_id: int, emitter: Emitter) -> list[str]:
        problems = []
        for a in self.pending():
            fn = ingest.ingest_cdr if a["dataset"] == "cdr_activity" else ingest.ingest_reference
            name = f"ingest:{a['dataset']}:{a['area'] or '-'}:{a['file_date']}#{a['arrival_id']}"
            try:
                r = self.task(run_id, name, fn, self.con, self.cfg, self.models, a, run_id, emitter)
                if r.get("status") == "PROCESSED" and a["dataset"] == "cdr_activity":
                    print(f"   ingested {a['area']} {a['file_date']} #{a['arrival_id']}: read {r['rows_read']:,} "
                          f"quarantined {r['quarantined']:,} dupes {r['exact_dupes']:,} | "
                          f"+{r['inserted']:,} ~{r['updated']:,} -{r['deleted']:,} ={r['unchanged']:,}")
                elif r.get("status") == "PROCESSED":
                    print(f"   ingested {a['dataset']} snapshot {a['file_date']}: new {r['new']}, changed {r['changed']}, closed {r['closed']}")
                elif r.get("status") != "PROCESSED":
                    problems.append(f"{name}: {r['status']}")
            except ingest.QualityGateFailed as e:
                print(f"   QUALITY GATE: {e}")
                problems.append(str(e))
        return problems

    def build(self, run_id: int, emitter: Emitter, cadence: str, up_to: date, ensure: list[date] = ()) -> list[dict]:
        out = []
        for m in mdl.topo(self.models):
            if m.cadence != cadence:
                continue
            keys = set(mdl.dirty_keys(self.con, m.name, up_to))
            keys |= {k for k in ensure if not mdl.is_built(self.con, m.name, k)}
            for k in sorted(keys):
                if self._no_data(m, k):
                    continue
                r = self.task(run_id, f"build:{m.name}:{k}", mdl.build_partition, self.con, m, k, run_id, emitter,
                              self.cfg.tz, self.cycle_len, self.areas, self.models, self.anchor)
                out.append(r)
                tag = f" (restated v{r['version']})" if r["restated"] else ""
                if m.layer == "gold":
                    print(f"   built {m.name}[{k}] {r['rows']:,} rows {r['status']}{tag}")
        return out

    def _no_data(self, m: mdl.Model, k: date) -> bool:
        days = mdl.span(m.partition_by, k, self.cycle_len)
        has = self.con.execute("SELECT count(*) FROM ops.landing_manifest WHERE dataset='cdr_activity' AND status='PROCESSED' "
                               "AND file_date BETWEEN ? AND ?", [min(days), max(days)]).fetchone()[0]
        return has == 0

    # ------------------------------------------------------------ cadences

    def _run(self, command: str, cadence: str | None, logical: date | None, params: dict, body):
        run_id, em = self.start_run(command, cadence, logical, params)
        try:
            problems = body(run_id, em) or []
            self.end_run(run_id, "FAILED" if problems else "SUCCESS", "; ".join(problems) or None)
            return run_id, problems
        except Exception as e:
            self.end_run(run_id, "FAILED", f"{type(e).__name__}: {e}")
            traceback.print_exc()
            raise

    def run_daily(self, d: date):
        def body(run_id, em):
            problems = self.ingest_pending(run_id, em)
            self.build(run_id, em, "daily", up_to=d, ensure=[d])
            return problems
        print(f"-- daily run for {d} (Europe/Rome day {d} 00:00 -> {d + timedelta(days=1)} 00:00)")
        return self._run("run", "daily", d, {"day": d}, body)

    def run_weekly(self, ws: date):
        def body(run_id, em):
            problems = self.ingest_pending(run_id, em)
            self.build(run_id, em, "daily", up_to=ws + timedelta(days=6))
            self.build(run_id, em, "weekly", up_to=ws, ensure=[ws])
            return problems
        print(f"-- weekly run for ISO week {ws} .. {ws + timedelta(days=6)}")
        return self._run("run", "weekly", ws, {"week_start": ws}, body)

    def run_fortnightly(self, cs: date):
        ce = cs + timedelta(days=self.cycle_len - 1)

        def body(run_id, em):
            problems = self.ingest_pending(run_id, em)
            self.build(run_id, em, "daily", up_to=ce)
            # only weeks that have fully closed by the end of the cycle; the open week waits for its Monday
            self.build(run_id, em, "weekly", up_to=ce - timedelta(days=6))
            self.build(run_id, em, "fortnightly", up_to=cs, ensure=[cs])
            rec = self.con.execute("SELECT area, status, daily_mismatch_days, weekly_mismatch_days, days_without_closed_week "
                                   "FROM gold.fortnight_reconciliation WHERE cycle_start = ?", [cs]).fetchall()
            for area, st, dmm, wmm, pend in rec:
                print(f"   reconciliation {cs} {area}: {st}" + (f" (weekly pending for {len(pend)} days)" if pend else ""))
                if st == "MISMATCH":
                    problems.append(f"reconciliation mismatch {cs} {area}: daily {dmm} weekly {wmm}")
            return problems
        print(f"-- fortnightly run for cycle {cs} .. {ce}")
        return self._run("run", "fortnightly", cs, {"cycle_start": cs}, body)

    def due(self, today: date) -> list[tuple[str, date]]:
        jobs = [("daily", today - timedelta(days=1))]
        if today.weekday() == 0:
            jobs.append(("weekly", today - timedelta(days=7)))
        if today > self.anchor and cal.cycle_start(today, self.anchor, self.cycle_len) == today:
            jobs.append(("fortnightly", today - timedelta(days=self.cycle_len)))
        return jobs

    def tick(self, today: date):
        results = []
        for cadence, key in self.due(today):
            fn = {"daily": self.run_daily, "weekly": self.run_weekly, "fortnightly": self.run_fortnightly}[cadence]
            results.append((cadence, key, fn(key)))
        return results

    # ------------------------------------------------------------ recovery

    def replay(self, area: str, d: date, full: bool = False):
        """Re-run the latest arrival for (area, file_date) from landing. Idempotent by construction."""
        a = self.con.execute("""
            SELECT arrival_id, dataset, area, file_date, path, sha256 FROM ops.landing_manifest
            WHERE dataset = 'cdr_activity' AND area = ? AND file_date = ? AND status IN ('PROCESSED', 'HELD_LATE', 'FAILED_QUALITY')
            ORDER BY arrival_id DESC LIMIT 1""", [area, d]).fetchone()
        if not a:
            raise SystemExit(f"nothing landed for {area} {d}")
        arrival = dict(zip(["arrival_id", "dataset", "area", "file_date", "path", "sha256"], a))
        arrival.update(force=True, full=full)

        def body(run_id, em):
            r = self.task(run_id, f"replay:{area}:{d}#{arrival['arrival_id']}", ingest.ingest_cdr,
                          self.con, self.cfg, self.models, arrival, run_id, em)
            print(f"   replay {area} {d} from arrival {arrival['arrival_id']}: +{r['inserted']:,} ~{r['updated']:,} "
                  f"-{r['deleted']:,} ={r['unchanged']:,}")
            newest = self._newest_published()
            self.build(run_id, em, "daily", up_to=newest or d)
            return []
        print(f"-- replay {area} {d}{' (full rewrite)' if full else ''}")
        return self._run("replay", None, d, {"area": area, "day": d, "full": full}, body)

    def backfill(self, start: date, end: date, full: bool = False):
        """Replay every (area, day) in the range, then rebuild whatever that made dirty.

        Safe to run twice: each replay is a partition-scoped hash merge, so a second
        pass finds every row unchanged and writes nothing.
        """
        def body(run_id, em):
            for d in cal.daterange(start, end):
                for area in self.areas:
                    a = self.con.execute("""SELECT arrival_id, dataset, area, file_date, path, sha256 FROM ops.landing_manifest
                        WHERE dataset='cdr_activity' AND area=? AND file_date=? AND status IN ('PROCESSED','HELD_LATE')
                        ORDER BY arrival_id DESC LIMIT 1""", [area, d]).fetchone()
                    if not a:
                        print(f"   {area} {d}: nothing landed, skipped")
                        continue
                    arrival = dict(zip(["arrival_id", "dataset", "area", "file_date", "path", "sha256"], a))
                    arrival.update(force=True, full=full)
                    r = self.task(run_id, f"backfill:{area}:{d}", ingest.ingest_cdr, self.con, self.cfg, self.models,
                                  arrival, run_id, em)
                    print(f"   {area} {d}: +{r['inserted']:,} ~{r['updated']:,} -{r['deleted']:,} ={r['unchanged']:,}")
            newest = max(self._newest_published() or end, end)
            self.build(run_id, em, "daily", up_to=newest)
            self.build(run_id, em, "weekly", up_to=cal.week_start(newest) - timedelta(days=7))
            closed = cal.cycle_start(newest, self.anchor, self.cycle_len) - timedelta(days=self.cycle_len)
            self.build(run_id, em, "fortnightly", up_to=closed)
            return []
        print(f"-- backfill {start} .. {end}")
        return self._run("backfill", None, start, {"from": start, "to": end, "full": full}, body)

    def _newest_published(self):
        return self.con.execute("SELECT max(partition_key) FROM ops.partition_state WHERE model='gold.daily_cell_usage' "
                                "AND status LIKE 'PUBLISHED%'").fetchone()[0]
