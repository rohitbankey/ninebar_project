"""SQL model registry, partition state, and the partition builder.

A model is a .sql file under models/ with a comment header:

    -- model: gold.daily_cell_usage
    -- cadence: daily | weekly | fortnightly
    -- partition_by: local_date | week_start | cycle_start
    -- owner / freshness / quality: free text, shown in blast-radius reports
    -- check: unique(cell_id, local_date)        (zero or more)
    -- check: not_null(cell_id)
    -- check: expr(slots_observed <= expected_slots)
    -- affects_next: true                        (a change in partition p also changes p+1, e.g. week-over-week)

The SQL body is a SELECT for exactly one partition. {{p}} is the partition key
as a DATE literal, {{tz}} the timezone, {{cycle_len}} the cycle length.

Dependencies between models are not declared; they are read from the SQL with
sqlglot when the registry is loaded. Build order is a topological sort of that.
"""
from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path

import sqlglot
from sqlglot import exp

from . import calendar as cal
from . import faults
from .lineage import Emitter, catalog, sql_edges

HEADER = re.compile(r"^--\s*(\w+):\s*(.*)$")


class QualityCheckFailed(Exception):
    pass


@dataclass
class Model:
    name: str
    path: Path
    sql: str
    cadence: str
    partition_by: str
    owner: str = ""
    freshness: str = ""
    quality: str = ""
    checks: list[str] = field(default_factory=list)
    affects_next: bool = False
    upstream: list[str] = field(default_factory=list)

    @property
    def layer(self) -> str:
        return self.name.split(".")[0]


def parse_model(path: Path) -> Model:
    text = path.read_text()
    meta: dict = {"check": []}
    for line in text.splitlines():
        m = HEADER.match(line.strip())
        if m:
            k, v = m.group(1), m.group(2).strip()
            if k == "check":
                meta["check"].append(v)
            else:
                meta[k] = v
    body = "\n".join(l for l in text.splitlines() if not l.strip().startswith("--"))
    probe = render(body, date(2000, 1, 3), "UTC", 14)
    tree = sqlglot.parse_one(probe, dialect="duckdb")
    ctes = {c.alias for c in tree.find_all(exp.CTE)}
    ups = sorted({exp.table_name(t) for t in tree.find_all(exp.Table)
                  if t.db and exp.table_name(t) not in ctes})
    return Model(name=meta["model"], path=path, sql=body, cadence=meta["cadence"],
                 partition_by=meta["partition_by"], owner=meta.get("owner", ""),
                 freshness=meta.get("freshness", ""), quality=meta.get("quality", ""),
                 checks=meta["check"], affects_next=meta.get("affects_next", "false") == "true",
                 upstream=ups)


def render(sql: str, p: date, tz: str, cycle_len: int) -> str:
    return (sql.replace("{{p}}", f"DATE '{p.isoformat()}'")
               .replace("{{tz}}", f"'{tz}'")
               .replace("{{cycle_len}}", str(cycle_len)))


def load_models(models_dir: Path) -> dict[str, Model]:
    ms = {}
    for p in sorted(models_dir.rglob("*.sql")):
        m = parse_model(p)
        ms[m.name] = m
    return ms


def topo(models: dict[str, Model]) -> list[Model]:
    done: list[str] = []
    seen: set[str] = set()

    def visit(n: str, stack=()):
        if n in seen or n not in models:
            return
        if n in stack:
            raise ValueError(f"cycle in models: {' -> '.join(stack + (n,))}")
        for u in models[n].upstream:
            visit(u, stack + (n,))
        seen.add(n)
        done.append(n)

    for n in sorted(models):
        visit(n)
    return [models[n] for n in done]


def sync_registry(con, models: dict[str, Model]) -> None:
    for m in models.values():
        sha = hashlib.sha256(m.sql.encode()).hexdigest()[:16]
        con.execute("DELETE FROM ops.model_registry WHERE model = ?", [m.name])  # list columns can't be upserted in duckdb 1.1
        con.execute("""INSERT INTO ops.model_registry VALUES (?,?,?,?,?,?,?,?,?,?)""",
                    [m.name, m.layer, m.cadence, m.partition_by, m.owner, m.freshness, m.quality,
                     str(m.path), sha, m.upstream])


# ---------------------------------------------------------------- partitions

def key_for(partition_by: str, d: date, anchor: date, cycle_len: int) -> date:
    if partition_by == "local_date":
        return d
    if partition_by == "week_start":
        return cal.week_start(d)
    if partition_by == "cycle_start":
        return cal.cycle_start(d, anchor, cycle_len)
    raise ValueError(partition_by)


def span(partition_by: str, key: date, cycle_len: int) -> list[date]:
    if partition_by == "local_date":
        return [key]
    if partition_by == "week_start":
        return cal.week_days(key)
    return cal.cycle_days(key, cycle_len)


def next_key(partition_by: str, key: date, cycle_len: int) -> date:
    return key + timedelta(days={"local_date": 1, "week_start": 7, "cycle_start": cycle_len}[partition_by])


def mark_dirty(con, model: str, key: date, reason: str) -> None:
    cur = con.execute("SELECT version, built_run_id, built_at FROM ops.partition_state WHERE model=? AND partition_key=?",
                      [model, key]).fetchone()
    con.execute("DELETE FROM ops.partition_state WHERE model=? AND partition_key=?", [model, key])
    con.execute("INSERT INTO ops.partition_state VALUES (?, ?, 'DIRTY', ?, ?, ?, ?)",
                [model, key, cur[0] if cur else 0, cur[1] if cur else None, cur[2] if cur else None, reason])


def invalidate(con, models: dict[str, Model], dataset: str, dates, reason: str, anchor: date, cycle_len: int,
               only_built: bool = False) -> int:
    """Mark the partitions of direct consumers of `dataset` that cover `dates` as dirty."""
    n = 0
    dates = sorted(set(dates))
    for m in models.values():
        if dataset not in m.upstream:
            continue
        keys = {key_for(m.partition_by, d, anchor, cycle_len) for d in dates}
        if m.affects_next:
            keys |= {next_key(m.partition_by, k, cycle_len) for k in keys}
        for k in sorted(keys):
            if only_built:
                st = con.execute("SELECT status FROM ops.partition_state WHERE model=? AND partition_key=?", [m.name, k]).fetchone()
                if not st:
                    continue
            mark_dirty(con, m.name, k, reason)
            n += 1
    return n


def dirty_keys(con, model: str, up_to: date) -> list[date]:
    rows = con.execute("SELECT partition_key FROM ops.partition_state WHERE model = ? AND status = 'DIRTY' "
                       "AND partition_key <= ? ORDER BY 1", [model, up_to]).fetchall()
    return [r[0] for r in rows]


def is_built(con, model: str, key: date) -> bool:
    r = con.execute("SELECT status FROM ops.partition_state WHERE model=? AND partition_key=?", [model, key]).fetchone()
    return bool(r) and r[0].startswith("PUBLISHED")


def sources_complete(con, days: list[date], areas: list[str]) -> bool:
    n = con.execute("""SELECT count(DISTINCT (area, file_date)) FROM ops.landing_manifest
                       WHERE dataset = 'cdr_activity' AND status = 'PROCESSED' AND file_date BETWEEN ? AND ?""",
                    [min(days), max(days)]).fetchone()[0]
    return n >= len(days) * len(areas)


# ---------------------------------------------------------------- building

def _check_sql(check: str, table: str, pcol: str, p: date) -> tuple[str, str]:
    where = f"{pcol} = DATE '{p.isoformat()}'"
    kind, _, arg = check.partition("(")
    arg = arg.rstrip(")")
    if kind == "unique":
        return check, f"SELECT count(*) FROM (SELECT {arg} FROM {table} WHERE {where} GROUP BY ALL HAVING count(*) > 1)"
    if kind == "not_null":
        cond = " OR ".join(f"{c.strip()} IS NULL" for c in arg.split(","))
        return check, f"SELECT count(*) FROM {table} WHERE {where} AND ({cond})"
    if kind == "expr":
        return check, f"SELECT count(*) FROM {table} WHERE {where} AND NOT ({arg})"
    raise ValueError(check)


def build_partition(con, m: Model, p: date, run_id: int, emitter: Emitter, tz: str, cycle_len: int,
                    areas: list[str], models: dict[str, Model], anchor: date) -> dict:
    sql = render(m.sql, p, tz, cycle_len)
    table = m.name
    exists = con.execute("SELECT count(*) FROM information_schema.tables WHERE table_schema||'.'||table_name = ?",
                         [table]).fetchone()[0]
    if not exists:
        con.execute(f"CREATE TABLE {table} AS SELECT *, CAST(NULL AS INTEGER) AS _run_id FROM ({sql}) WHERE false")

    prev = con.execute("SELECT version, status FROM ops.partition_state WHERE model=? AND partition_key=?", [m.name, p]).fetchone()
    version = (prev[0] if prev else 0) + 1
    complete = sources_complete(con, span(m.partition_by, p, cycle_len), areas)
    status = "PUBLISHED" if complete else "PUBLISHED_PARTIAL"

    con.begin()
    try:
        con.execute(f"DELETE FROM {table} WHERE {m.partition_by} = ?", [p])
        faults.checkpoint("build", f"{m.name}:{p}")
        n = con.execute(f"INSERT INTO {table} BY NAME SELECT *, {run_id} AS _run_id FROM ({sql})").fetchone()[0]
        failures = []
        for chk in m.checks:
            label, q = _check_sql(chk, table, m.partition_by, p)
            bad = con.execute(q).fetchone()[0]
            if bad:
                failures.append(f"{label}: {bad} rows")
        if failures:
            raise QualityCheckFailed(f"{m.name} [{p}] " + "; ".join(failures))
        con.execute("DELETE FROM ops.partition_state WHERE model=? AND partition_key=?", [m.name, p])
        con.execute("INSERT INTO ops.partition_state VALUES (?, ?, ?, ?, ?, now(), NULL)", [m.name, p, status, version, run_id])
        # anything reading this model now has a stale partition - but only for days that have data,
        # otherwise a partial first week would dirty a billing cycle that ended before the data starts
        days = [r[0] for r in con.execute(
            "SELECT DISTINCT file_date FROM ops.landing_manifest WHERE dataset = 'cdr_activity' AND status = 'PROCESSED' "
            "AND file_date BETWEEN ? AND ?", [min(span(m.partition_by, p, cycle_len)), max(span(m.partition_by, p, cycle_len))]).fetchall()]
        invalidate(con, models, m.name, days, f"upstream {m.name}[{p}] rebuilt v{version}", anchor, cycle_len)
        con.commit()
    except Exception:
        con.rollback()
        raise

    edges = sql_edges(sql, catalog(con), m.name)
    emitter.emit(f"model.{m.name}", inputs=m.upstream, outputs=[m.name], edges=edges,
                 facets={"partition": {"key": p.isoformat(), "version": version, "status": status},
                         "sql": {"path": str(m.path)}, "rows": n})
    return {"model": m.name, "partition": p.isoformat(), "rows": n, "version": version, "status": status,
            "restated": version > 1}
