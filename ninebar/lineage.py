"""Runtime lineage.

Every job that writes data calls `Emitter.emit` after it commits. The event is
OpenLineage-shaped (job, run, inputs, outputs, columnLineage facet) and goes to
two places:
  * data/lineage/<run_id>.jsonl  - the raw event stream, one JSON per line
  * lineage.events / lineage.column_edges in DuckDB - so it can be queried

Column edges are not written by hand. They come from parsing the SQL that was
just executed (sqlglot), resolved against the live catalog of the warehouse.
Two kinds of edge are recorded:
  projection  src column flows into a named output column
  reference   src column is used by the job (join key, filter, group by, window)
              but does not flow into a specific output column. dst_column = '*'.
A rename of a column that is only *referenced* still breaks the model, so the
blast-radius walk follows both kinds.
"""
from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from pathlib import Path

import sqlglot
from sqlglot import exp
from sqlglot.lineage import lineage as sg_lineage
from sqlglot.optimizer.qualify import qualify
from sqlglot.optimizer.scope import traverse_scope

NAMESPACE = "ninebar"


def catalog(con, extra: dict | None = None) -> dict:
    """{schema: {table: {column: type}}} for everything in the warehouse, plus temp tables."""
    rows = con.execute(
        "SELECT table_schema, table_name, column_name, data_type FROM information_schema.columns "
        "WHERE table_catalog = current_database() OR table_catalog = 'temp'").fetchall()
    out: dict = {}
    for s, t, c, ty in rows:
        out.setdefault(s, {}).setdefault(t, {})[c] = ty
    for k, v in (extra or {}).items():
        s, t = k.split(".")
        out.setdefault(s, {})[t] = v
    return out


def sql_edges(sql: str, schema: dict, dst: str, rename_sources: dict | None = None) -> list[tuple]:
    """Column-level edges for a SELECT that produces `dst`.

    rename_sources maps a table name as it appears in the SQL (e.g. a temp staging
    table) to the dataset name that should appear in the graph.
    """
    rename_sources = rename_sources or {}
    tree = sqlglot.parse_one(sql, dialect="duckdb")
    q = qualify(tree, schema=schema, dialect="duckdb", validate_qualify_columns=False)

    def ds_name(t: exp.Table) -> str:
        name = exp.table_name(t)
        if name.startswith("temp.main."):
            name = name[len("temp.main."):]
        if name.startswith("main."):
            name = name[len("main."):]
        return rename_sources.get(name, name)

    edges: set[tuple] = set()
    for sel in q.selects:
        out_col = sel.alias_or_name
        if out_col == "*":
            continue
        try:
            node = sg_lineage(out_col, q, schema=schema, dialect="duckdb")
        except Exception:  # noqa: BLE001 - a column sqlglot cannot trace is still reported, as unknown
            edges.add(("?", "?", dst, out_col, "projection"))
            continue
        for n in node.walk():
            if isinstance(n.source, exp.Table) and not n.downstream:
                col = n.name.split(".")[-1].strip('"')
                edges.add((ds_name(n.source), col, dst, out_col, "projection"))

    for scope in traverse_scope(q):
        for col in scope.columns:
            src = scope.sources.get(col.table)
            if isinstance(src, exp.Table):
                edges.add((ds_name(src), col.name, dst, "*", "reference"))
    return sorted(edges)


class Emitter:
    def __init__(self, con, events_dir: Path, run_id: int):
        self.con = con
        self.run_id = run_id
        self.path = events_dir / f"run_{run_id:05d}.jsonl"
        events_dir.mkdir(parents=True, exist_ok=True)

    def emit(self, job: str, inputs: list[str], outputs: list[str], edges: list[tuple],
             facets: dict | None = None, event_type: str = "COMPLETE") -> str:
        event_id = str(uuid.uuid4())
        now = datetime.now(timezone.utc)
        fields: dict = {}
        for s_ds, s_col, d_ds, d_col, kind in edges:
            if kind == "projection":
                fields.setdefault(d_ds, {}).setdefault(d_col, {"inputFields": []})["inputFields"].append(
                    {"namespace": NAMESPACE, "name": s_ds, "field": s_col})
        payload = {
            "eventType": event_type,
            "eventTime": now.isoformat(),
            "producer": "https://github.com/ninebar-exercise/ninebar",
            "run": {"runId": f"{self.run_id}", "facets": facets or {}},
            "job": {"namespace": NAMESPACE, "name": job},
            "inputs": [{"namespace": NAMESPACE, "name": i} for i in sorted(set(inputs))],
            "outputs": [{"namespace": NAMESPACE, "name": o,
                         "facets": {"columnLineage": {"fields": fields.get(o, {})}}} for o in sorted(set(outputs))],
        }
        with open(self.path, "a") as f:
            f.write(json.dumps(payload, default=str) + "\n")
        self.con.execute("INSERT INTO lineage.events VALUES (?,?,?,?,?,?)",
                         [event_id, self.run_id, job, event_type, now, json.dumps(payload, default=str)])
        if edges:
            self.con.executemany(
                "INSERT INTO lineage.column_edges (event_id, run_id, job, src_dataset, src_column, dst_dataset, dst_column, edge_kind) "
                "VALUES (?,?,?,?,?,?,?,?)",
                [[event_id, self.run_id, job, *e] for e in edges])
        return event_id


# ---------------------------------------------------------------- queries over the emitted graph

DOWNSTREAM_SQL = """
WITH RECURSIVE walk(ds, col, depth) AS (
    SELECT ?::VARCHAR, ?::VARCHAR, 0
    UNION
    SELECT e.dst_dataset, e.dst_column, w.depth + 1
    FROM walk w
    JOIN lineage.current_edges e
      ON e.src_dataset = w.ds AND (e.src_column = w.col OR w.col = '*')
    WHERE w.depth < 25
)
SELECT ds AS dataset, col AS column_name, min(depth) AS hops
FROM walk WHERE depth > 0
GROUP BY ALL ORDER BY hops, dataset, column_name
"""

UPSTREAM_SQL = """
WITH RECURSIVE walk(ds, col, depth) AS (
    SELECT ?::VARCHAR, ?::VARCHAR, 0
    UNION
    SELECT e.src_dataset, e.src_column, w.depth + 1
    FROM walk w
    JOIN lineage.current_edges e
      ON e.dst_dataset = w.ds AND e.dst_column = w.col AND e.edge_kind = 'projection'
    WHERE w.depth < 25
)
SELECT ds AS dataset, col AS column_name, min(depth) AS hops
FROM walk WHERE depth > 0
GROUP BY ALL ORDER BY hops DESC, dataset, column_name
"""


VALUE_SQL = DOWNSTREAM_SQL.replace("ON e.src_dataset = w.ds AND (e.src_column = w.col OR w.col = '*')",
                                   "ON e.src_dataset = w.ds AND e.src_column = w.col AND e.edge_kind = 'projection'")


def downstream(con, dataset: str, column: str, values_only: bool = False):
    """Everything that depends on dataset.column. values_only=True follows projection edges only:
    the columns whose *values* come from it, as opposed to every model that would fail without it."""
    return con.execute(VALUE_SQL if values_only else DOWNSTREAM_SQL, [dataset, column]).fetchall()


def impact_table(con, dataset: str, columns: list[str]) -> str:
    out = ["| source column | models that fail if it disappears or changes type | output columns whose values derive from it |",
           "|---|---|---|"]
    for c in columns:
        fails = sorted({ds for ds, _, _ in downstream(con, dataset, c)})
        vals: dict[str, list[str]] = {}
        for ds, col, _ in downstream(con, dataset, c, values_only=True):
            vals.setdefault(ds, []).append(col)
        v = "<br>".join(f"`{ds}`: {', '.join(sorted(cs))}" for ds, cs in sorted(vals.items()))
        out.append(f"| `{c}` | {', '.join(f'`{f}`' for f in fails)} | {v} |")
    return "\n".join(out)


def upstream(con, dataset: str, column: str):
    return con.execute(UPSTREAM_SQL, [dataset, column]).fetchall()


def blast_radius(con, dataset: str, column: str, change: str) -> str:
    """Markdown report: what breaks downstream of dataset.column, who owns it, what it promises."""
    hits = downstream(con, dataset, column)
    by_ds: dict[str, list] = {}
    for ds, col, hops in hits:
        by_ds.setdefault(ds, []).append((col, hops))
    lines = [f"# Blast radius: `{dataset}.{column}` ({change})", ""]
    if not hits:
        lines.append("Nothing downstream consumes this column in the current lineage graph.")
        return "\n".join(lines)
    lines += ["Computed from `lineage.current_edges` (the edges emitted by the most recent run of each job).", "",
              "| dataset | hops | affected columns | owner | cadence | freshness | quality expectation | published partitions |",
              "|---|---|---|---|---|---|---|---|"]
    for ds, cols in sorted(by_ds.items(), key=lambda kv: min(h for _, h in kv[1])):
        meta = con.execute("SELECT owner, cadence, freshness, quality FROM ops.model_registry WHERE model = ?", [ds]).fetchone()
        if meta is None and ds == "silver.cdr_activity":
            meta = ("network-data-platform", "per arrival", "within the daily run", "contract cdr_activity; quarantine <= 1%")
        meta = meta or ("?", "?", "?", "?")
        parts = con.execute("SELECT count(*) FROM ops.partition_state WHERE model = ? AND status LIKE 'PUBLISHED%'", [ds]).fetchone()[0]
        named = sorted({c for c, _ in cols if c != "*"})
        col_txt = ", ".join(named) if named else "(used in joins/filters only)"
        if any(c == "*" for c, _ in cols):
            col_txt += " + whole model (referenced in join/filter/group)"
        lines.append(f"| `{ds}` | {min(h for _, h in cols)} | {col_txt} | {meta[0]} | {meta[1]} | {meta[2]} | {meta[3]} | {parts} |")
    return "\n".join(lines)


def export_graph(con, out: Path) -> None:
    """Dump the current graph as JSON and as a Mermaid diagram (generated, not drawn)."""
    edges = con.execute("SELECT src_dataset, src_column, dst_dataset, dst_column, edge_kind, job FROM lineage.current_edges "
                        "ORDER BY ALL").fetchall()
    out.mkdir(parents=True, exist_ok=True)
    (out / "lineage_current_edges.json").write_text(json.dumps(
        [dict(zip(["src_dataset", "src_column", "dst_dataset", "dst_column", "edge_kind", "job"], e)) for e in edges], indent=1))
    ds_edges = sorted({(e[0], e[2]) for e in edges if e[0] != e[2]})
    ids = {}

    def nid(n):
        return ids.setdefault(n, f"n{len(ids)}")

    mm = ["flowchart LR"]
    for a, b in ds_edges:
        mm.append(f'  {nid(a)}["{a}"] --> {nid(b)}["{b}"]')
    (out / "lineage_graph.mmd").write_text("\n".join(mm) + "\n")
