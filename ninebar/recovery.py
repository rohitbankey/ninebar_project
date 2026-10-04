"""Reading the pipeline's state back: run history, reconciliation, tracing."""
from __future__ import annotations

from datetime import date, timedelta
from itertools import islice
from pathlib import Path

from . import lineage


def _table(con, sql: str, params=None) -> str:
    rel = con.execute(sql, params or [])
    cols = [d[0] for d in rel.description]
    rows = rel.fetchall()
    out = ["| " + " | ".join(cols) + " |", "|" + "---|" * len(cols)]
    for r in rows:
        out.append("| " + " | ".join("" if v is None else (f"{v:,.4f}" if isinstance(v, float) else str(v)) for v in r) + " |")
    return "\n".join(out)


def print_runs(con, limit: int, run_id: int | None):
    if run_id:
        print(con.sql(f"""SELECT task, attempt, status, round(epoch(ended_at - started_at), 2) AS secs, error
                          FROM ops.tasks WHERE run_id = {int(run_id)} ORDER BY started_at"""))
        return
    print(con.sql(f"""
        SELECT r.run_id, r.command, r.cadence, r.logical_date, r.status,
               round(epoch(r.ended_at - r.started_at), 1) AS secs,
               count(t.task) FILTER (WHERE t.status = 'SUCCESS') AS ok_tasks,
               count(t.task) FILTER (WHERE t.status = 'RETRYING') AS retries,
               left(r.error, 80) AS error
        FROM ops.runs r LEFT JOIN ops.tasks t USING (run_id)
        GROUP BY ALL ORDER BY r.run_id DESC LIMIT {int(limit)}"""))


def print_status(con):
    print("landing manifest:")
    print(con.sql("""SELECT dataset, status, count(*) AS arrivals, sum(row_count) AS lines FROM ops.landing_manifest
                     GROUP BY ALL ORDER BY ALL"""))
    print("partitions:")
    print(con.sql("""SELECT model, status, count(*) AS partitions, max(version) AS max_version, min(partition_key) AS first,
                     max(partition_key) AS last FROM ops.partition_state GROUP BY ALL ORDER BY ALL"""))
    print("quarantine by reason:")
    print(con.sql("""SELECT dataset, r AS reason, count(*) AS rows FROM (SELECT dataset, unnest(reasons) AS r FROM quarantine.records)
                     GROUP BY ALL ORDER BY rows DESC"""))
    print("late / out-of-order events:")
    print(con.sql("""SELECT kind, count(*) AS events, min(file_date), max(file_date) FROM ops.late_events GROUP BY ALL"""))


# ---------------------------------------------------------------- reconciliation

def reconcile(con) -> str:
    """Rows from source to silver to gold, per arrival and per day.

    Per arrival:  lines in file  = header + quarantined + collapsed exact dupes + accepted
    Per (area, file_date): silver rows owned by that file = accepted rows of its latest processing
    Per local day: silver rows = sum(source_rows) in silver.usage_cell_day = sum(source_rows) in gold.daily_cell_usage
    """
    con.execute("""
        CREATE OR REPLACE TEMP VIEW rc_arrival AS
        WITH last AS (
            SELECT arrival_id, arg_max(run_id, recorded_at) AS run_id FROM ops.ingest_stats GROUP BY arrival_id
        )
        SELECT m.arrival_id, m.area, m.file_date, m.status, m.origin,
               m.row_count AS file_lines,
               m.row_count - s.rows_read AS header_lines,
               s.rows_quarantined, s.exact_dupes_collapsed, s.accepted,
               -- a gate-rejected arrival merges nothing by design, so its rows are not "unexplained"
               CASE WHEN m.status = 'FAILED_QUALITY' THEN NULL
                    ELSE s.rows_read - s.rows_quarantined - s.exact_dupes_collapsed - s.accepted END AS unexplained
        FROM ops.landing_manifest m
        JOIN last l USING (arrival_id)
        JOIN ops.ingest_stats s ON s.arrival_id = l.arrival_id AND s.run_id = l.run_id
        WHERE m.dataset = 'cdr_activity'
    """)
    con.execute("""
        CREATE OR REPLACE TEMP VIEW rc_file AS
        WITH owner AS (
            SELECT area, file_date, max(arrival_id) AS arrival_id FROM ops.landing_manifest
            WHERE dataset='cdr_activity' AND status='PROCESSED' GROUP BY ALL
        ),
        silver AS (SELECT area, _file_date AS file_date, count(*) AS silver_rows FROM silver.cdr_activity GROUP BY ALL)
        SELECT o.area, o.file_date, o.arrival_id, a.accepted, s.silver_rows, s.silver_rows - a.accepted AS diff
        FROM owner o JOIN rc_arrival a ON a.arrival_id = o.arrival_id
        LEFT JOIN silver s ON s.area = o.area AND s.file_date = o.file_date
    """)
    con.execute("""
        CREATE OR REPLACE TEMP VIEW rc_day AS
        WITH s AS (SELECT local_date, count(*) AS silver_rows FROM silver.cdr_activity GROUP BY 1),
             u AS (SELECT local_date, sum(source_rows) AS usage_rows FROM silver.usage_cell_day GROUP BY 1),
             g AS (SELECT local_date, sum(source_rows) AS daily_mart_rows FROM gold.daily_cell_usage GROUP BY 1)
        SELECT s.local_date, s.silver_rows, u.usage_rows, g.daily_mart_rows,
               (s.silver_rows = u.usage_rows AND u.usage_rows = g.daily_mart_rows) AS ok
        FROM s LEFT JOIN u USING (local_date) LEFT JOIN g USING (local_date)
    """)
    bad_a = con.execute("SELECT count(*) FROM rc_arrival WHERE unexplained <> 0").fetchone()[0]
    bad_f = con.execute("SELECT count(*) FROM rc_file WHERE diff <> 0").fetchone()[0]
    bad_d = con.execute("SELECT count(*) FROM rc_day WHERE NOT coalesce(ok, false)").fetchone()[0]
    parts = [
        "## Row-count reconciliation",
        "",
        f"* arrivals where lines != header + quarantined + collapsed dupes + accepted: **{bad_a}** "
        "(arrivals rejected by the quality gate are excluded: they merge nothing by design)",
        f"* (area, file_date) where silver rows != accepted rows of the owning arrival: **{bad_f}**",
        f"* local days where silver rows != usage_cell_day rows != daily mart rows: **{bad_d}**",
        "",
        "### Per arrival (source -> accepted)",
        _table(con, "SELECT * FROM rc_arrival ORDER BY file_date, area, arrival_id"),
        "",
        "### Per day (silver -> silver.usage_cell_day -> gold.daily_cell_usage)",
        _table(con, "SELECT * FROM rc_day ORDER BY local_date"),
    ]
    if bad_f:
        parts += ["", "### Files where silver disagrees", _table(con, "SELECT * FROM rc_file WHERE diff <> 0")]
    return "\n".join(parts)


# ---------------------------------------------------------------- trace

PERIOD_COLS = {"local_date": 1, "week_start": 7, "cycle_start": None}


def _read_line(path: str, line_no: int) -> str | None:
    try:
        with open(path, encoding="utf-8", errors="replace") as f:
            return next(islice(f, line_no - 1, None), "").rstrip("\n")
    except OSError:
        return None


def trace(con, model: str, key: dict, metric: str, top: int = 5, cycle_len: int = 14) -> str:
    """Follow `model.metric` up the lineage graph to the landing column, then pull the
    silver rows behind this one output row and show the raw landing lines they came from."""
    out = [f"# Trace `{model}.{metric}` for {key}", ""]
    row = con.execute(f"SELECT {metric} FROM {model} WHERE " + " AND ".join(f"{k} = ?" for k in key),
                      list(key.values())).fetchall()
    if not row:
        return "\n".join(out + ["no such row"])
    out.append(f"value in {model}: **{row[0][0]}**")
    path = lineage.upstream(con, model, metric)
    out += ["", "## Lineage path (from lineage.current_edges, projection edges only)", ""]
    for ds, col, hops in path:
        out.append(f"* {hops} hop(s) up: `{ds}.{col}`")
    silver_cols = [c for ds, c, _ in path if ds == "silver.cdr_activity"]
    if not silver_cols:
        return "\n".join(out + ["", "metric does not trace back to silver.cdr_activity"])
    measure = " + ".join(f"coalesce({c}, 0)" for c in silver_cols)

    where, params = [], []
    for k, v in key.items():
        if k in PERIOD_COLS:
            d0 = date.fromisoformat(v)
            span = PERIOD_COLS[k] or cycle_len
            where.append("local_date >= ? AND local_date < ?")
            params += [d0, d0 + timedelta(days=span)]
        elif k in ("cell_id", "area", "country_code"):
            where.append(f"{k} = ?")
            params.append(v)
    total = con.execute(f"SELECT sum({measure}), count(*) FROM silver.cdr_activity WHERE {' AND '.join(where)}", params).fetchone()
    out += ["", f"silver rows behind this value: {total[1]:,}, sum of ({' + '.join(silver_cols)}) = {total[0]}", "",
            f"## Top {top} contributing source records", ""]
    rows = con.execute(f"""
        SELECT s.cell_id, s.slot_start_utc, s.country_code, {measure} AS contribution, s._arrival_id, s._line_no,
               m.origin, m.path, m.arrived_at, s._contract_version
        FROM silver.cdr_activity s JOIN ops.landing_manifest m ON m.arrival_id = s._arrival_id
        WHERE {' AND '.join('s.' + w if not w.startswith('local_date') else w.replace('local_date', 's.local_date') for w in where)}
        ORDER BY contribution DESC LIMIT {int(top)}""", params).fetchall()
    tot = total[0] or 1
    for cell, slot, cc, contrib, aid, ln, origin, p, arrived, cv in rows:
        raw = _read_line(p, ln)
        out += [f"* {cell} {slot} cc={cc}: {contrib:.4f} ({contrib / tot:.1%} of the value)",
                f"  - arrival {aid} ({origin}, landed {arrived}, contract v{cv}) line {ln} of `{Path(p).name}`",
                f"  - raw: `{raw}`"]
    return "\n".join(out)


# ---------------------------------------------------------------- report

def write_report(con, cfg) -> None:
    out = cfg.path("reports")
    out.mkdir(parents=True, exist_ok=True)
    lineage.export_graph(con, out)
    (out / "reconciliation_rowcounts.md").write_text(reconcile(con) + "\n")
    samples = {
        "gold_daily_cell_usage_sample.csv": "SELECT * FROM gold.daily_cell_usage WHERE internet_rank_in_area <= 5 ORDER BY local_date, area, internet_rank_in_area",
        "gold_weekly_cell_rollup_sample.csv": "SELECT * FROM gold.weekly_cell_rollup WHERE cell_id IN ('MI-05060','MI-05161','TN-05200','TN-10000') ORDER BY cell_id, week_start",
        "gold_fortnightly_settlement.csv": "SELECT * FROM gold.fortnightly_settlement ORDER BY cycle_start, area, zone, rate_internet",
        "gold_fortnight_reconciliation.csv": "SELECT * FROM gold.fortnight_reconciliation ORDER BY cycle_start, area",
        "ops_runs.csv": "SELECT * FROM ops.runs ORDER BY run_id",
        "ops_tasks_retries_and_failures.csv": "SELECT * FROM ops.tasks WHERE status <> 'SUCCESS' ORDER BY started_at",
        "ops_landing_manifest.csv": "SELECT * FROM ops.landing_manifest ORDER BY arrival_id",
        "ops_ingest_stats.csv": "SELECT * FROM ops.ingest_stats ORDER BY recorded_at",
        "ops_partition_state.csv": "SELECT * FROM ops.partition_state ORDER BY model, partition_key",
        "ops_late_events.csv": "SELECT * FROM ops.late_events ORDER BY detected_at",
        "ops_contract_registry.csv": "SELECT dataset, version, source, registered_at, spec FROM ops.contract_registry ORDER BY dataset, registered_at",
        "quarantine_summary.csv": "SELECT dataset, arrival_id, r AS reason, count(*) AS rows, any_value(raw_line) AS example FROM (SELECT *, unnest(reasons) AS r FROM quarantine.records) GROUP BY ALL ORDER BY rows DESC",
        "silver_dim_cell_site_scd2_changes.csv": "SELECT * EXCLUDE (attr_hash) FROM silver.dim_cell_site WHERE cell_id IN (SELECT cell_id FROM silver.dim_cell_site GROUP BY 1 HAVING count(*) > 1) ORDER BY cell_id, valid_from LIMIT 60",
        "silver_dim_tariff_plan.csv": "SELECT * EXCLUDE (attr_hash) FROM silver.dim_tariff_plan ORDER BY country_code, valid_from",
    }
    for name, sql in samples.items():
        try:
            con.execute(f"COPY ({sql}) TO '{out / name}' (HEADER, DELIMITER ',')")
        except Exception as e:  # noqa: BLE001 - a missing table should not stop the rest of the report
            print(f"   skipped {name}: {e}")
    print(f"report written to {out}")
