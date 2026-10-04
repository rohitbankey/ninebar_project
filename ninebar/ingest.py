"""Landing -> silver.

One arrival at a time:

  1. contract gate   resolve the file against the registry (match / additive / breaking)
  2. stage           read every physical line as one string, keep its line number
  3. type + check    cast per contract, collect *all* failing reasons per line
  4. standardise     cell_id with area prefix, epoch ms -> UTC instant + Europe/Rome date,
                     referential check against the cell-site dimension (as of that date)
  5. dedupe          exact duplicates collapse (counted); same key with different
                     values -> last line wins, earlier lines quarantined
  6. quality gate    quarantined share above the contract threshold fails the arrival
  7. merge           hash diff against what silver already holds for this
                     (area, file_date) and apply only inserts/updates/deletes

Steps 7's writes, the quarantine rows, the manifest status and the partition
invalidation all commit in one transaction. If the process dies in the middle,
DuckDB rolls the whole thing back and the arrival is still PENDING, so the next
run simply does it again.
"""
from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

from . import contracts, faults
from . import models as mdl
from .lineage import Emitter, blast_radius, catalog, sql_edges

MEASURES = ["sms_in", "sms_out", "call_in", "call_out", "internet"]
CDR_BASE = ["square_id", "time_interval", "country_code", *MEASURES]
KEY = ["cell_id", "slot_start_utc", "country_code"]
FAR_FUTURE = date(9999, 12, 31)


class QualityGateFailed(Exception):
    pass


def q(s: str) -> str:
    return "'" + str(s).replace("'", "''") + "'"


def ident(s: str) -> str:
    return '"' + s.replace('"', '""') + '"'


# ---------------------------------------------------------------- shared steps

def stage_lines(con, path: Path, delimiter: str) -> None:
    # One VARCHAR per physical line: the CSV delimiter is a control char that never occurs,
    # so a short, long or garbled row still arrives here instead of being skipped by the reader.
    # Split once and materialise (temp, in memory) - views over string_split get re-evaluated
    # for every column reference and were 5x slower.
    d = "chr(9)" if delimiter == "\t" else q(delimiter)
    con.execute("DROP TABLE IF EXISTS stg_split")
    con.execute(f"""
        CREATE TEMP TABLE stg_split AS
        SELECT line_no, line, string_split(line, {d}) AS f
        FROM (SELECT row_number() OVER () AS line_no, line
              FROM read_csv({q(path)}, columns = {{'line': 'VARCHAR'}}, delim = '\x01', quote = '', escape = '',
                            header = false, auto_detect = false))
    """)


def type_and_check(con, v: contracts.Version, res: contracts.Resolution, ncols: int) -> None:
    inner, cases = [], [
        "CASE WHEN line IS NULL OR trim(line) = '' THEN 'empty_line' END",
        f"CASE WHEN n_fields <> {ncols} THEN 'field_count:' || n_fields END",
    ]
    for c in v.columns:
        n = c["name"]
        raw = f"nullif(trim(f[{res.positions[n] + 1}]), '')"
        inner.append(f"{raw} AS {ident(n + '__raw')}")
        inner.append(f"TRY_CAST({raw} AS {c['type']}) AS {ident(n)}")
        if not c.get("nullable", True):
            cases.append(f"CASE WHEN {ident(n + '__raw')} IS NULL THEN 'null_required:{n}' END")
        cases.append(f"CASE WHEN {ident(n + '__raw')} IS NOT NULL AND {ident(n)} IS NULL THEN 'bad_type:{n}' END")
        if c.get("min") is not None:
            cases.append(f"CASE WHEN {ident(n)} < {c['min']} THEN 'out_of_range:{n}' END")
        if c.get("max") is not None:
            cases.append(f"CASE WHEN {ident(n)} > {c['max']} THEN 'out_of_range:{n}' END")
        if c.get("accepted"):
            vals = ", ".join(q(x) if isinstance(x, str) else str(x) for x in c["accepted"])
            cases.append(f"CASE WHEN {ident(n)} NOT IN ({vals}) THEN 'not_accepted:{n}' END")
    skip = 1 if res.has_header else 0
    con.execute("DROP TABLE IF EXISTS stg_cast")
    con.execute(f"""CREATE TEMP TABLE stg_cast AS
                    SELECT line_no, line, len(f) AS n_fields, {', '.join(inner)} FROM stg_split WHERE line_no > {skip}""")
    con.execute("DROP TABLE stg_split")
    con.execute(f"""CREATE OR REPLACE TEMP VIEW stg_typed AS
                    SELECT *, list_filter([{', '.join(cases)}], x -> x IS NOT NULL) AS reasons FROM stg_cast""")


def quarantine(con, dataset: str, arrival_id: int, run_id: int, source: str) -> int:
    return con.execute(f"""
        INSERT INTO quarantine.records (dataset, arrival_id, line_no, raw_line, reasons, run_id)
        SELECT {q(dataset)}, {arrival_id}, line_no, line, reasons, {run_id} FROM {source}
    """).fetchone()[0]


def set_status(con, arrival_id: int, status: str, reason: str | None, version: str | None, run_id: int) -> None:
    con.execute("""UPDATE ops.landing_manifest SET status = ?, status_reason = ?, contract_version = ?, processed_run_id = ?
                   WHERE arrival_id = ?""", [status, reason, version, run_id, arrival_id])


def gate(dataset: str, res: contracts.Resolution, con, arrival: dict, run_id: int, reports: Path) -> contracts.Resolution | None:
    """Contract gate. Returns the resolution to use, or None if the arrival is held."""
    if res.kind == "breaking":
        why = "; ".join(res.problems)
        set_status(con, arrival["arrival_id"], "HELD_SCHEMA", why, None, run_id)
        reports.mkdir(parents=True, exist_ok=True)
        out = []
        for c in res.version.columns:
            src = res.version.source_name(c["name"])
            if any(f"'{src}'" in p for p in res.problems):
                out.append(blast_radius(con, f"landing.{dataset}", src, f"breaking change detected on arrival {arrival['arrival_id']}: {why}"))
        path = reports / f"blast_radius_arrival_{arrival['arrival_id']}.md"
        path.write_text("\n\n".join(out) + "\n\nThe arrival is held (status HELD_SCHEMA). Apply a migration under "
                        "contracts/migrations/ with `ninebar contract migrate <file>` to release it.\n")
        print(f"   HELD arrival {arrival['arrival_id']}: {why}\n   blast radius -> {path}")
        return None
    if res.kind == "additive":
        v = contracts.register_additive(con, res)
        print(f"   additive drift on arrival {arrival['arrival_id']}: registered {dataset} v{v.version}")
    return res


# ---------------------------------------------------------------- CDR feed

SILVER_CDR_DDL = """
CREATE TABLE IF NOT EXISTS silver.cdr_activity (
    cell_id VARCHAR, area VARCHAR, square_id INTEGER,
    slot_start_utc TIMESTAMPTZ, local_date DATE, country_code INTEGER,
    sms_in DOUBLE, sms_out DOUBLE, call_in DOUBLE, call_out DOUBLE, internet DOUBLE,
    row_hash UBIGINT, _arrival_id INTEGER, _file_date DATE, _line_no BIGINT, _run_id INTEGER, _contract_version VARCHAR
)
"""


def ingest_cdr(con, cfg, models, arrival: dict, run_id: int, emitter: Emitter) -> dict:
    aid, area, fd = arrival["arrival_id"], arrival["area"], arrival["file_date"]
    path = Path(arrival["path"])
    tz = cfg.tz
    con.execute(SILVER_CDR_DDL)

    # late beyond horizon: compare to the newest published day, not to the wall clock,
    # so replays of old windows behave the same as live runs
    newest = con.execute("SELECT max(partition_key) FROM ops.partition_state WHERE model = 'gold.daily_cell_usage' "
                         "AND status LIKE 'PUBLISHED%'").fetchone()[0]
    horizon = cfg.raw["late_data"]["auto_restate_horizon_days"]
    if newest and fd < newest - timedelta(days=horizon) and not arrival.get("force"):
        set_status(con, aid, "HELD_LATE", f"file_date {fd} is more than {horizon} days behind newest published day {newest}", None, run_id)
        con.execute("INSERT INTO ops.late_events (arrival_id, run_id, kind, file_date, detail) VALUES (?,?,?,?,?)",
                    [aid, run_id, "held_beyond_horizon", fd, f"newest published {newest}"])
        return {"arrival_id": aid, "status": "HELD_LATE"}

    if not con.execute("SELECT count(*) FROM information_schema.tables WHERE table_schema='silver' "
                       "AND table_name='dim_cell_site'").fetchone()[0]:
        # referential checks need the cell-site dimension; leave the arrival pending
        con.execute("UPDATE ops.landing_manifest SET status_reason = 'waiting for cell_site reference' WHERE arrival_id = ?", [aid])
        return {"arrival_id": aid, "status": "WAITING_FOR_REFERENCE"}

    obs = contracts.observe(path, "\t")
    res = gate("cdr_activity", contracts.resolve(con, "cdr_activity", obs), con, arrival, run_id, cfg.path("reports"))
    if res is None:
        return {"arrival_id": aid, "status": "HELD_SCHEMA"}
    v = res.version
    extras = [c for c in v.columns if c["name"] not in CDR_BASE]
    existing = {r[0] for r in con.execute("SELECT column_name FROM information_schema.columns WHERE table_schema='silver' "
                                          "AND table_name='cdr_activity'").fetchall()}
    for c in extras:
        if c["name"] not in existing:
            con.execute(f"ALTER TABLE silver.cdr_activity ADD COLUMN {ident(c['name'])} {c['type']}")

    stage_lines(con, path, "\t")
    type_and_check(con, v, res, obs.ncols)

    # conform: the projection that defines silver columns. Also the SQL we derive ingest lineage from.
    extra_sel = "".join(f", {ident(c['name'])}" for c in extras)
    conform_sql = f"""
        SELECT {q(area)} AS area,
               {q(area)} || '-' || lpad(CAST(square_id AS VARCHAR), 5, '0') AS cell_id,
               square_id,
               CAST(epoch_ms(time_interval) AS TIMESTAMPTZ) AS slot_start_utc,
               CAST(timezone({q(tz)}, CAST(epoch_ms(time_interval) AS TIMESTAMPTZ)) AS DATE) AS local_date,
               country_code, sms_in, sms_out, call_in, call_out, internet{extra_sel},
               time_interval, line_no, line, reasons
        FROM main.stg_typed
    """
    con.execute(f"CREATE OR REPLACE TEMP VIEW stg_std AS {conform_sql}")

    check_sql = f"""
        SELECT s.* EXCLUDE (reasons),
               list_concat(s.reasons, list_filter([
                   CASE WHEN s.time_interval % 600000 <> 0 THEN 'slot_not_aligned' END,
                   CASE WHEN s.local_date NOT BETWEEN DATE '{fd}' - 1 AND DATE '{fd}' + 1 THEN 'timestamp_outside_file_window' END,
                   CASE WHEN s.square_id IS NOT NULL AND c.cell_id IS NULL THEN 'unknown_cell' END
               ], x -> x IS NOT NULL)) AS reasons
        FROM main.stg_std s
        LEFT JOIN silver.dim_cell_site c
          ON c.cell_id = s.cell_id AND s.local_date >= c.valid_from AND s.local_date < c.valid_to
    """
    con.execute("DROP TABLE IF EXISTS stg_checked")
    con.execute(f"CREATE TEMP TABLE stg_checked AS {check_sql}")

    measures = ", ".join(ident(c) for c in MEASURES + [c["name"] for c in extras])
    # exact duplicates (same key, same values): keep the first line, count the rest.
    # same key with different values: the later line wins, earlier ones go to quarantine.
    con.execute("DROP TABLE IF EXISTS stg_ranked")
    con.execute(f"""CREATE TEMP TABLE stg_ranked AS
        SELECT *, row_number() OVER (PARTITION BY {', '.join(KEY)} ORDER BY line_no DESC) AS rn
        FROM (
            SELECT * EXCLUDE (ern) FROM (
                SELECT *, row_number() OVER (PARTITION BY {', '.join(KEY)}, row_hash ORDER BY line_no) AS ern
                FROM (SELECT * EXCLUDE (reasons), hash({measures}) AS row_hash FROM stg_checked WHERE len(reasons) = 0)
            ) WHERE ern = 1
        )""")
    # a key already owned by a *different* file (out-of-order row colliding with that day's own file)
    con.execute("DROP TABLE IF EXISTS stg_clash")
    con.execute(f"""CREATE TEMP TABLE stg_clash AS
        SELECT r.line_no, any_value(s._file_date) AS owner_file_date
        FROM stg_ranked r JOIN silver.cdr_activity s
          ON s.local_date = r.local_date AND s.cell_id = r.cell_id AND s.slot_start_utc = r.slot_start_utc
         AND s.country_code = r.country_code AND NOT (s.area = {q(area)} AND s._file_date = DATE '{fd}')
        WHERE r.local_date <> DATE '{fd}'
        GROUP BY r.line_no""")
    con.execute("""CREATE OR REPLACE TEMP VIEW stg_rejects AS
        SELECT line_no, line, reasons FROM stg_checked WHERE len(reasons) > 0
        UNION ALL
        SELECT line_no, line, ['superseded_duplicate_key'] FROM stg_ranked WHERE rn > 1
        UNION ALL
        SELECT r.line_no, r.line, ['key_owned_by_file:' || c.owner_file_date] FROM stg_ranked r JOIN stg_clash c USING (line_no)
        WHERE r.rn = 1""")
    con.execute("""CREATE OR REPLACE TEMP VIEW stg_clean AS SELECT * EXCLUDE (rn) FROM stg_ranked
                   WHERE rn = 1 AND line_no NOT IN (SELECT line_no FROM stg_clash)""")

    rows_read, n_bad, n_ok = con.execute(
        "SELECT count(*), count(*) FILTER (WHERE len(reasons) > 0), count(*) FILTER (WHERE len(reasons) = 0) FROM stg_checked").fetchone()
    n_ranked, n_superseded = con.execute("SELECT count(*), count(*) FILTER (WHERE rn > 1) FROM stg_ranked").fetchone()
    n_clash = con.execute("SELECT count(*) FROM stg_clash").fetchone()[0]
    n_rej = n_bad + n_superseded + n_clash
    n_clean = n_ranked - n_superseded - n_clash
    exact = n_ok - n_ranked
    ooo = con.execute(f"SELECT count(*) FROM stg_clean WHERE local_date <> DATE '{fd}'").fetchone()[0]
    threshold = v.spec.get("quarantine_threshold", 0.01)

    if rows_read and n_rej / rows_read > threshold:
        con.begin()
        quarantine(con, "cdr_activity", aid, run_id, "stg_rejects")
        set_status(con, aid, "FAILED_QUALITY", f"{n_rej}/{rows_read} rows quarantined > {threshold:.1%}", v.version, run_id)
        con.execute("INSERT INTO ops.ingest_stats VALUES (?,?,?,?,?,?,?,0,0,0,0, now())",
                    [aid, run_id, rows_read, n_rej, exact, 0, ooo])
        con.commit()
        raise QualityGateFailed(f"arrival {aid} ({area} {fd}): {n_rej}/{rows_read} rows quarantined, threshold {threshold:.1%}")

    # hash diff against what this (area, file_date) currently contributes to silver
    con.execute("DROP TABLE IF EXISTS stg_delta")
    con.execute(f"""
        CREATE TEMP TABLE stg_delta AS
        SELECT coalesce(n.cell_id, c.cell_id) AS cell_id,
               coalesce(n.slot_start_utc, c.slot_start_utc) AS slot_start_utc,
               coalesce(n.country_code, c.country_code) AS country_code,
               coalesce(n.local_date, c.local_date) AS local_date,
               CASE WHEN c.cell_id IS NULL THEN 'I' WHEN n.cell_id IS NULL THEN 'D'
                    WHEN n.row_hash <> c.row_hash THEN 'U' ELSE 'S' END AS op
        FROM stg_clean n
        FULL OUTER JOIN (SELECT cell_id, slot_start_utc, country_code, local_date, row_hash FROM silver.cdr_activity
                         WHERE area = {q(area)} AND _file_date = DATE '{fd}') c
          USING ({', '.join(KEY)})
    """)
    ops = dict(con.execute("SELECT op, count(*) FROM stg_delta GROUP BY op").fetchall())
    only_changed = "" if arrival.get("full") else "WHERE op <> 'S'"
    dirty_dates = [r[0] for r in con.execute(f"SELECT DISTINCT local_date FROM stg_delta {only_changed}").fetchall()]
    prior = con.execute("""SELECT count(*) FROM ops.landing_manifest WHERE dataset='cdr_activity' AND area=? AND file_date=?
                           AND status='PROCESSED' AND arrival_id <> ?""", [area, fd, aid]).fetchone()[0]

    con.begin()
    try:
        if arrival.get("full"):
            # full rewrite of the source partition: same end state as the hash merge, more I/O.
            # Used when silver itself is suspect (someone edited it, a bug in an older version).
            con.execute(f"DELETE FROM silver.cdr_activity WHERE area = {q(area)} AND _file_date = DATE '{fd}'")
            con.execute("UPDATE stg_delta SET op = 'U' WHERE op = 'S'")
        else:
            con.execute(f"""
                DELETE FROM silver.cdr_activity s USING stg_delta d
                WHERE s.area = {q(area)} AND s._file_date = DATE '{fd}' AND d.op IN ('U', 'D')
                  AND s.cell_id = d.cell_id AND s.slot_start_utc = d.slot_start_utc AND s.country_code = d.country_code
            """)
        faults.checkpoint("silver_merge", f"{area}:{fd}")
        extra_cols = "".join(f", n.{ident(c['name'])}" for c in extras)
        con.execute(f"""
            INSERT INTO silver.cdr_activity BY NAME
            SELECT n.cell_id, n.area, n.square_id, n.slot_start_utc, n.local_date, n.country_code,
                   n.sms_in, n.sms_out, n.call_in, n.call_out, n.internet{extra_cols},
                   n.row_hash, {aid} AS _arrival_id, DATE '{fd}' AS _file_date, n.line_no AS _line_no,
                   {run_id} AS _run_id, {q(v.version)} AS _contract_version
            FROM stg_clean n SEMI JOIN (SELECT * FROM stg_delta WHERE op IN ('I', 'U')) d
              ON n.cell_id = d.cell_id AND n.slot_start_utc = d.slot_start_utc AND n.country_code = d.country_code
        """)
        quarantine(con, "cdr_activity", aid, run_id, "stg_rejects")
        set_status(con, aid, "PROCESSED", None, v.version, run_id)
        con.execute("INSERT INTO ops.ingest_stats VALUES (?,?,?,?,?,?,?,?,?,?,?, now())",
                    [aid, run_id, rows_read, n_rej, exact, n_clean, ooo,
                     ops.get("I", 0), ops.get("U", 0), ops.get("D", 0), ops.get("S", 0)])
        # late / correction detection: did any of these days already go out?
        for d in dirty_dates:
            pub = con.execute("SELECT version FROM ops.partition_state WHERE model = 'gold.daily_cell_usage' "
                              "AND partition_key = ? AND status LIKE 'PUBLISHED%'", [d]).fetchone()
            if pub:
                kind = "replay" if arrival.get("force") else "correction" if prior else "late_file"
                con.execute("INSERT INTO ops.late_events (arrival_id, run_id, kind, file_date, local_date, detail) VALUES (?,?,?,?,?,?)",
                            [aid, run_id, kind, fd, d, f"daily mart v{pub[0]} already published; will be restated"])
        if ooo:
            for d, n in con.execute(f"SELECT local_date, count(*) FROM stg_clean WHERE local_date <> DATE '{fd}' GROUP BY 1").fetchall():
                con.execute("INSERT INTO ops.late_events (arrival_id, run_id, kind, file_date, local_date, detail) VALUES (?,?,?,?,?,?)",
                            [aid, run_id, "out_of_order_rows", fd, d, f"{n} rows routed to {d}"])
        mdl.invalidate(con, models, "silver.cdr_activity", dirty_dates,
                       f"arrival {aid} ({area} {fd}) changed {sum(ops.get(k, 0) for k in 'IUD')} rows",
                       cfg.fortnight_anchor, cfg.fortnight_days)
        con.commit()
    except Exception:
        con.rollback()
        raise

    # lineage: derived from the conform + check SQL that just ran, mapped back to the
    # column names *as they appeared in this file* (so a renamed source column shows up)
    cat = catalog(con)
    edges = []
    for s_ds, s_col, d_ds, d_col, kind in sql_edges(conform_sql, cat, "silver.cdr_activity", {"stg_typed": "landing.cdr_activity"}):
        if d_col in ("line_no", "line", "reasons", "time_interval", "_line_no") or s_col in ("line_no", "line", "reasons", "n_fields"):
            continue
        if s_ds == "landing.cdr_activity":
            s_col = v.source_name(s_col)
        edges.append((s_ds, s_col, d_ds, d_col, kind))
    for e in sql_edges(check_sql, cat, "silver.cdr_activity", {}):
        if e[0] == "silver.dim_cell_site" and e[4] == "reference":
            edges.append(e)
    emitter.emit("ingest.cdr_activity", inputs=["landing.cdr_activity", "silver.dim_cell_site"],
                 outputs=["silver.cdr_activity", "quarantine.records"], edges=sorted(set(edges)),
                 facets={"arrival": {"arrival_id": aid, "area": area, "file_date": str(fd), "path": str(path),
                                     "contract_version": v.version, "sha256": arrival["sha256"]},
                         "dataQuality": {"rows_read": rows_read, "quarantined": n_rej, "exact_dupes": exact,
                                         "inserted": ops.get("I", 0), "updated": ops.get("U", 0),
                                         "deleted": ops.get("D", 0), "unchanged": ops.get("S", 0)}})
    return {"arrival_id": aid, "status": "PROCESSED", "rows_read": rows_read, "quarantined": n_rej,
            "exact_dupes": exact, "out_of_order": ooo, **{k: ops.get(c, 0) for k, c in
                                                          [("inserted", "I"), ("updated", "U"), ("deleted", "D"), ("unchanged", "S")]}}


# ---------------------------------------------------------------- reference data (SCD2)

DIMS = {
    "cell_site": {"table": "silver.dim_cell_site", "key": ["cell_id"]},
    "tariff_plan": {"table": "silver.dim_tariff_plan", "key": ["country_code"]},
}


def ingest_reference(con, cfg, models, arrival: dict, run_id: int, emitter: Emitter) -> dict:
    ds, aid, snap = arrival["dataset"], arrival["arrival_id"], arrival["file_date"]
    path = Path(arrival["path"])
    dim = DIMS[ds]
    table, key = dim["table"], dim["key"]

    obs = contracts.observe(path, ",")
    res = gate(ds, contracts.resolve(con, ds, obs), con, arrival, run_id, cfg.path("reports"))
    if res is None:
        return {"arrival_id": aid, "status": "HELD_SCHEMA"}
    v = res.version

    stage_lines(con, path, ",")
    type_and_check(con, v, res, obs.ncols)

    # mask anything flagged pii in the contract; the raw value never leaves landing
    salt = cfg.mask_salt
    sel = []
    attrs = []
    for c in v.columns:
        n = c["name"]
        if c.get("pii") == "mask":
            out = n.replace("_msisdn", "") + "_hash" if n.endswith("_msisdn") else n + "_hash"
            sel.append(f"CASE WHEN {ident(n)} IS NULL THEN NULL ELSE 'h:' || substr(sha256({q(salt)} || {ident(n)}), 1, 16) END AS {ident(out)}")
            attrs.append(out)
        else:
            sel.append(ident(n))
            if n not in key:
                attrs.append(n)
    conform_sql = f"SELECT {', '.join(sel)}, line_no, line, reasons FROM main.stg_typed"
    con.execute("DROP TABLE IF EXISTS stg_ref")
    con.execute(f"""CREATE TEMP TABLE stg_ref AS
        SELECT * EXCLUDE (reasons),
               list_concat(reasons, CASE WHEN count(*) OVER (PARTITION BY {', '.join(key)}) > 1
                                         THEN ['duplicate_key'] ELSE [] END) AS reasons
        FROM ({conform_sql})""")
    rows_read = con.execute("SELECT count(*) FROM stg_ref").fetchone()[0]
    n_rej = con.execute("SELECT count(*) FROM stg_ref WHERE len(reasons) > 0").fetchone()[0]
    if rows_read and n_rej / rows_read > v.spec.get("quarantine_threshold", 0.0):
        con.begin()
        quarantine(con, ds, aid, run_id, "(SELECT line_no, line, reasons FROM stg_ref WHERE len(reasons) > 0)")
        set_status(con, aid, "FAILED_QUALITY", f"{n_rej}/{rows_read} rows failed checks", v.version, run_id)
        con.commit()
        raise QualityGateFailed(f"reference {ds} snapshot {snap}: {n_rej} bad rows")

    attr_list = ", ".join(ident(a) for a in attrs)
    key_list = ", ".join(key)
    con.execute(f"""CREATE TABLE IF NOT EXISTS {table} AS
        SELECT {key_list}, {attr_list}, CAST(0 AS UBIGINT) AS attr_hash, DATE '2000-01-01' AS valid_from,
               DATE '2000-01-01' AS valid_to, true AS is_current, 0 AS _arrival_id, 0 AS _run_id
        FROM stg_ref WHERE false""")

    newest = con.execute(f"SELECT max(valid_from) FROM {table}").fetchone()[0]
    if newest and snap < newest:
        set_status(con, aid, "HELD_LATE", f"snapshot {snap} is older than the newest version ({newest}); rebuild the dimension to apply it", v.version, run_id)
        return {"arrival_id": aid, "status": "HELD_LATE"}

    con.execute("DROP TABLE IF EXISTS stg_snap")
    con.execute(f"CREATE TEMP TABLE stg_snap AS SELECT {key_list}, {attr_list}, hash({attr_list}) AS attr_hash FROM stg_ref WHERE len(reasons) = 0")
    join = " AND ".join(f"s.{k} = d.{k}" for k in key)
    changed = con.execute(f"""SELECT count(*) FROM stg_snap s JOIN {table} d ON {join} AND d.is_current
                              WHERE s.attr_hash <> d.attr_hash""").fetchone()[0]
    new = con.execute(f"SELECT count(*) FROM stg_snap s ANTI JOIN (SELECT * FROM {table} WHERE is_current) d ON {join}").fetchone()[0]
    gone = con.execute(f"SELECT count(*) FROM (SELECT * FROM {table} WHERE is_current) d ANTI JOIN stg_snap s ON {join}").fetchone()[0]

    con.begin()
    try:
        con.execute(f"""
            UPDATE {table} d SET valid_to = DATE '{snap}', is_current = false
            WHERE d.is_current AND (
                EXISTS (SELECT 1 FROM stg_snap s WHERE {join} AND s.attr_hash <> d.attr_hash)
                OR NOT EXISTS (SELECT 1 FROM stg_snap s WHERE {join}))
        """)
        con.execute(f"""
            INSERT INTO {table} BY NAME
            SELECT s.*, DATE '{snap}' AS valid_from, DATE '{FAR_FUTURE}' AS valid_to, true AS is_current,
                   {aid} AS _arrival_id, {run_id} AS _run_id
            FROM stg_snap s ANTI JOIN (SELECT * FROM {table} WHERE is_current) d ON {join}
        """)
        set_status(con, aid, "PROCESSED", None, v.version, run_id)
        con.execute("INSERT INTO ops.ingest_stats VALUES (?,?,?,?,0,?,0,?,?,?,?, now())",
                    [aid, run_id, rows_read, n_rej, rows_read - n_rej, new, changed, gone, rows_read - n_rej - new - changed])
        if changed or gone:
            # versions that start on `snap` change every already-built partition from that day on
            last = con.execute("SELECT max(partition_key) FROM ops.partition_state").fetchone()[0]
            days = [snap + timedelta(days=i) for i in range(((last - snap).days + 1) if last and last >= snap else 0)]
            mdl.invalidate(con, models, table, days, f"{ds} snapshot {snap}: {changed} changed, {gone} closed",
                           cfg.fortnight_anchor, cfg.fortnight_days, only_built=True)
        con.commit()
    except Exception:
        con.rollback()
        raise

    edges = [(s_ds, v.source_name(s_col) if s_ds == f"landing.{ds}" else s_col, d_ds, d_col, kind)
             for s_ds, s_col, d_ds, d_col, kind in sql_edges(conform_sql, catalog(con), table, {"stg_typed": f"landing.{ds}"})
             if d_col not in ("line_no", "line", "reasons") and s_col not in ("line_no", "line", "reasons", "n_fields")]
    emitter.emit(f"ingest.{ds}", inputs=[f"landing.{ds}"], outputs=[table], edges=edges,
                 facets={"arrival": {"arrival_id": aid, "snapshot": str(snap), "contract_version": v.version},
                         "scd2": {"new": new, "changed": changed, "closed": gone}})
    return {"arrival_id": aid, "status": "PROCESSED", "dataset": ds, "new": new, "changed": changed, "closed": gone}
