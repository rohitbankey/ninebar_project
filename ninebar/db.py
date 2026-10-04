"""Warehouse connection and the operational schema.

Everything lives in one DuckDB file. Data tables (silver/gold) are created by the
code that owns them; this module only creates the bookkeeping tables that the
rest of the pipeline relies on.
"""
from __future__ import annotations

import os
from pathlib import Path

import duckdb

DDL = """
CREATE SCHEMA IF NOT EXISTS ops;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS quarantine;
CREATE SCHEMA IF NOT EXISTS lineage;


CREATE TABLE IF NOT EXISTS ops.landing_manifest (
    arrival_id       INTEGER NOT NULL,      -- max()+1, see orchestrator.start_run
    dataset          VARCHAR NOT NULL,
    area             VARCHAR,
    file_date        DATE NOT NULL,
    path             VARCHAR NOT NULL,
    sha256           VARCHAR NOT NULL,
    bytes            BIGINT NOT NULL,
    row_count        BIGINT NOT NULL,       -- physical lines, including a header if present
    arrived_at       TIMESTAMPTZ NOT NULL,
    origin           VARCHAR NOT NULL,      -- 'dataverse', 'generated', or 'scenario:<name>'
    status           VARCHAR NOT NULL,      -- PENDING | PROCESSED | DUPLICATE | HELD_SCHEMA | HELD_LATE | FAILED_QUALITY
    status_reason    VARCHAR,
    contract_version VARCHAR,
    processed_run_id INTEGER
);

CREATE TABLE IF NOT EXISTS ops.runs (
    run_id       INTEGER NOT NULL,          -- max()+1, see orchestrator.start_run
    command      VARCHAR NOT NULL,
    cadence      VARCHAR,
    logical_date DATE,
    params       JSON,
    started_at   TIMESTAMPTZ NOT NULL,
    ended_at     TIMESTAMPTZ,
    status       VARCHAR NOT NULL,          -- RUNNING | SUCCESS | FAILED  (RUNNING after the fact = crashed)
    error        VARCHAR
);

CREATE TABLE IF NOT EXISTS ops.tasks (
    run_id     INTEGER NOT NULL,
    task       VARCHAR NOT NULL,
    attempt    INTEGER NOT NULL,
    status     VARCHAR NOT NULL,            -- SUCCESS | FAILED | RETRYING | SKIPPED
    started_at TIMESTAMPTZ NOT NULL,
    ended_at   TIMESTAMPTZ,
    detail     JSON,
    error      VARCHAR
);

CREATE TABLE IF NOT EXISTS ops.ingest_stats (
    arrival_id             INTEGER NOT NULL,
    run_id                 INTEGER NOT NULL,
    rows_read              BIGINT,
    rows_quarantined       BIGINT,
    exact_dupes_collapsed  BIGINT,
    accepted               BIGINT,
    out_of_order           BIGINT,
    inserted               BIGINT,
    updated                BIGINT,
    deleted                BIGINT,
    unchanged              BIGINT,
    recorded_at            TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS ops.contract_registry (
    dataset       VARCHAR NOT NULL,
    version       VARCHAR NOT NULL,
    spec          JSON NOT NULL,            -- columns, header policy, rename map
    source        VARCHAR NOT NULL,         -- seed | auto-additive | migration:<id>
    registered_at TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (dataset, version)
);

CREATE TABLE IF NOT EXISTS ops.applied_migrations (
    migration_id VARCHAR PRIMARY KEY,
    dataset      VARCHAR,
    new_version  VARCHAR,
    applied_at   TIMESTAMPTZ DEFAULT now(),
    released     INTEGER                    -- how many held arrivals it released
);

CREATE TABLE IF NOT EXISTS ops.partition_state (
    model         VARCHAR NOT NULL,
    partition_key DATE NOT NULL,
    status        VARCHAR NOT NULL,         -- DIRTY | PUBLISHED | PUBLISHED_PARTIAL
    version       INTEGER NOT NULL DEFAULT 0,
    built_run_id  INTEGER,
    built_at      TIMESTAMPTZ,
    dirty_reason  VARCHAR
    -- (model, partition_key) is unique; enforced by delete+insert in models.py rather than a PK,
    -- because DuckDB 1.1 raises false duplicate-key errors when indexed rows are updated
);

CREATE TABLE IF NOT EXISTS ops.late_events (
    arrival_id   INTEGER,
    run_id       INTEGER,
    kind         VARCHAR,                   -- late_file | correction | out_of_order_rows | held_beyond_horizon
    file_date    DATE,
    local_date   DATE,
    detail       VARCHAR,
    detected_at  TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS ops.model_registry (
    model           VARCHAR PRIMARY KEY,
    layer           VARCHAR,
    cadence         VARCHAR,
    partition_by    VARCHAR,
    owner           VARCHAR,
    freshness       VARCHAR,
    quality         VARCHAR,
    sql_path        VARCHAR,
    sql_sha         VARCHAR,
    upstream        VARCHAR[]
);

CREATE TABLE IF NOT EXISTS quarantine.records (
    dataset     VARCHAR NOT NULL,
    arrival_id  INTEGER NOT NULL,
    line_no     BIGINT,
    raw_line    VARCHAR,
    reasons     VARCHAR[] NOT NULL,
    run_id      INTEGER,
    quarantined_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS lineage.events (
    event_id    VARCHAR PRIMARY KEY,
    run_id      INTEGER,
    job         VARCHAR,
    event_type  VARCHAR,
    event_time  TIMESTAMPTZ,
    payload     JSON
);

CREATE TABLE IF NOT EXISTS lineage.column_edges (
    event_id    VARCHAR,
    run_id      INTEGER,
    job         VARCHAR,
    src_dataset VARCHAR,
    src_column  VARCHAR,
    dst_dataset VARCHAR,
    dst_column  VARCHAR,                    -- '*' means "used by the model" (filter/join/group), not projected
    edge_kind   VARCHAR,                    -- projection | reference
    emitted_at  TIMESTAMPTZ DEFAULT now()
);

-- The current graph: for each job, the edges from its most recent emitted event.
CREATE OR REPLACE VIEW lineage.current_edges AS
WITH latest AS (
    SELECT job, arg_max(event_id, event_time) AS event_id
    FROM lineage.events WHERE event_type = 'COMPLETE' GROUP BY job
)
SELECT DISTINCT e.job, e.src_dataset, e.src_column, e.dst_dataset, e.dst_column, e.edge_kind, e.run_id
FROM lineage.column_edges e JOIN latest l USING (job, event_id);
"""


def connect(path: Path, read_only: bool = False) -> duckdb.DuckDBPyConnection:
    path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(path), read_only=read_only)
    con.execute("SET TimeZone = 'UTC'")  # store and compare instants in UTC; local dates are computed explicitly
    # staging lives in temp tables; cap memory so a big file spills to disk instead of pushing the box into swap
    con.execute(f"SET memory_limit = '{os.environ.get('NINEBAR_MEMORY_LIMIT', '6GB')}'")
    con.execute(f"SET temp_directory = '{path.parent / 'tmp'}'")
    if not read_only:
        con.execute(DDL)
    return con
