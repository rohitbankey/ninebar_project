# Design notes

This is the reasoning behind the pipeline: what I chose, what I gave up, and what breaks it. The README covers how to run it.

## The problem as I read it

Option 2 asks for four things that pull against each other a bit:

1. process only what changed, but stay replayable from raw
2. let harmless schema changes through without letting harmful ones through
3. three cadences that compute different things from one base and still agree with each other
4. lineage good enough to answer "what breaks if this changes" and "where did this number come from"

The Telecom Italia data shaped most of the decisions:

- It arrives as one file per area per local day. There's no event stream, so "what changed" is decided per file, not per event.
- A file is a full snapshot of its day. A correction is a whole new file, not a patch.
- Rows are keyed by (square, 10-min slot, country code). Values are anonymised activity levels, not minutes or MB.
- Milano and Trentino both number their squares from 1 (Trentino starts at 38, but the ranges overlap). Square ids alone are not unique.
- Empty measure fields are normal. About half the values in a row are blank.

## Stack

Python 3.10 + DuckDB 1.1 + sqlglot + PyYAML. No services, no cluster.

I considered Spark, dbt and Dagster. Two months of this data is about 32 GB and 480M rows. DuckDB reads a 5M-row Milano file, types it, checks it and merges it in about 30 seconds on a laptop. Spark would add a JVM and a lot of startup cost for nothing at this size. dbt would give me the model DAG and lineage, but the brief says hand-maintained or tool-implied lineage doesn't count unless it's emitted at runtime. More importantly, I wanted the contract gate, quarantine and hash merge in the same transaction as the manifest update. That is awkward to do across dbt's boundaries.

The cost: my orchestrator is about 250 lines and has no UI. In production I'd keep the same steps and run them from Dagster or Airflow. The steps are already idempotent tasks, so that move is mechanical.

## Zones

```
data/raw_cache/          downloads; not part of the pipeline's state
data/landing/            BRONZE. immutable. one file per arrival + .manifest.json sidecar
  cdr_activity/area=MI/file_date=2013-11-07/20261002T....._<sha12>.txt   (read-only, chmod 444)
warehouse (DuckDB)
  ops.*         landing_manifest, runs, tasks, ingest_stats, contract_registry, partition_state, late_events, model_registry
  quarantine.*  records (every rejected line, with all its reasons)
  silver.*      cdr_activity (conformed, incremental), dim_cell_site / dim_tariff_plan (SCD2),
                usage_cell_day, usage_zone_day
  gold.*        daily_cell_usage, weekly_cell_rollup, weekly_day_control, fortnightly_settlement, fortnight_reconciliation
  lineage.*     events (OpenLineage-shaped JSON), column_edges, current_edges (view)
data/lineage/run_NNNNN.jsonl   same events as a file stream
```

Landing is append only. The file name includes the arrival time and content hash, so a re-delivery of "MI 2013-11-07" lands next to the first one instead of replacing it. Replays always read from landing. Nothing downstream is a source of truth for anything upstream.

## Incremental processing: hash keys per source partition

I didn't use a time watermark. The source re-delivers whole days, so "rows newer than X" is the wrong question. The question is "what is different about this day's file compared to what silver already holds for it".

The unit is the **source partition** (area, file_date). For each arrival:

1. Every clean row gets a 64-bit hash of its measure columns (`row_hash`). The business key is (cell_id, slot_start_utc, country_code).
2. The arrival is full-outer-joined to the silver rows that currently belong to the same (area, file_date):
   - key only in the new file → insert
   - key in both, hash differs → update
   - key only in silver → delete (the corrected file no longer has it)
   - same hash → nothing
3. Only the inserts, updates and deletes touch silver. Only the local days they touch are marked dirty downstream.

On the demo's corrected re-delivery of MI 2013-11-07, that comes out as 4 updated rows out of ~5M. Exactly one daily partition, one week and one cycle are rebuilt.

Why this is replay-safe: the merge is defined by "make silver's rows for this source partition equal the clean rows of this file". Running it twice gives the same result. Replaying an old arrival, backfilling a range twice, or re-running after a crash all end up in the same state. The tests check this, and the demo replays a partition twice and writes the counts to `outputs/replay_check.md`.

`--full` does a delete-all-and-reinsert for the partition instead of the diff. Same end state, more I/O. It's there for when silver itself is suspect.

### Atomicity

The silver delete/insert, the quarantine rows, the manifest status change, the ingest stats and the downstream invalidation all commit in **one DuckDB transaction**. If the process dies anywhere inside it, DuckDB rolls it back on the next open, the arrival is still `PENDING`, and the next run does it again.

The demo kills a real run with `os._exit` between the DELETE and the INSERT (`NINEBAR_CRASH_AT=silver_merge:TN:2013-11-08`). It then checks that TN 2013-11-08 has 0 rows in silver and the arrival is still PENDING, re-runs, and checks again for no duplicate keys. The run that died shows up in history as `CRASHED`.

Gold partitions work the same way: DELETE the partition, INSERT the new one, run the model's checks, update partition_state, then COMMIT. A failed check rolls the whole partition back, so a bad build never replaces a good one.

## Cleaning

| rule | what happens | why |
|---|---|---|
| read every physical line as one string, split myself | malformed lines reach the checks instead of being skipped by the CSV reader | DuckDB's `ignore_errors` would drop rows silently |
| `square_id` → `cell_id = 'MI-00001'` / `'TN-00038'` | area prefix | the two grids overlap in id space |
| `time_interval` (epoch ms, UTC) → `slot_start_utc` TIMESTAMPTZ + `local_date` (Europe/Rome) | explicit | files are cut on the local day; DST days have 138/150 slots |
| empty measure | kept NULL in silver, counted as 0 in sums | empty means "below provider threshold", not unknown |
| exact duplicate line (same key, same values) | collapsed, count stored in `ops.ingest_stats.exact_dupes_collapsed` | nothing dropped silently |
| same key, different values, same file | last line wins; earlier lines go to quarantine as `superseded_duplicate_key` | a file that contradicts itself is worth seeing |
| key already owned by another day's file | quarantined `key_owned_by_file:<date>` | one owner per key keeps the partition-replace semantics honest |
| `site_manager_msisdn` in cell-site reference | `site_manager_hash = 'h:' + sha256(salt + msisdn)[:16]` | PII never reaches silver; raw stays only in landing |

The msisdn is invented (the open data has no subscriber fields). I added it to the cell-site reference so the masking rule has something real to work on.

## Validation and quarantine

Every contract column gets checks generated from its spec: required, type (TRY_CAST), min/max, accepted values. On top of that, for CDR rows: slot aligned to 10 minutes, timestamp within ±1 day of the file date, and the cell must exist in `silver.dim_cell_site` as of that local date (referential integrity).

A line collects **all** its failing reasons, not just the first one, e.g. `['out_of_range:square_id', 'unknown_cell']`. Rejected lines go to `quarantine.records` with the raw line, line number and arrival id. Counts are in `ops.ingest_stats` and in the run output.

Quality gate: if more than 1% of an arrival's lines are quarantined (configurable per contract), the whole arrival fails as `FAILED_QUALITY`. None of its rows reach silver, but its quarantine rows are still written so the reason is visible. The run is marked FAILED, and the affected day publishes as `PUBLISHED_PARTIAL`. In the demo, MI 2013-11-04 arrives with 2% garbage and is rejected. The real file comes the next day and goes through as a late file. TN 2013-11-03 arrives with 0.1% garbage, gets quarantined, and the run stays green.

Reference data has a 0% threshold. A cell-site snapshot with a single bad row is rejected whole, because every CDR row is checked against it.

Model-level checks (`-- check: unique(...)`, `not_null(...)`, `expr(...)` in the SQL header) run inside the gold partition's transaction, before commit.

## Contracts and schema evolution

`contracts/*.yaml` seed `ops.contract_registry`. After that the registry table is authoritative, because versions are also added at runtime.

Resolution per arrival (`contracts.resolve`) samples 5,000 lines, detects a header (first line entirely non-numeric), and tries each registered version, newest first:

- **match**: every contract column is present (by header name, or by position if there is no header) and at least half of the sampled values cast to the declared type.
- **additive**: matches, plus columns the contract doesn't know. Policy: **auto**. Register `<major>.<minor+1>` with the new columns as nullable, with the type inferred from the sample. `ALTER TABLE silver.cdr_activity ADD COLUMN`, carry on. Older rows have NULL. Nothing downstream selects the new column yet, so nothing downstream changes. The demo does this with `roaming_sessions` on MI 2013-11-19 (contract v1.1).
- **breaking**: no version matches (a missing column, a rename, or a type change). Policy: **hold**. The arrival is set to `HELD_SCHEMA` with the problems in `status_reason`. A blast-radius report is generated from the lineage graph (`outputs/blast_radius_arrival_<id>.md`), and the rest of the run continues. A rename gets a hint: "missing column X, unknown column Y of the same type at the same position → probable rename X → Y".

A breaking change is applied deliberately, through a migration file (`contracts/migrations/0001_cdr_internet_renamed.yaml`) and `ninebar contract migrate <file>`. That registers a new major version whose `rename` map reads the new source name into the old canonical column, and releases held arrivals to PENDING. Silver keeps the canonical name, so no model has to change. The lineage edge from that run shows `landing.cdr_activity.internet_activity → silver.cdr_activity.internet`, so the mapping is visible.

Why the cast threshold is 50% and not 99%: a column whose type changed fails to cast almost everywhere. A column with some junk in it fails in a few places. The first is a schema problem, the second is a data-quality problem. At 99% the gate would hold files for what the quarantine should handle. The unit tests cover both cases.

What I didn't build: type-change migrations with a cast expression (only `rename` and `retype` are supported), or dropping a column on purpose. Both would be more fields in the migration file using the same mechanism.

## Reference data: SCD2

Cell sites and tariff plans arrive as full snapshots (CSV with header) and become SCD2 tables with half-open validity `[valid_from, valid_to)`, `is_current`, and `attr_hash`. A key whose attributes changed gets its current version closed on the snapshot date and a new one opened. A key missing from the snapshot is closed. Re-processing the same snapshot changes nothing.

The generated data has one change each, both inside the window:
- `cell_site` 2013-11-18: ~4% of sites upgraded to LTE800+1800, a few site managers changed
- `tariff_plan` 2013-11-20: EU internet rate cut from 0.20 to 0.12. That date is in the middle of the second billing cycle, so the fortnightly mart has to price each day with the rate valid on that day. It shows two EU rows for that cycle.

Tariff lookup is longest-prefix match on the dialing code, because the data has codes like 1204, 97259 and 39. Codes with no match get zone `UNMAPPED` with rate 0. They're kept and visible, never dropped.

A snapshot older than the newest version already applied is held (`HELD_LATE`). Inserting history into the middle of an SCD2 chain is a rebuild, not an append, and I'd rather that be a deliberate operation.

When a dimension changes, already-built partitions from the snapshot date onwards are invalidated, so the marts pick up the new attributes.

## Three cadences

| | anchor | partition key | what it computes that the others don't |
|---|---|---|---|
| daily | Europe/Rome calendar day, 00:00–24:00 local (same cut as the source files) | `local_date` | per-cell totals, slot completeness vs the day's real slot count (144, or 138/150 on DST days), peak slot, rank within area, **internet per km²** (Milano cells are 235 m, Trentino 1 km, so raw totals are not comparable across areas) |
| weekly | ISO week, Monday 00:00 Europe/Rome | `week_start` | weekday vs weekend average, weekend/weekday ratio, busiest day, week-over-week change (only when both weeks have 7 days) |
| fortnightly | 14-day billing cycle anchored **Friday 2013-11-01 00:00 Europe/Rome** | `cycle_start` | interconnect/roaming settlement by zone, priced per day with the tariff valid that day (SCD2 as-of), plus a reconciliation of the whole cycle against daily, weekly and silver |

Because the cycle starts on a Friday and weeks start on Monday, every cycle covers the tail of one ISO week, a full week, and the head of another (Nov 1–14 = Fri–Sun of week 44, all of week 45, Mon–Thu of week 46). You can't add up weeklies to get a fortnight. That is the point: the fortnightly job has to reconcile, not repeat.

**Reconciliation** (`gold.fortnight_reconciliation`) compares totals per day, which is the only grain the three share. Rows and internet volume are compared:
- silver vs daily mart, for every day of the cycle
- silver vs the weekly build's per-day control totals (`gold.weekly_day_control`), for days whose ISO week has been published
- silver vs the settlement, for the whole cycle

Days whose ISO week hasn't closed yet are listed in `days_without_closed_week`, and the status is `MATCH_WEEKLY_PENDING` rather than a failure. When that week publishes, the reconciliation partition is invalidated. The next fortnightly run rebuilds it and it goes to `MATCH`. In the demo, cycle 1 ends up `MATCH` (rebuilt once week 46 closed) and cycle 2 is `MATCH_WEEKLY_PENDING`, because the window ends before its last week does. A `MISMATCH` fails the run.

I looked at semi-monthly cycles (1st–15th, 16th–end), which is how a lot of telecom billing actually works. I went with fixed 14-day cycles because semi-monthly cycles have unequal lengths (13 to 16 days), which makes cycle-over-cycle comparisons misleading. A fixed anchor that isn't a Monday is enough to make weekly and fortnightly disagree on boundaries.

### Schedules

`ninebar tick --date T` means "it is the morning of T, run what's due":
- daily: always, for T-1 (cron: 06:00 Europe/Rome)
- weekly: if T is a Monday, for the week T-7..T-1 (07:00)
- fortnightly: if T is a cycle start, for the cycle that just closed (07:30)

`simulate` calls `tick` once per day across the window. Each cadence run first ingests anything pending. The weekly and fortnightly runs also rebuild any dirty daily partitions in their range before building their own, so they never read stale daily data.

## Late, missing and out-of-order data

**Rule:** a file is late if it arrives after the daily partition it feeds has been published.

- Missing at publish time: the day is published as `PUBLISHED_PARTIAL`. Completeness is checked against the manifest: both areas' files for every day in the partition's span must be PROCESSED.
- Arrives later, within 14 days of the newest published day: applied normally. A `late_file` event is logged (or `correction` if an earlier version of that file was already processed). The affected daily, weekly (and the next week, for week-over-week) and fortnightly partitions are rebuilt as `version + 1`. Nothing is overwritten without the version moving.
- More than 14 days behind the newest published day: held as `HELD_LATE`, with a `held_beyond_horizon` event. It needs `ninebar replay`. At that point the fortnight has gone out and restating it should be a decision, not a side effect.
- Rows inside a file whose timestamp falls on a neighbouring day: routed to their real `local_date`, counted as `out_of_order_rows` events, and the right partitions get invalidated. More than ±1 day away is quarantined (`timestamp_outside_file_window`). The real files didn't contain any, but the path is there.

The horizon is measured against the newest published day, not the wall clock. Replaying 2013 data in 2026 then behaves the same as it would have live.

In the demo: TN 2013-11-05 is withheld, Nov 5 publishes PARTIAL, and the file lands on Nov 12, after week 45 was published. Then daily Nov 5 goes to v2, week 45 to v2, week 46 is flagged for its week-over-week value, and cycle 1 hadn't been published yet so it is built once, correctly.

## Lineage

**How it's emitted.** Every job that writes data calls `Emitter.emit` after it commits:

- ingest jobs: the edges come from parsing the conform SELECT that just ran (sqlglot, resolved against the live catalog, including temp staging views). Staging columns are then mapped back to the **column names as they appeared in that file**, using the contract version resolved for that arrival. A renamed source column appears in the graph under its new name.
- model builds: the edges come from parsing the rendered SQL of the partition that was just built.

Each event is OpenLineage-shaped (job, run, inputs, outputs, `columnLineage` facet, plus facets for partition, version and data-quality counts). It is written to `data/lineage/run_NNNNN.jsonl` and to `lineage.events` / `lineage.column_edges`. Nothing about the graph is declared by hand. Delete the warehouse, re-run, and it rebuilds from what executed.

**Two kinds of edge**
- `projection`: values of the source column flow into this output column.
- `reference`: the job's query uses the column anywhere (join key, filter, group by, window, or projection). `dst_column = '*'`. If that column disappears, the whole job fails, not just one output column.

The blast-radius walk follows both kinds: it is asking what breaks. The trace walk follows only projection edges: it is asking where a value came from.

**Current graph.** `lineage.current_edges` keeps the edges from the latest event of each job. Old runs stay in `lineage.events` for history.

**Limits.** The column lineage is only as good as sqlglot's resolution. If it can't trace an output column it records `?` rather than guessing. Python-side transformations (the masking expression, hashing) are inside SQL here, so they're covered. The ingest's `row_hash` isn't in the conform SQL, so it isn't in the graph.

## Failure modes I considered

| failure | what happens | where to see it |
|---|---|---|
| process killed mid-merge | transaction rolls back; arrival stays PENDING; next run redoes it; old run marked CRASHED | `ops.runs`, demo log Nov 8 |
| transient error (I/O, lock) | task retried with backoff, up to 3 attempts | `ops.tasks` rows with RETRYING |
| download half-finished | `.part` file, renamed only when complete; landing never sees a partial file | `data/raw_cache` |
| same file delivered twice | arrival recorded as DUPLICATE (same sha256), not processed | `ops.landing_manifest` |
| file with junk lines | quarantined with reasons; gate fails the file if > 1% | `quarantine.records`, `ops.ingest_stats` |
| file missing at publish time | day published PARTIAL; restated when it lands | `ops.partition_state` |
| corrected re-delivery | hash diff applies only changed rows; partitions restated | `ops.ingest_stats`, `ops.late_events` |
| additive column | new minor contract version; column added to silver | `ops.contract_registry` |
| renamed / retyped column | arrival held; blast radius report; migration releases it | manifest `HELD_SCHEMA`, `outputs/blast_radius_*.md` |
| CDR before reference data | arrival waits (`waiting for cell_site reference`) | manifest `status_reason` |
| reference snapshot out of order | held, needs a dimension rebuild | manifest `HELD_LATE` |
| very late file | held beyond 14-day horizon, needs replay | `ops.late_events held_beyond_horizon` |
| gold model produces bad output | partition check fails, transaction rolls back, previous version stays | `ops.tasks` FAILED with check name |
| weekly built before a late file | reconciliation flags the day as weekly mismatch until the week is rebuilt | `gold.fortnight_reconciliation` |
| someone edits silver by hand | `replay` (hash diff) or `replay --full` restores it from landing | `outputs/replay_check.md` |
| sequence reuse after a crash | found in testing: DuckDB handed out the same run_id after a hard kill; switched to max()+1 | `orchestrator.start_run` comment |

## Trade-offs, stated plainly

- **One DuckDB file, one writer.** No concurrent runs. Fine for a daily batch on one machine. It would not survive two schedulers firing at once. Production fix: a run lock, or Postgres/Iceberg for state.
- **Silver holds every row.** ~7M rows/day for both areas. Two months is ~430M rows, which DuckDB handles, but the merge's per-partition scan grows with it. Production fix: physically partition silver by file_date (Parquet/Iceberg), so the merge only touches one partition's files.
- **Landing hardlinks the cache.** Cheap, but `chmod 444` on landing also makes the cache file read-only. Copying instead would double disk usage.
- **Line numbers depend on DuckDB preserving file order** in `row_number() OVER ()`. It does for a single-file CSV scan (I checked line 1,000,000 against `sed`). The trace output prints the raw line next to the parsed values, so a mismatch would be obvious.
- **Restatement is versioned but not kept.** `ops.partition_state.version` goes up and the previous rows are replaced. If consumers need old versions, gold needs to keep them (append with version, or table snapshots).
- **Generated reference data.** Cell attributes and tariffs are invented, deterministically. The geometry is real. The settlement "charge" is a shape, not money, because the source units are anonymised.
- **No data in the window before Nov 1 for the first ISO week**, so week 44 (Oct 28–Nov 3) is PARTIAL with 3 days, and its week-over-week is null by design.

## What I'd do with more time

1. Store silver and gold as partitioned Parquet (or Iceberg), so a replay rewrites one partition's files instead of deleting rows from a big table, and gold can keep old versions for audit.
2. Move orchestration into Dagster: assets = models, partitions = the three calendars, with my partition_state as its materialisation state. Keep the lineage emitter; Dagster's own lineage is table-level.
3. Push the lineage events to Marquez as real OpenLineage. The payloads are already the right shape.
4. Column-level contracts for gold too, so a model change that drops a column consumers rely on gets caught the same way a source rename does.
5. Freshness SLAs as checks: alert if daily D isn't PUBLISHED by 06:30 on D+1.
6. Run the full 124-day window regularly. I ran the 28-day default; the code doesn't assume it, but I haven't timed the full two months end to end.
