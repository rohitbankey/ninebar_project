# Runbook

One section per failure class. Each one says what you'll see, how to confirm it, and what to do. Commands assume the repo root. `ninebar` means `python3 -m ninebar`.

Two commands are worth running first, whatever the problem:

```
ninebar runs                 # last 30 runs: status, retries, first 80 chars of the error
ninebar status               # manifest by status, partitions by status, quarantine by reason, late events
```

and the SQL escape hatch, for anything else:

```
ninebar lineage sql "SELECT * FROM ops.landing_manifest WHERE status <> 'PROCESSED'"
```

---

## 1. A run crashed (process died, OOM, machine rebooted)

**You see:** a run stuck in `RUNNING` in `ninebar runs`, or `CRASHED` if a newer run has started since.

**Confirm nothing half-written got through:**
```
ninebar lineage sql "SELECT area, file_date, status FROM ops.landing_manifest WHERE status = 'PENDING'"
ninebar lineage sql "SELECT local_date, count(*) FROM (SELECT local_date, cell_id, slot_start_utc, country_code FROM silver.cdr_activity GROUP BY ALL HAVING count(*) > 1) GROUP BY 1"
```
The second query should return nothing. Each arrival's silver merge, quarantine rows and manifest update are one transaction, so an interrupted arrival is still PENDING with nothing in silver.

**Do:** run the same tick again: `ninebar tick --date <morning>`. It marks the dead run CRASHED and picks up where it stopped. Nothing to clean up by hand.

## 2. A task failed, then succeeded on retry

**You see:** `retries > 0` in `ninebar runs`; `ninebar runs --tasks <run_id>` shows a `RETRYING` row followed by `SUCCESS`.

**Do:** nothing, unless it keeps happening. Retries are for `TransientError` and DuckDB I/O errors (3 attempts, exponential backoff, see `config/pipeline.yaml`). Anything else fails straight away, because retrying a logic error just fails three times.

## 3. An arrival failed the quality gate

**You see:** run status `FAILED`, error `QUALITY GATE: arrival N (...) X/Y rows quarantined, threshold 1.0%`. Manifest status `FAILED_QUALITY`. The day is `PUBLISHED_PARTIAL`.

**Look at why:**
```
ninebar lineage sql "SELECT unnest(reasons) AS reason, count(*) FROM quarantine.records WHERE arrival_id = N GROUP BY 1 ORDER BY 2 DESC"
ninebar lineage sql "SELECT line_no, reasons, raw_line FROM quarantine.records WHERE arrival_id = N LIMIT 20"
```

**Do:** either
- get a clean re-delivery from the source, and land it (`ninebar land-file <path> --area MI --date 2013-11-04`). The next run treats it as a late file and restates the day. Or
- if the rows really are acceptable (e.g. a range in the contract is too tight), fix the contract, then `ninebar replay --area MI --date 2013-11-04`.

Don't lower the threshold to get one file through. Change it in the contract on purpose, or not at all.

## 4. Schema drift

### Additive (new column)
**You see:** `additive drift on arrival N: registered cdr_activity v1.x` in the run output. The run is green.

**Do:** nothing is required. The column is in `silver.cdr_activity`, NULL for older rows, and nothing downstream uses it yet. `ninebar contract show cdr_activity` lists the versions. Use the column in a model when someone needs it.

### Breaking (rename, removed column, type change)
**You see:** `HELD arrival N: missing column 'internet' ...; probable rename: 'internet' -> 'internet_activity'`. Manifest `HELD_SCHEMA`. Run `FAILED`. The day is `PUBLISHED_PARTIAL`. A report at `outputs/blast_radius_arrival_N.md`.

**Do:**
1. Read the blast-radius report. It lists every silver/gold model and column that reads the affected column, with owner, cadence, freshness promise and quality expectation. Tell those owners.
2. Decide. If the source really changed, write a migration in `contracts/migrations/` (copy `0001_cdr_internet_renamed.yaml`; `rename` maps new source name → canonical name; `retype` changes a type).
3. `ninebar contract migrate contracts/migrations/000N_....yaml`. This registers a new major version and releases the held arrivals to PENDING.
4. The next run loads them as late files and restates the affected days.

To check a file before landing it: `ninebar contract check <file>`.

## 5. A file is missing or late

**You see:** a day `PUBLISHED_PARTIAL` in `ninebar status`. When the file finally lands: `late_file` in `ops.late_events`, and `restated v2` lines in the run output.

**Do:** nothing, as long as it lands within 14 days of the newest published day. It is applied and the daily, weekly and fortnightly partitions it touches are rebuilt with version + 1.

If it's later than that, it lands as `HELD_LATE` (`held_beyond_horizon` event). The fortnight it belongs to has probably gone out already. If you do want to restate it:
```
ninebar replay --area TN --date 2013-11-05
ninebar run weekly --date <week_start>          # rebuild dirty weeks
ninebar run fortnightly --date <cycle_start>    # rebuild the cycle and re-reconcile
```

## 6. A corrected file was re-delivered

**You see:** `correction` in `ops.late_events`. In the run output, the ingest line shows a small `~N` (updated) count and `=M` unchanged, e.g. `+0 ~4 -0 =4,9xx,xxx`.

**Do:** nothing. Only changed rows are applied, and only the days they touch are restated. To see exactly what changed: `SELECT * FROM ops.ingest_stats WHERE arrival_id = N`.

## 7. Reconciliation mismatch

**You see:** the fortnightly run is `FAILED` with `reconciliation mismatch <cycle> <area>: daily [...] weekly [...]`.

**Look:**
```
ninebar lineage sql "SELECT * FROM gold.fortnight_reconciliation WHERE cycle_start = '2013-11-15'"
ninebar reconcile          # row counts landing -> silver -> silver.usage_cell_day -> gold.daily_cell_usage, per arrival and per day
```
`daily_mismatch_days` lists the days where silver and the daily mart disagree; `weekly_mismatch_days` the days where the weekly controls disagree.

**Usual causes and fixes:**
- a weekly partition built before a late file and not rebuilt since → `ninebar run weekly --date <week_start>`, then `ninebar run fortnightly --date <cycle_start>`
- a daily partition still DIRTY → `ninebar run daily --date <day>`
- silver edited outside the pipeline → `ninebar replay --area X --date D --full`

`MATCH_WEEKLY_PENDING` is not an error. It means some days of the cycle are in an ISO week that hasn't closed. It turns into `MATCH` on the first fortnightly run after that week publishes.

## 8. Someone says a number is wrong

**Do:** trace it.
```
ninebar trace --model gold.daily_cell_usage --key cell_id=MI-05060,local_date=2013-11-07 --metric internet_total
```
This prints the lineage path from the gold column down to the landing column (from the emitted graph), the number of silver rows behind the value, and the top contributing source records. Each record comes with its arrival, origin (dataverse / scenario / re-delivery), contract version, landing file, line number, and the **raw line** read back from the immutable landing file.

If the culprit is a bad source record: get a correction, land it (section 6). If it's a bad transformation: fix the model SQL, then `ninebar backfill --from ... --to ...` to rebuild.

## 9. Rebuild a range (backfill)

```
ninebar backfill --from 2013-11-14 --to 2013-11-16          # hash merge: only differences are written
ninebar backfill --from 2013-11-14 --to 2013-11-16 --full   # rewrite each partition from landing
```
For each (area, day) it replays the latest processed arrival from landing, then rebuilds the dirty daily partitions, closed weeks and closed cycles. Running it twice is safe: the second pass finds every row unchanged (`outputs/backfill_check.md`).

## 10. Reference data problems

- **CDR arrived before any cell-site snapshot:** arrival stays PENDING with reason `waiting for cell_site reference`. Land the snapshot; the next run picks both up.
- **Snapshot older than the current version:** `HELD_LATE`. Applying it means rewriting SCD2 history. Rebuild the dimension from all snapshots in landing in order (drop `silver.dim_*`, set the snapshots' manifest status to PENDING, run), then backfill the affected range.
- **Snapshot with any bad row:** rejected whole (`FAILED_QUALITY`, 0% threshold). Every CDR row is checked against this table, so a partial dimension is worse than an old one.
