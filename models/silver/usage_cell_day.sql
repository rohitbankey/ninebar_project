-- model: silver.usage_cell_day
-- cadence: daily
-- partition_by: local_date
-- owner: network-analytics
-- freshness: rebuilt in the daily run for every local day an arrival touched
-- quality: one row per cell per Europe/Rome day; slots_observed never above the day's slot count
-- check: unique(cell_id, local_date)
-- check: not_null(cell_id, area)
-- check: expr(slots_observed BETWEEN 1 AND 150)
--
-- Per-cell daily usage. The conformed base for every gold mart.
-- Empty measures in the source mean "no activity above the provider's threshold",
-- so they count as 0 in sums. They stay NULL in silver.cdr_activity.
WITH slot AS (
    SELECT a.cell_id, a.area, a.local_date, a.slot_start_utc,
           count(*)                         AS source_rows,
           sum(coalesce(a.sms_in, 0))       AS sms_in,
           sum(coalesce(a.sms_out, 0))      AS sms_out,
           sum(coalesce(a.call_in, 0))      AS call_in,
           sum(coalesce(a.call_out, 0))     AS call_out,
           sum(coalesce(a.internet, 0))     AS internet
    FROM silver.cdr_activity a
    WHERE a.local_date = {{p}}
    GROUP BY a.cell_id, a.area, a.local_date, a.slot_start_utc
)
SELECT s.local_date,
       s.cell_id,
       any_value(s.area)                       AS area,
       count(*)                                AS slots_observed,
       sum(s.source_rows)                      AS source_rows,
       sum(s.sms_in)                           AS sms_in,
       sum(s.sms_out)                          AS sms_out,
       sum(s.call_in)                          AS call_in,
       sum(s.call_out)                         AS call_out,
       sum(s.internet)                         AS internet,
       arg_max(s.slot_start_utc, s.internet)   AS peak_internet_slot_utc,
       max(s.internet)                         AS peak_internet_slot_value
FROM slot s
GROUP BY s.local_date, s.cell_id
