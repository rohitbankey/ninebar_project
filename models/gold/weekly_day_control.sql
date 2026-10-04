-- model: gold.weekly_day_control
-- cadence: weekly
-- partition_by: week_start
-- owner: network-analytics
-- freshness: built in the same run as gold.weekly_cell_rollup
-- quality: control totals per day of the week as the weekly build saw them; used only for reconciliation
-- check: unique(area, local_date)
--
-- Weekly and fortnightly periods do not line up (Monday vs Friday anchor), so the
-- weekly mart cannot be summed into a fortnight. This table keeps the weekly
-- build's per-day control totals so a fortnight can be reconciled day by day.
SELECT CAST(date_trunc('week', u.local_date) AS DATE) AS week_start,
       u.local_date,
       u.area,
       sum(u.source_rows)               AS source_rows,
       sum(u.internet)                  AS internet_total,
       sum(u.call_in + u.call_out)      AS call_total,
       sum(u.sms_in + u.sms_out)        AS sms_total
FROM silver.usage_cell_day u
WHERE u.local_date >= {{p}} AND u.local_date < {{p}} + 7
GROUP BY ALL
