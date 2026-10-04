-- model: gold.weekly_cell_rollup
-- cadence: weekly
-- partition_by: week_start
-- affects_next: true
-- owner: network-analytics
-- freshness: ISO week starting Monday 00:00 Europe/Rome; published Monday 07:00 for the week that just closed
-- quality: one row per cell per week; week-over-week only when both weeks have 7 days of data
-- check: unique(cell_id, week_start)
-- check: expr(days_present BETWEEN 1 AND 7)
--
-- Weekly rollup. What it adds over the daily mart is shape, not just a bigger sum:
-- weekday vs weekend behaviour, the busiest day, and week-over-week change.
-- affects_next: a restated week also changes the next week's wow figure.
WITH d AS (
    SELECT u.cell_id, u.area, u.local_date, u.internet, u.call_in, u.call_out, u.sms_in, u.sms_out,
           CAST(date_trunc('week', u.local_date) AS DATE) AS week_start,
           isodow(u.local_date) >= 6                      AS is_weekend
    FROM silver.usage_cell_day u
    WHERE u.local_date >= {{p}} - 7 AND u.local_date < {{p}} + 7
),
w AS (
    SELECT d.week_start, d.cell_id,
           any_value(d.area)                                        AS area,
           count(*)                                                 AS days_present,
           sum(d.internet)                                          AS internet_total,
           sum(d.call_in + d.call_out)                              AS call_total,
           sum(d.sms_in + d.sms_out)                                AS sms_total,
           avg(d.internet) FILTER (WHERE NOT d.is_weekend)          AS weekday_avg_internet,
           avg(d.internet) FILTER (WHERE d.is_weekend)              AS weekend_avg_internet,
           arg_max(d.local_date, d.internet)                        AS busiest_day
    FROM d
    GROUP BY d.week_start, d.cell_id
)
SELECT c.week_start,
       c.cell_id,
       c.area,
       c.days_present,
       c.days_present = 7                                              AS is_complete_week,
       c.internet_total,
       c.call_total,
       c.sms_total,
       c.weekday_avg_internet,
       c.weekend_avg_internet,
       c.weekend_avg_internet / nullif(c.weekday_avg_internet, 0)      AS weekend_to_weekday_ratio,
       c.busiest_day,
       CASE WHEN c.days_present = 7 AND p.days_present = 7
            THEN c.internet_total / nullif(p.internet_total, 0) - 1 END AS internet_wow_change
FROM w c
LEFT JOIN w p ON p.cell_id = c.cell_id AND p.week_start = c.week_start - 7
WHERE c.week_start = {{p}}
