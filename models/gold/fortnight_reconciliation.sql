-- model: gold.fortnight_reconciliation
-- cadence: fortnightly
-- partition_by: cycle_start
-- owner: network-data-platform
-- freshness: same run as gold.fortnightly_settlement
-- quality: status MATCH means silver, daily mart, weekly controls and settlement agree on rows and internet volume for every covered day
-- check: unique(area)
--
-- The fortnight does not start on a Monday, so a cycle covers the tail of one ISO
-- week, one full week, and the head of another. Totals are compared per day:
--   silver.cdr_activity  vs  gold.daily_cell_usage   (every day of the cycle)
--   silver.cdr_activity  vs  gold.weekly_day_control (days already covered by a published week)
--   silver.cdr_activity  vs  gold.fortnightly_settlement (whole cycle)
-- Days whose ISO week has not closed yet are listed, not treated as errors.
WITH s AS (
    SELECT a.local_date, a.area, count(*) AS rows_, sum(coalesce(a.internet, 0)) AS internet
    FROM silver.cdr_activity a
    WHERE a.local_date >= {{p}} AND a.local_date < {{p}} + {{cycle_len}}
    GROUP BY a.local_date, a.area
),
dm AS (
    SELECT g.local_date, g.area, sum(g.source_rows) AS rows_, sum(g.internet_total) AS internet
    FROM gold.daily_cell_usage g
    WHERE g.local_date >= {{p}} AND g.local_date < {{p}} + {{cycle_len}}
    GROUP BY g.local_date, g.area
),
wk AS (
    SELECT w.local_date, w.area, sum(w.source_rows) AS rows_, sum(w.internet_total) AS internet
    FROM gold.weekly_day_control w
    WHERE w.local_date >= {{p}} AND w.local_date < {{p}} + {{cycle_len}}
    GROUP BY w.local_date, w.area
),
st AS (
    SELECT f.area, sum(f.source_rows) AS rows_, sum(f.internet_units) AS internet
    FROM gold.fortnightly_settlement f
    WHERE f.cycle_start = {{p}}
    GROUP BY f.area
),
cmp AS (
    SELECT s.area, s.local_date, s.rows_ AS s_rows, s.internet AS s_internet,
           dm.rows_ AS d_rows, dm.internet AS d_internet,
           wk.rows_ AS w_rows, wk.internet AS w_internet,
           abs(s.internet - coalesce(dm.internet, 0)) > 1e-6 * greatest(1, abs(s.internet)) OR s.rows_ <> coalesce(dm.rows_, -1) AS daily_off,
           wk.local_date IS NOT NULL AND (abs(s.internet - wk.internet) > 1e-6 * greatest(1, abs(s.internet)) OR s.rows_ <> wk.rows_) AS weekly_off
    FROM s
    LEFT JOIN dm ON dm.local_date = s.local_date AND dm.area = s.area
    LEFT JOIN wk ON wk.local_date = s.local_date AND wk.area = s.area
),
agg AS (
    SELECT c.area,
           count(*)                                                    AS days_in_silver,
           count(c.d_rows)                                             AS days_in_daily_mart,
           count(c.w_rows)                                             AS days_in_weekly_controls,
           sum(c.s_rows)                                               AS silver_rows,
           sum(c.d_rows)                                               AS daily_mart_rows,
           sum(c.w_rows)                                               AS weekly_rows_on_covered_days,
           sum(c.s_rows) FILTER (WHERE c.w_rows IS NOT NULL)           AS silver_rows_on_covered_days,
           sum(c.s_internet)                                           AS silver_internet,
           sum(c.d_internet)                                           AS daily_mart_internet,
           sum(c.w_internet)                                           AS weekly_internet_on_covered_days,
           list(c.local_date ORDER BY c.local_date) FILTER (WHERE c.daily_off)        AS daily_mismatch_days,
           list(c.local_date ORDER BY c.local_date) FILTER (WHERE c.weekly_off)       AS weekly_mismatch_days,
           list(c.local_date ORDER BY c.local_date) FILTER (WHERE c.w_rows IS NULL)   AS days_without_closed_week
    FROM cmp c
    GROUP BY c.area
)
SELECT {{p}}                                            AS cycle_start,
       a.area,
       a.days_in_silver,
       a.days_in_daily_mart,
       a.days_in_weekly_controls,
       a.silver_rows,
       a.daily_mart_rows,
       st.rows_                                         AS settlement_rows,
       a.silver_rows_on_covered_days,
       a.weekly_rows_on_covered_days,
       a.silver_internet,
       a.daily_mart_internet,
       st.internet                                      AS settlement_internet,
       a.weekly_internet_on_covered_days,
       a.daily_mismatch_days,
       a.weekly_mismatch_days,
       a.days_without_closed_week,
       CASE WHEN len(a.daily_mismatch_days) > 0
              OR len(a.weekly_mismatch_days) > 0
              OR st.rows_ IS DISTINCT FROM a.silver_rows
              OR abs(coalesce(st.internet, 0) - a.silver_internet) > 1e-6 * greatest(1, abs(a.silver_internet))
            THEN 'MISMATCH'
            WHEN len(a.days_without_closed_week) > 0 THEN 'MATCH_WEEKLY_PENDING'
            ELSE 'MATCH' END                            AS status
FROM agg a
LEFT JOIN st ON st.area = a.area
