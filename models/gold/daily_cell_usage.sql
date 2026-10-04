-- model: gold.daily_cell_usage
-- cadence: daily
-- partition_by: local_date
-- owner: network-analytics
-- freshness: day D (00:00-24:00 Europe/Rome) published 06:00 on D+1; restated with version+1 if a late or corrected file lands
-- quality: one row per cell per day; slot completeness reported, rows below 0.9 flagged not dropped
-- check: unique(cell_id, local_date)
-- check: not_null(site_class, radio_band)
-- check: expr(slot_completeness <= 1.0)
--
-- Daily mart. Cells are 235 m (Milano) vs 1 km (Trentino) squares, so raw totals are
-- not comparable across areas; internet_per_km2 is the cross-area figure.
-- expected_slots is 144 except on DST change days (138 / 150).
WITH base AS (
    SELECT u.local_date, u.cell_id, u.area, c.site_class, c.radio_band, c.cell_size_m,
           u.slots_observed,
           CAST((epoch(timezone({{tz}}, CAST(u.local_date + 1 AS TIMESTAMP)))
                 - epoch(timezone({{tz}}, CAST(u.local_date AS TIMESTAMP)))) / 600 AS INTEGER) AS expected_slots,
           u.source_rows,
           u.sms_in + u.sms_out     AS sms_total,
           u.call_in + u.call_out   AS call_total,
           u.internet               AS internet_total,
           u.peak_internet_slot_utc
    FROM silver.usage_cell_day u
    JOIN silver.dim_cell_site c
      ON c.cell_id = u.cell_id AND u.local_date >= c.valid_from AND u.local_date < c.valid_to
    WHERE u.local_date = {{p}}
)
SELECT b.local_date,
       b.cell_id,
       b.area,
       b.site_class,
       b.radio_band,
       b.cell_size_m,
       b.slots_observed,
       b.expected_slots,
       b.slots_observed / b.expected_slots                               AS slot_completeness,
       b.slots_observed < 0.9 * b.expected_slots                         AS low_completeness,
       b.source_rows,
       b.sms_total,
       b.call_total,
       b.internet_total,
       b.internet_total / (b.cell_size_m * b.cell_size_m / 1e6)          AS internet_per_km2,
       rank() OVER (PARTITION BY b.area ORDER BY b.internet_total DESC)  AS internet_rank_in_area,
       timezone({{tz}}, b.peak_internet_slot_utc)                        AS peak_internet_slot_local
FROM base b
