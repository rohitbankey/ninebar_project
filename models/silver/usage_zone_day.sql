-- model: silver.usage_zone_day
-- cadence: daily
-- partition_by: local_date
-- owner: wholesale-billing
-- freshness: rebuilt in the daily run for every local day an arrival or tariff change touched
-- quality: every country code appears once per day and area; codes with no tariff are kept as zone UNMAPPED
-- check: unique(local_date, area, country_code)
-- check: not_null(zone)
--
-- Usage by counterpart country code, priced with the tariff version valid on that
-- day (SCD2 as-of join). Tariffs are keyed by dialing prefix, longest match wins,
-- so 1204 resolves to the "1" plan unless a more specific row exists.
WITH cc AS (
    SELECT a.local_date, a.area, a.country_code,
           count(*)                                                  AS source_rows,
           sum(coalesce(a.sms_in, 0) + coalesce(a.sms_out, 0))       AS sms_units,
           sum(coalesce(a.call_in, 0) + coalesce(a.call_out, 0))     AS call_units,
           sum(coalesce(a.internet, 0))                              AS internet_units
    FROM silver.cdr_activity a
    WHERE a.local_date = {{p}}
    GROUP BY a.local_date, a.area, a.country_code
),
best AS (
    SELECT cc.local_date, cc.area, cc.country_code,
           arg_max(t.country_code, length(CAST(t.country_code AS VARCHAR))) AS matched_prefix
    FROM cc
    JOIN silver.dim_tariff_plan t
      ON starts_with(CAST(cc.country_code AS VARCHAR), CAST(t.country_code AS VARCHAR))
     AND cc.local_date >= t.valid_from AND cc.local_date < t.valid_to
    GROUP BY cc.local_date, cc.area, cc.country_code
)
SELECT cc.local_date,
       cc.area,
       cc.country_code,
       coalesce(t.zone, 'UNMAPPED')      AS zone,
       t.plan_name,
       b.matched_prefix,
       t.valid_from                      AS plan_valid_from,
       coalesce(t.rate_sms, 0)           AS rate_sms,
       coalesce(t.rate_call, 0)          AS rate_call,
       coalesce(t.rate_internet, 0)      AS rate_internet,
       cc.source_rows,
       cc.sms_units,
       cc.call_units,
       cc.internet_units
FROM cc
LEFT JOIN best b
  ON b.local_date = cc.local_date AND b.area = cc.area AND b.country_code = cc.country_code
LEFT JOIN silver.dim_tariff_plan t
  ON t.country_code = b.matched_prefix AND cc.local_date >= t.valid_from AND cc.local_date < t.valid_to
