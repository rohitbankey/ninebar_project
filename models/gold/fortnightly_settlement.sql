-- model: gold.fortnightly_settlement
-- cadence: fortnightly
-- partition_by: cycle_start
-- owner: wholesale-billing
-- freshness: 14-day cycle anchored Friday 2013-11-01 00:00 Europe/Rome; published 07:30 the day after the cycle closes
-- quality: each day priced with the tariff version valid that day; UNMAPPED codes reported with zero rate, never dropped
-- check: unique(area, zone, plan_name, rate_sms, rate_call, rate_internet)
-- check: expr(days_present BETWEEN 1 AND 14)
--
-- Interconnect / roaming settlement estimate per billing cycle. One row per area,
-- zone and tariff version, so a mid-cycle tariff change shows up as two rows
-- instead of being blended into one rate. Units are anonymised activity, so the
-- charge is illustrative, not euros.
SELECT {{p}}                                   AS cycle_start,
       {{p}} + ({{cycle_len}} - 1)             AS cycle_end,
       z.area,
       z.zone,
       z.plan_name,
       z.rate_sms,
       z.rate_call,
       z.rate_internet,
       min(z.local_date)                       AS priced_from,
       max(z.local_date)                       AS priced_to,
       count(DISTINCT z.local_date)            AS days_present,
       count(DISTINCT z.country_code)          AS country_codes,
       sum(z.source_rows)                      AS source_rows,
       sum(z.sms_units)                        AS sms_units,
       sum(z.call_units)                       AS call_units,
       sum(z.internet_units)                   AS internet_units,
       sum(z.sms_units * z.rate_sms + z.call_units * z.rate_call + z.internet_units * z.rate_internet) AS est_charge
FROM silver.usage_zone_day z
WHERE z.local_date >= {{p}} AND z.local_date < {{p}} + {{cycle_len}}
GROUP BY z.area, z.zone, z.plan_name, z.rate_sms, z.rate_call, z.rate_internet
