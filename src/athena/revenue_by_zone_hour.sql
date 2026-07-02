CREATE TABLE "nyctlc-glue-database".gold_revenue_by_zone_hour
WITH (
    format = 'PARQUET',
    parquet_compression = 'SNAPPY',
    external_location = 's3://nyctlc-s3-gold/revenue_by_zone_hour/'
) AS
SELECT
    t.service_type,
    z.borough,
    z.zone,
    day_of_week(t.pickup_datetime)            AS jour_semaine,   
    hour(t.pickup_datetime)                   AS heure,
    COUNT(*)                                  AS nb_courses,
    SUM(t.total_amount)                       AS revenu_total,
    SUM(t.tip_amount)                         AS tip_total,
    SUM(t.trip_distance)                      AS distance_total,
    SUM(t.trip_duration_sec)                  AS duree_total_sec,
    SUM(t.total_amount) / COUNT(*)            AS revenu_moyen,
    SUM(t.total_amount) / NULLIF(SUM(t.trip_duration_sec)/60.0, 0) AS revenu_par_min,
    SUM(t.tip_amount)   / NULLIF(SUM(t.fare_amount), 0) * 100.0    AS tip_pct,
    SUM(t.trip_distance) / COUNT(*)           AS distance_moyenne
FROM "nyctlc-glue-database".gold_trips_unified t
LEFT JOIN "nyctlc-glue-database"."silver-taxi_zone_lookup_csv" z
       ON CAST(t.pu_location_id AS bigint) = z.locationid
WHERE t.total_amount > 0
  AND t.trip_duration_sec > 0
GROUP BY
    t.service_type, z.borough, z.zone,
    day_of_week(t.pickup_datetime), hour(t.pickup_datetime)
HAVING COUNT(*) >= 5
 
