CREATE TABLE "nyctlc-glue-database".gold_demand_timeseries
WITH (
    format = 'PARQUET',
    parquet_compression = 'SNAPPY',
    external_location = 's3://nyctlc-s3-gold/demand_timeseries/'
) AS
SELECT
    CAST(t.pickup_datetime AS date)          AS date_course,
    hour(t.pickup_datetime)                  AS heure,
    day_of_week(t.pickup_datetime)           AS jour_semaine,
    t.service_type,
    COUNT(*)                                 AS nb_courses,
    SUM(t.trip_distance) / COUNT(*)          AS distance_moy,
    SUM(t.trip_duration_sec) / COUNT(*)      AS duree_moy_sec,
    -- vitesse moyenne (mph) : distance(miles) / duree(heures)
    SUM(t.trip_distance) / NULLIF(SUM(t.trip_duration_sec)/3600.0, 0) AS vitesse_moy_mph
FROM "nyctlc-glue-database".gold_trips_unified t
WHERE t.trip_duration_sec > 0
GROUP BY
    CAST(t.pickup_datetime AS date),
    hour(t.pickup_datetime),
    day_of_week(t.pickup_datetime),
    t.service_type
HAVING COUNT(*) >= 5;
 
