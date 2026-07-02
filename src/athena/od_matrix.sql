CREATE TABLE "nyctlc-glue-database".gold_od_matrix
WITH (
    format = 'PARQUET',
    parquet_compression = 'SNAPPY',
    external_location = 's3://nyctlc-s3-gold/od_matrix/'
) AS
SELECT
    pz.borough                               AS pu_borough,
    dz.borough                               AS do_borough,
    t.service_type,
    COUNT(*)                                 AS nb_courses,
    SUM(t.trip_distance) / COUNT(*)          AS distance_moy,
    SUM(t.total_amount)  / COUNT(*)          AS revenu_moy
FROM "nyctlc-glue-database".gold_trips_unified t
LEFT JOIN "nyctlc-glue-database"."silver-taxi_zone_lookup_csv" pz
       ON CAST(t.pu_location_id AS bigint) = pz.locationid
LEFT JOIN "nyctlc-glue-database"."silver-taxi_zone_lookup_csv" dz
       ON CAST(t.do_location_id AS bigint) = dz.locationid
WHERE t.total_amount > 0
GROUP BY pz.borough, dz.borough, t.service_type
HAVING COUNT(*) >= 5;
