CREATE TABLE "nyctlc-glue-database".gold_service_comparison
WITH (
    format = 'PARQUET',
    parquet_compression = 'SNAPPY',
    external_location = 's3://nyctlc-s3-gold/service_comparison/'
) AS
WITH base AS (
    SELECT
        t.service_type,
        z.borough,
        COUNT(*)                                       AS nb_courses,
        APPROX_PERCENTILE(t.total_amount, 0.5)         AS panier_median,
        SUM(t.tip_amount) / NULLIF(SUM(t.fare_amount),0) * 100.0 AS tip_pct,
        -- exclusif fhvhv : part de la remuneration chauffeur dans le prix paye
        CASE WHEN t.service_type = 'fhvhv'
             THEN SUM(t.driver_pay) / NULLIF(SUM(t.total_amount),0) * 100.0
             ELSE NULL END                             AS driver_share_pct
    FROM "nyctlc-glue-database".gold_trips_unified t
    LEFT JOIN "nyctlc-glue-database"."silver-taxi_zone_lookup_csv" z
           ON CAST(t.pu_location_id AS bigint) = z.locationid
    WHERE t.total_amount > 0
    GROUP BY t.service_type, z.borough
    HAVING COUNT(*) >= 5
)
SELECT
    service_type,
    borough,
    nb_courses,
    -- part de marche du service DANS le borough (somme = 100% par borough)
    nb_courses * 100.0 / SUM(nb_courses) OVER (PARTITION BY borough) AS part_marche_pct,
    panier_median,
    tip_pct,
    driver_share_pct
FROM base;
