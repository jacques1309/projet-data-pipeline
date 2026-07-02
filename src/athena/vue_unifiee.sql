CREATE OR REPLACE VIEW "nyctlc-glue-database".gold_trips_unified AS
SELECT 'yellow' AS service_type, pickup_datetime, dropoff_datetime,
       pu_location_id, do_location_id, trip_distance, trip_duration_sec,
       fare_amount, tip_amount, total_amount, driver_pay, payment_type,
       year, month
FROM "nyctlc-glue-database"."silver-service_type_yellow"
UNION ALL
SELECT 'green' AS service_type, pickup_datetime, dropoff_datetime,
       pu_location_id, do_location_id, trip_distance, trip_duration_sec,
       fare_amount, tip_amount, total_amount, driver_pay, payment_type,
       year, month
FROM "nyctlc-glue-database"."silver-service_type_green"
UNION ALL
SELECT 'fhvhv' AS service_type, pickup_datetime, dropoff_datetime,
       pu_location_id, do_location_id, trip_distance, trip_duration_sec,
       fare_amount, tip_amount, total_amount, driver_pay, payment_type,
       year, month
FROM "nyctlc-glue-database"."silver-service_type_fhvhv";
