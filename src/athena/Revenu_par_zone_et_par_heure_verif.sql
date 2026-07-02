SELECT zone, service_type, SUM(revenu_total) AS revenu
FROM "nyctlc-glue-database"."gold_revenue_by_zone_hour"
WHERE zone = '"JFK Airport"'
GROUP BY zone, service_type
ORDER BY revenu DESC;