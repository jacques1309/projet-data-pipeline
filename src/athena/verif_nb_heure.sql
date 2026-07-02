SELECT heure, COUNT(*) AS nb_lignes, SUM(nb_courses) AS courses
FROM "nyctlc-glue-database"."gold_revenue_by_zone_hour"
WHERE zone IN ('"JFK Airport"', 'LaGuardia Airport')
GROUP BY heure
ORDER BY heure;