"""
Détermine la prochaine période (YYYY-MM) à traiter pour un dataset donné,
en mode backfill incrémental "un mois par exécution".
 
Logique:
  1. Query DynamoDB: dernier mois dont silver_status == DONE pour ce dataset.
  2. S'il n'existe aucun mois DONE  -> on démarre à START_PERIOD (2024-01).
     Sinon                          -> on prend le mois suivant.
  3. Si la période calculée dépasse la borne haute (aujourd'hui - PUBLICATION_LAG
     mois, à cause du délai de publication TLC) -> hasWork = False (rien à faire).
 
Entrée d'execution (depuis l'itérateur Map de Step Functions):

{
  "datasets": [
    "yellow",
    "green",
    "fhvhv"
  ],
  "datamarts": [
    {
      "drop": "DROP TABLE IF EXISTS `nyctlc-glue-database`.gold_revenue_by_zone_hour",
      "prefix": "revenue_by_zone_hour/",
      "ctas": "CREATE TABLE gold_revenue_by_zone_hour WITH (format='PARQUET', parquet_compression='SNAPPY', external_location='s3://nyctlc-s3-gold/revenue_by_zone_hour/') AS SELECT t.service_type, z.borough, z.zone, day_of_week(t.pickup_datetime) AS jour_semaine, hour(t.pickup_datetime) AS heure, COUNT(*) AS nb_courses, SUM(t.total_amount) AS revenu_total, SUM(t.tip_amount) AS tip_total, SUM(t.trip_distance) AS distance_total, SUM(t.trip_duration_sec) AS duree_total_sec, SUM(t.total_amount)/COUNT(*) AS revenu_moyen, SUM(t.total_amount)/NULLIF(SUM(t.trip_duration_sec)/60.0,0) AS revenu_par_min, SUM(t.tip_amount)/NULLIF(SUM(t.fare_amount),0)*100.0 AS tip_pct, SUM(t.trip_distance)/COUNT(*) AS distance_moyenne FROM \"nyctlc-glue-database\".gold_trips_unified t LEFT JOIN \"nyctlc-glue-database\".\"silver-taxi_zones\" z ON CAST(t.pu_location_id AS bigint)=z.locationid WHERE t.total_amount>0 AND t.trip_duration_sec>0 GROUP BY t.service_type, z.borough, z.zone, day_of_week(t.pickup_datetime), hour(t.pickup_datetime) HAVING COUNT(*)>=5"
    },
    {
      "drop": "DROP TABLE IF EXISTS `nyctlc-glue-database`.gold_service_comparison",
      "prefix": "service_comparison/",
      "ctas": "CREATE TABLE gold_service_comparison WITH (format='PARQUET', parquet_compression='SNAPPY', external_location='s3://nyctlc-s3-gold/service_comparison/') AS WITH base AS (SELECT t.service_type, z.borough, COUNT(*) AS nb_courses, APPROX_PERCENTILE(t.total_amount,0.5) AS panier_median, SUM(t.tip_amount)/NULLIF(SUM(t.fare_amount),0)*100.0 AS tip_pct, CASE WHEN t.service_type='fhvhv' THEN SUM(t.driver_pay)/NULLIF(SUM(t.total_amount),0)*100.0 ELSE NULL END AS driver_share_pct FROM \"nyctlc-glue-database\".gold_trips_unified t LEFT JOIN \"nyctlc-glue-database\".\"silver-taxi_zones\" z ON CAST(t.pu_location_id AS bigint)=z.locationid WHERE t.total_amount>0 GROUP BY t.service_type, z.borough HAVING COUNT(*)>=5) SELECT service_type, borough, nb_courses, nb_courses*100.0/SUM(nb_courses) OVER (PARTITION BY borough) AS part_marche_pct, panier_median, tip_pct, driver_share_pct FROM base"
    },
    {
      "drop": "DROP TABLE IF EXISTS `nyctlc-glue-database`.gold_demand_timeseries",
      "prefix": "demand_timeseries/",
      "ctas": "CREATE TABLE gold_demand_timeseries WITH (format='PARQUET', parquet_compression='SNAPPY', external_location='s3://nyctlc-s3-gold/demand_timeseries/') AS SELECT CAST(t.pickup_datetime AS date) AS date_course, hour(t.pickup_datetime) AS heure, day_of_week(t.pickup_datetime) AS jour_semaine, t.service_type, COUNT(*) AS nb_courses, SUM(t.trip_distance)/COUNT(*) AS distance_moy, SUM(t.trip_duration_sec)/COUNT(*) AS duree_moy_sec, SUM(t.trip_distance)/NULLIF(SUM(t.trip_duration_sec)/3600.0,0) AS vitesse_moy_mph FROM \"nyctlc-glue-database\".gold_trips_unified t WHERE t.trip_duration_sec>0 GROUP BY CAST(t.pickup_datetime AS date), hour(t.pickup_datetime), day_of_week(t.pickup_datetime), t.service_type HAVING COUNT(*)>=5"
    },
    {
      "drop": "DROP TABLE IF EXISTS `nyctlc-glue-database`.gold_od_matrix",
      "prefix": "od_matrix/",
      "ctas": "CREATE TABLE gold_od_matrix WITH (format='PARQUET', parquet_compression='SNAPPY', external_location='s3://nyctlc-s3-gold/od_matrix/') AS SELECT pz.borough AS pu_borough, dz.borough AS do_borough, t.service_type, COUNT(*) AS nb_courses, SUM(t.trip_distance)/COUNT(*) AS distance_moy, SUM(t.total_amount)/COUNT(*) AS revenu_moy FROM \"nyctlc-glue-database\".gold_trips_unified t LEFT JOIN \"nyctlc-glue-database\".\"silver-taxi_zones\" pz ON CAST(t.pu_location_id AS bigint)=pz.locationid LEFT JOIN \"nyctlc-glue-database\".\"silver-taxi_zones\" dz ON CAST(t.do_location_id AS bigint)=dz.locationid WHERE t.total_amount>0 GROUP BY pz.borough, dz.borough, t.service_type HAVING COUNT(*)>=5"
    }
  ]
}

Sortie d'execution (vers l'itérateur Map de Step Functions):

{
  "MessageId": "1cc11e15-a557-5104-986a-7cd5a67fc095",
  "SdkHttpMetadata": {
    "AllHttpHeaders": {
      "x-amzn-RequestId": [
        "6c80ac8b-b299-4ac7-a994-2f741de9fc6f"
      ],
      "connection": [
        "keep-alive"
      ],
      "Content-Length": [
        "294"
      ],
      "Date": [
        "Thu, 02 Jul 2026 06:10:48 GMT"
      ],
      "Content-Type": [
        "text/xml"
      ]
    },
    "HttpHeaders": {
      "connection": "keep-alive",
      "Content-Length": "294",
      "Content-Type": "text/xml",
      "Date": "Thu, 02 Jul 2026 06:10:48 GMT",
      "x-amzn-RequestId": "6c80ac8b-b299-4ac7-a994-2f741de9fc6f"
    },
    "HttpStatusCode": 200
  },
  "SdkResponseMetadata": {
    "RequestId": "6c80ac8b-b299-4ac7-a994-2f741de9fc6f"
  }
}

"""

import os
import boto3
from datetime import date
from boto3.dynamodb.conditions import Key

TABLE_NAME = os.environ("STATE_TABLE")
START_PERIOD = os.environ("START_PERIOD")   # borne basse du backfill (mois le plus ancien à traiter)
PUBLICATION_LAG = int(os.environ("PUBLICATION_LAG")) # délai publication TLC (mois) 

dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(TABLE_NAME)


def add_one_month(period: str) -> str:
    """'2024-12' -> '2025-01' (gère le passage d'année)."""
    year, month = map(int, period.split("-"))
    month += 1
    if month > 12:
        month = 1
        year += 1
    return f"{year:04d}-{month:02d}"


def upper_bound() -> str:
    """Dernier mois publiable = aujourd'hui - PUBLICATION_LAG mois."""
    today = date.today()
    year, month = today.year, today.month
    month -= PUBLICATION_LAG
    while month <= 0:
        month += 12
        year -= 1
    return f"{year:04d}-{month:02d}"


def last_done_period(dataset: str) -> str | None:
    """
    Renvoie le plus grand 'period' avec silver_status == DONE pour ce dataset,
    ou None si aucun. On lit en ordre décroissant (ScanIndexForward=False) et on
    s'arrête au premier DONE rencontré.
    """
    resp = table.query(
        KeyConditionExpression=Key("dataset").eq(dataset),
        ScanIndexForward=False,   # tri décroissant sur la sort key (period)
    )
    for item in resp.get("Items", []):
        if item.get("silver_status") == "DONE":
            return item["period"]
    return None


def lambda_handler(event, context):
    dataset = event["dataset"]

    last = last_done_period(dataset)
    if last is None:
        candidate = START_PERIOD
    else:
        candidate = add_one_month(last)

    # Borne haute: ne pas tenter de traiter un mois non encore publié.
    if candidate > upper_bound():
        return {
            "dataset": dataset,
            "period": None,
            "year": None,
            "month": None,
            "hasWork": False,
        }

    # On renvoie period (string "YYYY-MM") ET year/month (int), pour que chaque
    # consommateur pioche son format: la Lambda bronze veut year/month, le job
    # silver peut vouloir period pour le chemin de partition.
    year, month = map(int, candidate.split("-"))
    return {
        "dataset": dataset,
        "period": candidate,
        "year": year,
        "month": month,
        "hasWork": True,
    }