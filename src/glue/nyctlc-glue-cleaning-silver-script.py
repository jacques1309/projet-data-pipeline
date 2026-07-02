import sys
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
import pyspark.sql.functions as F
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.dynamicframe import DynamicFrame

glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'dataset', 'period'])
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

DATASET = args['dataset']    
PERIOD  = args['period']     
DATABASE = "nyctlc-glue-database"

# --- 1. READ depuis le Catalog (le rôle gère le déchiffrement KMS) ---
src = (glueContext.create_dynamic_frame
       .from_catalog(database=DATABASE, table_name=f"bronze-{DATASET}")
       .toDF())

# --- Schéma  silver (ordre figé) : (nom, type) ---
CANON = [
    ("service_type",         "string"),
    ("pickup_datetime",      "timestamp"),
    ("dropoff_datetime",     "timestamp"),
    ("pu_location_id",       "int"),
    ("do_location_id",       "int"),
    ("passenger_count",      "int"),     # absent en fhvhv -> NULL
    ("trip_distance",        "double"),
    ("trip_duration_sec",    "double"),
    ("fare_amount",          "double"),
    ("tip_amount",           "double"),
    ("tolls_amount",         "double"),
    ("total_amount",         "double"),
    ("congestion_surcharge", "double"),
    ("airport_fee",          "double"),
    ("payment_type",         "int"),     # absent en fhvhv -> NULL
    ("driver_pay",           "double"),  # fhvhv only (KPI driver_share P2)
    ("dispatching_base_num", "string"),  # fhvhv only -> pseudonymisé étape 4
    ("originating_base_num", "string"),  # fhvhv only -> pseudonymisé étape 4
    ("hvfhs_license_num",    "string"),  # fhvhv only -> pseudonymisé étape 4
]

# --- Mapping source -> canonique, par dataset (expressions brutes, cast centralisé) ---
MAPPING = {
    "yellow": {
        "service_type":         F.lit("yellow"),
        "pickup_datetime":      F.col("tpep_pickup_datetime"),
        "dropoff_datetime":     F.col("tpep_dropoff_datetime"),
        "pu_location_id":       F.col("pulocationid"),
        "do_location_id":       F.col("dolocationid"),
        "passenger_count":      F.col("passenger_count"),
        "trip_distance":        F.col("trip_distance"),
        "fare_amount":          F.col("fare_amount"),
        "tip_amount":           F.col("tip_amount"),
        "tolls_amount":         F.col("tolls_amount"),
        "total_amount":         F.col("total_amount"),
        "congestion_surcharge": F.col("congestion_surcharge"),
        "airport_fee":          F.col("airport_fee"),
        "payment_type":         F.col("payment_type"),
    },
    "green": {
        "service_type":         F.lit("green"),
        "pickup_datetime":      F.col("lpep_pickup_datetime"),
        "dropoff_datetime":     F.col("lpep_dropoff_datetime"),
        "pu_location_id":       F.col("pulocationid"),
        "do_location_id":       F.col("dolocationid"),
        "passenger_count":      F.col("passenger_count"),
        "trip_distance":        F.col("trip_distance"),
        "fare_amount":          F.col("fare_amount"),
        "tip_amount":           F.col("tip_amount"),
        "tolls_amount":         F.col("tolls_amount"),
        "total_amount":         F.col("total_amount"),
        "congestion_surcharge": F.col("congestion_surcharge"),
        # green n'a pas d'airport_fee -> NULL automatique
        "payment_type":         F.col("payment_type"),
    },
    "fhvhv": {
        "service_type":         F.lit("fhvhv"),
        "pickup_datetime":      F.col("pickup_datetime"),
        "dropoff_datetime":     F.col("dropoff_datetime"),
        "pu_location_id":       F.col("pulocationid"),
        "do_location_id":       F.col("dolocationid"),
        # passenger_count / payment_type absents -> NULL automatique
        "trip_distance":        F.col("trip_miles"),
        "trip_duration_sec":    F.col("trip_time"),          # déjà en secondes
        "fare_amount":          F.col("base_passenger_fare"),
        "tip_amount":           F.col("tips"),
        "tolls_amount":         F.col("tolls"),
        "congestion_surcharge": F.col("congestion_surcharge"),
        "airport_fee":          F.col("airport_fee"),
        "driver_pay":           F.col("driver_pay"),
        "dispatching_base_num": F.col("dispatching_base_num"),
        "originating_base_num": F.col("originating_base_num"),
        "hvfhs_license_num":    F.col("hvfhs_license_num"),
        # total_amount n'existe pas en fhvhv -> on le dérive (somme des composantes)
        "total_amount": (
            F.col("base_passenger_fare") + F.col("tolls") + F.col("bcf")
            + F.col("sales_tax") + F.col("congestion_surcharge")
            + F.coalesce(F.col("airport_fee"), F.lit(0.0)) + F.col("tips")
        ),
    },
}

# --- 2. ALIGN : projette vers le schéma canonique ---
def align(df, dataset):
    m = MAPPING[dataset]
    cols = [
        (m[name] if name in m else F.lit(None)).cast(typ).alias(name)
        for name, typ in CANON
    ]
    out = df.select(*cols)
    # durée : fournie pour fhvhv (trip_time), calculée pour yellow/green
    out = out.withColumn(
        "trip_duration_sec",
        F.when(F.col("trip_duration_sec").isNotNull(), F.col("trip_duration_sec"))
         .otherwise(F.col("dropoff_datetime").cast("long") - F.col("pickup_datetime").cast("long"))
         .cast("double"),
    )
    return out

aligned = align(src, DATASET)




# === ÉTAPE 3 — CLEAN (dédoublonnage + aberrants + borne temporelle) ===

period_start = F.to_timestamp(F.lit(f"{PERIOD}-01"))
period_end   = F.add_months(period_start, 1)   # 1er du mois suivant (calculé, pas codé)


valid = (
    F.col("pickup_datetime").isNotNull() &
    F.col("dropoff_datetime").isNotNull() &
    F.col("trip_distance").isNotNull() &
    F.col("trip_duration_sec").isNotNull() &
    F.col("total_amount").isNotNull() &
    F.col("pu_location_id").isNotNull() &
    F.col("do_location_id").isNotNull() &
    (F.col("pickup_datetime") >= period_start) &                          # borne dérivée de PERIOD
    (F.col("pickup_datetime") <  period_end) &                            # élimine 2002, 2007, etc.
    (F.col("dropoff_datetime") > F.col("pickup_datetime")) &              # chronologie cohérente
    (F.col("trip_duration_sec") > 0) & (F.col("trip_duration_sec") <= 86400) &  # 0 < durée ≤ 24h
    (F.col("trip_distance") > 0) & (F.col("trip_distance") <= 200) &      # 0 < distance ≤ 200 mi
    (F.col("fare_amount") >= 0) &
    (F.col("total_amount") >= 0) &
    F.col("pu_location_id").between(1, 265) &                             # zones TLC valides
    F.col("do_location_id").between(1, 265)
)

cleaned = aligned.filter(valid).dropDuplicates()



# === ÉTAPE 4 — PSEUDONYMISATION (identifiants indirects fhvhv) ===
PII_INDIRECT = ["dispatching_base_num", "originating_base_num", "hvfhs_license_num"]
PEPPER = "nyctlc-2026-silver"   

pseudo = cleaned
for c in PII_INDIRECT:
    pseudo = pseudo.withColumn(
        c,
        F.when(
            F.col(c).isNotNull(),
            F.sha2(F.concat_ws("§", F.lit(PEPPER), F.col(c)), 256)
        ).otherwise(F.lit(None).cast("string"))
    )
    
    
# === ÉTAPE 5 — DATA QUALITY ===

DQ_RULESET = """
Rules = [
    RowCount > 0,
    IsComplete "service_type",
    IsComplete "pickup_datetime",
    IsComplete "pu_location_id",
    ColumnValues "trip_distance" > 0,
    ColumnValues "trip_duration_sec" between 0 and 86401,
    ColumnValues "total_amount" >= 0,
    ColumnValues "pu_location_id" between 0 and 266,
    Completeness "fare_amount" > 0.95
]
"""

dyf = DynamicFrame.fromDF(pseudo, glueContext, "dyf_silver")

dq = EvaluateDataQuality.apply(
    frame=dyf,
    ruleset=DQ_RULESET,
    publishing_options={
        "dataQualityEvaluationContext": f"silver_{DATASET}",
        "enableDataQualityCloudWatchMetrics": True,
        "enableDataQualityResultsPublishing": True,
    },
    additional_options={"performanceTuning.caching": "CACHE_NOTHING"},
)

res = dq.toDF()
res.cache()                 # force le calcul + le publishing maintenant

failed = res.filter(F.col("Outcome") == "Failed").count()
if failed > 0:
    raise Exception(f"[DQ] {failed} règle(s) en échec sur {DATASET} — écriture silver bloquée.")
print(f"[DQ] {DATASET} : toutes les règles passent")
SILVER_PATH = "s3://nyctlc-s3-silver/trips/"

final = (pseudo
    .withColumn("year",  F.year("pickup_datetime"))
    .withColumn("month", F.month("pickup_datetime"))
)


# === ÉTAPE 6 — Ecriture idempotent ===

# Idempotence : n'écrase QUE les partitions présentes dans ce run
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# Nombre de fichiers de sortie cible par partition (≈128 Mo/fichier)
FILES_PER_PARTITION = {"fhvhv": 4, "yellow": 2, "green": 1}
n_files = FILES_PER_PARTITION.get(DATASET, 1)

(final
    .repartition(n_files)          # repartition NUMÉRIQUE : N partitions équilibrées, shuffle léger
    .write
    .mode("overwrite")
    .partitionBy("service_type", "year", "month")
    .parquet(SILVER_PATH))
    
print(f"{DATASET} -> écrit dans {SILVER_PATH}")
job.commit()