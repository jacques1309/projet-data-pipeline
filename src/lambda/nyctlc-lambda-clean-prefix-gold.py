"""
Vide un prefixe S3 gold avant un CTAS Athena.
 
Pourquoi : DROP TABLE en Athena retire la table du catalogue mais LAISSE les
fichiers Parquet dans S3. Au CTAS suivant, Athena refuse d'ecrire dans un
external_location non vide (HIVE_PATH_ALREADY_EXISTS). On nettoie donc le
prefixe avant chaque reconstruction.
 
Entree (depuis Step Functions, Map BuildDatamarts) :
  { "prefix": "revenue_by_zone_hour/" }
 
Sortie :
  { "deleted": 12, "prefix": "revenue_by_zone_hour/" }
 
Variable d'environnement attendue :
  GOLD_BUCKET = nyctlc-s3-gold
"""

import os
import boto3

GOLD_BUCKET = os.environ("GOLD_BUCKET")

s3 = boto3.client("s3")


def lambda_handler(event, context):
    prefix = event["prefix"].lstrip("/")

    paginator = s3.get_paginator("list_objects_v2")
    deleted = 0
    to_delete = []

    for page in paginator.paginate(Bucket=GOLD_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            to_delete.append({"Key": obj["Key"]})
            # DeleteObjects traite max 1000 cles par appel
            if len(to_delete) == 1000:
                s3.delete_objects(Bucket=GOLD_BUCKET, Delete={"Objects": to_delete})
                deleted += len(to_delete)
                to_delete = []

    if to_delete:
        s3.delete_objects(Bucket=GOLD_BUCKET, Delete={"Objects": to_delete})
        deleted += len(to_delete)

    return {"deleted": deleted, "prefix": prefix}