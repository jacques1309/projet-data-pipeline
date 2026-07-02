import os
import urllib.request
import boto3
from botocore.exceptions import ClientError

BRONZE_BUCKET = os.environ["BRONZE_BUCKET"]

TLC_BASE_URL = os.environ.get(
    "TLC_BASE_URL", "https://d37ci6vzurychx.cloudfront.net/trip-data"
)


DATASET_FILE_PREFIX = {
    "yellow": "yellow_tripdata",
    "green": "green_tripdata",
    "fhvhv": "fhvhv_tripdata",
}


def parse_period(period):
    """'2024-07' -> (2024, 7)."""
    year, month = period.split("-")
    return int(year), int(month)


def source_url(dataset, year, month):
    """URL source TLC pour un dataset/année/mois donné."""
    prefix = DATASET_FILE_PREFIX[dataset]
    return f"{TLC_BASE_URL}/{prefix}_{year:04d}-{month:02d}.parquet"


def bronze_key(dataset, year, month):
    """
    Clé S3 destination, partitionnée (dataset/year=/month=)
    pour être directement exploitable par le crawler Glue.
    """
    prefix = DATASET_FILE_PREFIX[dataset]
    return (
        f"{dataset}/"
        f"year={year:04d}/month={month:02d}/"
        f"{prefix}_{year:04d}-{month:02d}.parquet"
    )


s3 = boto3.client("s3")


def _already_ingested(bucket, key):
    """Idempotence: skip si l'objet existe deja dans le bucket bronze"""
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            return False
        raise


def lambda_handler(event, context):
    dataset = event["dataset"]

    # Source de verite : 'period' (YYYY-MM) envoye par GetNextPeriod.
    # Retrocompat : accepte encore year/month separes.
    if event.get("period"):
        year, month = parse_period(event["period"])
        period = event["period"]
    else:
        year = int(event["year"])
        month = int(event["month"])
        period = f"{year:04d}-{month:02d}"

    if dataset not in DATASET_FILE_PREFIX:
        raise ValueError(
            f"dataset invalide: {dataset!r}. "
            f"Attendu: {list(DATASET_FILE_PREFIX)}"
        )

    bucket = BRONZE_BUCKET
    key = bronze_key(dataset, year, month)
    url = source_url(dataset, year, month)

    if _already_ingested(bucket, key):
        return {
            "dataset": dataset,
            "period": period,
            "status": "skipped",
            "s3_uri": f"s3://{bucket}/{key}",
        }

    with urllib.request.urlopen(url, timeout=60) as resp:
        s3.upload_fileobj(resp, bucket, key)

    return {
        "dataset": dataset,
        "period": period,
        "status": "ingested",
        "s3_uri": f"s3://{bucket}/{key}",
    }