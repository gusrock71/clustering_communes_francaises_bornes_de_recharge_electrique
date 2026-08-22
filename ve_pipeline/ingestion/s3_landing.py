"""Wrapper fin autour de boto3 pour la landing zone S3 (raw data, sans transformation).

Convention de nommage des objets :
    raw/<source>/dt=<YYYY-MM-DD>/<file_key>.<ext>

Le partitionnement par date d'ingestion (`dt=`) permet de conserver un
historique des snapshots bruts, de rejouer un jour donné, et de détecter
une source silencieusement en panne (absence de nouvelle partition).
"""

from __future__ import annotations

import os
from datetime import date, datetime, timezone

import boto3
from botocore.exceptions import ClientError


def get_s3_client(endpoint_url: str | None = None):
    """Construit un client S3 boto3.

    - Sans `endpoint_url` et avec des credentials AWS réels dans l'environnement :
      pointe vers le vrai S3 (AWS).
    - Avec `endpoint_url` (ou la variable d'env `S3_ENDPOINT_URL`) : pointe vers un
      endpoint S3-compatible local (MinIO, LocalStack).
    - Dans un contexte de test sous `moto.mock_aws()` : boto3 est intercepté par
      moto, `endpoint_url` est ignoré, aucun appel réseau réel n'est effectué.
    """
    return boto3.client(
        "s3",
        endpoint_url=endpoint_url or os.environ.get("S3_ENDPOINT_URL"),
        region_name=os.environ.get("AWS_REGION", "eu-west-3"),
    )


def ensure_bucket(s3_client, bucket: str) -> None:
    """Crée le bucket s'il n'existe pas déjà (idempotent)."""
    existing = {b["Name"] for b in s3_client.list_buckets().get("Buckets", [])}
    if bucket in existing:
        return
    region = s3_client.meta.region_name
    if region == "us-east-1":
        s3_client.create_bucket(Bucket=bucket)
    else:
        s3_client.create_bucket(
            Bucket=bucket,
            CreateBucketConfiguration={"LocationConstraint": region},
        )


def landing_key(source_name: str, file_key: str, ext: str, as_of: date | None = None) -> str:
    as_of = as_of or datetime.now(timezone.utc).date()
    return f"raw/{source_name}/dt={as_of.isoformat()}/{file_key}.{ext}"


def upload_bytes(s3_client, bucket: str, key: str, data: bytes, content_type: str) -> None:
    s3_client.put_object(Bucket=bucket, Key=key, Body=data, ContentType=content_type)


def object_exists(s3_client, bucket: str, key: str) -> bool:
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "")
        if code in ("404", "NoSuchKey", "NotFound"):
            return False
        raise


def read_object(s3_client, bucket: str, key: str) -> bytes:
    return s3_client.get_object(Bucket=bucket, Key=key)["Body"].read()


def count_objects_with_prefix(s3_client, bucket: str, prefix: str) -> int:
    resp = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    return resp.get("KeyCount", 0)


def latest_partition(s3_client, bucket: str, prefix: str) -> str | None:
    """Retourne le nom ("dt=YYYY-MM-DD") de la partition la plus récente sous un préfixe.

    Utilisé pour ne pas dépendre de la date du jour : l'ingestion (J1) et le
    chargement en base peuvent avoir lieu des jours différents, donc "today"
    n'est pas une hypothèse fiable pour retrouver les objets déjà déposés.
    """
    resp = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix, Delimiter="/")
    dts = [
        cp["Prefix"].rstrip("/").rsplit("/", 1)[-1]
        for cp in resp.get("CommonPrefixes", [])
        if cp["Prefix"].rstrip("/").rsplit("/", 1)[-1].startswith("dt=")
    ]
    return sorted(dts)[-1] if dts else None
