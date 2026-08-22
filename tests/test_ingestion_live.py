"""Test de bon fonctionnement de bout en bout AVEC les vraies sources externes.

Contrairement à `test_ingestion_mocked.py`, ce test appelle réellement
data.gouv.fr et data.enedis.fr. Il nécessite donc un accès réseau sortant
vers ces domaines — indisponible dans le sandbox de développement utilisé
pour écrire ce pipeline (réseau restreint par allowlist).

À exécuter sur ta machine / en CI, où le réseau sortant est ouvert :

    RUN_LIVE_TESTS=1 pytest tests/test_ingestion_live.py -v -m live

Par défaut, le dépôt se fait vers un bucket S3 mocké (moto) pour ne pas
nécessiter de vrai compte AWS juste pour valider la connectivité aux
sources. Pour tester aussi un vrai bucket S3, positionne en plus :

    AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=... \\
    VE_PIPELINE_TEST_BUCKET=mon-bucket-reel \\
    VE_PIPELINE_USE_REAL_S3=1 RUN_LIVE_TESTS=1 pytest tests/test_ingestion_live.py -v -m live
"""

from __future__ import annotations

import os

import boto3
import pytest
from moto import mock_aws

from ve_pipeline.ingestion import connector, s3_landing
from ve_pipeline.ingestion.config import SOURCES

pytestmark = pytest.mark.live

LIVE_BUCKET = os.environ.get("VE_PIPELINE_TEST_BUCKET", "ve-pipeline-landing-live-test")
USE_REAL_S3 = os.environ.get("VE_PIPELINE_USE_REAL_S3") == "1"

requires_network = pytest.mark.skipif(
    os.environ.get("RUN_LIVE_TESTS") != "1",
    reason=(
        "Tests réseau réels désactivés par défaut. Lancer avec RUN_LIVE_TESTS=1 "
        "depuis un environnement ayant un accès sortant vers data.gouv.fr et data.enedis.fr."
    ),
)


@requires_network
@pytest.mark.parametrize("source_name", list(SOURCES))
def test_real_source_lands_in_s3(source_name):
    if USE_REAL_S3:
        s3_client = boto3.client("s3", region_name=os.environ.get("AWS_REGION", "eu-west-3"))
        _run_and_assert(s3_client, source_name)
    else:
        with mock_aws():
            s3_client = boto3.client("s3", region_name=os.environ.get("AWS_REGION", "eu-west-3"))
            _run_and_assert(s3_client, source_name)


def _run_and_assert(s3_client, source_name: str) -> None:
    results = connector.ingest_source(s3_client, LIVE_BUCKET, source_name)
    for r in results:
        assert r.status == "ok", f"{r.file_key}: {r.error}"
        assert r.size_bytes > 0
        assert s3_landing.object_exists(s3_client, LIVE_BUCKET, r.s3_key)
