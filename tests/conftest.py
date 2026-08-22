import boto3
import pytest
from moto import mock_aws

TEST_BUCKET = "ve-pipeline-landing-test"
TEST_REGION = "eu-west-3"


@pytest.fixture
def s3_client(monkeypatch):
    """Client S3 branché sur un backend AWS entièrement mocké (moto).

    Aucun appel réseau réel n'est effectué : ce fixture permet de tester
    toute la logique d'upload/lecture/listing S3 de façon rapide et
    déterministe, sans dépendre d'un vrai compte AWS.
    """
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_SECURITY_TOKEN", "testing")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "testing")
    monkeypatch.setenv("AWS_REGION", TEST_REGION)
    monkeypatch.delenv("S3_ENDPOINT_URL", raising=False)

    with mock_aws():
        client = boto3.client("s3", region_name=TEST_REGION)
        yield client
