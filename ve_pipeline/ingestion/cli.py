"""CLI d'ingestion : télécharge une ou toutes les sources et les dépose dans la landing zone S3.

Exemples :
    python -m ve_pipeline.ingestion.cli --bucket ve-pipeline-landing --source all
    python -m ve_pipeline.ingestion.cli --bucket ve-pipeline-landing --source irve
    python -m ve_pipeline.ingestion.cli --bucket landing --endpoint-url http://localhost:9000  # MinIO local
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from dataclasses import asdict

from dotenv import load_dotenv

from . import connector, s3_landing
from .config import SOURCES


def main(argv: list[str] | None = None) -> int:
    load_dotenv()  # charge .env à la racine du projet (AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY / AWS_REGION)
    parser = argparse.ArgumentParser(description="Ingestion brute des sources VE vers la landing zone S3")
    parser.add_argument("--source", choices=[*SOURCES.keys(), "all"], default="all")
    parser.add_argument("--bucket", required=True, help="Nom du bucket S3 (landing zone)")
    parser.add_argument(
        "--endpoint-url",
        default=None,
        help="Endpoint S3 alternatif (MinIO/LocalStack). Omettre pour AWS réel.",
    )
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args(argv)

    logging.basicConfig(level=args.log_level, format="%(asctime)s %(levelname)s %(message)s")
    s3_client = s3_landing.get_s3_client(args.endpoint_url)

    if args.source == "all":
        results_by_source = connector.ingest_all(s3_client, args.bucket)
        flat = [r for rs in results_by_source.values() for r in rs]
    else:
        flat = connector.ingest_source(s3_client, args.bucket, args.source)

    ok = [r for r in flat if r.status == "ok"]
    ko = [r for r in flat if r.status == "error"]

    print(json.dumps([asdict(r) for r in flat], indent=2, ensure_ascii=False))
    print(f"\n{len(ok)} fichier(s) déposé(s) avec succès, {len(ko)} en échec.", file=sys.stderr)

    return 0 if not ko else 1


if __name__ == "__main__":
    raise SystemExit(main())
