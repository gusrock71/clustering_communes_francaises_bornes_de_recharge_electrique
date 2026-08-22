"""CLI de chargement : lit les raw CSV déjà déposés sur S3 et les charge en base.

Exemples :
    python -m ve_pipeline.loading.cli --bucket ve-pipeline-landing --source all
    DATABASE_URL=postgresql+psycopg2://user:pass@host/db \\
        python -m ve_pipeline.loading.cli --bucket ve-pipeline-landing --source enedis_conso

Sans DATABASE_URL, charge par défaut dans un fichier SQLite local
(ve_pipeline_staging.db) — pratique pour un premier essai sans base distante.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from dataclasses import asdict
from datetime import date

from dotenv import load_dotenv

from ..ingestion import s3_landing
from ..ingestion.config import SOURCES
from . import postgres_loader


def main(argv: list[str] | None = None) -> int:
    load_dotenv()  # charge .env à la racine du projet (DATABASE_URL, AWS_*)
    parser = argparse.ArgumentParser(description="Chargement des raw S3 vers Postgres (staging)")
    parser.add_argument("--source", choices=[*SOURCES.keys(), "all"], default="all")
    parser.add_argument("--bucket", required=True, help="Bucket S3 contenant la landing zone")
    parser.add_argument("--s3-endpoint-url", default=None, help="Endpoint S3 alternatif (MinIO/LocalStack)")
    parser.add_argument("--database-url", default=None, help="DSN SQLAlchemy (sinon DATABASE_URL ou SQLite local)")
    parser.add_argument(
        "--dt",
        default=None,
        help=(
            "Partition dt=YYYY-MM-DD à forcer (ex: --dt 2026-08-16). Sans cette option, "
            "charge automatiquement la partition la plus récente disponible sur S3 pour "
            "chaque source (elle peut différer d'une source à l'autre si les runs "
            "d'ingestion n'ont pas eu lieu le même jour)."
        ),
    )
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args(argv)

    logging.basicConfig(level=args.log_level, format="%(asctime)s %(levelname)s %(message)s")

    as_of = date.fromisoformat(args.dt) if args.dt else None

    s3_client = s3_landing.get_s3_client(args.s3_endpoint_url)
    engine = postgres_loader.get_engine(args.database_url)

    if args.source == "all":
        results = postgres_loader.load_all_sources_from_s3(s3_client, engine, args.bucket, as_of)
    else:
        source = SOURCES[args.source]
        results = [
            postgres_loader.load_source_from_s3(
                s3_client, engine, args.bucket, source.name, f.key, f.expected_ext, as_of
            )
            for f in source.files
        ]

    ok = [r for r in results if r.status == "ok"]
    ko = [r for r in results if r.status == "error"]

    print(json.dumps([asdict(r) for r in results], indent=2, ensure_ascii=False))
    print(f"\n{len(ok)} table(s) chargée(s), {len(ko)} en échec.", file=sys.stderr)

    return 0 if not ko else 1


if __name__ == "__main__":
    raise SystemExit(main())
