"""Démo locale de la brique d'ingestion, sans dépendance réseau ni compte AWS.

Simule les 3 sources (contenu CSV factice mais structurellement valide) et
un bucket S3 (moto), pour montrer le pipeline d'ingestion tourner de bout
en bout : téléchargement -> validation -> dépôt -> relecture d'intégrité.

Usage :
    python3 scripts/demo_local_run.py

Ceci NE remplace PAS le test de bon fonctionnement contre les vraies
sources (voir tests/test_ingestion_live.py et le README), qui nécessite un
accès réseau sortant indisponible dans cet environnement de développement.
"""

from __future__ import annotations

import json
import sys
from dataclasses import asdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import boto3
import responses
from moto import mock_aws

from ve_pipeline.ingestion import connector, s3_landing
from ve_pipeline.ingestion.config import SOURCES

DEMO_BUCKET = "ve-pipeline-landing-demo"

SAMPLES = {
    "irve_consolide": (
        b"id_pdc_itinerance;code_insee_commune;puissance_nominale\n"
        b"FRXXXP0001;75056;22.0\n"
        b"FRXXXP0002;69123;150.0\n"
    ),
    "immatriculations_neuf": b"codgeo;epci;annee;nb_ve\n75056;200054781;2025;120\n",
    "immatriculations_occasion": b"codgeo;annee;nb_vt\n75056;2025;530\n",
}

# Enedis est paginé (API data-fair) : 2 pages factices reliées par un header
# HTTP `Link: rel="next"`, comme observé en conditions réelles. En-tête et BOM
# calqués sur le vrai fichier (colonnes "Année"/"Code Commune", pas de
# snake_case, BOM UTF-8 en tête).
ENEDIS_NEXT_PAGE_URL = "https://opendata.enedis.fr/data-fair/api/v1/datasets/j75xc8cglfk5cp800y9uwqx9/lines?after=fakecursor"
ENEDIS_HEADER = "Année,Code Commune,conso"
BOM = b"\xef\xbb\xbf"
ENEDIS_PAGE_1 = BOM + f"{ENEDIS_HEADER}\n2024,75056,123456\n".encode("utf-8")
ENEDIS_PAGE_2 = f"{ENEDIS_HEADER}\n2024,69123,98765\n".encode("utf-8")


@mock_aws
@responses.activate
def main() -> None:
    for source in SOURCES.values():
        for file_cfg in source.files:
            if file_cfg.paginated:
                continue
            responses.add(
                responses.GET,
                file_cfg.url,
                body=SAMPLES[file_cfg.key],
                status=200,
                content_type="text/csv",
            )

    enedis_url = SOURCES["enedis_conso"].files[0].url
    responses.add(
        responses.GET,
        enedis_url,
        body=ENEDIS_PAGE_1,
        status=200,
        content_type="text/csv",
        headers={"Link": f'<{ENEDIS_NEXT_PAGE_URL}>; rel="next"'},
    )
    responses.add(
        responses.GET,
        ENEDIS_NEXT_PAGE_URL,
        body=ENEDIS_PAGE_2,
        status=200,
        content_type="text/csv",
    )

    s3_client = s3_landing.get_s3_client()
    results_by_source = connector.ingest_all(s3_client, DEMO_BUCKET)

    flat = [r for rs in results_by_source.values() for r in rs]
    print(json.dumps([asdict(r) for r in flat], indent=2, ensure_ascii=False))

    print("\nContenu de la landing zone (bucket simulé) :")
    for obj in s3_client.list_objects_v2(Bucket=DEMO_BUCKET).get("Contents", []):
        print(f"  s3://{DEMO_BUCKET}/{obj['Key']}  ({obj['Size']} octets)")

    ok = sum(1 for r in flat if r.status == "ok")
    ko = sum(1 for r in flat if r.status == "error")
    print(f"\n{ok} fichier(s) déposé(s), {ko} en échec.")


if __name__ == "__main__":
    main()
