"""Tests de bon fonctionnement de la brique d'ingestion (source -> S3), entièrement
mockés (HTTP via `responses`, S3 via `moto`) donc exécutables sans accès réseau réel.

Ils couvrent :
  1. le dépôt réussi d'une source dans la landing zone,
  2. l'intégrité bit-à-bit entre le contenu source et l'objet S3 relu,
  3. la convention de nommage des objets (raw/<source>/dt=.../<file>.<ext>),
  4. l'ingestion des 4 sources en une fois,
  5. la détection d'un fichier vide (source cassée),
  6. la détection d'une dérive de schéma (colonnes attendues absentes),
  7. la gestion d'une source injoignable (erreurs HTTP répétées),
  8. l'idempotence d'un rejeu le même jour (pas de doublon dans la landing zone),
  9. la pagination data-fair d'Enedis (recollage des pages, en-tête gardé une fois),
  10. le garde-fou de filtre serveur (valeurs hors périmètre détectées),
  11. le garde-fou anti-boucle infinie de pagination (max_pages dépassé).
"""

from __future__ import annotations

import pytest
import responses

from ve_pipeline.ingestion import connector, s3_landing
from ve_pipeline.ingestion.config import SOURCES, SourceFile

from .conftest import TEST_BUCKET

IRVE_SAMPLE = (
    b"id_pdc_itinerance;code_insee_commune;puissance_nominale\n"
    b"FRXXXP0001;75056;22.0\n"
)
IMMAT_SAMPLE_1 = b"codgeo;epci;annee;nb_ve\n75056;200054781;2025;120\n"
IMMAT_SAMPLE_2 = b"codgeo;annee;nb_vt\n75056;2025;530\n"

# Enedis est paginé (API data-fair) : on simule 2 pages reliées par un header
# HTTP `Link: rel="next"`, comme observé en conditions réelles. En-tête et BOM
# UTF-8 calqués sur le vrai fichier récupéré le 2026-08-16 (colonnes "Année"
# et "Code Commune", pas de snake_case).
ENEDIS_NEXT_PAGE_URL = "https://opendata.enedis.fr/data-fair/api/v1/datasets/j75xc8cglfk5cp800y9uwqx9/lines?after=fakecursor"
ENEDIS_HEADER = "Année,Code Commune,conso"
BOM = b"\xef\xbb\xbf"  # BOM UTF-8, présent en tête du vrai fichier Enedis
ENEDIS_PAGE_1 = BOM + f"{ENEDIS_HEADER}\n2024,75056,123456\n".encode("utf-8")
ENEDIS_PAGE_2 = f"{ENEDIS_HEADER}\n2024,69123,98765\n".encode("utf-8")

SAMPLES = {
    "irve_consolide": IRVE_SAMPLE,
    "immatriculations_neuf": IMMAT_SAMPLE_1,
    "immatriculations_occasion": IMMAT_SAMPLE_2,
}


def _mock_all_sources() -> None:
    for source in SOURCES.values():
        for file_cfg in source.files:
            if file_cfg.paginated:
                continue  # Enedis : mocké séparément via _mock_enedis_pagination()
            responses.add(
                responses.GET,
                file_cfg.url,
                body=SAMPLES[file_cfg.key],
                status=200,
                content_type="text/csv",
            )
    _mock_enedis_pagination()


def _mock_enedis_pagination() -> None:
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
        # pas de header Link -> dernière page
    )


@responses.activate
def test_ingest_single_source_lands_in_s3(s3_client):
    _mock_all_sources()

    results = connector.ingest_source(s3_client, TEST_BUCKET, "irve")

    assert len(results) == 1
    assert results[0].status == "ok", results[0].error
    assert results[0].size_bytes == len(IRVE_SAMPLE)
    assert s3_landing.object_exists(s3_client, TEST_BUCKET, results[0].s3_key)


@responses.activate
def test_readback_matches_source_exactly(s3_client):
    _mock_all_sources()

    results = connector.ingest_source(s3_client, TEST_BUCKET, "irve")
    content = s3_landing.read_object(s3_client, TEST_BUCKET, results[0].s3_key)

    assert content == IRVE_SAMPLE


@responses.activate
def test_landing_key_naming_convention(s3_client):
    _mock_all_sources()

    results = connector.ingest_source(s3_client, TEST_BUCKET, "enedis_conso")

    assert results[0].status == "ok", results[0].error
    assert results[0].s3_key.startswith("raw/enedis_conso/dt=")
    assert results[0].s3_key.endswith("/enedis_conso_commune_2024.csv")


@responses.activate
def test_ingest_all_sources(s3_client):
    _mock_all_sources()

    results_by_source = connector.ingest_all(s3_client, TEST_BUCKET)
    flat = [r for rs in results_by_source.values() for r in rs]

    expected_count = sum(len(s.files) for s in SOURCES.values())
    assert len(flat) == expected_count
    assert all(r.status == "ok" for r in flat), [r for r in flat if r.status != "ok"]


@responses.activate
def test_empty_file_is_rejected_and_not_landed(s3_client):
    for source in SOURCES.values():
        for file_cfg in source.files:
            responses.add(responses.GET, file_cfg.url, body=b"", status=200, content_type="text/csv")

    results = connector.ingest_source(s3_client, TEST_BUCKET, "irve")

    assert results[0].status == "error"
    assert "vide" in results[0].error
    assert s3_landing.count_objects_with_prefix(s3_client, TEST_BUCKET, "raw/irve/") == 0


@responses.activate
def test_missing_expected_columns_raises_schema_drift(s3_client):
    responses.add(
        responses.GET,
        SOURCES["irve"].files[0].url,
        body=b"colonne_inattendue;autre_colonne\nvaleur1;valeur2\n",
        status=200,
        content_type="text/csv",
    )

    results = connector.ingest_source(s3_client, TEST_BUCKET, "irve")

    assert results[0].status == "error"
    assert "schéma" in results[0].error.lower() or "absentes" in results[0].error.lower()
    assert s3_landing.count_objects_with_prefix(s3_client, TEST_BUCKET, "raw/irve/") == 0


@responses.activate
def test_source_unreachable_after_retries(s3_client):
    url = SOURCES["irve"].files[0].url
    for _ in range(connector.DEFAULT_RETRIES):
        responses.add(responses.GET, url, status=503)

    results = connector.ingest_source(s3_client, TEST_BUCKET, "irve")

    assert results[0].status == "error"
    assert "Impossible de joindre" in results[0].error
    assert s3_landing.count_objects_with_prefix(s3_client, TEST_BUCKET, "raw/irve/") == 0


@responses.activate
def test_rerun_same_day_overwrites_not_duplicates(s3_client):
    _mock_all_sources()

    first = connector.ingest_source(s3_client, TEST_BUCKET, "irve")
    second = connector.ingest_source(s3_client, TEST_BUCKET, "irve")

    assert first[0].s3_key == second[0].s3_key
    assert s3_landing.count_objects_with_prefix(s3_client, TEST_BUCKET, "raw/irve/") == 1


@responses.activate
def test_bucket_is_created_if_missing(s3_client):
    _mock_all_sources()

    buckets_before = {b["Name"] for b in s3_client.list_buckets()["Buckets"]}
    assert TEST_BUCKET not in buckets_before

    connector.ingest_source(s3_client, TEST_BUCKET, "irve")

    buckets_after = {b["Name"] for b in s3_client.list_buckets()["Buckets"]}
    assert TEST_BUCKET in buckets_after


@responses.activate
def test_enedis_pagination_concatenates_pages_with_single_header(s3_client):
    _mock_all_sources()

    results = connector.ingest_source(s3_client, TEST_BUCKET, "enedis_conso")

    assert results[0].status == "ok", results[0].error
    content = s3_landing.read_object(s3_client, TEST_BUCKET, results[0].s3_key)
    expected = BOM + f"{ENEDIS_HEADER}\n2024,75056,123456\n2024,69123,98765".encode("utf-8")
    assert content == expected
    # l'en-tête n'apparaît qu'une seule fois malgré les 2 pages récupérées
    assert content.count(ENEDIS_HEADER.encode("utf-8")) == 1


@responses.activate
def test_enedis_value_filter_rejects_rows_outside_scope(s3_client):
    bad_page = BOM + f"{ENEDIS_HEADER}\n2024,75056,123456\n2023,69123,98765\n".encode("utf-8")
    enedis_url = SOURCES["enedis_conso"].files[0].url
    responses.add(responses.GET, enedis_url, body=bad_page, status=200, content_type="text/csv")

    results = connector.ingest_source(s3_client, TEST_BUCKET, "enedis_conso")

    assert results[0].status == "error"
    assert "filtre serveur" in results[0].error.lower()
    assert "2023" in results[0].error
    assert s3_landing.count_objects_with_prefix(s3_client, TEST_BUCKET, "raw/enedis_conso/") == 0


@responses.activate
def test_pagination_stops_with_clear_error_if_max_pages_exceeded(s3_client):
    # Simule une source qui ne termine jamais (chaque page renvoie un Link
    # rel=next) pour vérifier le garde-fou anti-boucle infinie.
    endless_url = "https://example.test/endless/lines"
    for i in range(5):
        page_url = endless_url if i == 0 else f"https://example.test/endless/lines?page={i + 1}"
        next_url = f"https://example.test/endless/lines?page={i + 2}"
        responses.add(
            responses.GET,
            page_url,
            body=f"col\nval{i}\n".encode(),
            status=200,
            content_type="text/csv",
            headers={"Link": f'<{next_url}>; rel="next"'},
        )

    source = SOURCES["enedis_conso"]
    runaway_file = SourceFile(
        key="runaway",
        url=endless_url,
        expected_ext="csv",
        paginated=True,
        max_pages=3,
    )
    s3_landing.ensure_bucket(s3_client, TEST_BUCKET)

    with pytest.raises(connector.SchemaDriftError, match="Pagination interrompue"):
        connector.ingest_file(s3_client, TEST_BUCKET, source, runaway_file)

    assert s3_landing.count_objects_with_prefix(s3_client, TEST_BUCKET, "raw/enedis_conso/runaway") == 0
