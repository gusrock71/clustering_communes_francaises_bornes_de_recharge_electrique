"""Tests de ve_pipeline/ingestion/freshness.py (vérification de fraîcheur du
filtre serveur Enedis). HTTP mocké via `responses`, même convention que le
reste du projet (voir tests/test_streamlit_app.py)."""

from __future__ import annotations

import pytest
import responses

from ve_pipeline.ingestion import freshness
from ve_pipeline.ingestion.config import Source, SourceFile

FAKE_PAGE_URL = "https://opendata.enedis.fr/datasets/consommation-electrique-par-secteur-dactivite-commune"


def _fake_catalog_html(coverage_end_year: int) -> str:
    # Reproduit la structure texte réelle observée le 2026-08-26 (balises
    # quelconques autour du texte -- le parseur ne dépend d'aucun tag précis,
    # cf. docstring de _TextExtractor).
    return f"""
    <html><body>
      <script>var noise = "Couverture temporelle : 1 janvier 1900 - 31 decembre 1999";</script>
      <dl>
        <dt>Couverture temporelle</dt>
        <dd>1 janvier 2011 - 31 décembre {coverage_end_year}</dd>
        <dt>Fréquence de mise à jour</dt>
        <dd>Tous les ans</dd>
      </dl>
    </body></html>
    """


def _sources_with_year(year: str) -> dict[str, Source]:
    return {
        "enedis_conso": Source(
            name="enedis_conso",
            description="test",
            files=(
                SourceFile(
                    key="enedis_conso_commune_test",
                    url="https://example.invalid/lines",
                    expected_ext="csv",
                    paginated=True,
                    max_pages=1,
                    value_filter=("année", year),
                ),
            ),
        )
    }


def test_html_to_text_ignores_script_content():
    html = "<html><body><script>ignored()</script><p>Texte utile</p></body></html>"
    assert freshness.html_to_text(html) == "Texte utile"


def test_parse_latest_covered_year_extracts_end_year():
    text = freshness.html_to_text(_fake_catalog_html(2024))
    assert freshness.parse_latest_covered_year(text) == 2024


def test_parse_latest_covered_year_ignores_scripted_noise():
    # Le faux commentaire injecté dans <script> (années 1900-1999) ne doit
    # jamais être capté -- seule la vraie mention dans <dd> compte.
    text = freshness.html_to_text(_fake_catalog_html(2025))
    assert freshness.parse_latest_covered_year(text) == 2025


def test_parse_latest_covered_year_raises_on_unrecognized_format():
    with pytest.raises(freshness.EnedisFreshnessCheckError):
        freshness.parse_latest_covered_year("Cette page ne contient pas la mention attendue.")


def test_get_configured_enedis_year_reads_value_filter():
    sources = _sources_with_year("2024")
    assert freshness.get_configured_enedis_year(sources) == 2024


def test_check_enedis_freshness_not_stale_when_years_match():
    sources = _sources_with_year("2024")
    with responses.RequestsMock() as rsps:
        rsps.add(responses.GET, FAKE_PAGE_URL, body=_fake_catalog_html(2024), status=200)
        report = freshness.check_enedis_freshness(sources=sources, url=FAKE_PAGE_URL)

    assert report.configured_year == 2024
    assert report.latest_covered_year == 2024
    assert report.is_stale is False


def test_check_enedis_freshness_stale_when_newer_year_published():
    sources = _sources_with_year("2024")
    with responses.RequestsMock() as rsps:
        rsps.add(responses.GET, FAKE_PAGE_URL, body=_fake_catalog_html(2025), status=200)
        report = freshness.check_enedis_freshness(sources=sources, url=FAKE_PAGE_URL)

    assert report.latest_covered_year == 2025
    assert report.is_stale is True


def test_check_enedis_freshness_raises_on_unreachable_page():
    sources = _sources_with_year("2024")
    with responses.RequestsMock() as rsps:
        rsps.add(responses.GET, FAKE_PAGE_URL, status=500)
        with pytest.raises(freshness.EnedisFreshnessCheckError):
            freshness.check_enedis_freshness(sources=sources, url=FAKE_PAGE_URL)


def test_get_configured_enedis_year_raises_when_no_value_filter_present():
    sources = {
        "enedis_conso": Source(
            name="enedis_conso",
            description="test",
            files=(
                SourceFile(key="k", url="https://example.invalid", expected_ext="csv"),
            ),
        )
    }
    with pytest.raises(freshness.EnedisFreshnessCheckError):
        freshness.get_configured_enedis_year(sources)
