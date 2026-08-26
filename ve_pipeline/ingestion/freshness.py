"""Vérification de fraîcheur du filtre serveur Enedis (`value_filter` dans
`config.py`, actuellement `("année", "2024")`).

Contexte : le dataset Enedis (`consommation-electrique-par-secteur-dactivite-
commune`) est republié une fois par an (confirmé le 2026-08-26 sur la page
catalogue officielle : "Fréquence de mise à jour : Tous les ans"). Notre
connecteur (`connector.py`) ne télécharge qu'une seule année à la fois, fixée
en dur dans `config.py` via `qs=annee:2024` -- rien ne prévient
automatiquement ce pipeline quand Enedis publie une nouvelle année (2025,
puis 2026...). Sans vérification explicite, le filtre resterait figé sur
2024 indéfiniment, même après une nouvelle publication.

Choix technique -- pourquoi lire la page catalogue HTML plutôt que l'API
data-fair : l'API `/data-fair/api/v1/datasets/.../lines` (celle qu'utilise
`connector.py` pour le téléchargement paginé réel) s'est révélée injoignable
depuis l'environnement d'assistance utilisé pour écrire ce module (timeout /
connexion refusée sur `/data-fair/api/v1/...`, alors que la page catalogue
publique `opendata.enedis.fr/datasets/...` répond normalement) -- cf. mémoire
projet pour le précédent similaire déjà rencontré sur data.gouv.fr/
data.enedis.fr. La page catalogue affiche une "Couverture temporelle"
explicite (ex: "1 janvier 2011 - 31 décembre 2024") qui donne directement la
dernière année couverte, sans avoir à paginer le jeu de données complet
(3,47M lignes) juste pour connaître son année la plus récente.

Limite assumée : ce module dépend du texte affiché sur une page HTML
publique, pas d'un contrat d'API stable -- si Enedis change significativement
la formulation de "Couverture temporelle", `parse_latest_covered_year`
échouera explicitement (`EnedisFreshnessCheckError`) plutôt que de renvoyer
une année incorrecte en silence.
"""

from __future__ import annotations

import re
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from html.parser import HTMLParser

import requests

from .config import SOURCES

DEFAULT_ENEDIS_DATASET_PAGE_URL = (
    "https://opendata.enedis.fr/datasets/consommation-electrique-par-secteur-dactivite-commune"
)
DEFAULT_TIMEOUT = 15

# Capture la dernière année d'une plage "Couverture temporelle" du type
# "1 janvier 2011 - 31 décembre 2024" (mois en toutes lettres, tiret normal
# ou cadratin). Insensible à la casse par prudence, le texte réel observé le
# 2026-08-26 est en minuscules sur "Couverture temporelle".
_COVERAGE_PATTERN = re.compile(
    r"Couverture temporelle\s*:?\s*\d{1,2}\s+\S+\s+\d{4}\s*[-–]\s*\d{1,2}\s+\S+\s+(\d{4})",
    re.IGNORECASE,
)


class EnedisFreshnessCheckError(RuntimeError):
    """La vérification n'a pas pu aboutir (page injoignable, ou format de la
    page catalogue non reconnu). Volontairement distincte d'un simple
    "pas de nouvelle donnée" : on ne veut jamais confondre "vérifié, à jour"
    avec "la vérification elle-même a échoué"."""


class _TextExtractor(HTMLParser):
    """Extracteur de texte minimal (stdlib, pas de dépendance BeautifulSoup)
    -- ignore le contenu de <script>/<style>, concatène le reste. Suffisant
    ici : on ne dépend d'aucune structure de balise précise, seulement du
    texte visible, ce qui rend le parsing robuste à un changement de mise en
    page côté Enedis (contrairement à un sélecteur CSS/XPath figé)."""

    def __init__(self) -> None:
        super().__init__()
        self._chunks: list[str] = []
        self._skip_depth = 0

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag in ("script", "style"):
            self._skip_depth += 1

    def handle_endtag(self, tag: str) -> None:
        if tag in ("script", "style") and self._skip_depth > 0:
            self._skip_depth -= 1

    def handle_data(self, data: str) -> None:
        if self._skip_depth == 0 and data.strip():
            self._chunks.append(data.strip())

    def get_text(self) -> str:
        return " ".join(self._chunks)


def html_to_text(html: str) -> str:
    extractor = _TextExtractor()
    extractor.feed(html)
    return extractor.get_text()


def parse_latest_covered_year(page_text: str) -> int:
    """Extrait l'année de fin de la "Couverture temporelle" d'un texte de
    page catalogue Enedis déjà converti en texte brut (cf. `html_to_text`)."""
    match = _COVERAGE_PATTERN.search(page_text)
    if not match:
        raise EnedisFreshnessCheckError(
            "Impossible de trouver la mention 'Couverture temporelle' (avec une plage "
            "d'années) dans la page catalogue Enedis -- la page a peut-être changé de "
            "format. Vérifier manuellement : " + DEFAULT_ENEDIS_DATASET_PAGE_URL
        )
    return int(match.group(1))


def get_configured_enedis_year(sources: dict = SOURCES) -> int:
    """Lit l'année actuellement figée dans le filtre serveur Enedis
    (`SourceFile.value_filter`, ex: `("année", "2024")`)."""
    enedis_files = sources["enedis_conso"].files
    for file_cfg in enedis_files:
        if file_cfg.value_filter is not None:
            _column_hint, expected_value = file_cfg.value_filter
            return int(expected_value)
    raise EnedisFreshnessCheckError(
        "Aucun value_filter trouvé sur la source 'enedis_conso' dans config.py -- "
        "impossible de savoir sur quelle année le filtre est actuellement figé."
    )


@dataclass
class EnedisFreshnessReport:
    checked_at: str
    dataset_page_url: str
    configured_year: int
    latest_covered_year: int
    is_stale: bool

    def to_dict(self) -> dict:
        return asdict(self)


def check_enedis_freshness(
    sources: dict = SOURCES,
    url: str = DEFAULT_ENEDIS_DATASET_PAGE_URL,
    timeout: int = DEFAULT_TIMEOUT,
) -> EnedisFreshnessReport:
    """Compare l'année configurée dans `config.py` à la dernière année
    réellement couverte par le dataset Enedis (lue sur la page catalogue).

    `is_stale=True` signifie qu'une année plus récente que celle configurée
    a été publiée par Enedis -- action requise : mettre à jour `qs=annee:...`
    et `value_filter` dans `config.py`, PUIS relancer l'ingestion Enedis pour
    récupérer la nouvelle année (ce script ne fait que détecter, jamais de
    mise à jour automatique du filtre -- décision produit à confirmer par
    l'utilisateur à chaque fois, comme pour toute autre décision de ce
    pipeline)."""
    configured_year = get_configured_enedis_year(sources)

    try:
        response = requests.get(url, timeout=timeout, headers={"User-Agent": "ve-pipeline-freshness/1.0"})
        response.raise_for_status()
    except requests.RequestException as exc:
        raise EnedisFreshnessCheckError(f"Page catalogue Enedis injoignable ({url}) : {exc}") from exc

    page_text = html_to_text(response.text)
    latest_covered_year = parse_latest_covered_year(page_text)

    return EnedisFreshnessReport(
        checked_at=datetime.now(timezone.utc).isoformat(),
        dataset_page_url=url,
        configured_year=configured_year,
        latest_covered_year=latest_covered_year,
        is_stale=latest_covered_year > configured_year,
    )
