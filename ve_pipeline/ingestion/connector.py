"""Logique d'ingestion brute : téléchargement source -> validation -> dépôt S3.

Aucune transformation métier n'est appliquée ici (conforme à la consigne
J1 : "stocker les 3 sources brutes telles quelles dans la landing zone").
La seule validation effectuée est une validation *technique* :
  - le fichier n'est pas vide,
  - son Content-Type est plausible,
  - son en-tête contient les colonnes clés attendues (garde-fou anti-dérive
    de schéma),
  - pour les sources paginées avec filtre serveur (ex: Enedis limité à
    l'année 2024), toutes les lignes reçues respectent bien ce filtre.
"""

from __future__ import annotations

import csv
import io
import logging
from dataclasses import dataclass
from datetime import date

import requests

from . import s3_landing
from .config import SOURCES, Source, SourceFile

logger = logging.getLogger(__name__)

DEFAULT_TIMEOUT = 30
DEFAULT_RETRIES = 3


class SourceUnreachableError(RuntimeError):
    """La source n'a pas pu être jointe après plusieurs tentatives."""


class SchemaDriftError(RuntimeError):
    """Le fichier téléchargé ne correspond pas au schéma attendu.

    Signale que la source a probablement changé de format sans préavis —
    on préfère échouer bruyamment plutôt que déposer un fichier
    inexploitable dans la landing zone.
    """


@dataclass
class IngestionResult:
    source: str
    file_key: str
    s3_key: str
    size_bytes: int
    status: str  # "ok" | "error"
    error: str | None = None


def _download(url: str, timeout: int = DEFAULT_TIMEOUT, retries: int = DEFAULT_RETRIES) -> tuple[bytes, str]:
    logger.info("Téléchargement de %s ...", url)
    last_exc: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(url, timeout=timeout, headers={"User-Agent": "ve-pipeline-ingestion/1.0"})
            resp.raise_for_status()
            logger.info("Reçu %.1f Ko", len(resp.content) / 1024)
            return resp.content, resp.headers.get("Content-Type", "")
        except requests.RequestException as exc:
            last_exc = exc
            logger.warning("Tentative %s/%s échouée pour %s : %s", attempt, retries, url, exc)
    raise SourceUnreachableError(f"Impossible de joindre {url} après {retries} tentatives : {last_exc}")


def _download_one_page(url: str, timeout: int = DEFAULT_TIMEOUT, retries: int = DEFAULT_RETRIES) -> tuple[bytes, str | None]:
    """Télécharge une page et renvoie (contenu, url_page_suivante_ou_None).

    L'URL de la page suivante est lue depuis le header HTTP standard
    `Link: <url>; rel="next"` (mécanisme de pagination par curseur de
    data-fair) — `requests` le parse automatiquement via `resp.links`.
    """
    last_exc: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(url, timeout=timeout, headers={"User-Agent": "ve-pipeline-ingestion/1.0"})
            resp.raise_for_status()
            next_url = resp.links.get("next", {}).get("url")
            return resp.content, next_url
        except requests.RequestException as exc:
            last_exc = exc
            logger.warning("Tentative %s/%s échouée pour %s : %s", attempt, retries, url, exc)
    raise SourceUnreachableError(f"Impossible de joindre {url} après {retries} tentatives : {last_exc}")


def _download_paginated(start_url: str, max_pages: int, timeout: int = DEFAULT_TIMEOUT, retries: int = DEFAULT_RETRIES) -> bytes:
    """Télécharge un jeu de données paginé (API data-fair) et recolle les pages
    en un seul CSV, en ne conservant la ligne d'en-tête que de la première page.

    Ne reconstruit jamais l'URL de la page suivante nous-mêmes : on suit
    littéralement le lien fourni par le serveur, ce qui évite de devoir
    deviner le format exact du curseur de pagination.
    """
    url = start_url
    pages: list[bytes] = []

    for page_num in range(1, max_pages + 1):
        logger.info("Page %s/%s : %s", page_num, max_pages, url)
        content, next_url = _download_one_page(url, timeout, retries)
        content = content.rstrip(b"\r\n")
        if not content:
            logger.info("Page %s vide -> fin de la pagination", page_num)
            break

        if not pages:
            pages.append(content)
        else:
            _header, _sep, body = content.partition(b"\n")
            if body:
                pages.append(body)
        logger.info("Page %s reçue (%.1f Ko cumulés)", page_num, sum(len(p) for p in pages) / 1024)

        if not next_url:
            logger.info("Pas de page suivante -> fin de la pagination (%s pages au total)", page_num)
            break
        url = next_url
    else:
        raise SchemaDriftError(
            f"Pagination interrompue après {max_pages} pages sans atteindre la fin des "
            "données. Le filtre serveur (paramètre 'qs') n'a probablement pas été "
            "appliqué — volume anormalement élevé. Vérifie la syntaxe du filtre dans "
            "config.py, ou augmente max_pages si c'est volontaire."
        )

    return b"\n".join(pages)


def _validate_value_filter(file_cfg: SourceFile, content: bytes) -> None:
    """Vérifie qu'un filtre serveur (ex: année) a bien été appliqué.

    Le filtrage se fait via un paramètre de requête dont on ne peut pas
    garantir a priori la syntaxe exacte côté serveur (ex: `qs=annee:2024`
    sur une API data-fair). Si le filtre n'a pas fonctionné, on préfère
    échouer explicitement plutôt que déposer des données hors périmètre.
    """
    if not file_cfg.value_filter:
        return

    column_hint, expected_value = file_cfg.value_filter
    text = content.decode("utf-8-sig", errors="replace")
    reader = csv.DictReader(io.StringIO(text))

    if not reader.fieldnames:
        raise SchemaDriftError(f"Impossible de lire l'en-tête CSV pour '{file_cfg.key}'")

    matching_col = next((f for f in reader.fieldnames if column_hint.lower() in f.lower()), None)
    if matching_col is None:
        raise SchemaDriftError(
            f"Colonne '{column_hint}' introuvable dans l'en-tête de '{file_cfg.key}' "
            f"({reader.fieldnames}) — impossible de vérifier le filtre serveur."
        )

    bad_values = sorted({row[matching_col] for row in reader if row.get(matching_col) != expected_value})
    if bad_values:
        raise SchemaDriftError(
            f"Le filtre serveur '{column_hint}={expected_value}' pour '{file_cfg.key}' n'a "
            f"pas été correctement appliqué : valeurs inattendues trouvées {bad_values[:5]}. "
            "Vérifie la syntaxe du paramètre de filtre dans config.py."
        )


def _validate_content(file_cfg: SourceFile, content: bytes, content_type: str) -> None:
    if len(content) == 0:
        raise SchemaDriftError(f"Fichier vide reçu pour '{file_cfg.key}'")

    if file_cfg.expected_content_type and not any(ct in content_type for ct in file_cfg.expected_content_type):
        logger.warning(
            "Content-Type inattendu pour '%s' : reçu '%s', attendu un de %s",
            file_cfg.key,
            content_type,
            file_cfg.expected_content_type,
        )

    if file_cfg.schema_hint:
        # utf-8-sig : certains exports (ex: Enedis) démarrent par un BOM UTF-8,
        # ce qui décalerait/polluerait sinon le nom de la toute première colonne.
        head = content[:4096].decode("utf-8-sig", errors="replace").lower()
        missing = [h for h in file_cfg.schema_hint if h.lower() not in head]
        if missing:
            header_line = content.split(b"\n", 1)[0].decode("utf-8-sig", errors="replace")
            raise SchemaDriftError(
                f"Colonnes attendues absentes de l'en-tête de '{file_cfg.key}' : {missing}. "
                f"En-tête réel reçu : {header_line!r}. "
                "Le schéma de la source a peut-être changé (ou schema_hint est mal orthographié)."
            )


def ingest_file(s3_client, bucket: str, source: Source, file_cfg: SourceFile, as_of: date | None = None) -> IngestionResult:
    logger.info("=== %s / %s ===", source.name, file_cfg.key)
    if file_cfg.paginated:
        content = _download_paginated(file_cfg.url, file_cfg.max_pages)
        content_type = "text/csv"
    else:
        content, content_type = _download(file_cfg.url)

    _validate_content(file_cfg, content, content_type)
    _validate_value_filter(file_cfg, content)

    key = s3_landing.landing_key(source.name, file_cfg.key, file_cfg.expected_ext, as_of)
    s3_landing.upload_bytes(s3_client, bucket, key, content, content_type or "application/octet-stream")

    if not s3_landing.object_exists(s3_client, bucket, key):
        raise RuntimeError(f"Upload S3 non confirmé pour '{key}'")

    readback = s3_landing.read_object(s3_client, bucket, key)
    if readback != content:
        raise RuntimeError(f"Intégrité S3 invalide pour '{key}' (relecture différente du contenu source)")

    logger.info("OK -> s3://%s/%s (%.1f Ko)", bucket, key, len(content) / 1024)
    return IngestionResult(source=source.name, file_key=file_cfg.key, s3_key=key, size_bytes=len(content), status="ok")


def ingest_source(s3_client, bucket: str, source_name: str, as_of: date | None = None) -> list[IngestionResult]:
    source = SOURCES[source_name]
    s3_landing.ensure_bucket(s3_client, bucket)

    results: list[IngestionResult] = []
    for file_cfg in source.files:
        try:
            results.append(ingest_file(s3_client, bucket, source, file_cfg, as_of))
        except (SourceUnreachableError, SchemaDriftError, RuntimeError) as exc:
            logger.error("Échec ingestion %s/%s : %s", source_name, file_cfg.key, exc)
            results.append(
                IngestionResult(
                    source=source_name,
                    file_key=file_cfg.key,
                    s3_key="",
                    size_bytes=0,
                    status="error",
                    error=str(exc),
                )
            )
    return results


def ingest_all(s3_client, bucket: str, as_of: date | None = None) -> dict[str, list[IngestionResult]]:
    return {name: ingest_source(s3_client, bucket, name, as_of) for name in SOURCES}
