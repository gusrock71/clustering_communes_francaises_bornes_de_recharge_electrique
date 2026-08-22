"""Chargement des fichiers bruts déposés sur S3 (raw/<source>/dt=.../*.csv)
vers une base Postgres (staging), en amont des futurs modèles dbt.

Le loader est volontairement agnostique de la façon dont les données sont
arrivées sur S3 (Airbyte ou connecteur Python custom pour Enedis) : il ne lit
que la convention de nommage `raw/<source>/dt=<date>/<file_key>.<ext>` déjà en
place (ve_pipeline/ingestion/s3_landing.py). Toutes les sources convergent
donc vers UN SEUL chemin de chargement, plutôt que de dupliquer la logique
d'écriture Postgres dans Airbyte ET dans le connecteur Enedis.

Ce module ne fait AUCUNE transformation métier — même principe "raw" que la
brique d'ingestion : les colonnes sont chargées telles quelles (en texte), y
compris les en-têtes accentués/avec espaces d'Enedis (ex: "Code Commune"),
simplement normalisées en identifiants SQL sûrs. Le renommage et le typage
métier restent le rôle de dbt (ou, pour l'instant, des vues DuckDB de
ve_pipeline/jointure/build_staging.py qui lisent directement S3).

Exception assumée (2026-08-21) : `DROPPED_COLUMNS`/`KEPT_COLUMNS` ci-dessous
excluent certaines colonnes de la copie Postgres — pas une transformation
métier mais une contrainte d'infra (quota gratuit Neon, 512 Mo/projet). Le
fichier S3 correspondant reste toujours intact et complet (source de vérité
"raw" inchangée) ; seule la copie Postgres est allégée :
  - immatriculations (neuf/occasion) : `DROPPED_COLUMNS` exclut
    `immat_2010`...`immat_2017` (16 colonnes -> 8). ATTENTION, précision
    apportée le 2026-08-21 après relecture détaillée de
    `build_staging.py` : `croissance_immat_ve_pct`/`demarrage_ve_tardif` ne
    comparent que 2018-2020 vs 2023-2025, MAIS la jointure calcule aussi un
    détail année par année (`immat_ve_2010`...`immat_ve_2025`) sur
    l'intégralité de la période, déjà testé
    (`test_immatriculations_exposes_individual_year_columns`). Le pipeline
    actuel (DuckDB sur S3, jamais Postgres) n'est pas affecté par cette
    exclusion. Mais une future migration dbt/Postgres de cette jointure ne
    pourrait PAS reproduire le détail 2010-2017 avec la copie Postgres
    actuelle -- écart accepté explicitement par l'utilisateur le 2026-08-21
    (quota Neon prioritaire, signal quasi nul sur cette période : 95,3% des
    communes à 0 immatriculation VE en 2010).
  - irve / enedis_conso : `KEPT_COLUMNS` restreint aux seules colonnes lues
    par le nettoyage (`ve_pipeline/cleaning/irve_code_commune.py`) et la
    jointure (`build_staging.py`) -- respectivement 9/52 et 8/49 colonnes.
    Une allowlist plutôt qu'une exclude-list ici : la source a beaucoup plus
    de colonnes inutilisées que d'utiles, et une allowlist reste correcte
    même si la source ajoute une nouvelle colonne un jour (une exclude-list,
    elle, laisserait passer silencieusement toute colonne non encore listée).

Choix technique : SQLAlchemy + pandas.to_sql() plutôt que `COPY` psycopg2 brut.
Pour les volumes de ce projet (quelques centaines de milliers de lignes au
maximum), la différence de performance est négligeable, et ça permet de
tester ce module en conditions réelles avec SQLite (aucune infra Postgres
nécessaire dans ce sandbox) : le même code fonctionne ensuite tel quel contre
Neon/Supabase en production, seule la chaîne de connexion change.
"""

from __future__ import annotations

import csv
import io
import logging
import os
import re
import unicodedata
from dataclasses import dataclass
from datetime import date

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from ..ingestion import s3_landing
from ..ingestion.config import SOURCES

logger = logging.getLogger(__name__)


class LoadIntegrityError(RuntimeError):
    """Le contenu chargé en base ne correspond pas au fichier source.

    Même philosophie que côté ingestion (SchemaDriftError) : on préfère
    échouer explicitement plutôt que laisser une table Postgres incomplète
    ou tronquée sans que personne ne le remarque.
    """


@dataclass
class LoadResult:
    source: str
    file_key: str
    table: str
    row_count: int
    status: str  # "ok" | "error"
    error: str | None = None


# Colonnes exclues de la copie Postgres pour certains fichiers (clé =
# (source_name, file_key)), voir l'explication en tête de module. Le fichier
# S3 correspondant reste complet ; seules ces colonnes ne sont pas insérées
# en base.
DROPPED_COLUMNS: dict[tuple[str, str], set[str]] = {
    ("immatriculations", "immatriculations_neuf"): {f"immat_{year}" for year in range(2010, 2018)},
    ("immatriculations", "immatriculations_occasion"): {f"immat_{year}" for year in range(2010, 2018)},
}

# Allowlist de colonnes (noms déjà normalisés par _sanitize_column) pour les
# fichiers où la source a beaucoup plus de colonnes qu'utilisées en aval.
# Liste dérivée directement des colonnes lues dans
# ve_pipeline/jointure/build_staging.py (build_views) — si cette jointure
# évolue et lit une colonne supplémentaire, il faudra l'ajouter ici aussi.
KEPT_COLUMNS: dict[tuple[str, str], set[str]] = {
    ("irve", "irve_consolide"): {
        "code_insee_commune",
        "id_station_itinerance",
        "puissance_nominale",
        "consolidated_is_code_insee_verified",
        # adresse_station : nécessaire à la reconstitution des code_insee_commune
        # manquants (ve_pipeline/cleaning/irve_code_commune.py, extraction de
        # code postal par regex). Demandé explicitement le 2026-08-21.
        "adresse_station",
        # id_pdc_itinerance / id_pdc_local / date_maj / last_modified :
        # nécessaires au dédoublonnage des points de charge (plusieurs
        # versions du même pdc dans le flux consolidé, cf. build_dedup_view
        # dans irve_code_commune.py) si ce nettoyage est un jour porté en
        # dbt/Postgres plutôt que gardé sur DuckDB/S3. Gardées explicitement
        # le 2026-08-21.
        "id_pdc_itinerance",
        "id_pdc_local",
        "date_maj",
        "last_modified",
    },
    ("enedis_conso", "enedis_conso_commune_2024"): {
        "code_commune",
        "nb_sites",
        "conso_totale_mwh",
        "conso_moyenne_mwh",
        "nombre_d_habitants",
        "part_thermosensible",
        "taux_de_chauffage_electrique",
        "code_grand_secteur",  # sert de filtre (RESIDENTIEL) en aval, pas gardé dans la sortie finale
    },
}


def get_engine(dsn: str | None = None) -> Engine:
    """Crée un engine SQLAlchemy.

    - En production : positionner `DATABASE_URL` dans l'environnement, ex.
      `postgresql+psycopg2://user:password@host/dbname` (Neon, Supabase, ou
      tout Postgres accessible).
    - Sans `DATABASE_URL` (par défaut) : utilise un SQLite fichier local
      (`sqlite:///ve_pipeline_staging.db`), pratique pour un premier test ou
      un run local sans dépendance externe.
    """
    dsn = dsn or os.environ.get("DATABASE_URL", "sqlite:///ve_pipeline_staging.db")
    return create_engine(dsn)


def _sanitize_column(name: str) -> str:
    """Normalise un nom de colonne CSV (accents, espaces, majuscules) en
    identifiant SQL sûr, ex. "Code Commune" -> "code_commune"."""
    normalized = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode("ascii")
    normalized = re.sub(r"[^a-zA-Z0-9]+", "_", normalized).strip("_").lower()
    return normalized or "col"


def raw_table_name(source_name: str, file_key: str) -> str:
    """Nom de table à plat (pas de schéma SQL séparé) : fonctionne à
    l'identique sur SQLite (tests) et Postgres (prod), sans code spécifique
    à un dialecte."""
    return f"raw__{source_name}__{file_key}"


def load_csv_bytes(
    engine: Engine,
    table: str,
    content: bytes,
    if_exists: str = "replace",
    drop_columns: set[str] | None = None,
    keep_columns: set[str] | None = None,
) -> int:
    """Charge un CSV brut dans une table, colonnes en texte, sans
    transformation métier. Vérifie que le nombre de lignes chargées
    correspond exactement au nombre de lignes de données du fichier source.

    `if_exists="replace"` (par défaut) : la table est recréée à chaque
    chargement, cohérent avec la convention "overwrite" déjà utilisée côté
    S3 (dt=<date> écrasé) et côté Airbyte (Full refresh | Overwrite).

    `drop_columns` (optionnel) : noms de colonnes (déjà normalisés,
    ex. "immat_2010") à exclure de la table Postgres. `keep_columns`
    (optionnel) : à l'inverse, allowlist de colonnes à conserver (utile quand
    la source a beaucoup plus de colonnes inutiles qu'utiles, ex. IRVE/Enedis
    — voir `KEPT_COLUMNS`). Ni l'un ni l'autre ne change le fichier S3 ni le
    nombre de lignes chargées — seulement une contrainte de volume.
    `keep_columns` est appliqué au niveau des lignes brutes, avant
    construction du DataFrame, pour limiter la mémoire utilisée sur les
    fichiers larges (IRVE : 52 colonnes brutes, Enedis : 49).
    """
    # utf-8-sig : Enedis démarre par un BOM UTF-8 (cf. connector.py) ; sans ce
    # décodage la première colonne serait "﻿année" au lieu de "année".
    text_content = content.decode("utf-8-sig", errors="replace")

    # Les sources de ce projet n'utilisent pas toutes le même délimiteur : IRVE
    # et immatriculations (SDES) sont en ";", Enedis en ",". On détecte plutôt
    # que de coder un délimiteur par source, pour rester robuste si une source
    # change de convention (déjà arrivé deux fois avec Enedis).
    try:
        dialect = csv.Sniffer().sniff(text_content[:2048], delimiters=";,\t|")
    except csv.Error:
        dialect = csv.excel  # repli : virgule standard

    # Le Sniffer devine aussi doublequote/escapechar à partir du seul
    # échantillon (2048 caractères), et se trompe en pratique : sur le vrai
    # fichier IRVE consolidé, il a déduit `doublequote=False` faute d'avoir vu
    # de guillemet échappé dans l'échantillon, alors que des champs plus loin
    # dans le fichier (ex: adresses avec guillemets internes, ~700k lignes)
    # utilisent bien l'échappement standard `""` (RFC 4180). Avec
    # `doublequote=False`, ces lignes-là étaient mal re-découpées (1672 lignes
    # avec 65 colonnes au lieu de 52, cf. incident du 2026-08-21). On ne fait
    # confiance au Sniffer que pour le délimiteur ; le reste suit le standard.
    dialect.doublequote = True

    rows = list(csv.reader(io.StringIO(text_content), dialect))

    if not rows:
        raise LoadIntegrityError(f"Fichier vide, impossible de charger la table '{table}'")

    header = [_sanitize_column(c) for c in rows[0]]
    data_rows = rows[1:]

    # keep_columns filtré au niveau des lignes brutes (avant DataFrame) pour
    # limiter le pic mémoire sur les fichiers larges (IRVE : 52 colonnes
    # brutes, Enedis : 49) — contrairement à drop_columns (peu de colonnes à
    # retirer, filtré après coup via un df.drop vectorisé, plus simple).
    if keep_columns:
        keep_idx = [i for i, col in enumerate(header) if col in keep_columns]
        n_dropped = len(header) - len(keep_idx)
        if n_dropped:
            logger.info(
                "Colonnes filtrées (allowlist, contrainte de volume) pour '%s' : %d/%d colonnes conservées",
                table, len(keep_idx), len(header),
            )
        header = [header[i] for i in keep_idx]
        data_rows = [[row[i] for i in keep_idx] for row in data_rows]

    df = pd.DataFrame(data_rows, columns=header, dtype=str)

    if drop_columns:
        to_drop = [c for c in df.columns if c in drop_columns]
        if to_drop:
            logger.info("Colonnes exclues de la table '%s' (contrainte de volume) : %s", table, to_drop)
            df = df.drop(columns=to_drop)

    df.to_sql(table, engine, if_exists=if_exists, index=False)

    with engine.connect() as conn:
        loaded_count = conn.execute(text(f'SELECT COUNT(*) FROM "{table}"')).scalar_one()

    if loaded_count != len(data_rows):
        raise LoadIntegrityError(
            f"Table '{table}' : {loaded_count} lignes chargées en base, {len(data_rows)} "
            "attendues d'après le fichier source. Chargement incomplet ou corrompu."
        )

    return loaded_count


def _resolve_as_of(s3_client, bucket: str, source_name: str, as_of: date | None) -> date | None:
    """Si `as_of` n'est pas fourni explicitement, ne PAS supposer "aujourd'hui" :
    l'ingestion (J1, ve_pipeline.ingestion.cli) et le chargement (ce module)
    n'ont aucune raison de tourner le même jour. On va donc chercher la
    partition `dt=` la plus récente réellement présente sur S3 pour cette
    source, comme le fait déjà scripts/explore_s3_pyspark.py.

    Retourne `as_of` inchangé s'il est fourni (override explicite --dt), sinon
    la date de la dernière partition trouvée, sinon None (auquel cas
    `landing_key` retombera sur la date du jour, et l'échec sera explicite).
    """
    if as_of is not None:
        return as_of
    prefix = f"raw/{source_name}/"
    latest = s3_landing.latest_partition(s3_client, bucket, prefix)
    if latest is None:
        return None
    # latest est de la forme "dt=YYYY-MM-DD"
    return date.fromisoformat(latest.removeprefix("dt="))


def load_source_from_s3(
    s3_client,
    engine: Engine,
    bucket: str,
    source_name: str,
    file_key: str,
    expected_ext: str,
    as_of: date | None = None,
) -> LoadResult:
    resolved_as_of = _resolve_as_of(s3_client, bucket, source_name, as_of)
    key = s3_landing.landing_key(source_name, file_key, expected_ext, resolved_as_of)
    table = raw_table_name(source_name, file_key)
    try:
        content = s3_landing.read_object(s3_client, bucket, key)
        drop_columns = DROPPED_COLUMNS.get((source_name, file_key))
        keep_columns = KEPT_COLUMNS.get((source_name, file_key))
        row_count = load_csv_bytes(engine, table, content, drop_columns=drop_columns, keep_columns=keep_columns)
        logger.info("OK -> table '%s' (%s lignes) depuis s3://%s/%s", table, row_count, bucket, key)
        return LoadResult(source=source_name, file_key=file_key, table=table, row_count=row_count, status="ok")
    except Exception as exc:  # noqa: BLE001 - on capture toute erreur (S3 introuvable, CSV
        # corrompu, connexion DB, intégrité) pour ne pas arrêter le chargement des
        # autres sources ; chaque échec reste visible et explicite dans LoadResult.
        logger.error("Échec chargement %s/%s -> %s : %s", source_name, file_key, table, exc)
        return LoadResult(source=source_name, file_key=file_key, table=table, row_count=0, status="error", error=str(exc))


def load_all_sources_from_s3(s3_client, engine: Engine, bucket: str, as_of: date | None = None) -> list[LoadResult]:
    results: list[LoadResult] = []
    for source in SOURCES.values():
        for file_cfg in source.files:
            results.append(
                load_source_from_s3(
                    s3_client, engine, bucket, source.name, file_cfg.key, file_cfg.expected_ext, as_of
                )
            )
    return results
