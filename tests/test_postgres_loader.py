"""Tests de bon fonctionnement du loader S3 -> Postgres (staging).

Utilise SQLite (fichier, via tmp_path) comme stand-in réel de Postgres :
même code SQLAlchemy des deux côtés, seule la chaîne de connexion change en
production. Un SQLite en mémoire poserait un piège classique (chaque
`engine.connect()` ouvrirait une base vide différente) ; un fichier sur
disque évite ce problème sans configuration de pool particulière.

Couvre :
  1. le chargement d'un fichier unique avec le bon nombre de lignes,
  2. la normalisation des noms de colonnes accentués/avec espaces (Enedis),
  3. l'idempotence d'un rechargement (table remplacée, pas dupliquée),
  4. le rejet d'un fichier vide,
  5. le pipeline complet source -> S3 (moto) -> Postgres (SQLite) pour les 4
     fichiers réels du projet, répondant directement à la question "peut-on
     charger les raw directement en base, ou faut-il passer par S3 ?".
"""

from __future__ import annotations

from datetime import date, timedelta

import pandas as pd
import pytest
import responses
from sqlalchemy import text

from ve_pipeline.ingestion import connector, s3_landing
from ve_pipeline.ingestion.config import SOURCES
from ve_pipeline.loading import postgres_loader

from .conftest import TEST_BUCKET


def _sqlite_engine(tmp_path):
    return postgres_loader.get_engine(f"sqlite:///{tmp_path}/staging.db")


def test_load_csv_bytes_creates_table_with_correct_row_count(tmp_path):
    engine = _sqlite_engine(tmp_path)
    content = b"id_pdc_itinerance;code_insee_commune\nFRXXXP0001;75056\nFRXXXP0002;69123\n"

    row_count = postgres_loader.load_csv_bytes(engine, "raw__irve__test", content)

    assert row_count == 2


def test_column_names_are_sanitized(tmp_path):
    engine = _sqlite_engine(tmp_path)
    # En-tête façon Enedis réel : accents, espaces, majuscules, BOM UTF-8.
    content = "﻿Année,Code Commune,Conso totale (MWh)\n2024,75056,123456\n".encode("utf-8")

    postgres_loader.load_csv_bytes(engine, "raw__enedis_conso__test", content)

    df = pd.read_sql_table("raw__enedis_conso__test", engine)
    assert list(df.columns) == ["annee", "code_commune", "conso_totale_mwh"]
    assert df.iloc[0]["code_commune"] == "75056"


def test_embedded_escaped_quotes_beyond_sniffer_sample_are_parsed_correctly(tmp_path):
    """Reproduit l'incident réel du 2026-08-21 sur irve_consolide.csv : le
    Sniffer ne regarde que les 2048 premiers caractères pour déduire
    doublequote/escapechar. S'il n'y voit aucun guillemet échappé, il déduit
    `doublequote=False`, et une ligne plus loin dans le fichier contenant un
    `""` (échappement RFC4180 standard, ex: adresse avec guillemets internes)
    est alors mal re-découpée (colonnes en trop). Le fix force
    `doublequote=True` après sniffing, quel que soit ce que le Sniffer a
    déduit du seul échantillon."""
    engine = _sqlite_engine(tmp_path)
    header = "col_a,col_b,col_c\n"
    # lignes de remplissage sans aucun guillemet, pour dépasser les 2048
    # caractères que le Sniffer inspecte et l'amener à déduire doublequote=False
    filler = "".join(f"val{i}a,val{i}b,val{i}c\n" for i in range(120))
    problem_row = 'x,"adresse avec ""guillemets"" internes",y\n'
    content = (header + filler + problem_row).encode("utf-8")
    assert len(header + filler) > 2048  # confirme que la ligne à risque est bien hors échantillon

    row_count = postgres_loader.load_csv_bytes(engine, "raw__irve__test_quotes", content)

    assert row_count == 121  # 120 lignes de remplissage + la ligne à guillemets
    df = pd.read_sql_table("raw__irve__test_quotes", engine)
    assert df.iloc[-1]["col_b"] == 'adresse avec "guillemets" internes'


def test_keep_columns_allowlist_restricts_to_used_columns_but_keeps_row_count(tmp_path):
    """Contrainte d'infra (quota Neon) pour IRVE/Enedis, cf. postgres_loader.KEPT_COLUMNS :
    seules les colonnes réellement lues par build_staging.py doivent atterrir en
    base, sans que ça touche le nombre de lignes."""
    engine = _sqlite_engine(tmp_path)
    header = "code_insee_commune,nom_amenageur,puissance_nominale,observations\n"
    content = (header + "75056,ChargePoint,22.0,rien a signaler\n").encode("utf-8")

    row_count = postgres_loader.load_csv_bytes(
        engine, "raw__irve__test_keep", content,
        keep_columns={"code_insee_commune", "puissance_nominale"},
    )

    assert row_count == 1
    df = pd.read_sql_table("raw__irve__test_keep", engine)
    assert list(df.columns) == ["code_insee_commune", "puissance_nominale"]


def test_load_source_from_s3_applies_kept_columns_for_irve(s3_client, tmp_path):
    """Vérifie le branchement complet load_source_from_s3 -> KEPT_COLUMNS pour IRVE."""
    s3_landing.ensure_bucket(s3_client, TEST_BUCKET)
    content = (
        "code_insee_commune,nom_amenageur,puissance_nominale,adresse_station,"
        "id_station_itinerance,consolidated_is_code_insee_verified,id_pdc_itinerance,"
        "id_pdc_local,date_maj,last_modified,observations\n"
        "75056,ChargePoint,22.0,\"3 Rue X, 75056 Paris\",STA1,True,PDC1,PDC1L,2024-01-01,2024-01-01T00:00:00Z,rien\n"
    ).encode("utf-8")
    key = s3_landing.landing_key("irve", "irve_consolide", "csv")
    s3_landing.upload_bytes(s3_client, TEST_BUCKET, key, content, "text/csv")

    engine = _sqlite_engine(tmp_path)
    result = postgres_loader.load_source_from_s3(
        s3_client, engine, TEST_BUCKET, "irve", "irve_consolide", "csv"
    )

    assert result.status == "ok", result.error
    df = pd.read_sql_table(result.table, engine)
    assert set(df.columns) == postgres_loader.KEPT_COLUMNS[("irve", "irve_consolide")]
    assert "nom_amenageur" not in df.columns
    assert "observations" not in df.columns


def test_drop_columns_excludes_old_immat_years_but_keeps_row_count(tmp_path):
    """Contrainte d'infra (quota Neon 512 Mo, cf.
    postgres_loader._immatriculation_years_to_exclude) : les colonnes
    immat_YYYY hors fenêtre ne doivent pas atterrir en base pour les
    fichiers immatriculations, sans que ça touche le nombre de lignes ni les
    autres colonnes. Le fichier S3 source, lui, reste inchangé (non testé
    ici, propre à load_csv_bytes qui ne voit que des bytes déjà en
    mémoire). Ce test couvre le mécanisme générique `drop_columns` de
    load_csv_bytes, indépendamment du calcul de la fenêtre glissante
    (testé séparément ci-dessous, avec une date figée)."""
    engine = _sqlite_engine(tmp_path)
    header = "commune_code;immat_2017;immat_2018;immat_2025\n"
    content = (header + "75056;3;4;5\n").encode("utf-8")

    row_count = postgres_loader.load_csv_bytes(
        engine, "raw__immatriculations__test", content, drop_columns={"immat_2017"}
    )

    assert row_count == 1
    df = pd.read_sql_table("raw__immatriculations__test", engine)
    assert list(df.columns) == ["commune_code", "immat_2018", "immat_2025"]


def test_immatriculation_years_to_exclude_uses_a_sliding_8_year_window():
    """Fenêtre glissante (décision du 2026-08-29) : `today` est injecté
    explicitement plutôt que de dépendre de la date réelle d'exécution du
    test (`date.today()` n'est pas monkey-patchable, `datetime.date` étant
    un type C immuable) -- ce test reste donc vrai indéfiniment, contrairement
    à l'ancienne version figée "2010-2017" qu'il remplace."""
    # Le 1er mars 2026 : dernier exercice complet = 2025, fenêtre = 2018-2025
    # (identique à l'ancien comportement figé, coïncidence de calendrier).
    excluded_2026 = postgres_loader._immatriculation_years_to_exclude(today=date(2026, 3, 1))
    assert "immat_2017" in excluded_2026
    assert "immat_2010" in excluded_2026
    assert "immat_2018" not in excluded_2026
    assert "immat_2025" not in excluded_2026
    assert "immat_2026" in excluded_2026  # exercice en cours, pas encore complet

    # Un an plus tard : la fenêtre glisse d'un an (2019-2026), comme demandé
    # explicitement par l'utilisateur ("2018-2025, 2019-2026, 2020-2027...").
    excluded_2027 = postgres_loader._immatriculation_years_to_exclude(today=date(2027, 6, 1))
    assert "immat_2018" in excluded_2027  # sorti de la fenêtre
    assert "immat_2019" not in excluded_2027
    assert "immat_2026" not in excluded_2027
    assert "immat_2027" in excluded_2027  # exercice en cours


def test_load_source_from_s3_applies_dropped_columns_for_immatriculations(s3_client, tmp_path):
    """Vérifie le branchement complet load_source_from_s3 ->
    _immatriculation_years_to_exclude pour les 2 fichiers immatriculations
    réels. Le fichier de test couvre une plage d'années large (2000-2099)
    plutôt que des bornes hardcodées : les colonnes attendues sont dérivées
    de la fonction réelle (date du jour, pas figée), donc ce test reste
    vrai quelle que soit la date à laquelle il s'exécute -- contrairement à
    l'ancienne version qui hardcodait "2010/2017/2018/2025", correcte
    seulement tant que la fenêtre réelle restait 2018-2025."""
    s3_landing.ensure_bucket(s3_client, TEST_BUCKET)
    toutes_annees = list(range(2000, 2100))
    header = "COMMUNE_CODE;" + ";".join(f"IMMAT_{a}" for a in toutes_annees)
    ligne = "75056;" + ";".join("1" for _ in toutes_annees)
    content = f"{header}\n{ligne}\n".encode("utf-8")
    key = s3_landing.landing_key("immatriculations", "immatriculations_neuf", "csv")
    s3_landing.upload_bytes(s3_client, TEST_BUCKET, key, content, "text/csv")

    engine = _sqlite_engine(tmp_path)
    result = postgres_loader.load_source_from_s3(
        s3_client, engine, TEST_BUCKET, "immatriculations", "immatriculations_neuf", "csv"
    )

    assert result.status == "ok", result.error
    df = pd.read_sql_table(result.table, engine)

    exclues = postgres_loader._immatriculation_years_to_exclude()
    colonnes_annees_attendues = {f"immat_{a}" for a in toutes_annees} - exclues
    assert set(df.columns) - {"commune_code"} == colonnes_annees_attendues
    # La fenêtre fait toujours 8 ans, quelle que soit la date d'exécution.
    assert len(colonnes_annees_attendues) == 8


def test_reload_replaces_table_not_duplicates(tmp_path):
    engine = _sqlite_engine(tmp_path)
    content = b"col_a,col_b\nval1,val2\n"

    first = postgres_loader.load_csv_bytes(engine, "raw__irve__test", content)
    second = postgres_loader.load_csv_bytes(engine, "raw__irve__test", content)

    assert first == second == 1


def test_reload_succeeds_even_with_a_dependent_view(tmp_path):
    """Incident réel du 2026-08-29 : le premier run automatisé GitHub Actions
    a échoué sur `psycopg2.errors.DependentObjectsStillExist` -- un `dbt run`
    précédent avait créé des vues (staging.stg_irve, etc.) dépendant des
    tables raw__*, et l'ancien `to_sql(if_exists='replace')` émettait un
    DROP TABLE simple, qui échoue dans ce cas sous Postgres. Corrigé par
    `_drop_table_if_exists` (DROP TABLE IF EXISTS ... CASCADE), validé en
    conditions réelles sur une branche Neon temporaire (reproduction exacte
    de l'erreur, puis correctif confirmé, cf. session du 2026-08-29).

    SQLite ne supporte pas les vues dépendant explicitement d'un DROP TABLE
    de la même façon que Postgres (pas de CASCADE, et SQLite autorise le DROP
    même avec une vue dessus -- elle devient simplement invalide à l'usage) :
    ce test ne peut donc pas reproduire l'erreur elle-même sous SQLite, mais
    vérifie que le mécanisme de rechargement continue de fonctionner sans
    régression une fois une vue créée par-dessus la table."""
    engine = _sqlite_engine(tmp_path)
    postgres_loader.load_csv_bytes(engine, "raw__irve__test", b"col_a\nval1\n")
    with engine.begin() as conn:
        conn.execute(text("CREATE VIEW vue_test AS SELECT col_a FROM raw__irve__test"))

    # Ne doit pas lever d'exception malgré la vue dépendante.
    row_count = postgres_loader.load_csv_bytes(engine, "raw__irve__test", b"col_a\nval2\n")

    assert row_count == 1


def test_drop_table_if_exists_adds_cascade_only_for_postgresql():
    """`_drop_table_if_exists` ne doit ajouter CASCADE (spécifique Postgres,
    nécessaire pour supprimer les tables raw__* malgré les vues dbt qui en
    dépendent) que sur le dialecte postgresql -- SQLite ne supporte pas ce
    mot-clé (voir docstring de la fonction)."""

    class _FakeDialect:
        def __init__(self, name):
            self.name = name

    class _FakeConnection:
        def __init__(self):
            self.executed_sql: list[str] = []

        def execute(self, statement):
            self.executed_sql.append(str(statement))

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

    class _FakeEngine:
        def __init__(self, dialect_name):
            self.dialect = _FakeDialect(dialect_name)
            self.connection = _FakeConnection()

        def begin(self):
            return self.connection

    postgres_engine = _FakeEngine("postgresql")
    postgres_loader._drop_table_if_exists(postgres_engine, "raw__irve__test")
    assert "CASCADE" in postgres_engine.connection.executed_sql[0]

    sqlite_engine = _FakeEngine("sqlite")
    postgres_loader._drop_table_if_exists(sqlite_engine, "raw__irve__test")
    assert "CASCADE" not in sqlite_engine.connection.executed_sql[0]


def test_empty_file_raises_load_integrity_error(tmp_path):
    engine = _sqlite_engine(tmp_path)

    with pytest.raises(postgres_loader.LoadIntegrityError, match="vide"):
        postgres_loader.load_csv_bytes(engine, "raw__irve__test", b"")


@responses.activate
def test_full_pipeline_sources_to_s3_to_postgres(s3_client, tmp_path):
    """Répond directement à la question posée : source -> S3 -> Postgres, de
    bout en bout, sans jamais écrire dans Postgres directement depuis la
    source."""
    irve_sample = b"id_pdc_itinerance;code_insee_commune;puissance_nominale\nFRXXXP0001;75056;22.0\n"
    immat_neuf_sample = b"codgeo;epci;annee;nb_ve\n75056;200054781;2025;120\n"
    immat_occasion_sample = b"codgeo;annee;nb_vt\n75056;2025;530\n"
    enedis_page_1 = (
        "﻿Année,Code Commune,nb_sites,Conso totale (MWh),CODE GRAND SECTEUR\n"
        "2024,75056,12,123456,RESIDENTIEL\n"
    ).encode("utf-8")

    responses.add(responses.GET, SOURCES["irve"].files[0].url, body=irve_sample, status=200, content_type="text/csv")
    responses.add(
        responses.GET, SOURCES["immatriculations"].files[0].url, body=immat_neuf_sample, status=200, content_type="text/csv"
    )
    responses.add(
        responses.GET,
        SOURCES["immatriculations"].files[1].url,
        body=immat_occasion_sample,
        status=200,
        content_type="text/csv",
    )
    responses.add(
        responses.GET, SOURCES["enedis_conso"].files[0].url, body=enedis_page_1, status=200, content_type="text/csv"
    )

    # Étape 1 : ingestion réelle (déjà testée par ailleurs) -> S3
    connector.ingest_all(s3_client, TEST_BUCKET)

    # Étape 2 : le nouveau loader -> Postgres (SQLite ici en test)
    engine = _sqlite_engine(tmp_path)
    results = postgres_loader.load_all_sources_from_s3(s3_client, engine, TEST_BUCKET)

    assert len(results) == 4
    assert all(r.status == "ok" for r in results), [r for r in results if r.status != "ok"]

    irve_df = pd.read_sql_table("raw__irve__irve_consolide", engine)
    assert len(irve_df) == 1
    assert irve_df.iloc[0]["code_insee_commune"] == "75056"

    # "Année" est exclue par KEPT_COLUMNS (allowlist IRVE/Enedis, cf.
    # postgres_loader.KEPT_COLUMNS) : seules les colonnes réellement lues par
    # build_staging.py sont conservées en base.
    enedis_df = pd.read_sql_table("raw__enedis_conso__enedis_conso_commune_2024", engine)
    assert set(enedis_df.columns) == {"code_commune", "nb_sites", "conso_totale_mwh", "code_grand_secteur"}
    assert enedis_df.iloc[0]["code_commune"] == "75056"


def test_load_without_as_of_finds_most_recent_partition_not_today(s3_client, tmp_path):
    """Reproduit le bug rencontré en usage réel : l'ingestion (J1) a eu lieu un
    autre jour que le chargement (ce module). `as_of=None` ne doit PAS être
    interprété comme "aujourd'hui" (ce qui produit un NoSuchKey si personne n'a
    réingéré le jour même), mais retrouver la dernière partition dt= réelle."""
    stale_date = date.today() - timedelta(days=5)
    content = b"code_insee_commune;puissance_nominale\n75056;22.0\n"
    key = s3_landing.landing_key("irve", "irve_consolide", "csv", as_of=stale_date)
    s3_landing.ensure_bucket(s3_client, TEST_BUCKET)
    s3_landing.upload_bytes(s3_client, TEST_BUCKET, key, content, "text/csv")

    engine = _sqlite_engine(tmp_path)
    result = postgres_loader.load_source_from_s3(
        s3_client, engine, TEST_BUCKET, "irve", "irve_consolide", "csv"
    )

    assert result.status == "ok", result.error
    assert result.row_count == 1


def test_load_with_explicit_dt_overrides_latest_partition(s3_client, tmp_path):
    """--dt doit pouvoir forcer une partition précise même si une plus récente existe."""
    old_date = date.today() - timedelta(days=10)
    new_date = date.today() - timedelta(days=1)
    old_content = b"code_insee_commune;puissance_nominale\n75056;22.0\n"
    new_content = b"code_insee_commune;puissance_nominale\n69123;11.0\n"

    s3_landing.ensure_bucket(s3_client, TEST_BUCKET)
    s3_landing.upload_bytes(
        s3_client, TEST_BUCKET, s3_landing.landing_key("irve", "irve_consolide", "csv", old_date), old_content, "text/csv"
    )
    s3_landing.upload_bytes(
        s3_client, TEST_BUCKET, s3_landing.landing_key("irve", "irve_consolide", "csv", new_date), new_content, "text/csv"
    )

    engine = _sqlite_engine(tmp_path)
    result = postgres_loader.load_source_from_s3(
        s3_client, engine, TEST_BUCKET, "irve", "irve_consolide", "csv", as_of=old_date
    )

    assert result.status == "ok", result.error
    df = pd.read_sql_table(result.table, engine)
    assert df.iloc[0]["code_insee_commune"] == "75056"
