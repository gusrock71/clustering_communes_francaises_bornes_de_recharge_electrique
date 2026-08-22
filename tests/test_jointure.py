"""Tests de qualité de la jointure territoriale (J2).

Fixtures CSV calquées sur les vrais schémas confirmés en conditions réelles
le 2026-08-16 (voir memory `project_ve_pipeline_scope`). Exécutés en local
via DuckDB directement sur des fichiers temporaires (pas besoin de S3/mock
pour cette brique : `read_csv_auto` fonctionne aussi bien sur un chemin
local qu'un chemin `s3://`).

Décisions produit (2026-08-19) reflétées ici :
  1. IRVE : cette jointure lit désormais un fichier IRVE déjà nettoyé/dédupliqué
     en amont (ve_pipeline/cleaning/irve_code_commune.py). Elle ne doit PAS
     re-dédupliquer -- la fixture IRVE contient volontairement un doublon
     `id_pdc_itinerance` pour prouver que les 2 lignes sont conservées telles
     quelles (pass-through), pas fusionnées.
  2. Immatriculations : agrégées sur toutes les années IMMAT_2010...IMMAT_2025.
     `croissance_immat_ve_pct` compare désormais une fenêtre de référence
     (somme 2018-2020) à une fenêtre récente (somme 2023-2025) -- les
     fixtures placent donc leurs valeurs sur IMMAT_2020/IMMAT_2025, pas sur
     IMMAT_2010/IMMAT_2025 comme avant.
  3. Enedis : restreint aux lignes "CODE GRAND SECTEUR" = 'RESIDENTIEL'.

Jeu de communes choisi pour couvrir les cas limites :
  - 75056 (Paris)      : présente dans les 3 sources.
  - 69123 (Lyon)       : présente dans IRVE + Enedis, absente des immatriculations.
  - 13055 (Marseille)  : présente uniquement dans les immatriculations.
  - 44109 (Nantes)     : présente dans les immatriculations mais avec
    uniquement une ligne "Essence" (aucune ligne rechargeable, même à 0) --
    reproduit le bug SUM(...)FILTER(...) qui renvoie NULL (pas 0) quand
    aucune ligne ne correspond au filtre CARBURANT (corrigé le 2026-08-19).
  - code commune vide dans IRVE : doit être exclu de l'agrégation mais compté
    dans le rapport qualité comme "commune invalide".
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from ve_pipeline.jointure.build_staging import (
    IMMAT_BASELINE_YEARS,
    IMMAT_RECENT_YEARS,
    IMMAT_YEAR_LAST,
    SourcePaths,
    build_views,
    compute_quality_report,
)

IMMAT_BASELINE_COL = f"immat_ve_baseline_{IMMAT_BASELINE_YEARS[0]}_{IMMAT_BASELINE_YEARS[-1]}"
IMMAT_RECENT_COL = f"immat_ve_recent_{IMMAT_RECENT_YEARS[0]}_{IMMAT_RECENT_YEARS[-1]}"

# IRVE nettoyé (source déjà dédupliquée en amont) : FRXXXP0001 apparaît 2 fois
# volontairement -> ce module ne doit PAS fusionner ces lignes, il fait
# confiance à l'entrée.
IRVE_CSV = """id_pdc_itinerance,id_station_itinerance,code_insee_commune,puissance_nominale,consolidated_is_code_insee_verified
FRXXXP0001,FRXXXS0001,75056,22.0,true
FRXXXP0001,FRXXXS0001,75056,22.0,true
FRXXXP0002,FRXXXS0001,75056,150.0,true
FRXXXP0003,FRXXXS0002,69123,50.0,false
FRXXXP0004,FRXXXS0003,,7.4,false
"""

IMMAT_YEAR_COLUMNS = ",".join(f"IMMAT_{y}" for y in range(2010, 2026))


def _immat_row(prefix_fields: str, baseline_value: int, recent_value: int) -> str:
    """Construit une ligne avec 16 valeurs IMMAT_2010..IMMAT_2025 :
    IMMAT_2020 (dans la fenêtre de référence 2018-2020) vaut baseline_value,
    IMMAT_2025 (dans la fenêtre récente 2023-2025) vaut recent_value, toutes
    les autres années sont à 0. IMMAT_2010 reste donc volontairement à 0,
    cohérent avec le constat sur les vraies données (marché VE quasi
    inexistant en 2010)."""
    values = [0] * len(range(2010, 2026))
    values[2020 - 2010] = baseline_value
    values[2025 - 2010] = recent_value
    return f"{prefix_fields}," + ",".join(str(v) for v in values)


IMMAT_NEUF_CSV = f"""COMMUNE_CODE,COMMUNE_NOM,CARBURANT,STATUT_UTILISATEUR,GROUPE,CATEGORIE,{IMMAT_YEAR_COLUMNS}
{_immat_row("75056,Paris,Electrique et hydrogène,Particulier,VP,Citadine", 5, 200)}
{_immat_row("75056,Paris,Hybride rechargeable,Particulier,VP,Citadine", 0, 40)}
{_immat_row("75056,Paris,Essence,Particulier,VP,Citadine", 500, 900)}
{_immat_row("13055,Marseille,Electrique et hydrogène,Particulier,VP,Citadine", 0, 30)}
{_immat_row("44109,Nantes,Essence,Particulier,VP,Citadine", 300, 250)}
"""

IMMAT_OCCASION_CSV = f"""COMMUNE_CODE,COMMUNE_NOM,CARBURANT,STATUT_UTILISATEUR,GROUPE,CATEGORIE,CRIT_AIR,{IMMAT_YEAR_COLUMNS}
{_immat_row("75056,Paris,Electrique et hydrogène,Particulier,VP,Citadine,1", 2, 60)}
{_immat_row("13055,Marseille,Hybride rechargeable,Particulier,VP,Citadine,1", 0, 8)}
"""

# Enedis : plusieurs secteurs par commune, dont RESIDENTIEL qui est le seul
# retenu (décision produit 2026-08-19). Une ligne RESIDENTIEL a une valeur
# masquée (secret statistique) pour vérifier que TRY_CAST reste défensif
# après filtrage.
ENEDIS_CSV = """Code Commune,CODE GRAND SECTEUR,nb_sites,Conso totale (MWh),Conso moyenne (MWh),nombre_d_habitants,part_thermosensible,Taux de chauffage électrique
75056,RESIDENTIEL,500,10000,20.0,2100000,0.3,0.25
75056,TERTIAIRE,999,99999,99.9,2100000,0.9,0.9
69123,RESIDENTIEL,300,s,16.6,520000,0.28,0.30
69123,INDUSTRIE,111,1111,1.1,520000,0.1,0.1
"""


@pytest.fixture
def source_paths(tmp_path: Path) -> SourcePaths:
    irve = tmp_path / "irve.csv"
    irve.write_text(IRVE_CSV, encoding="utf-8")

    immat_neuf = tmp_path / "immat_neuf.csv"
    immat_neuf.write_text(IMMAT_NEUF_CSV, encoding="utf-8")

    immat_occasion = tmp_path / "immat_occasion.csv"
    immat_occasion.write_text(IMMAT_OCCASION_CSV, encoding="utf-8")

    enedis = tmp_path / "enedis.csv"
    enedis.write_text(ENEDIS_CSV, encoding="utf-8")

    return SourcePaths(
        irve=str(irve),
        immatriculations_neuf=str(immat_neuf),
        immatriculations_occasion=str(immat_occasion),
        enedis=str(enedis),
    )


@pytest.fixture
def con(source_paths: SourcePaths) -> duckdb.DuckDBPyConnection:
    connection = duckdb.connect()
    build_views(connection, source_paths)
    return connection


def test_irve_source_does_not_dedup_already_clean_input(con):
    # FRXXXP0001 apparaît 2 fois dans la fixture -> les 2 lignes doivent être
    # conservées telles quelles : ce module fait confiance à l'entrée
    # (dédup déjà fait en amont par ve_pipeline/cleaning).
    rows = con.execute(
        "SELECT COUNT(*) FROM irve_source WHERE id_pdc_itinerance = 'FRXXXP0001'"
    ).fetchone()
    assert rows[0] == 2


def test_irve_commune_aggregation_counts_all_rows_passthrough(con):
    row = con.execute(
        "SELECT nb_stations, nb_points_charge FROM irve_commune WHERE code_commune = '75056'"
    ).fetchone()
    nb_stations, nb_points_charge = row
    assert nb_stations == 1  # une seule station (FRXXXS0001)
    # 3 lignes IRVE pour 75056 (FRXXXP0001 x2 + FRXXXP0002), aucune fusion ici
    assert nb_points_charge == 3


def test_irve_excludes_missing_code_commune_but_counts_it_in_quality(con):
    communes = {
        r[0] for r in con.execute("SELECT code_commune FROM irve_commune").fetchall()
    }
    assert "" not in communes
    assert None not in communes

    report = compute_quality_report(con)
    assert report.nb_pdc_irve_commune_invalide == 1


def test_immatriculations_filters_to_rechargeable_carburants_only(con):
    row = con.execute(
        f"SELECT immat_ve_{IMMAT_YEAR_LAST}, immat_toutes_motorisations_{IMMAT_YEAR_LAST} "
        "FROM immat_commune WHERE code_commune = '75056'"
    ).fetchone()
    immat_ve_last, immat_toutes = row
    # 200 (élec neuf) + 40 (hybride neuf) + 60 (élec occasion) = 300, l'essence (900) est exclue
    assert immat_ve_last == 300
    # toutes motorisations, neuf + occasion confondus (900+200+40 neuf, 60 occasion)
    assert immat_toutes == 1200


def test_immatriculations_exposes_individual_year_columns(con):
    row = con.execute(
        f"SELECT immat_ve_2010, immat_ve_2020, immat_ve_{IMMAT_YEAR_LAST} "
        "FROM immat_commune WHERE code_commune = '75056'"
    ).fetchone()
    immat_ve_2010, immat_ve_2020, immat_ve_last = row
    # 2010 : volontairement à 0 dans la fixture (cohérent avec le marché VE
    # quasi inexistant à cette date sur les vraies données)
    assert immat_ve_2010 == 0
    # 2020 : 5 (élec neuf) + 0 (hybride neuf) + 2 (élec occasion) = 7
    assert immat_ve_2020 == 7
    assert immat_ve_last == 300


def test_immat_ve_columns_are_zero_not_null_when_no_rechargeable_row_at_all(con):
    # 44109 (Nantes) n'a qu'une ligne "Essence" dans la fixture -- aucune
    # ligne "Electrique et hydrogène"/"Hybride rechargeable", même à 0. Sans
    # COALESCE, SUM(...)FILTER(...) renvoie NULL ici (aucune ligne ne
    # correspond au filtre), pas 0. Bug corrigé le 2026-08-19.
    row = con.execute(
        f"SELECT immat_ve_2020, {IMMAT_BASELINE_COL}, {IMMAT_RECENT_COL} "
        "FROM immat_commune WHERE code_commune = '44109'"
    ).fetchone()
    assert row == (0, 0, 0)

    # Conséquence directe : demarrage_ve_tardif doit être FALSE (une vraie
    # absence de VE rechargeable, pas un cas "pas assez d'information") et
    # non pas NULL comme si la commune n'avait aucune donnée d'immatriculation.
    flag = con.execute(
        "SELECT demarrage_ve_tardif FROM staging_territorial WHERE code_commune = '44109'"
    ).fetchone()[0]
    assert flag is False


def test_immat_commune_exposes_baseline_and_recent_windows(con):
    row = con.execute(
        f"SELECT {IMMAT_BASELINE_COL}, {IMMAT_RECENT_COL} "
        "FROM immat_commune WHERE code_commune = '75056'"
    ).fetchone()
    baseline, recent = row
    assert baseline == 7  # somme 2018-2020, filtrée motorisations rechargeables
    assert recent == 300  # somme 2023-2025, idem


def test_immatriculations_unions_neuf_and_occasion(con):
    # Marseille : 30 (neuf, élec) + 8 (occasion, hybride rechargeable) = 38
    row = con.execute(
        f"SELECT immat_ve_{IMMAT_YEAR_LAST} FROM immat_commune WHERE code_commune = '13055'"
    ).fetchone()
    assert row[0] == 38


def test_enedis_filters_to_residentiel_sector_only(con):
    # 75056 a une ligne RESIDENTIEL (nb_sites=500) et une TERTIAIRE (999) ->
    # seule la RESIDENTIEL doit compter. Si le filtre était absent, on
    # obtiendrait 1499.
    row = con.execute(
        "SELECT nb_sites_residentiel, population FROM enedis_commune WHERE code_commune = '75056'"
    ).fetchone()
    nb_sites, population = row
    assert nb_sites == 500
    assert population == 2100000


def test_enedis_handles_masked_values_after_sector_filter(con):
    # 69123 : ligne RESIDENTIEL avec "Conso totale (MWh)" masquée ("s") ->
    # TRY_CAST doit renvoyer NULL plutôt que de faire planter l'agrégation ;
    # la ligne INDUSTRIE (non masquée) ne doit pas être comptée à la place.
    row = con.execute(
        "SELECT conso_totale_residentielle_mwh FROM enedis_commune WHERE code_commune = '69123'"
    ).fetchone()
    assert row[0] is None


def test_spine_keeps_communes_present_in_only_one_source(con):
    # 13055 n'a ni IRVE ni Enedis mais doit être présente dans la table finale
    row = con.execute(
        "SELECT has_irve, has_immatriculations, has_enedis FROM staging_territorial WHERE code_commune = '13055'"
    ).fetchone()
    assert row == (False, True, False)


def test_irve_metrics_coalesced_to_zero_when_no_irve(con):
    # 13055 (Marseille) n'a aucun pdc IRVE -> ces colonnes doivent valoir 0
    # (vraie donnée : aucune borne), pas NULL (qui suggérerait une donnée
    # manquante à imputer). pct_pdc_code_insee_verifie reste NULL : métrique
    # de qualité non définie s'il n'y a aucun pdc à mesurer.
    row = con.execute(
        "SELECT nb_stations, nb_points_charge, puissance_totale_installee_kw, "
        "puissance_moyenne_pdc_kw, part_recharge_rapide, pct_pdc_code_insee_verifie "
        "FROM staging_territorial WHERE code_commune = '13055'"
    ).fetchone()
    assert row == (0, 0, 0, 0, 0, None)


def test_irve_metrics_untouched_when_irve_present(con):
    # 75056 a bien de l'IRVE -> le COALESCE ne doit rien changer aux vraies valeurs.
    row = con.execute(
        "SELECT nb_stations, nb_points_charge FROM staging_territorial WHERE code_commune = '75056'"
    ).fetchone()
    assert row == (1, 3)


def test_no_data_silently_lost_for_any_source(con):
    # Chaque commune vue dans une source d'origine doit se retrouver dans la table finale
    for view, col in [("irve_commune", "code_commune"), ("immat_commune", "code_commune"), ("enedis_commune", "code_commune")]:
        source_communes = {r[0] for r in con.execute(f"SELECT DISTINCT {col} FROM {view}").fetchall()}
        staging_communes = {r[0] for r in con.execute("SELECT code_commune FROM staging_territorial").fetchall()}
        assert source_communes <= staging_communes, f"communes perdues depuis {view}: {source_communes - staging_communes}"


def test_no_duplicate_commune_codes_in_staging(con):
    dup = con.execute(
        "SELECT COUNT(*) FROM (SELECT code_commune FROM staging_territorial GROUP BY code_commune HAVING COUNT(*) > 1)"
    ).fetchone()[0]
    assert dup == 0


def test_quality_report_computes_expected_coverage(con):
    report = compute_quality_report(con)
    assert report.nb_communes_total == 4  # 75056, 69123, 13055, 44109
    assert report.nb_communes_avec_irve == 2  # 75056, 69123
    assert report.nb_communes_avec_immatriculations == 3  # 75056, 13055, 44109
    assert report.nb_communes_avec_enedis == 2  # 75056, 69123
    assert report.nb_communes_toutes_sources == 1  # seule 75056 a les 3
    assert report.nb_doublons_code_commune == 0


def test_croissance_immat_ve_pct_computed_when_baseline_positive(con):
    row = con.execute(
        "SELECT croissance_immat_ve_pct FROM staging_territorial WHERE code_commune = '75056'"
    ).fetchone()
    # baseline (somme 2018-2020) = 7, récent (somme 2023-2025) = 300 -> (300-7)/7
    assert row[0] == pytest.approx((300 - 7) / 7)


def test_croissance_immat_ve_pct_null_when_baseline_is_zero(con):
    # Marseille (13055) : baseline 2018-2020 = 0 (les 2 lignes rechargeables
    # n'ont de valeur qu'en 2025) mais récent 2023-2025 = 38 > 0 -> le ratio
    # ne doit pas planter ni être deviné, il doit rester NULL.
    baseline = con.execute(
        f"SELECT {IMMAT_BASELINE_COL} FROM immat_commune WHERE code_commune = '13055'"
    ).fetchone()[0]
    assert baseline == 0

    row = con.execute(
        "SELECT croissance_immat_ve_pct FROM staging_territorial WHERE code_commune = '13055'"
    ).fetchone()
    assert row[0] is None


def test_demarrage_ve_tardif_true_when_baseline_zero_and_recent_positive(con):
    # Marseille (13055) : baseline=0, recent=38>0 -> démarrage tardif du marché VE
    row = con.execute(
        "SELECT demarrage_ve_tardif FROM staging_territorial WHERE code_commune = '13055'"
    ).fetchone()
    assert row[0] is True


def test_demarrage_ve_tardif_false_when_baseline_positive(con):
    # Paris (75056) : baseline=7>0 -> pas un démarrage tardif, même si en croissance
    row = con.execute(
        "SELECT demarrage_ve_tardif FROM staging_territorial WHERE code_commune = '75056'"
    ).fetchone()
    assert row[0] is False


def test_demarrage_ve_tardif_null_when_no_immatriculations_data(con):
    # Lyon (69123) : aucune donnée d'immatriculation du tout -> ni vrai ni
    # faux, NULL (pas assez d'information pour trancher).
    row = con.execute(
        "SELECT demarrage_ve_tardif FROM staging_territorial WHERE code_commune = '69123'"
    ).fetchone()
    assert row[0] is None
