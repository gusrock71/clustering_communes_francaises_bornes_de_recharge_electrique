"""Tests du module de préparation du dataset clustering (début de J3).

Décisions produit (2026-08-19) reflétées ici -- voir le docstring de
ve_pipeline/features/build_clustering_dataset.py pour le détail complet :
  1. Exclusion has_immatriculations=false.
  2. Imputation de croissance_immat_ve_pct par la médiane (Option A) pour
     demarrage_ve_tardif=true.
  3. Imputation à 0 pour demarrage_ve_tardif=false ET croissance NULL
     (jamais eu de VE sur toute la période).
  4. Exclusion has_enedis=false (même logique que le point 1).
  5. Imputation par médiane du résidu masqué sur les 6 colonnes Enedis
     (nb_sites_residentiel, conso_totale_residentielle_mwh,
     conso_moyenne_residentielle_mwh, population, taux_chauffage_electrique,
     part_thermosensible).

Jeu de communes fixture (staging_territorial simplifié, écrit en Parquet
local). Toutes les communes qui ne servent pas à tester le masquage Enedis
partagent le même vecteur de valeurs Enedis "propres" (ENEDIS_BASELINE), pour
que la médiane attendue soit triviale à vérifier (= ENEDIS_BASELINE) sans
calcul d'arrondi :
  - A1 : has_immatriculations=false -> exclue (le has_enedis=true de A1 ne
    doit pas empêcher son exclusion).
  - E1 : has_enedis=false -> exclue (le has_immatriculations=true de E1 ne
    doit pas empêcher son exclusion).
  - B1, B2 : demarrage_ve_tardif=true, croissance NULL -> imputées à la
    médiane des croissances calculables (C1/C2/C3 = 2.0/6.0/4.0 -> 4.0).
  - C1, C2 : demarrage_ve_tardif=false, croissance calculable, Enedis propre
    (contribuent à la médiane Enedis).
  - C3 : demarrage_ve_tardif=false, croissance calculable, mais TOUTES les
    colonnes Enedis masquées (NULL) malgré has_enedis=true -> résidu masqué,
    doit être imputé à ENEDIS_BASELINE.
  - D1 : demarrage_ve_tardif=false, croissance NULL (jamais eu de VE) ->
    imputée à 0, Enedis propre.
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from ve_pipeline.features.build_clustering_dataset import (
    ENEDIS_MEDIAN_IMPUTED_COLUMNS,
    build_clustering_dataset,
    compute_clustering_readiness_report,
)

# nb_sites_residentiel, conso_totale_residentielle_mwh, conso_moyenne_residentielle_mwh,
# population, taux_chauffage_electrique, part_thermosensible
ENEDIS_BASELINE = (10.0, 100.0, 10.0, 1000.0, 0.2, 0.2)
ENEDIS_MASKED = (None, None, None, None, None, None)

# code_commune, has_immatriculations, demarrage_ve_tardif, croissance_immat_ve_pct, has_enedis, *enedis_cols
STAGING_ROWS = [
    ("A1", False, None, None, True, *ENEDIS_BASELINE),
    ("E1", True, False, None, False, *ENEDIS_MASKED),
    ("B1", True, True, None, True, *ENEDIS_BASELINE),
    ("B2", True, True, None, True, *ENEDIS_BASELINE),
    ("C1", True, False, 2.0, True, *ENEDIS_BASELINE),
    ("C2", True, False, 6.0, True, *ENEDIS_BASELINE),
    ("C3", True, False, 4.0, True, *ENEDIS_MASKED),
    ("D1", True, False, None, True, *ENEDIS_BASELINE),
]


@pytest.fixture
def staging_path(tmp_path: Path) -> str:
    con = duckdb.connect()
    cols_sql = ", ".join(f"{c} DOUBLE" for c in ENEDIS_MEDIAN_IMPUTED_COLUMNS)
    con.execute(f"""
        CREATE TABLE staging_territorial (
            code_commune VARCHAR,
            has_immatriculations BOOLEAN,
            demarrage_ve_tardif BOOLEAN,
            croissance_immat_ve_pct DOUBLE,
            has_enedis BOOLEAN,
            {cols_sql}
        );
    """)
    placeholders = ", ".join(["?"] * (5 + len(ENEDIS_MEDIAN_IMPUTED_COLUMNS)))
    con.executemany(f"INSERT INTO staging_territorial VALUES ({placeholders})", STAGING_ROWS)
    path = tmp_path / "territorial.parquet"
    con.execute(f"COPY staging_territorial TO '{path}' (FORMAT PARQUET);")
    con.close()
    return str(path)


@pytest.fixture
def con_and_medians(staging_path: str):
    connection = duckdb.connect()
    median_croissance, enedis_medians = build_clustering_dataset(connection, staging_path)
    return connection, median_croissance, enedis_medians


@pytest.fixture
def con(con_and_medians):
    connection, _, _ = con_and_medians
    return connection


def test_excludes_communes_without_immatriculations_or_enedis(con):
    communes = {r[0] for r in con.execute("SELECT code_commune FROM clustering_dataset").fetchall()}
    assert "A1" not in communes  # has_immatriculations=false
    assert "E1" not in communes  # has_enedis=false
    assert communes == {"B1", "B2", "C1", "C2", "C3", "D1"}


def test_median_croissance_computed_dynamically_from_calculable_values_only(con_and_medians):
    _, median_croissance, _ = con_and_medians
    assert median_croissance == pytest.approx(4.0)


def test_demarrage_ve_tardif_rows_imputed_with_median(con):
    rows = con.execute(
        "SELECT code_commune, croissance_immat_ve_pct FROM clustering_dataset "
        "WHERE code_commune IN ('B1', 'B2') ORDER BY code_commune"
    ).fetchall()
    assert rows == [("B1", 4.0), ("B2", 4.0)]


def test_never_had_ve_rows_imputed_to_zero(con):
    row = con.execute(
        "SELECT croissance_immat_ve_pct FROM clustering_dataset WHERE code_commune = 'D1'"
    ).fetchone()
    assert row[0] == 0.0


def test_enedis_medians_equal_baseline_vector(con_and_medians):
    # A1, B1, B2, C1, C2, D1 partagent tous ENEDIS_BASELINE -> la médiane de
    # chaque colonne doit être exactement cette valeur, sans arrondi à vérifier.
    _, _, enedis_medians = con_and_medians
    for col, expected in zip(ENEDIS_MEDIAN_IMPUTED_COLUMNS, ENEDIS_BASELINE):
        assert enedis_medians[col] == pytest.approx(expected)


def test_enedis_masked_residual_imputed_to_median(con):
    # C3 : has_enedis=true mais toutes les colonnes Enedis sont masquées ->
    # doivent être imputées à ENEDIS_BASELINE (la médiane).
    cols_sql = ", ".join(ENEDIS_MEDIAN_IMPUTED_COLUMNS)
    row = con.execute(
        f"SELECT {cols_sql} FROM clustering_dataset WHERE code_commune = 'C3'"
    ).fetchone()
    assert row == ENEDIS_BASELINE


def test_enedis_clean_rows_left_untouched(con):
    cols_sql = ", ".join(ENEDIS_MEDIAN_IMPUTED_COLUMNS)
    row = con.execute(
        f"SELECT {cols_sql} FROM clustering_dataset WHERE code_commune = 'C1'"
    ).fetchone()
    assert row == ENEDIS_BASELINE


def test_readiness_report_counts(con_and_medians):
    connection, median_croissance, enedis_medians = con_and_medians
    report = compute_clustering_readiness_report(connection, median_croissance, enedis_medians)
    assert report.nb_communes_total_staging == 8
    assert report.nb_communes_exclues_sans_immatriculations == 1  # A1
    assert report.nb_communes_exclues_sans_enedis == 1  # E1
    assert report.nb_communes_exclues_total == 2  # pas de recouvrement dans cette fixture
    assert report.nb_communes_dataset == 6
    assert report.croissance_immat_ve_pct_mediane_imputee == pytest.approx(4.0)
    assert report.nb_communes_imputees_demarrage_tardif == 2
    assert report.nb_communes_croissance_encore_nulle == 0
    for col in ENEDIS_MEDIAN_IMPUTED_COLUMNS:
        assert report.nb_communes_enedis_encore_nulle[col] == 0
