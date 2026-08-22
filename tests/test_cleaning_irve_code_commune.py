"""Tests du cleaning IRVE : reconstitution des code_insee_commune manquants à
partir de l'adresse libre + référentiel officiel des codes postaux, et
normalisation de puissance_nominale en numérique.

Jeu de cas couvert :
  - CP présent une seule fois dans le référentiel -> résolu directement.
  - CP partagé par plusieurs communes, nom trouvé dans l'adresse -> résolu.
  - CP partagé, nom absent de l'adresse -> non résolu (pas de devinette).
  - CP absent du référentiel français (cas des bornes hors de France, ex:
    Belgique/Espagne repérées dans le vrai fichier IRVE) -> non résolu.
  - Ligne déjà pourvue d'un code -> laissée telle quelle, origine='brut'.
  - puissance_nominale propre -> castée en DOUBLE.
  - puissance_nominale mal formée (virgule décimale) -> NULL (TRY_CAST),
    comptée dans nb_puissance_non_castable plutôt que de planter le pipeline.
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from ve_pipeline.cleaning.irve_code_commune import (
    build_dedup_view,
    build_reconstitution_views,
    compute_dedup_report,
    compute_reconstitution_report,
)

IRVE_CSV = """id_pdc_itinerance,adresse_station,code_insee_commune,puissance_nominale
PDC_UNIQUE,"3 Rue X, 80330 Longueau",,22.0
PDC_NOM_TROUVE,"Rue Y, 54000 Alpha",,50
PDC_AMBIGU,"Rue Z, 54000",,
PDC_ETRANGER,"203 Herbesthaler Straße, 4700 Eupen",,"22,5"
PDC_DEJA_RENSEIGNE,"1 Place Test, 75000 Paris",75056,150.0
"""

REF_CSV = """Code_commune_INSEE;Nom_de_la_commune;Code_postal;Libelle_d_acheminement;Ligne_5
80489;LONGUEAU;80330;LONGUEAU;
12345;ALPHA;54000;ALPHA;
12346;BETA;54000;BETA;
"""


@pytest.fixture
def con(tmp_path: Path) -> duckdb.DuckDBPyConnection:
    irve = tmp_path / "irve.csv"
    irve.write_text(IRVE_CSV, encoding="utf-8")
    ref = tmp_path / "ref.csv"
    ref.write_text(REF_CSV, encoding="utf-8")

    connection = duckdb.connect()
    build_reconstitution_views(connection, str(irve), str(ref))
    return connection


def _row(con, pdc_id):
    return con.execute(
        "SELECT code_insee_commune, code_commune_origine FROM irve_avec_code_commune_trace "
        "WHERE id_pdc_itinerance = ?",
        [pdc_id],
    ).fetchone()


def test_cp_unique_is_resolved(con):
    code, origine = _row(con, "PDC_UNIQUE")
    assert code == "80489"
    assert origine == "cp_unique"


def test_cp_ambigu_resolu_par_nom_dans_adresse(con):
    code, origine = _row(con, "PDC_NOM_TROUVE")
    assert code == "12345"
    assert origine == "cp_nom_trouve_dans_adresse"


def test_cp_ambigu_sans_nom_reste_non_resolu(con):
    code, origine = _row(con, "PDC_AMBIGU")
    assert code is None
    assert origine == "non_resolu_probable_hors_france"


def test_cp_absent_du_referentiel_francais_reste_non_resolu(con):
    # Le CP belge 4700 (Eupen) n'existe pas dans le référentiel français ->
    # ne doit surtout pas être rattaché à une commune française au hasard.
    code, origine = _row(con, "PDC_ETRANGER")
    assert code is None
    assert origine == "non_resolu_probable_hors_france"


def test_ligne_deja_renseignee_reste_inchangee(con):
    code, origine = _row(con, "PDC_DEJA_RENSEIGNE")
    assert code == "75056"
    assert origine == "brut"


def test_quality_report_counts_match(con):
    report = compute_reconstitution_report(con)
    assert report.nb_pdc_total == 5
    assert report.nb_pdc_code_brut == 1
    assert report.nb_pdc_reconstitue_cp_unique == 1
    assert report.nb_pdc_reconstitue_cp_nom == 1
    assert report.nb_pdc_non_resolu == 2
    assert report.nb_puissance_non_castable == 1  # "22,5" (virgule décimale)


def test_puissance_nominale_is_cast_to_double(con):
    row = con.execute(
        "SELECT puissance_nominale FROM irve_avec_code_commune_trace WHERE id_pdc_itinerance = ?",
        ["PDC_UNIQUE"],
    ).fetchone()
    assert row == (22.0,)
    assert isinstance(row[0], float)


def test_puissance_nominale_malformee_devient_null(con):
    row = con.execute(
        "SELECT puissance_nominale FROM irve_avec_code_commune_trace WHERE id_pdc_itinerance = ?",
        ["PDC_ETRANGER"],
    ).fetchone()
    assert row == (None,)


def test_puissance_nominale_vide_reste_null_sans_compter_comme_non_castable(con):
    row = con.execute(
        "SELECT puissance_nominale FROM irve_avec_code_commune_trace WHERE id_pdc_itinerance = ?",
        ["PDC_AMBIGU"],
    ).fetchone()
    assert row == (None,)


# --- Dédoublonnage des points de charge -------------------------------------
#
# Jeu de cas calqué sur ce qui a été observé sur le vrai fichier IRVE :
#   - PDC_A : 2 versions, date_maj différente -> on garde la plus récente.
#   - PDC_B : 2 versions, MÊME date_maj -> date_maj seul ne départage pas,
#     last_modified si.
#   - "Non concerné" (placeholder, pas un vrai id) avec un id_pdc_local
#     renseigné et DIFFÉRENT sur 2 lignes -> ne doivent pas être fusionnées
#     entre elles (repli sur id_pdc_local).
#   - "Non concerné" ET id_pdc_local vide sur 2 lignes -> aucun identifiant
#     fiable, chacune doit rester unique (pas fusionnées entre elles non plus).
DEDUP_IRVE_CSV = """id_pdc_itinerance,id_pdc_local,adresse_station,code_insee_commune,puissance_nominale,date_maj,last_modified
PDC_A,LOC_A,"1 Rue A, 75000 Paris",75056,22.0,2026-06-01,2026-06-01T10:00:00+00:00
PDC_A,LOC_A,"1 Rue A, 75000 Paris",75056,22.0,2026-07-01,2026-07-01T10:00:00+00:00
PDC_B,LOC_B,"2 Rue B, 75000 Paris",75056,22.0,2026-06-01,2026-06-01T09:00:00+00:00
PDC_B,LOC_B,"2 Rue B, 75000 Paris",75056,22.0,2026-06-01,2026-06-01T11:00:00+00:00
Non concerné,LOC_C1,"3 Rue C, 75000 Paris",75056,22.0,2026-06-01,2026-06-01T10:00:00+00:00
Non concerné,LOC_C2,"4 Rue D, 75000 Paris",75056,44.0,2026-06-01,2026-06-01T10:00:00+00:00
Non concerné,,"5 Rue E, 75000 Paris",75056,10.0,2026-06-01,2026-06-01T10:00:00+00:00
Non concerné,,"6 Rue F, 75000 Paris",75056,11.0,2026-06-01,2026-06-01T10:00:00+00:00
"""


@pytest.fixture
def dedup_con(tmp_path: Path) -> duckdb.DuckDBPyConnection:
    irve = tmp_path / "irve_dedup.csv"
    irve.write_text(DEDUP_IRVE_CSV, encoding="utf-8")
    ref = tmp_path / "ref.csv"
    ref.write_text(REF_CSV, encoding="utf-8")  # pas utilisé : tous les code_insee_commune sont déjà renseignés

    connection = duckdb.connect()
    build_reconstitution_views(connection, str(irve), str(ref))
    build_dedup_view(connection)
    return connection


def test_dedup_keeps_latest_date_maj_version(dedup_con):
    rows = dedup_con.execute(
        "SELECT date_maj FROM irve_dedup WHERE id_pdc_local = 'LOC_A'"
    ).fetchall()
    assert len(rows) == 1
    assert str(rows[0][0]) == "2026-07-01"


def test_dedup_breaks_date_maj_tie_with_last_modified(dedup_con):
    # Comparaison par epoch plutôt que par chaîne : DuckDB affiche
    # last_modified dans le fuseau local (ex: +02:00), pas forcément "11:00:00".
    rows = dedup_con.execute(
        "SELECT epoch(last_modified) FROM irve_dedup WHERE id_pdc_local = 'LOC_B'"
    ).fetchall()
    assert len(rows) == 1
    expected = dedup_con.execute("SELECT epoch(TIMESTAMPTZ '2026-06-01T11:00:00+00:00')").fetchone()[0]
    assert rows[0][0] == expected


def test_dedup_does_not_merge_placeholder_ids_with_different_local_id(dedup_con):
    # Les 2 lignes "Non concerné" avec LOC_C1/LOC_C2 doivent rester 2 lignes distinctes.
    rows = dedup_con.execute(
        "SELECT id_pdc_local FROM irve_dedup WHERE id_pdc_local IN ('LOC_C1', 'LOC_C2') ORDER BY 1"
    ).fetchall()
    assert [r[0] for r in rows] == ["LOC_C1", "LOC_C2"]


def test_dedup_keeps_orphan_placeholder_rows_as_distinct(dedup_con):
    # Les 2 lignes "Non concerné" SANS id_pdc_local (aucun identifiant fiable)
    # ne doivent pas être fusionnées entre elles : on doit retrouver les 2
    # puissances distinctes (10.0 et 11.0), pas une seule ligne survivante.
    rows = dedup_con.execute(
        "SELECT puissance_nominale FROM irve_dedup "
        "WHERE lower(trim(id_pdc_itinerance)) = 'non concerné' AND (id_pdc_local IS NULL OR trim(id_pdc_local) = '') "
        "ORDER BY 1"
    ).fetchall()
    assert [r[0] for r in rows] == [10.0, 11.0]


def test_dedup_report_counts(dedup_con):
    report = compute_dedup_report(dedup_con)
    assert report.nb_lignes_avant_dedup == 8
    assert report.nb_lignes_apres_dedup == 6
    assert report.nb_doublons_supprimes == 2
    assert report.nb_pdc_id_non_fiable == 4  # les 4 lignes "Non concerné"
