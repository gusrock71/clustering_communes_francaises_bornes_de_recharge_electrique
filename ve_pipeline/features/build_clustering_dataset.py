"""[SAUVEGARDE / RÉFÉRENCE — remplacé par dbt le 2026-08-21]

Ce module (préparation du dataset clustering via DuckDB) n'est plus le
chemin principal : remplacé par `dbt/models/marts/mart_clustering_dataset.sql`,
qui lit `int_territorial_join` (Postgres/Neon) au lieu du parquet S3
`staging_territorial`. La même logique d'exclusion/imputation par médiane
dynamique (percentile_cont côté Postgres) a été validée contre les mêmes cas
de test (tests/test_features_clustering_dataset.py) avant la bascule. Gardé
en sauvegarde/référence à la demande explicite de l'utilisateur.

--- Docstring original ---

Préparation du dataset prêt pour le clustering K-Means (début de J3).

Part de `staging_territorial` (J2, voir ve_pipeline/jointure/build_staging.py)
et applique les décisions produit validées le 2026-08-19, discutées après
inspection des taux de nul réels de `staging_territorial` :

Bloc immatriculations (`croissance_immat_ve_pct`, taux de nul réel 29,3%) :
  1. Exclusion des communes sans aucune donnée d'immatriculation
     (`has_immatriculations = false`, 902 communes en conditions réelles) --
     pas assez d'information pour les positionner dans le clustering, on ne
     invente pas de valeur pour elles.
  2. Imputation par la **médiane des croissances réellement calculables**
     (calculée dynamiquement à l'exécution, jamais une valeur en dur) pour
     les communes `demarrage_ve_tardif = true` (marché VE qui démarre après
     2020 : baseline 2018-2020 nulle mais immatriculations VE sur
     2023-2025, 8 237 communes en conditions réelles). Option "valeur
     neutre" (Option A) : la colonne continue ne fabrique pas de signal
     extrême, c'est le flag `demarrage_ve_tardif` qui porte seul
     l'information "cas particulier" pour le modèle.
  3. Imputation à **0** pour les communes `demarrage_ve_tardif = false` dont
     la croissance est malgré tout NULL (baseline ET récent tous deux à 0 --
     jamais eu la moindre immatriculation VE/hybride rechargeable sur toute
     la période observée, 1 637 communes en conditions réelles). Ici 0 est
     une vraie valeur (absence de VE observée), pas un remplissage arbitraire.

Bloc Enedis (`nb_sites_residentiel`, `conso_totale_residentielle_mwh`,
`conso_moyenne_residentielle_mwh`, `population`, `taux_chauffage_electrique`,
`part_thermosensible`) :
  4. Exclusion des communes sans aucune donnée Enedis (`has_enedis = false`,
     3 714 communes en conditions réelles, dont 838 déjà exclues via le
     critère immatriculations -- exclusion totale ajoutée : 3 778 communes,
     10,53%, dataset final : 89,47% des communes). Même logique que pour les
     immatriculations : pas de valeur par défaut défendable pour une
     consommation ou une population totalement inconnue.
  5. Pour le résidu masqué (commune avec données Enedis mais valeur
     spécifique confidentielle -- secret statistique sur petit échantillon,
     ~3,3% pour `part_thermosensible`, ~0,15% pour
     `nb_sites`/`conso_totale`/`conso_moyenne`, 0% pour
     `population`/`taux_chauffage_electrique`) : imputation par la médiane
     calculée dynamiquement, même pattern que l'Option A -- ce résidu est un
     artefact de masquage statistique, pas un signal différenciant comme
     `demarrage_ve_tardif`.

Après ces 5 étapes, plus aucune de ces colonnes ne devrait contenir de NULL
dans `clustering_dataset` -- `compute_clustering_readiness_report` vérifie ce
fait plutôt que de le supposer silencieusement.
"""

from __future__ import annotations

import argparse
import logging
from dataclasses import dataclass, field

import duckdb

from ve_pipeline.duckdb_s3 import configure_s3

logger = logging.getLogger(__name__)

# Colonnes Enedis dont le résidu masqué (secret statistique, petit
# échantillon) est imputé par médiane dynamique -- cf. point 5 du docstring.
ENEDIS_MEDIAN_IMPUTED_COLUMNS = (
    "nb_sites_residentiel",
    "conso_totale_residentielle_mwh",
    "conso_moyenne_residentielle_mwh",
    "population",
    "taux_chauffage_electrique",
    "part_thermosensible",
)


def build_clustering_dataset(
    con: duckdb.DuckDBPyConnection, staging_path: str
) -> tuple[float, dict[str, float]]:
    """Construit la vue `clustering_dataset` à partir de `staging_path`
    (fichier `staging_territorial` au format Parquet, local ou `s3://...`).

    Retourne (médiane de croissance_immat_ve_pct, médianes des colonnes
    Enedis) -- utiles pour le rapport et la traçabilité, jamais de valeur en
    dur dans le code."""
    con.execute(f"""
        CREATE OR REPLACE VIEW staging_territorial_src AS
        SELECT * FROM read_parquet('{staging_path}');
    """)

    median_croissance = con.execute("""
        SELECT median(croissance_immat_ve_pct)
        FROM staging_territorial_src
        WHERE has_immatriculations = true AND croissance_immat_ve_pct IS NOT NULL
    """).fetchone()[0]

    if median_croissance is None:
        raise RuntimeError(
            "Impossible de calculer la médiane de croissance_immat_ve_pct : "
            "aucune valeur calculable dans staging_territorial_src."
        )

    enedis_medians: dict[str, float] = {}
    for col in ENEDIS_MEDIAN_IMPUTED_COLUMNS:
        m = con.execute(f"""
            SELECT median({col})
            FROM staging_territorial_src
            WHERE has_enedis = true AND {col} IS NOT NULL
        """).fetchone()[0]
        if m is None:
            raise RuntimeError(
                f"Impossible de calculer la médiane de {col} : "
                "aucune valeur calculable dans staging_territorial_src."
            )
        enedis_medians[col] = m

    enedis_impute_sql = ",\n            ".join(
        f"COALESCE({col}, {enedis_medians[col]}) AS {col}" for col in ENEDIS_MEDIAN_IMPUTED_COLUMNS
    )
    exclude_cols_sql = ", ".join(["croissance_immat_ve_pct", *ENEDIS_MEDIAN_IMPUTED_COLUMNS])

    con.execute(f"""
        CREATE OR REPLACE VIEW clustering_dataset AS
        SELECT
            * EXCLUDE ({exclude_cols_sql}),
            -- demarrage_ve_tardif=true -> médiane (Option A, valeur neutre).
            -- Sinon -> COALESCE à 0 : pour les communes demarrage_ve_tardif=false,
            -- une croissance NULL restante signifie "jamais eu de VE sur toute
            -- la période" (baseline ET récent tous deux à 0), une vraie valeur
            -- de 0, pas un remplissage arbitraire (décision 2026-08-19).
            CASE WHEN demarrage_ve_tardif = true THEN {median_croissance}
                 ELSE COALESCE(croissance_immat_ve_pct, 0) END AS croissance_immat_ve_pct,
            {enedis_impute_sql}
        FROM staging_territorial_src
        WHERE has_immatriculations = true AND has_enedis = true;
    """)

    return median_croissance, enedis_medians


@dataclass
class ClusteringReadinessReport:
    nb_communes_total_staging: int
    nb_communes_exclues_sans_immatriculations: int
    nb_communes_exclues_sans_enedis: int
    nb_communes_exclues_total: int
    nb_communes_dataset: int
    croissance_immat_ve_pct_mediane_imputee: float
    nb_communes_imputees_demarrage_tardif: int
    nb_communes_croissance_encore_nulle: int
    enedis_medianes_imputees: dict[str, float] = field(default_factory=dict)
    nb_communes_enedis_encore_nulle: dict[str, int] = field(default_factory=dict)


def compute_clustering_readiness_report(
    con: duckdb.DuckDBPyConnection, median_croissance: float, enedis_medians: dict[str, float]
) -> ClusteringReadinessReport:
    nb_total = con.execute("SELECT COUNT(*) FROM staging_territorial_src").fetchone()[0]
    nb_exclues_immat = con.execute(
        "SELECT COUNT(*) FROM staging_territorial_src WHERE has_immatriculations = false"
    ).fetchone()[0]
    nb_exclues_enedis = con.execute(
        "SELECT COUNT(*) FROM staging_territorial_src WHERE has_enedis = false"
    ).fetchone()[0]
    nb_exclues_total = con.execute(
        "SELECT COUNT(*) FROM staging_territorial_src WHERE has_immatriculations = false OR has_enedis = false"
    ).fetchone()[0]
    nb_dataset = con.execute("SELECT COUNT(*) FROM clustering_dataset").fetchone()[0]
    nb_imputees = con.execute(
        "SELECT COUNT(*) FROM clustering_dataset WHERE demarrage_ve_tardif = true"
    ).fetchone()[0]
    nb_encore_nulle = con.execute(
        "SELECT COUNT(*) FROM clustering_dataset WHERE croissance_immat_ve_pct IS NULL"
    ).fetchone()[0]

    nb_enedis_encore_nulle = {
        col: con.execute(f"SELECT COUNT(*) FROM clustering_dataset WHERE {col} IS NULL").fetchone()[0]
        for col in ENEDIS_MEDIAN_IMPUTED_COLUMNS
    }

    return ClusteringReadinessReport(
        nb_communes_total_staging=nb_total,
        nb_communes_exclues_sans_immatriculations=nb_exclues_immat,
        nb_communes_exclues_sans_enedis=nb_exclues_enedis,
        nb_communes_exclues_total=nb_exclues_total,
        nb_communes_dataset=nb_dataset,
        croissance_immat_ve_pct_mediane_imputee=median_croissance,
        nb_communes_imputees_demarrage_tardif=nb_imputees,
        nb_communes_croissance_encore_nulle=nb_encore_nulle,
        enedis_medianes_imputees=enedis_medians,
        nb_communes_enedis_encore_nulle=nb_enedis_encore_nulle,
    )


def write_clustering_dataset(con: duckdb.DuckDBPyConnection, destination: str) -> None:
    con.execute(f"COPY (SELECT * FROM clustering_dataset) TO '{destination}' (FORMAT PARQUET);")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Prépare le dataset de clustering (J3) à partir de staging_territorial"
    )
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--dt", required=True, help="Partition à traiter, format YYYY-MM-DD")
    parser.add_argument("--region", default="eu-west-3")
    parser.add_argument(
        "--staging-path",
        default=None,
        help="Chemin du parquet staging_territorial. Par défaut : "
        "s3://<bucket>/staging/territorial/dt=<dt>/territorial.parquet",
    )
    parser.add_argument(
        "--out",
        default=None,
        help="Chemin S3 de sortie (parquet). Par défaut : "
        "s3://<bucket>/features/clustering_dataset/dt=<dt>/clustering_dataset.parquet",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(level="INFO", format="%(asctime)s %(levelname)s %(message)s")

    staging_path = (
        args.staging_path
        or f"s3://{args.bucket}/staging/territorial/dt={args.dt}/territorial.parquet"
    )
    out = args.out or f"s3://{args.bucket}/features/clustering_dataset/dt={args.dt}/clustering_dataset.parquet"

    con = duckdb.connect()
    configure_s3(con, args.region)
    median_croissance, enedis_medians = build_clustering_dataset(con, staging_path)

    report = compute_clustering_readiness_report(con, median_croissance, enedis_medians)
    logger.info("Rapport de préparation du dataset clustering :")
    logger.info(
        "  Communes en staging : %s, exclues (sans immat=%s, sans enedis=%s, union=%s), "
        "dans le dataset final : %s",
        report.nb_communes_total_staging,
        report.nb_communes_exclues_sans_immatriculations,
        report.nb_communes_exclues_sans_enedis,
        report.nb_communes_exclues_total,
        report.nb_communes_dataset,
    )
    logger.info(
        "  Médiane croissance_immat_ve_pct : %.4f (appliquée à %s communes démarrage_ve_tardif=true)",
        report.croissance_immat_ve_pct_mediane_imputee,
        report.nb_communes_imputees_demarrage_tardif,
    )
    logger.info("  Médianes Enedis utilisées pour le résidu masqué : %s", report.enedis_medianes_imputees)
    if report.nb_communes_croissance_encore_nulle:
        logger.warning(
            "  %s communes ont encore croissance_immat_ve_pct=NULL après imputation -- "
            "ne devrait plus arriver, à investiguer avant un run K-Means réel.",
            report.nb_communes_croissance_encore_nulle,
        )
    for col, n in report.nb_communes_enedis_encore_nulle.items():
        if n:
            logger.warning(
                "  %s communes ont encore %s=NULL après imputation -- "
                "ne devrait plus arriver, à investiguer avant un run K-Means réel.",
                n, col,
            )

    write_clustering_dataset(con, out)
    logger.info("Dataset clustering écrit -> %s", out)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
