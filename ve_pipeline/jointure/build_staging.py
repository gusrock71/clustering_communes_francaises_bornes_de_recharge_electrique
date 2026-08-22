"""[SAUVEGARDE / RÉFÉRENCE — remplacé par dbt le 2026-08-21]

Ce module (jointure territoriale via DuckDB directement sur S3) n'est plus le
chemin principal du pipeline : il a été remplacé par les modèles dbt du
dossier `dbt/models/intermediate/int_territorial_join.sql` (+ marts), qui
lisent les tables `raw__*` chargées en Postgres/Neon
(ve_pipeline/loading/postgres_loader.py) plutôt que les CSV S3 directement.
Toute la logique métier ci-dessous (fenêtres baseline/récent, filtre
RESIDENTIEL, gestion des NULL via COALESCE...) a été portée telle quelle en
SQL Postgres et validée contre les mêmes cas de test (tests/test_jointure.py)
avant la bascule. Ce fichier est conservé en sauvegarde/référence, à la
demande explicite de l'utilisateur, mais n'est plus exécuté par le pipeline
de production.

--- Docstring original (J2) ---

Jointure territoriale (J2) : agrège IRVE, immatriculations et Enedis à la
maille commune et produit la table croisée de staging.

Principe : aucune source brute n'est modifiée. Tout se fait par des vues
DuckDB au moment de la lecture, directement sur les CSV bruts déposés en S3
par la brique d'ingestion (J1) — cohérent avec le principe "raw, sans
transformation" appliqué en amont.

Schémas réels confirmés en conditions réelles le 2026-08-16 (voir
ve_pipeline/ingestion/config.py pour le détail par source) :
  - IRVE : une ligne par point de charge (`id_pdc_itinerance`), clé commune
    `code_insee_commune`, motorisation/puissance dans `puissance_nominale`.
  - Immatriculations (neuf/occasion) : format large, une colonne par année
    (`IMMAT_2010` ... `IMMAT_2025`), clé commune `COMMUNE_CODE`, motorisation
    dans `CARBURANT`.
  - Enedis : ventilé par secteur d'activité, plusieurs lignes par commune,
    clé commune `"Code Commune"`, certaines valeurs peuvent être masquées
    pour raisons de confidentialité (d'où les `TRY_CAST` défensifs).

Décisions produit (2026-08-19) :
  1. IRVE : cette jointure lit désormais IRVE **nettoyé**
     (cleaned/irve/dt=.../irve_consolide.csv), produit par
     ve_pipeline/cleaning/irve_code_commune.py. Ce fichier est déjà
     dédupliqué par point de charge (repli id_pdc_local / clé par ligne pour
     les identifiants "Non concerné", cf. build_dedup_view) -- il ne faut
     donc PAS rejouer ici un dédup naïf sur `id_pdc_itinerance` seul, sous
     peine de re-fusionner à tort les points de charge dont l'identifiant
     national est un placeholder "Non concerné".
  2. Immatriculations : agrégées sur toutes les années disponibles
     (IMMAT_2010 ... IMMAT_2025), pas seulement 2020/2024.
     `croissance_immat_ve_pct` (retravaillé le 2026-08-19) compare une
     fenêtre de référence (somme 2018-2020) à une fenêtre récente (somme
     2023-2025) plutôt qu'un ratio point-à-point 2010 vs 2025, qui était nul
     pour 95,3% des communes (marché VE quasi inexistant en 2010).
  3. Enedis : restreint aux lignes `"CODE GRAND SECTEUR" = 'RESIDENTIEL'`
     (vérifié : au plus 1 ligne/commune, couvre 32 165/32 170 communes).
     Les colonnes de consommation sont renommées pour refléter ce
     périmètre résidentiel-only (nb_sites_residentiel,
     conso_totale_residentielle_mwh, conso_moyenne_residentielle_mwh).
"""

from __future__ import annotations

import argparse
import logging
from dataclasses import dataclass

import duckdb

from ve_pipeline.duckdb_s3 import configure_s3

logger = logging.getLogger(__name__)

# Décision produit (2026-08-16) : on inclut les hybrides rechargeables en plus
# du 100% électrique/hydrogène dans le "parc de véhicules électrifiés
# rechargeables", car les deux types de véhicules sollicitent les bornes de
# recharge publiques.
CARBURANTS_RECHARGEABLES = ("Electrique et hydrogène", "Hybride rechargeable")

# Toutes les années disponibles dans les fichiers immatriculations (format
# large IMMAT_2010 ... IMMAT_2025). Décision produit (2026-08-19) : on agrège
# sur l'ensemble de la période plutôt que sur 2 années isolées.
IMMAT_YEARS = tuple(range(2010, 2026))
IMMAT_YEAR_FIRST = IMMAT_YEARS[0]
IMMAT_YEAR_LAST = IMMAT_YEARS[-1]

# Fenêtres utilisées pour croissance_immat_ve_pct (retravaillé le 2026-08-19).
# Un ratio point-à-point 2010 vs 2025 est inexploitable : sur les vraies
# données, 95,3% des communes ont 0 immatriculation VE en 2010 (le marché
# n'existait quasiment pas), ce qui rend le dénominateur nul presque partout.
# Vérifié empiriquement : une somme sur 3 ans 2018-2020 (démarrage réel du
# marché VE en France) est non-nulle pour 71,8% des communes, contre 4,8%
# pour 2010 seul -- et sommer 3 ans lisse aussi les à-coups d'une seule année.
IMMAT_BASELINE_YEARS = (2018, 2019, 2020)
IMMAT_RECENT_YEARS = (2023, 2024, 2025)


@dataclass(frozen=True)
class SourcePaths:
    irve: str
    immatriculations_neuf: str
    immatriculations_occasion: str
    enedis: str


def build_views(con: duckdb.DuckDBPyConnection, paths: SourcePaths) -> None:
    carburants_sql = ", ".join(f"'{c}'" for c in CARBURANTS_RECHARGEABLES)

    # --- IRVE : lecture directe du fichier nettoyé (déjà dédupliqué en amont
    # par ve_pipeline/cleaning/irve_code_commune.py), puis agrégation commune.
    # Pas de dédup ici : cf. note en tête de module. ---
    con.execute(f"""
        CREATE OR REPLACE VIEW irve_source AS
        SELECT *
        FROM read_csv_auto(
            '{paths.irve}', union_by_name=true,
            types={{'code_insee_commune': 'VARCHAR', 'puissance_nominale': 'DOUBLE'}}
        );
    """)

    con.execute("""
        CREATE OR REPLACE VIEW irve_commune AS
        SELECT
            code_insee_commune AS code_commune,
            COUNT(DISTINCT id_station_itinerance) AS nb_stations,
            COUNT(*) AS nb_points_charge,
            SUM(puissance_nominale) AS puissance_totale_installee_kw,
            AVG(puissance_nominale) AS puissance_moyenne_pdc_kw,
            AVG(CASE WHEN puissance_nominale > 50 THEN 1.0 ELSE 0.0 END) AS part_recharge_rapide,
            AVG(CASE WHEN consolidated_is_code_insee_verified THEN 1.0 ELSE 0.0 END) AS pct_pdc_code_insee_verifie
        FROM irve_source
        WHERE code_insee_commune IS NOT NULL AND code_insee_commune <> ''
        GROUP BY code_insee_commune;
    """)

    # --- Immatriculations : neuf + occasion combinés, toutes années
    # IMMAT_2010...IMMAT_2025, filtrées sur les motorisations rechargeables ---
    immat_years_sql = ", ".join(f"IMMAT_{y}" for y in IMMAT_YEARS)
    con.execute(f"""
        CREATE OR REPLACE VIEW immat_union AS
        SELECT COMMUNE_CODE AS code_commune, CARBURANT, {immat_years_sql}
        FROM read_csv_auto(
            '{paths.immatriculations_neuf}', union_by_name=true,
            types={{'COMMUNE_CODE': 'VARCHAR'}}
        )
        UNION ALL
        SELECT COMMUNE_CODE AS code_commune, CARBURANT, {immat_years_sql}
        FROM read_csv_auto(
            '{paths.immatriculations_occasion}', union_by_name=true,
            types={{'COMMUNE_CODE': 'VARCHAR'}}
        );
    """)

    # COALESCE(..., 0) sur chaque SUM(...) FILTER(...) : en SQL, FILTER
    # renvoie NULL (pas 0) quand AUCUNE ligne de la commune ne correspond au
    # filtre (ex: la commune n'a même pas de ligne "Electrique et hydrogène"
    # ni "Hybride rechargeable" dans le fichier source, valeur ou pas).
    # Bug réel trouvé le 2026-08-19 : sans ce COALESCE, 1 031 communes se
    # retrouvaient avec une baseline/recent NULL au lieu de 0, ce qui les
    # faisait passer à tort par la branche ELSE de `demarrage_ve_tardif`
    # (NULL = 0 vaut NULL en SQL, pas TRUE) au lieu d'être traitées comme les
    # autres communes à 0 immatriculation VE réelle.
    immat_ve_cols_sql = ",\n            ".join(
        f"COALESCE(SUM(IMMAT_{y}) FILTER (WHERE CARBURANT IN ({carburants_sql})), 0) AS immat_ve_{y}"
        for y in IMMAT_YEARS
    )
    baseline_sum_sql = " + ".join(f"IMMAT_{y}" for y in IMMAT_BASELINE_YEARS)
    recent_sum_sql = " + ".join(f"IMMAT_{y}" for y in IMMAT_RECENT_YEARS)
    baseline_label = f"{IMMAT_BASELINE_YEARS[0]}_{IMMAT_BASELINE_YEARS[-1]}"
    recent_label = f"{IMMAT_RECENT_YEARS[0]}_{IMMAT_RECENT_YEARS[-1]}"
    con.execute(f"""
        CREATE OR REPLACE VIEW immat_commune AS
        SELECT
            code_commune,
            {immat_ve_cols_sql},
            SUM(IMMAT_{IMMAT_YEAR_LAST}) AS immat_toutes_motorisations_{IMMAT_YEAR_LAST},
            COALESCE(SUM({baseline_sum_sql}) FILTER (WHERE CARBURANT IN ({carburants_sql})), 0) AS immat_ve_baseline_{baseline_label},
            COALESCE(SUM({recent_sum_sql}) FILTER (WHERE CARBURANT IN ({carburants_sql})), 0) AS immat_ve_recent_{recent_label}
        FROM immat_union
        WHERE code_commune IS NOT NULL AND code_commune <> ''
        GROUP BY code_commune;
    """)

    # --- Enedis : restreint au secteur RESIDENTIEL (décision produit
    # 2026-08-19 ; vérifié : au plus 1 ligne/commune sur ce périmètre).
    # TRY_CAST car certaines valeurs peuvent être masquées (confidentialité,
    # petits volumes). ---
    con.execute(f"""
        CREATE OR REPLACE VIEW enedis_commune AS
        SELECT
            "Code Commune" AS code_commune,
            SUM(TRY_CAST(nb_sites AS DOUBLE)) AS nb_sites_residentiel,
            SUM(TRY_CAST("Conso totale (MWh)" AS DOUBLE)) AS conso_totale_residentielle_mwh,
            AVG(TRY_CAST("Conso moyenne (MWh)" AS DOUBLE)) AS conso_moyenne_residentielle_mwh,
            MAX(TRY_CAST(nombre_d_habitants AS DOUBLE)) AS population,
            AVG(TRY_CAST(part_thermosensible AS DOUBLE)) AS part_thermosensible,
            AVG(TRY_CAST("Taux de chauffage électrique" AS DOUBLE)) AS taux_chauffage_electrique
        FROM read_csv_auto(
            '{paths.enedis}', union_by_name=true,
            types={{'Code Commune': 'VARCHAR'}}
        )
        WHERE "Code Commune" IS NOT NULL AND "Code Commune" <> ''
          AND "CODE GRAND SECTEUR" = 'RESIDENTIEL'
        GROUP BY "Code Commune";
    """)

    # --- Spine : toutes les communes vues dans AU MOINS une source (pas d'INNER JOIN,
    # sinon on perdrait silencieusement les communes absentes d'une source) ---
    con.execute("""
        CREATE OR REPLACE VIEW communes_spine AS
        SELECT DISTINCT code_commune FROM irve_commune
        UNION
        SELECT DISTINCT code_commune FROM immat_commune
        UNION
        SELECT DISTINCT code_commune FROM enedis_commune;
    """)

    baseline_col = f"immat_ve_baseline_{IMMAT_BASELINE_YEARS[0]}_{IMMAT_BASELINE_YEARS[-1]}"
    recent_col = f"immat_ve_recent_{IMMAT_RECENT_YEARS[0]}_{IMMAT_RECENT_YEARS[-1]}"
    con.execute(f"""
        CREATE OR REPLACE VIEW staging_territorial AS
        SELECT
            s.code_commune,
            -- COALESCE(..., 0) : une commune sans aucun pdc IRVE (has_irve=false)
            -- a réellement 0 station / 0 kW installé, ce n'est pas une donnée
            -- manquante à imputer par une médiane. `pct_pdc_code_insee_verifie`
            -- reste en revanche NULL : c'est une métrique de qualité de données,
            -- pas définie s'il n'y a aucun pdc à mesurer.
            COALESCE(i.nb_stations, 0) AS nb_stations,
            COALESCE(i.nb_points_charge, 0) AS nb_points_charge,
            COALESCE(i.puissance_totale_installee_kw, 0) AS puissance_totale_installee_kw,
            COALESCE(i.puissance_moyenne_pdc_kw, 0) AS puissance_moyenne_pdc_kw,
            COALESCE(i.part_recharge_rapide, 0) AS part_recharge_rapide,
            i.pct_pdc_code_insee_verifie,
            m.* EXCLUDE (code_commune),
            -- Retravaillé le 2026-08-19 : un ratio point-à-point 2010 vs 2025
            -- était nul pour 95,3% des communes (0 immatriculation VE en 2010
            -- quasi partout). On compare désormais une fenêtre de référence
            -- (somme 2018-2020, démarrage réel du marché VE) à une fenêtre
            -- récente (somme 2023-2025) -- taux de nul réel : ~28,2%.
            CASE WHEN m.{baseline_col} > 0
                 THEN (m.{recent_col} - m.{baseline_col}) * 1.0 / m.{baseline_col}
                 ELSE NULL END AS croissance_immat_ve_pct,
            -- Flag ajouté le 2026-08-19 : 24,6% des communes ont une baseline
            -- 2018-2020 nulle (croissance_immat_ve_pct non calculable), mais
            -- 8 237 d'entre elles (23% du total) ont quand même des
            -- immatriculations VE sur 2023-2025 -- un marché qui démarre
            -- tardivement, pas une commune sans dynamique. Sans ce flag, un
            -- remplissage naïf de croissance_immat_ve_pct à 0 au moment du
            -- clustering (J3) leur donnerait à tort le même signal qu'une
            -- commune stagnante. NULL si la commune n'a aucune donnée
            -- d'immatriculation du tout (pas de baseline ni de recent à comparer).
            CASE WHEN m.code_commune IS NULL THEN NULL
                 WHEN m.{baseline_col} = 0 AND m.{recent_col} > 0 THEN TRUE
                 ELSE FALSE END AS demarrage_ve_tardif,
            e.nb_sites_residentiel,
            e.conso_totale_residentielle_mwh,
            e.conso_moyenne_residentielle_mwh,
            e.population,
            e.part_thermosensible,
            e.taux_chauffage_electrique,
            (i.code_commune IS NOT NULL) AS has_irve,
            (m.code_commune IS NOT NULL) AS has_immatriculations,
            (e.code_commune IS NOT NULL) AS has_enedis
        FROM communes_spine s
        LEFT JOIN irve_commune   i ON i.code_commune = s.code_commune
        LEFT JOIN immat_commune  m ON m.code_commune = s.code_commune
        LEFT JOIN enedis_commune e ON e.code_commune = s.code_commune;
    """)


@dataclass
class QualityReport:
    nb_communes_total: int
    nb_communes_avec_irve: int
    nb_communes_avec_immatriculations: int
    nb_communes_avec_enedis: int
    nb_communes_toutes_sources: int
    nb_pdc_irve_total: int
    nb_pdc_irve_code_insee_verifie: int
    nb_pdc_irve_commune_invalide: int
    pct_pdc_code_insee_verifie_global: float | None
    nb_doublons_code_commune: int


def compute_quality_report(con: duckdb.DuckDBPyConnection) -> QualityReport:
    coverage = con.execute("""
        SELECT
            COUNT(*),
            SUM(CASE WHEN has_irve THEN 1 ELSE 0 END),
            SUM(CASE WHEN has_immatriculations THEN 1 ELSE 0 END),
            SUM(CASE WHEN has_enedis THEN 1 ELSE 0 END),
            SUM(CASE WHEN has_irve AND has_immatriculations AND has_enedis THEN 1 ELSE 0 END)
        FROM staging_territorial
    """).fetchone()

    irve_quality = con.execute("""
        SELECT
            COUNT(*),
            SUM(CASE WHEN consolidated_is_code_insee_verified THEN 1 ELSE 0 END),
            SUM(CASE WHEN code_insee_commune IS NULL OR length(code_insee_commune) <> 5 THEN 1 ELSE 0 END)
        FROM irve_source
    """).fetchone()

    nb_pdc_total, nb_pdc_verifie, nb_pdc_invalide = irve_quality
    pct_verifie = (nb_pdc_verifie / nb_pdc_total) if nb_pdc_total else None

    nb_doublons = con.execute("""
        SELECT COUNT(*) FROM (
            SELECT code_commune FROM staging_territorial GROUP BY code_commune HAVING COUNT(*) > 1
        )
    """).fetchone()[0]

    return QualityReport(
        nb_communes_total=coverage[0],
        nb_communes_avec_irve=coverage[1],
        nb_communes_avec_immatriculations=coverage[2],
        nb_communes_avec_enedis=coverage[3],
        nb_communes_toutes_sources=coverage[4],
        nb_pdc_irve_total=nb_pdc_total,
        nb_pdc_irve_code_insee_verifie=nb_pdc_verifie,
        nb_pdc_irve_commune_invalide=nb_pdc_invalide,
        pct_pdc_code_insee_verifie_global=pct_verifie,
        nb_doublons_code_commune=nb_doublons,
    )


def write_staging(con: duckdb.DuckDBPyConnection, destination: str) -> None:
    con.execute(f"COPY (SELECT * FROM staging_territorial) TO '{destination}' (FORMAT PARQUET);")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Jointure territoriale J2 -> table de staging")
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--dt", required=True, help="Partition d'ingestion à joindre, format YYYY-MM-DD")
    parser.add_argument("--region", default="eu-west-3")
    parser.add_argument(
        "--out",
        default=None,
        help="Chemin S3 de sortie (parquet). Par défaut : s3://<bucket>/staging/territorial/dt=<dt>/territorial.parquet",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(level="INFO", format="%(asctime)s %(levelname)s %(message)s")

    paths = SourcePaths(
        irve=f"s3://{args.bucket}/cleaned/irve/dt={args.dt}/irve_consolide.csv",
        immatriculations_neuf=f"s3://{args.bucket}/raw/immatriculations/dt={args.dt}/immatriculations_neuf.csv",
        immatriculations_occasion=f"s3://{args.bucket}/raw/immatriculations/dt={args.dt}/immatriculations_occasion.csv",
        enedis=f"s3://{args.bucket}/raw/enedis_conso/dt={args.dt}/enedis_conso_commune_2024.csv",
    )
    out = args.out or f"s3://{args.bucket}/staging/territorial/dt={args.dt}/territorial.parquet"

    con = duckdb.connect()
    configure_s3(con, args.region)
    build_views(con, paths)

    report = compute_quality_report(con)
    logger.info("Rapport qualité de la jointure :")
    logger.info("  Communes dans la table finale : %s", report.nb_communes_total)
    logger.info(
        "  Couverture : IRVE=%s, immatriculations=%s, Enedis=%s, les 3 sources=%s",
        report.nb_communes_avec_irve,
        report.nb_communes_avec_immatriculations,
        report.nb_communes_avec_enedis,
        report.nb_communes_toutes_sources,
    )
    logger.info(
        "  IRVE : %s points de charge, %.1f%% avec code commune vérifié ETALAB, %s avec code commune invalide",
        report.nb_pdc_irve_total,
        (report.pct_pdc_code_insee_verifie_global or 0) * 100,
        report.nb_pdc_irve_commune_invalide,
    )
    if report.nb_doublons_code_commune:
        logger.warning("  %s code(s) commune en double dans la table finale !", report.nb_doublons_code_commune)

    write_staging(con, out)
    logger.info("Table de staging écrite -> %s", out)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
